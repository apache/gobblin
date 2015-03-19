/* (c) 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.compaction;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

import gobblin.hive.util.HiveJdbcConnector;


/**
 * A class for managing Hive external tables created on Avro files.
 *
 * @author ziliu
 */
public class AvroExternalTable extends HiveTable {

  private static final Logger LOG = LoggerFactory.getLogger(AvroExternalTable.class);
  private static final String HIVE_TMPSCHEMA_DIR = "hive.tmpschema.dir";
  private static final String HIVE_TMPDATA_DIR = "hive.tmpdata.dir";
  private static final String HIVE_TMPDATA_DIR_DEFAULT = "/";
  private static final String CREATE_TABLE_STMT = "CREATE EXTERNAL TABLE %1$s "
      + " ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'" + " STORED AS"
      + " INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'"
      + " OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'" + " LOCATION '%2$s'"
      + " TBLPROPERTIES ('avro.schema.url'='%3$s')";
  private final String dataLocationInHdfs;
  private final String schemaLocationInHdfs;
  private final boolean deleteSchemaAfterDone;
  private final boolean deleteDataAfterDone;

  public static class Builder extends HiveTable.Builder<AvroExternalTable.Builder> {

    private String dataLocationInHdfs = "";
    private String schemaLocationInHdfs = "";
    private boolean moveDataToTmpHdfsDir = false;
    private String extensionToBeMoved;

    public Builder withDataLocation(String dataLocationInHdfs) {
      this.dataLocationInHdfs = dataLocationInHdfs;
      return this;
    }

    public Builder withSchemaLocation(String schemaLocationInHdfs) {
      this.schemaLocationInHdfs = schemaLocationInHdfs;
      return this;
    }

    public Builder withMoveDataToTmpHdfsDir(String extensionToBeMoved) {
      this.moveDataToTmpHdfsDir = true;
      this.extensionToBeMoved = extensionToBeMoved;
      return this;
    }

    public AvroExternalTable build() throws IOException {
      return new AvroExternalTable(this);
    }
  }

  private AvroExternalTable(AvroExternalTable.Builder builder) throws IOException {
    super(builder);

    if (builder.moveDataToTmpHdfsDir) {
      this.dataLocationInHdfs = moveDataFileToSeparateHdfsDir(builder.dataLocationInHdfs, builder.extensionToBeMoved);
      this.deleteDataAfterDone = true;
    } else {
      this.dataLocationInHdfs = builder.dataLocationInHdfs;
      this.deleteDataAfterDone = false;
    }

    if (StringUtils.isNotBlank(builder.schemaLocationInHdfs)) {
      this.schemaLocationInHdfs = builder.schemaLocationInHdfs;
      this.attributes = getAttributesFromAvroSchemaFile();
      this.deleteSchemaAfterDone = false;
    } else {
      Schema schema = getSchemaFromAvroDataFile();
      this.attributes = parseSchema(schema);
      this.schemaLocationInHdfs = writeSchemaToHdfs(schema);
      this.deleteSchemaAfterDone = true;
    }
  }

  private List<HiveAttribute> getAttributesFromAvroSchemaFile() throws IOException {
    Closer closer = Closer.create();
    try {
      InputStream schemaInputStream = closer.register(new HdfsReader(this.schemaLocationInHdfs).getInputStream());
      Schema schema = new Schema.Parser().parse(schemaInputStream);
      return parseSchema(schema);
    } finally {
      closer.close();
    }
  }

  private Schema getSchemaFromAvroDataFile() throws IOException {
    String firstDataFilePath = HdfsReader.getFirstDataFilePathInDir(dataLocationInHdfs);
    LOG.info("Extracting schema for table " + name + " from avro data file " + firstDataFilePath);
    SeekableInput sin = new HdfsReader(firstDataFilePath).getFsInput();

    Closer closer = Closer.create();
    try {
      DataFileReader<Void> dfr = closer.register(new DataFileReader<Void>(sin, new GenericDatumReader<Void>()));
      Schema schema = dfr.getSchema();
      return schema;
    } finally {
      closer.close();
    }
  }

  private String writeSchemaToHdfs(Schema schema) throws IOException {
    String defaultTmpSchemaDir = getParentDir(dataLocationInHdfs);
    String tmpSchemaDir = CompactionRunner.jobProperties.getProperty(HIVE_TMPSCHEMA_DIR, defaultTmpSchemaDir);
    tmpSchemaDir = addSlash(tmpSchemaDir);
    String tmpSchemaPath = tmpSchemaDir + UUID.randomUUID().toString() + ".avsc";
    HdfsWriter writer = new HdfsWriter(tmpSchemaPath);
    LOG.info("writing schema to HDFS location " + tmpSchemaPath);
    writer.write(schema.toString(true));
    return tmpSchemaPath;
  }

  private String getParentDir(String filePathInHdfs) {
    return new Path(filePathInHdfs).getParent().toString();
  }

  private List<HiveAttribute> parseSchema(Schema schema) {
    List<HiveAttribute> attributes = new ArrayList<HiveAttribute>();
    List<Schema.Field> fields = schema.getFields();

    for (Schema.Field field : fields) {
      attributes.add(convertAvroSchemaFieldToHiveAttribute(field));
    }
    return attributes;
  }

  private HiveAttribute convertAvroSchemaFieldToHiveAttribute(Schema.Field field) {
    String avroFieldType = field.schema().getType().toString();
    if (avroFieldType.equalsIgnoreCase("UNION")) {
      avroFieldType = extractAvroTypeFromUnion(field);
    }
    if (HiveAttribute.fromAvroType(avroFieldType) == null) {
      throw new RuntimeException("Hive does not support attribute type '" + avroFieldType + "'");
    }
    return new HiveAttribute(field.name(), HiveAttribute.fromAvroType(avroFieldType));
  }

  private String extractAvroTypeFromUnion(Schema.Field field) {
    if (field.schema().getTypes().size() >= 3) {
      LOG.warn("Avro schema field " + field.name() + " has 3 or more types: using the first non-null type");
    }
    for (Schema schema : field.schema().getTypes()) {
      if (!schema.getType().toString().equalsIgnoreCase("NULL")) {
        return schema.getType().toString();
      }
    }
    String message =
        "Avro schema field " + field.name() + " is a union, but it does not contain a non-null field type.";
    LOG.error(message);
    throw new RuntimeException(message);
  }

  public String getDataLocationInHdfs() {
    return this.dataLocationInHdfs;
  }

  public String getSchemaLocationInHdfs() {
    return this.schemaLocationInHdfs;
  }

  @Override
  public void createTable(HiveJdbcConnector conn, String jobID) throws SQLException {
    String tableName = getNameWithJobId(jobID);
    String dropTableStmt = String.format(DROP_TABLE_STMT, tableName);
    String createTableStmt =
        String.format(CREATE_TABLE_STMT, tableName, HdfsIO.getHdfsUri() + this.dataLocationInHdfs, HdfsIO.getHdfsUri()
            + this.schemaLocationInHdfs);

    conn.executeStatements(dropTableStmt, createTableStmt);
  }

  @Override
  public HiveTable addNewColumnsInSchema(HiveJdbcConnector conn, HiveTable table, String jobId) throws SQLException {
    if (hasNoNewColumn(table)) {
      return this;
    }

    HiveManagedTable managedTable =
        new HiveManagedTable.Builder().withName(this.name).withPrimaryKeys(this.primaryKeys)
            .withAttributes(this.attributes).build();

    return managedTable.addNewColumnsInSchema(null, table, jobId);
  }

  protected void deleteTmpFilesIfNeeded() throws IllegalArgumentException, IOException {
    if (this.deleteSchemaAfterDone) {
      new HdfsWriter(this.schemaLocationInHdfs).delete();
    }
    if (this.deleteDataAfterDone) {
      new HdfsWriter(this.dataLocationInHdfs).delete();
    }
  }

  private String moveDataFileToSeparateHdfsDir(String sourceDir, String extension) throws IOException {
    String parentDir = CompactionRunner.jobProperties.getProperty(HIVE_TMPDATA_DIR, HIVE_TMPDATA_DIR_DEFAULT);
    parentDir = addSlash(parentDir);
    String destination = parentDir + UUID.randomUUID().toString();
    LOG.info("Moving data file of table " + this.getName() + " to " + destination);
    HdfsWriter.moveSelectFiles(extension, sourceDir, destination);
    LOG.info("Moved data file of table " + this.getName() + " to " + destination);
    return destination;
  }

  private String addSlash(String dir) {
    if (!dir.endsWith("/") && !dir.endsWith("\\")) {
      return dir + "/";
    }
    return dir;
  }

  public boolean hasSamePrimaryKey(AvroExternalTable other) {
    return this.primaryKeys.containsAll(other.primaryKeys) && other.primaryKeys.containsAll(this.primaryKeys);
  }
}
