/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.binary_creation;

import com.google.common.collect.AbstractIterator;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.gobblin.util.FileListUtils;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.filters.HiddenFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.hive.serde2.avro.AvroObjectInspectorGenerator;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import static org.apache.gobblin.binary_creation.AvroTestTools.*;


// A class that examines ORC-Format file in Purger Integration test.
@Slf4j
public class OrcTestTools extends DataTestTools<OrcTestTools.OrcRowIterator, TypeInfo> {

  /**
   *
   * @param expected
   * @param observed
   * @param allowDifferentOrder ORC tools will not use this parameter currently.
   * @param blacklistRecordFields ORC tools will not use this parameter currently.
   * @return If two sets of files are identical.
   * Note that there might be an ordering issue in this comparison method. When one is drafting an ORC integration
   * test, try to name all json files differently.
   */
  @Override
  public boolean checkSameFilesAndRecords(TreeMap<String, OrcRowIterator> expected,
      TreeMap<String, OrcRowIterator> observed, boolean allowDifferentOrder, Collection<String> blacklistRecordFields,
      boolean allowDifferentSchema) {
    Iterator<String> keys1 = expected.navigableKeySet().iterator();
    Iterator<String> keys2 = observed.navigableKeySet().iterator();

    return compareIterators(keys1, keys2, (key1, key2) -> {
      // ORC file doesn't have extension by Linkedin's convention.
      if (!removeExtension(key1).equals(key2)) {
        log.error(String.format("Mismatched files: %s and %s", key1, key2));
        return false;
      }

      OrcRowIterator it1 = expected.get(key1);
      OrcRowIterator it2 = observed.get(key2);

      if (!it1.getTypeInfo().equals(it2.getTypeInfo())) {
        log.error(String.format("Mismatched Typeinfo: %s and %s", key1, key2));
        return false;
      }

      boolean result = true;
      while (it1.hasNext()) {
        if (!it2.hasNext() || !result) {
          return false;
        }
        result = compareJavaRowAndOrcStruct(((AvroRow) it1.next()).getRow(), (OrcStruct) it2.next());
      }
      return result;
    });
  }

  /**
   * Given the fact that we couldn't access OrcStruct easily, here uses the hacky way(reflection)
   * to go around access modifier for integration test purpose only.
   * @param realRow A row containing a list of Java objects.
   * @param struct An {@link OrcStruct} which essentially is a list of {@link Writable} objects.
   */
  private boolean compareJavaRowAndOrcStruct(Object realRow, OrcStruct struct) {
    boolean isIdentical = true;
    ArrayList<Object> javaObjRow = (ArrayList) realRow;

    try {
      Field objectArr = OrcStruct.class.getDeclaredField("fields");
      objectArr.setAccessible(true);
      Object[] dataArr = (Object[]) objectArr.get(struct);

      int index = 0;
      for (Object dataField : dataArr) {
        if (dataField instanceof OrcStruct) {
          isIdentical = isIdentical && compareJavaRowAndOrcStruct(javaObjRow.get(index), (OrcStruct) dataField);
        } else {
          isIdentical = isIdentical && objCastHelper(javaObjRow.get(index), (Writable) dataField);
        }
        index++;
      }
    } catch (NoSuchFieldException | IllegalAccessException nfe) {
      throw new RuntimeException("Failed in compare a java object row and orcstruct");
    }

    return isIdentical;
  }

  /**
   * All Writable objects passed in here are guaranteed to be primitive writable objects.
   */
  private boolean objCastHelper(Object javaObj, Writable obj) {
    if (obj instanceof IntWritable) {
      return ((IntWritable) obj).get() == (Integer) javaObj;
    } else if (obj instanceof Text) {
      return (obj).toString().equals(javaObj);
    } else if (obj instanceof LongWritable) {
      return ((LongWritable) obj).get() == (Long) javaObj;
    } else if (obj instanceof ShortWritable) {
      return ((ShortWritable) obj).get() == (Short) javaObj;
    } else if (obj instanceof DoubleWritable) {
      return ((DoubleWritable) obj).get() == (Double) javaObj;
    } else {
      throw new RuntimeException("Cannot recognize the writable type, please enrich the castHelper function");
    }
  }

  /**
   * Materialize records in a classpath package into HDFS ORC records.
   * @param baseResource name of the package. The package should contain the following:
   *                     - Exactly one resource called orcSchema containing the schema of the records
   *                       (or an explicit schema passed as an argument).
   *                     - One or more data files called *.json containing the records.
   *                     Note that .avsc will not be used in Orc related operation.
   *
   * @param fs
   * @param targetPath the path where the records will be written.
   * @param schema
   * @return
   */
  @Override
  public TypeInfo writeJsonResourceRecordsAsBinary(String baseResource, @Nullable FileSystem fs, Path targetPath,
      @Nullable TypeInfo schema) throws IOException {
    TreeMap<String, OrcRowIterator> recordMap = readAllRecordsInJsonResource(baseResource, schema);

    TypeInfo outputSchema = recordMap.lastEntry().getValue().getTypeInfo();
    for (Map.Entry<String, OrcRowIterator> entry : recordMap.entrySet()) {
      writeAsOrcBinary(entry.getValue(), outputSchema, new Path(targetPath, removeExtension(entry.getKey())));
    }

    return outputSchema;
  }

  /**
   * AvroRow version of writeAsOrcBinary
   */
  private void writeAsOrcBinary(OrcRowIterator input, TypeInfo schema, Path outputPath) throws IOException {
    Configuration configuration = new Configuration();

    // Note that it doesn't support schema evolution at all.
    // If the schema in realRow is inconsistent with given schema, writing into disk
    // would run into failure.
    ObjectInspector oi = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(schema);
    OrcFile.WriterOptions options = OrcFile.writerOptions(configuration).inspector(oi);
    Writer writer = null;

    while (input.hasNext()) {
      AvroRow avroRow = (AvroRow) input.next();
      if (writer == null) {
        options.inspector(avroRow.getInspector());
        writer = OrcFile.createWriter(outputPath, options);
      }
      writer.addRow(avroRow.realRow);
    }
    if (writer != null) {
      writer.close();
    }
  }

  // ORC-File Reading related functions

  // There's no GenericRecord for ORC existed(so that OrcStruct even doesn't provide readFields as
  // it is responsible to transform a Writable into GenericRecord in Avro world.
  @Override
  public TreeMap<String, OrcRowIterator> readAllRecordsInJsonResource(String baseResource,
      @Nullable TypeInfo schema) throws IOException {
    TypeInfo orcSchema;
    try {
      if (schema == null) {
        File schemaFile = new File(baseResource, "schema.avsc");
        String schemaResource = schemaFile.toString();
        orcSchema = convertAvroSchemaToOrcSchema(readAvscSchema(schemaResource, OrcTestTools.class));
      } else {
        orcSchema = schema;
      }
    } catch (SerDeException se) {
      throw new RuntimeException("Provided Avro Schema cannot be transformed to ORC schema", se);
    }

    TreeMap<String, OrcRowIterator> output = new TreeMap<>();
    for (String file : getJsonFileSetByResourceRootName(baseResource)) {
      log.info("Reading json record from " + file);
      String name = PathUtils.relativizePath(new Path(file), new Path(baseResource)).toString();
      output.put(name, readRecordsFromJsonInputStream(orcSchema, file));
    }

    return output;
  }

  public static class AvroRow implements Writable {
    Object realRow;
    ObjectInspector inspector;

    public AvroRow(Object row, ObjectInspector inspector) {
      this.realRow = row;
      this.inspector = inspector;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      throw new UnsupportedOperationException("can't write the bundle");
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      throw new UnsupportedOperationException("can't read the bundle");
    }

    ObjectInspector getInspector() {
      return inspector;
    }

    Object getRow() {
      return realRow;
    }
  }

  /**
   * Deserialize json object into a list of java object as a row, and transform each of java object
   * into {@link Writable} counterpart for constructing {@link OrcStruct}, in convenience of Orc reading and writing.
   *
   * @param typeInfo The ORC schema in {@link TypeInfo} format.
   * @param file The file name in String format.
   * @return
   */
  private OrcRowIterator readRecordsFromJsonInputStream(TypeInfo typeInfo, String file) throws IOException {

    InputStream is = OrcTestTools.class.getClassLoader().getResourceAsStream(file);


    // This getParent.getParent is dirty due to we need to simulate multiple-partitions scenarios in iTest.
    String schemaResourceName = new File(new File(file).getParentFile().getParent(), "schema.avsc").toString();

    Schema attemptedSchema = readAvscSchema(schemaResourceName, OrcTestTools.class);
    final Schema avroSchema =
        attemptedSchema == null ? readAvscSchema(new File(new File(file).getParent(), "schema.avsc").toString(),
            OrcTestTools.class) : attemptedSchema;

    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(avroSchema);
    Decoder decoder = DecoderFactory.get().jsonDecoder(avroSchema, is);

    return new OrcRowIterator(typeInfo, new AbstractIterator<Writable>() {
      @Override
      protected Writable computeNext() {
        try {
          GenericRecord record = reader.read(null, decoder);
          return getAvroWritable(record, avroSchema);
        } catch (IOException e) {
          try {
            is.close();
          } catch (IOException ioec) {
            log.warn("Failed to read record from inputstream, will close it immediately", ioec);
          }
          endOfData();
          return null;
        }
      }
    });
  }

  /**
   * From each record, transformed to {@link AvroRow} object for writing.
   * One can also choose to use OrcSerDe to obtain ORC-associated writable object.
   *
   * Using return object of this method would enable a self-maintained ORC writer(not from OrcOutputFormat)
   * to write object.
   */
  private Writable getAvroWritable(GenericRecord record, Schema avroSchema) {
    try {
      // Construct AvroSerDe with proper schema and deserialize into Hive object.
      AvroSerDe serDe = new AvroSerDe();
      Properties propertiesWithSchema = new Properties();
      propertiesWithSchema.setProperty(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName(),
          avroSchema.toString());
      serDe.initialize(null, propertiesWithSchema);
      AvroGenericRecordWritable avroGenericRecordWritable = new AvroGenericRecordWritable(record);
      avroGenericRecordWritable.setFileSchema(avroSchema);
      Object avroDeserialized = serDe.deserialize(avroGenericRecordWritable);
      ObjectInspector avroOI = new AvroObjectInspectorGenerator(avroSchema).getObjectInspector();

      return new AvroRow(avroDeserialized, avroOI);
    } catch (SerDeException se) {
      throw new RuntimeException("Failed in SerDe exception:", se);
    }
  }

  /**
   * Reading ORC file into in-memory representation.
   */
  @Override
  public TreeMap<String, OrcRowIterator> readAllRecordsInBinaryDirectory(FileSystem fs, Path path) throws IOException {
    TreeMap<String, OrcRowIterator> output = new TreeMap<>();
    if (!fs.exists(path)) {
      return output;
    }
    PathFilter pathFilter = new HiddenFilter();
    for (FileStatus status : FileListUtils.listFilesRecursively(fs, path, pathFilter)) {
      String key = PathUtils.relativizePath(status.getPath(), path).toString();
      Reader orcReader = OrcFile.createReader(fs, status.getPath());
      RecordReader recordReader = orcReader.rows();

      output.put(key, new OrcRowIterator(TypeInfoUtils.getTypeInfoFromObjectInspector(orcReader.getObjectInspector()),
          new AbstractIterator<Writable>() {
            @Override
            protected Writable computeNext() {
              try {
                if (recordReader.hasNext()) {
                  return (Writable) recordReader.next(null);
                } else {
                  recordReader.close();
                  endOfData();
                  return null;
                }
              } catch (IOException ioe) {
                log.warn("Failed to process orc record reader, will terminate reader immediately", ioe);
                endOfData();
                return null;
              }
            }
          }));
    }

    return output;
  }

  /**
   * An iterator over {@link OrcStruct} which is also aware of schema( Represented in {@link TypeInfo}).
   */
  @AllArgsConstructor
  public static class OrcRowIterator implements Iterator<Writable> {

    @Getter
    private final TypeInfo typeInfo;
    @Delegate
    private final Iterator<Writable> it;
  }

  /**
   * Convert Avro schema into TypeInfo.
   * Current version of Hive used by Gobblin open-source(1.0.1) doesn't have {@link org.apache.orc.TypeDescription}
   * and utilities associated with it. So instead {@link TypeInfo} is being used to represent Orc schema.
   * Note that {@link TypeInfo} is not case preserving as it is actually the internal schema representation of Hive.
   */
  public static TypeInfo convertAvroSchemaToOrcSchema(Schema avroSchema) throws SerDeException {
        return TypeInfoUtils.getTypeInfoFromObjectInspector(
            new AvroObjectInspectorGenerator(avroSchema).getObjectInspector());
  }
}