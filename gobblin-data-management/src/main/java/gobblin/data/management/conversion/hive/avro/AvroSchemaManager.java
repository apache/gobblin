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
package gobblin.data.management.conversion.hive.avro;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.data.management.conversion.hive.query.HiveAvroORCQueryGenerator;
import gobblin.hive.avro.HiveAvroSerDeManager;
import gobblin.util.AvroUtils;
import gobblin.util.HadoopUtils;


/**
 * Avro schema for a {@link Partition} or {@link Table} is available at multiple locations. This class is used to decide
 * the schema to use. It also creates a temporary schema file on the {@link FileSystem}.
 *
 * <ul>
 * 1. The {@link Schema} can be set as a literal in the serde info<br>
 * 2. The {@link Schema} url can set as a property in the serde info<br>
 * 3. The {@link Schema} can be inferred using the physical data location of the {@link Table} or {@link Partition}<br>
 * </ul>
 *
 * Callers request for the schema url using {@link #getSchemaUrl(Partition)} or {@link #getSchemaUrl(Table)}.
 *<ul>
 * In case (1.), the literal is written as a {@link Schema} file under {@link #schemaDir}. The {@link Path} to this file
 * is uses as the {@link Schema} url<br>
 * In case (2.), the url itself is used as the {@link Schema} url<br>
 * In case (3.), a {@link Schema} file is created under {@link #schemaDir} for {@link Schema} of latest data file.<br>
 *</ul>
 *
 * In all three cases the mapping of {@link Schema} to temporary Schema file path is cached.
 * If multiple {@link Partition}s have the same {@link Schema} a duplicate schema file in not created. Already existing
 * {@link Schema} url for this {@link Schema} is used.
 */
@Slf4j
public class AvroSchemaManager {

  private static final String HIVE_SCHEMA_TEMP_DIR_PATH_KEY = "hive.schema.dir";
  private static final String DEFAULT_HIVE_SCHEMA_TEMP_DIR_PATH_KEY = "/tmp/gobblin_schemas";

  private final FileSystem fs;
  /**
   * A mapping of {@link Schema} hash to its {@link Path} on {@link FileSystem}
   */
  private final Map<String, Path> schemaPaths;

  /**
   * A temporary directory to hold all Schema files. The path is job id specific.
   * Deleting it will not affect other job executions
   */
  private final Path schemaDir;

  public AvroSchemaManager(FileSystem fs, State state) {
    this.fs = fs;
    this.schemaPaths = Maps.newHashMap();
    this.schemaDir = new Path(state.getProp(HIVE_SCHEMA_TEMP_DIR_PATH_KEY, DEFAULT_HIVE_SCHEMA_TEMP_DIR_PATH_KEY),
            state.getProp(ConfigurationKeys.JOB_ID_KEY));
  }

  /**
   * Get the url to <code>table</code>'s avro {@link Schema} file.
   *
   * @param table whose avro schema is to be returned
   * @return a {@link Path} to table's avro {@link Schema} file.
   */
  public Path getSchemaUrl(Table table) throws IOException {
    return getSchemaUrl(table.getTTable().getSd());
  }

  /**
   * Get the url to <code>partition</code>'s avro {@link Schema} file.
   *
   * @param partition whose avro schema is to be returned
   * @return a {@link Path} to table's avro {@link Schema} file.
   */
  public Path getSchemaUrl(Partition partition) throws IOException {
    return getSchemaUrl(partition.getTPartition().getSd());
  }

  /**
   * Delete the temporary {@link #schemaDir}
   */
  public void cleanupTempSchemas() throws IOException {
    HadoopUtils.deleteIfExists(this.fs, this.schemaDir, true);
  }

  public static Schema getSchemaFromUrl(Path schemaUrl, FileSystem fs) throws IOException {
    return AvroUtils.parseSchemaFromFile(schemaUrl, fs);
  }

  private Path getSchemaUrl(StorageDescriptor sd) throws IOException {

    String schemaString = StringUtils.EMPTY;
    try {
      // Try to fetch from SCHEMA URL
      if (sd.getSerdeInfo().getParameters().containsKey(HiveAvroSerDeManager.SCHEMA_URL)) {
        String schemaUrl = sd.getSerdeInfo().getParameters().get(HiveAvroSerDeManager.SCHEMA_URL);
        if (schemaUrl.startsWith("http")) {
          // Fetch schema literal via HTTP GET if scheme is http(s)
          schemaString = IOUtils.toString(new URI(schemaUrl), StandardCharsets.UTF_8);
          log.debug("Schema string is: " + schemaString);
          Schema schema = HiveAvroORCQueryGenerator.readSchemaFromString(schemaString);

          return getOrGenerateSchemaFile(schema);
        } else {
          // .. else fetch from HDFS or local filesystem
          return new Path(sd.getSerdeInfo().getParameters().get(HiveAvroSerDeManager.SCHEMA_URL));
        }
      }
      // Try to fetch from SCHEMA LITERAL
      else if (sd.getSerdeInfo().getParameters().containsKey(HiveAvroSerDeManager.SCHEMA_LITERAL)) {
        schemaString = sd.getSerdeInfo().getParameters().get(HiveAvroSerDeManager.SCHEMA_LITERAL);
        log.debug("Schema string is: " + schemaString);
        Schema schema = HiveAvroORCQueryGenerator.readSchemaFromString(schemaString);

        return getOrGenerateSchemaFile(schema);
      }
    } catch (URISyntaxException e) {
      log.error(String.format("Failed to parse schema from schema string. Falling back to HDFS schema: %s",
          schemaString), e);
    }

    // Try to fetch from HDFS
    Schema schema = AvroUtils.getDirectorySchema(new Path(sd.getLocation()), this.fs, true);

    if (schema == null) {
      throw new SchemaNotFoundException("Failed to get avro schema");
    }

    return getOrGenerateSchemaFile(schema);
  }

  /**
   * If url for schema already exists, return the url. If not create a new temporary schema file and return a the url.
   */
  private Path getOrGenerateSchemaFile(Schema schema) throws IOException {

    Preconditions.checkNotNull(schema, "Avro Schema should not be null");

    String hashedSchema = Hashing.sha256().hashString(schema.toString(), StandardCharsets.UTF_8).toString();

    if (!this.schemaPaths.containsKey(hashedSchema)) {

      Path schemaFilePath = new Path(this.schemaDir, String.valueOf(System.currentTimeMillis() + ".avsc"));
      AvroUtils.writeSchemaToFile(schema, schemaFilePath, fs, true);

      this.schemaPaths.put(hashedSchema, schemaFilePath);
    }

    return this.schemaPaths.get(hashedSchema);
  }
}
