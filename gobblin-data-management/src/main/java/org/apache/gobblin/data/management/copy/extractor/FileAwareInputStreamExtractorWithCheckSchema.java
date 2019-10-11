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

package org.apache.gobblin.data.management.copy.extractor;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.gobblin.data.management.copy.CopySource;
import org.apache.gobblin.util.schema_check.AvroSchemaCheckStrategy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.data.management.copy.FileAwareInputStream;
import org.apache.gobblin.source.extractor.DataRecordException;

/**
 * Used instead of {@link FileAwareInputStreamExtractor} that extracts {@link InputStream}s. This extractor will first
 * check if the schema matches the expected schema. If not it will abort the job.
 */

public class FileAwareInputStreamExtractorWithCheckSchema extends FileAwareInputStreamExtractor {

  public FileAwareInputStreamExtractorWithCheckSchema(FileSystem fs, CopyableFile file, WorkUnitState state) {
    super(fs, file, state);
  }

  public FileAwareInputStreamExtractorWithCheckSchema(FileSystem fs, CopyableFile file) {
    this(fs, file, null);
  }

  @Override
  protected FileAwareInputStream buildStream(FileSystem fsFromFile) throws DataRecordException, IOException {
    if (!schemaChecking(fsFromFile)) {
      throw new DataRecordException("Schema does not match the expected schema");
    }
    return super.buildStream(fsFromFile);
  }

  /**
   * Use {@link AvroSchemaCheckStrategy} to make sure the real schema and the expected schema have matching field names and types
   * @param fsFromFile
   * @return
   * @throws IOException
   */
  protected boolean schemaChecking(FileSystem fsFromFile) throws IOException {
    if( !this.state.getPropAsBoolean(CopySource.SCHEMA_CHECK_ENABLED, CopySource.DEFAULT_SCHEMA_CHECK_ENABLED) ) {
      return true;
    }
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
    DataFileReader<GenericRecord> dataFileReader =
        new DataFileReader(new FsInput(this.file.getFileStatus().getPath(), new Configuration()), datumReader);
    Schema schema = dataFileReader.getSchema();
    if(this.state.getProp(ConfigurationKeys.COPY_EXPECTED_SCHEMA) == null) {
      throw new IOException("Expected schema is not set properly");
    }
    Schema expectedSchema = new Schema.Parser().parse(this.state.getProp(ConfigurationKeys.COPY_EXPECTED_SCHEMA));
    AvroSchemaCheckStrategy strategy = AvroSchemaCheckStrategy.AvroSchemaCheckStrategyFactory.create(this.state);
    if(strategy == null) {
      throw new IOException("schema check strategy cannot be initialized");
    }
    return strategy.compare(expectedSchema,schema);
  }

}
