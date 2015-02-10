/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.test;

import java.io.IOException;
import java.net.URI;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;


/**
 * An implementation of {@link Extractor} for integration test.
 *
 * @author ynli
 */
public class TestExtractor implements Extractor<String, String> {

  private static final Logger LOG = LoggerFactory.getLogger(TestExtractor.class);

  private static final String SOURCE_FILE_KEY = "source.file";

  // Test Avro schema
  private static final String AVRO_SCHEMA = "{\"namespace\": \"example.avro\",\n" +
      " \"type\": \"record\",\n" +
      " \"name\": \"User\",\n" +
      " \"fields\": [\n" +
      "     {\"name\": \"name\", \"type\": \"string\"},\n" +
      "     {\"name\": \"favorite_number\",  \"type\": \"int\"},\n" +
      "     {\"name\": \"favorite_color\", \"type\": \"string\"}\n" +
      " ]\n" +
      "}";

  private static final int TOTAL_RECORDS = 1000;

  private DataFileReader<GenericRecord> dataFileReader;

  public TestExtractor(WorkUnitState workUnitState) {
    //super(workUnitState);
    Schema schema = new Schema.Parser().parse(AVRO_SCHEMA);
    Path sourceFile = new Path(workUnitState.getWorkunit().getProp(SOURCE_FILE_KEY));
    LOG.info("Reading from source file " + sourceFile);
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
    try {
      FileSystem fs = FileSystem
          .get(URI.create(workUnitState.getProp(ConfigurationKeys.FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI)),
              new Configuration());
      fs.makeQualified(sourceFile);
      this.dataFileReader =
          new DataFileReader<GenericRecord>(new FsInput(sourceFile, new Configuration()), datumReader);
    } catch (IOException ioe) {
      LOG.error("Failed to read the source file " + sourceFile, ioe);
    }
  }

  @Override
  public String getSchema() {
    return AVRO_SCHEMA;
  }

  @Override
  public String readRecord(String reuse) {
    if (this.dataFileReader == null) {
      return null;
    }

    if (this.dataFileReader.hasNext()) {
      return this.dataFileReader.next().toString();
    }

    return null;
  }

  @Override
  public void close() {
    try {
      this.dataFileReader.close();
    } catch (IOException ioe) {
      // ignored
    }
  }

  @Override
  public long getExpectedRecordCount() {
    return TOTAL_RECORDS;
  }

  @Override
  public long getHighWatermark() {
    // TODO Auto-generated method stub
    return 0;
  }
}
