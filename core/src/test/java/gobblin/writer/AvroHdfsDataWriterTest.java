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

package gobblin.writer;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;


/**
 * Unit tests for {@link AvroHdfsDataWriter}.
 *
 * @author ynli
 */
@Test(groups = {"gobblin.writer"})
public class AvroHdfsDataWriterTest {

  private static final Type FIELD_ENTRY_TYPE = new TypeToken<Map<String, Object>>() {
  }.getType();

  private Schema schema;
  private DataWriter<GenericRecord> writer;
  private String filePath;

  @BeforeClass
  @SuppressWarnings("unchecked")
  public void setUp()
      throws Exception {
    // Making the staging and/or output dirs if necessary
    File stagingDir = new File(TestConstants.TEST_STAGING_DIR);
    File outputDir = new File(TestConstants.TEST_OUTPUT_DIR);
    if (!stagingDir.exists()) {
      stagingDir.mkdirs();
    }
    if (!outputDir.exists()) {
      outputDir.mkdirs();
    }

    this.schema = new Schema.Parser().parse(TestConstants.AVRO_SCHEMA);

    this.filePath = TestConstants.TEST_EXTRACT_NAMESPACE.replaceAll("\\.", "/") + "/" +
        TestConstants.TEST_EXTRACT_TABLE + "/" + TestConstants.TEST_EXTRACT_ID + "_" +
        TestConstants.TEST_EXTRACT_PULL_TYPE;

    State properties = new State();
    properties.setProp(ConfigurationKeys.WRITER_BUFFER_SIZE, ConfigurationKeys.DEFAULT_BUFFER_SIZE);
    properties.setProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, TestConstants.TEST_FS_URI);
    properties.setProp(ConfigurationKeys.WRITER_STAGING_DIR, TestConstants.TEST_STAGING_DIR);
    properties.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, TestConstants.TEST_OUTPUT_DIR);
    properties.setProp(ConfigurationKeys.WRITER_FILE_PATH, this.filePath);
    properties.setProp(ConfigurationKeys.WRITER_FILE_NAME, TestConstants.TEST_FILE_NAME);

    // Build a writer to write test records
    this.writer = new AvroDataWriterBuilder().writeTo(Destination.of(Destination.DestinationType.HDFS, properties))
        .writeInFormat(WriterOutputFormat.AVRO).withWriterId(TestConstants.TEST_WRITER_ID).withSchema(this.schema)
        .forBranch(-1).build();
  }

  @Test
  public void testWrite()
      throws IOException {
    // Write all test records
    for (String record : TestConstants.JSON_RECORDS) {
      this.writer.write(convertRecord(record));
    }

    Assert.assertEquals(this.writer.recordsWritten(), 3);

    this.writer.close();
    this.writer.commit();

    File outputFile = new File(TestConstants.TEST_OUTPUT_DIR + Path.SEPARATOR + this.filePath,
        TestConstants.TEST_FILE_NAME + "." + TestConstants.TEST_WRITER_ID + "." +
            TestConstants.TEST_FILE_EXTENSION);
    DataFileReader<GenericRecord> reader =
        new DataFileReader<GenericRecord>(outputFile, new GenericDatumReader<GenericRecord>(this.schema));

    // Read the records back and assert they are identical to the ones written
    GenericRecord user1 = reader.next();
    // Strings are in UTF8, so we have to call toString() here and below
    Assert.assertEquals(user1.get("name").toString(), "Alyssa");
    Assert.assertEquals(user1.get("favorite_number"), 256);
    Assert.assertEquals(user1.get("favorite_color").toString(), "yellow");

    GenericRecord user2 = reader.next();
    Assert.assertEquals(user2.get("name").toString(), "Ben");
    Assert.assertEquals(user2.get("favorite_number"), 7);
    Assert.assertEquals(user2.get("favorite_color").toString(), "red");

    GenericRecord user3 = reader.next();
    Assert.assertEquals(user3.get("name").toString(), "Charlie");
    Assert.assertEquals(user3.get("favorite_number"), 68);
    Assert.assertEquals(user3.get("favorite_color").toString(), "blue");

    reader.close();
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    // Clean up the staging and/or output directories if necessary
    File testRootDir = new File(TestConstants.TEST_ROOT_DIR);
    if (testRootDir.exists()) {
      FileUtil.fullyDelete(testRootDir);
    }
  }

  private GenericRecord convertRecord(String inputRecord) {
    Gson gson = new Gson();
    JsonElement element = gson.fromJson(inputRecord, JsonElement.class);
    Map<String, Object> fields = gson.fromJson(element, FIELD_ENTRY_TYPE);
    GenericRecord outputRecord = new GenericData.Record(schema);
    for (Map.Entry<String, Object> entry : fields.entrySet()) {
      outputRecord.put(entry.getKey(), entry.getValue());
    }

    return outputRecord;
  }
}
