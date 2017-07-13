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

package gobblin.writer.partitioner;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.stream.RecordEnvelope;
import gobblin.writer.AvroDataWriterBuilder;
import gobblin.writer.DataWriter;
import gobblin.writer.DataWriterBuilder;
import gobblin.writer.Destination;
import gobblin.writer.PartitionedDataWriter;
import gobblin.writer.WriterOutputFormat;


/**
 * Tests for {@link TimeBasedAvroWriterPartitioner}.
 */
@Test(groups = { "gobblin.writer.partitioner" })
public class TimeBasedAvroWriterPartitionerTest {

  private static final String SIMPLE_CLASS_NAME = TimeBasedAvroWriterPartitionerTest.class.getSimpleName();

  private static final String TEST_ROOT_DIR = SIMPLE_CLASS_NAME + "-test";
  private static final String STAGING_DIR = TEST_ROOT_DIR + Path.SEPARATOR + "staging";
  private static final String OUTPUT_DIR = TEST_ROOT_DIR + Path.SEPARATOR + "output";
  private static final String BASE_FILE_PATH = "base";
  private static final String FILE_NAME = SIMPLE_CLASS_NAME + "-name.avro";
  private static final String PARTITION_COLUMN_NAME = "timestamp";
  private static final String WRITER_ID = "writer-1";

  private static final String AVRO_SCHEMA =
      "{" + "\"type\" : \"record\"," + "\"name\" : \"User\"," + "\"namespace\" : \"example.avro\"," + "\"fields\" : [ {"
          + "\"name\" : \"" + PARTITION_COLUMN_NAME + "\"," + "\"type\" : \"long\"" + "} ]" + "}";

  private Schema schema;
  private DataWriter<GenericRecord> writer;

  @BeforeClass
  public void setUp() throws IOException {
    File stagingDir = new File(STAGING_DIR);
    File outputDir = new File(OUTPUT_DIR);

    if (!stagingDir.exists()) {
      stagingDir.mkdirs();
    } else {
      FileUtils.deleteDirectory(stagingDir);
    }

    if (!outputDir.exists()) {
      outputDir.mkdirs();
    } else {
      FileUtils.deleteDirectory(outputDir);
    }

    this.schema = new Schema.Parser().parse(AVRO_SCHEMA);

    State properties = new State();
    properties.setProp(TimeBasedAvroWriterPartitioner.WRITER_PARTITION_COLUMNS, PARTITION_COLUMN_NAME);
    properties.setProp(ConfigurationKeys.WRITER_BUFFER_SIZE, ConfigurationKeys.DEFAULT_BUFFER_SIZE);
    properties.setProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, ConfigurationKeys.LOCAL_FS_URI);
    properties.setProp(ConfigurationKeys.WRITER_STAGING_DIR, STAGING_DIR);
    properties.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, OUTPUT_DIR);
    properties.setProp(ConfigurationKeys.WRITER_FILE_PATH, BASE_FILE_PATH);
    properties.setProp(ConfigurationKeys.WRITER_FILE_NAME, FILE_NAME);
    properties.setProp(TimeBasedWriterPartitioner.WRITER_PARTITION_PATTERN, "yyyy/MM/dd");
    properties.setProp(ConfigurationKeys.WRITER_PARTITIONER_CLASS, TimeBasedAvroWriterPartitioner.class.getName());

    // Build a writer to write test records
    DataWriterBuilder<Schema, GenericRecord> builder = new AvroDataWriterBuilder()
        .writeTo(Destination.of(Destination.DestinationType.HDFS, properties)).writeInFormat(WriterOutputFormat.AVRO)
        .withWriterId(WRITER_ID).withSchema(this.schema).withBranches(1).forBranch(0);
    this.writer = new PartitionedDataWriter<Schema, GenericRecord>(builder, properties);
  }

  @Test
  public void testWriter() throws IOException {

    // Write three records, each should be written to a different file
    GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(this.schema);

    // This timestamp corresponds to 2015/01/01
    genericRecordBuilder.set("timestamp", 1420099200000l);
    this.writer.writeEnvelope(new RecordEnvelope<>(genericRecordBuilder.build()));

    // This timestamp corresponds to 2015/01/02
    genericRecordBuilder.set("timestamp", 1420185600000l);
    this.writer.writeEnvelope(new RecordEnvelope<>(genericRecordBuilder.build()));

    // This timestamp corresponds to 2015/01/03
    genericRecordBuilder.set("timestamp", 1420272000000l);
    this.writer.writeEnvelope(new RecordEnvelope<>(genericRecordBuilder.build()));

    // Check that the writer reports that 3 records have been written
    Assert.assertEquals(this.writer.recordsWritten(), 3);

    this.writer.close();
    this.writer.commit();

    // Check that 3 files were created
    Assert.assertEquals(FileUtils.listFiles(new File(TEST_ROOT_DIR), new String[] { "avro" }, true).size(), 3);

    // Check if each file exists, and in the correct location
    File baseOutputDir = new File(OUTPUT_DIR, BASE_FILE_PATH);
    Assert.assertTrue(baseOutputDir.exists());

    File outputDir20150101 =
        new File(baseOutputDir, "2015" + Path.SEPARATOR + "01" + Path.SEPARATOR + "01" + Path.SEPARATOR + FILE_NAME);
    Assert.assertTrue(outputDir20150101.exists());

    File outputDir20150102 =
        new File(baseOutputDir, "2015" + Path.SEPARATOR + "01" + Path.SEPARATOR + "02" + Path.SEPARATOR + FILE_NAME);
    Assert.assertTrue(outputDir20150102.exists());

    File outputDir20150103 =
        new File(baseOutputDir, "2015" + Path.SEPARATOR + "01" + Path.SEPARATOR + "03" + Path.SEPARATOR + FILE_NAME);
    Assert.assertTrue(outputDir20150103.exists());
  }

  @AfterClass
  public void tearDown() throws IOException {
    this.writer.close();
    FileUtils.deleteDirectory(new File(TEST_ROOT_DIR));
  }
}
