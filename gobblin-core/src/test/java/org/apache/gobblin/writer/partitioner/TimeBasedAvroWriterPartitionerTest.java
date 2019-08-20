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

package org.apache.gobblin.writer.partitioner;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.stream.RecordEnvelope;
import org.apache.gobblin.writer.AvroDataWriterBuilder;
import org.apache.gobblin.writer.DataWriter;
import org.apache.gobblin.writer.DataWriterBuilder;
import org.apache.gobblin.writer.Destination;
import org.apache.gobblin.writer.PartitionedDataWriter;
import org.apache.gobblin.writer.WriterOutputFormat;


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
  }

  /**
   * Test
   *  1. Record timestamp of type long
   *  2. Partition path of a given record
   */
  @Test
  public void testWriter() throws IOException {

    Schema schema = getRecordSchema("long");
    State state = getBasicState();
    // Write three records, each should be written to a different file
    GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(schema);

    DataWriter<GenericRecord> millisPartitionWriter = getWriter(schema, state);

    // This timestamp corresponds to 2015/01/01
    genericRecordBuilder.set("timestamp", 1420099200000l);
    millisPartitionWriter.writeEnvelope(new RecordEnvelope<>(genericRecordBuilder.build()));

    // This timestamp corresponds to 2015/01/02
    genericRecordBuilder.set("timestamp", 1420185600000l);
    millisPartitionWriter.writeEnvelope(new RecordEnvelope<>(genericRecordBuilder.build()));

    millisPartitionWriter.close();
    millisPartitionWriter.commit();
    // Check that the writer reports that 2 records have been written
    Assert.assertEquals(millisPartitionWriter.recordsWritten(), 2);

    state.setProp(TimeBasedWriterPartitioner.WRITER_PARTITION_TIMEUNIT, "seconds");
    DataWriter<GenericRecord> secsPartitionWriter = getWriter(schema, state);
    // This timestamp corresponds to 2015/01/03
    genericRecordBuilder.set("timestamp", 1420272000L);
    secsPartitionWriter.writeEnvelope(new RecordEnvelope<>(genericRecordBuilder.build()));
    secsPartitionWriter.close();
    secsPartitionWriter.commit();
    // Check that the writer reports that 1 record has been written
    Assert.assertEquals(secsPartitionWriter.recordsWritten(), 1);

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

  @Test
  public void testGetRecordTimestamp() {

    // Test for string record timestamp in millis partition time unit
    State state = getBasicState();
    TimeBasedAvroWriterPartitioner partitioner = new TimeBasedAvroWriterPartitioner(state);
    GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(getRecordSchema("string"));
    genericRecordBuilder.set("timestamp", "1557786583000");
    // Test without parsing as string
    Assert.assertTrue(partitioner.getRecordTimestamp(genericRecordBuilder.build()) > 1557786583000L);

    // Test with parsing as string
    state.setProp(TimeBasedAvroWriterPartitioner.WRITER_PARTITION_ENABLE_PARSE_AS_STRING, true);
    partitioner = new TimeBasedAvroWriterPartitioner(state);
    Assert.assertEquals(partitioner.getRecordTimestamp(genericRecordBuilder.build()), 1557786583000L);
    // Test for Utf8
    genericRecordBuilder.set("timestamp", new Utf8("1557786583000"));
    Assert.assertEquals(partitioner.getRecordTimestamp(genericRecordBuilder.build()), 1557786583000L);
    // Test for null value
    genericRecordBuilder.set("timestamp", null);
    Assert.assertTrue(
        partitioner.getRecordTimestamp(genericRecordBuilder.build()) <= System.currentTimeMillis());

    // Test for string type in seconds partition time unit
    state.setProp(TimeBasedWriterPartitioner.WRITER_PARTITION_TIMEUNIT, "seconds");
    partitioner = new TimeBasedAvroWriterPartitioner(state);
    genericRecordBuilder.set("timestamp", "1557786583");
    Assert.assertEquals(partitioner.getRecordTimestamp(genericRecordBuilder.build()), 1557786583L);
  }

  @AfterClass
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(new File(TEST_ROOT_DIR));
  }

  private DataWriter<GenericRecord> getWriter(Schema schema, State state)
      throws IOException {
    // Build a writer to write test records
    DataWriterBuilder<Schema, GenericRecord> builder = new AvroDataWriterBuilder()
        .writeTo(Destination.of(Destination.DestinationType.HDFS, state)).writeInFormat(WriterOutputFormat.AVRO)
        .withWriterId(WRITER_ID).withSchema(schema).withBranches(1).forBranch(0);
    return new PartitionedDataWriter<Schema, GenericRecord>(builder, state);
  }

  private State getBasicState() {
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

    return properties;
  }

  private Schema getRecordSchema(String timestampType) {
    return new Schema.Parser().parse("{" + "\"type\" : \"record\"," + "\"name\" : \"User\"," + "\"namespace\" : \"example.avro\"," + "\"fields\" : [ {"
        + "\"name\" : \"" + PARTITION_COLUMN_NAME + "\"," + "\"type\" : [\"null\", \"" + timestampType + "\"]} ]" + "}");
  }
}
