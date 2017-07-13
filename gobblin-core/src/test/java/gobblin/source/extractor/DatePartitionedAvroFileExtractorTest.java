/*
 * Copyright (C) 2016 Lorand Bendig All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.DatePartitionedAvroFileSource;
import gobblin.source.workunit.Extract.TableType;
import gobblin.source.workunit.MultiWorkUnit;
import gobblin.source.workunit.WorkUnit;
import gobblin.stream.RecordEnvelope;
import gobblin.writer.AvroDataWriterBuilder;
import gobblin.writer.DataWriter;
import gobblin.writer.DataWriterBuilder;
import gobblin.writer.Destination;
import gobblin.writer.PartitionedDataWriter;
import gobblin.writer.WriterOutputFormat;
import gobblin.writer.partitioner.TimeBasedAvroWriterPartitioner;
import gobblin.writer.partitioner.TimeBasedWriterPartitioner;


/**
 * Unit tests for {@link DatePartitionedAvroFileExtractor}.
 *
 * @author Lorand Bendig
 */
@Test(groups = { "gobblin.source.extractor." })
public class DatePartitionedAvroFileExtractorTest {

  private static final String SIMPLE_CLASS_NAME = DatePartitionedAvroFileExtractorTest.class.getSimpleName();

  private static final String TEST_ROOT_DIR = "/tmp/" + SIMPLE_CLASS_NAME + "-test";
  private static final String STAGING_DIR = TEST_ROOT_DIR + Path.SEPARATOR + "staging";
  private static final String OUTPUT_DIR = TEST_ROOT_DIR + Path.SEPARATOR + "job-output";
  private static final String FILE_NAME = SIMPLE_CLASS_NAME + "-name.avro";
  private static final String PARTITION_COLUMN_NAME = "timestamp";

  private static final String PREFIX = "minutes";
  private static final String SUFFIX = "test";
  private static final String SOURCE_ENTITY = "testsource";

  private static final String DATE_PATTERN = "yyyy/MM/dd/HH_mm";
  private static final int RECORD_SIZE = 4;

  private static final String AVRO_SCHEMA =
    "{" +
      "\"type\" : \"record\"," +
      "\"name\" : \"User\"," +
      "\"namespace\" : \"example.avro\"," +
        "\"fields\" : [" +
        "{" +
          "\"name\" : \"" + PARTITION_COLUMN_NAME + "\"," +
          "\"type\" : \"long\"" +
        "}" +
      "]" +
    "}";

  private Schema schema;
  private DataWriter<GenericRecord> writer;

  private DateTime startDateTime;
  private long[] recordTimestamps = new long[RECORD_SIZE];

  private static final DateTimeZone TZ = DateTimeZone.forID(ConfigurationKeys.PST_TIMEZONE_NAME);

  @BeforeClass
  public void setUp() throws IOException {

    this.schema = new Schema.Parser().parse(AVRO_SCHEMA);

    //set up datetime objects
    DateTime now = new DateTime(TZ).minusHours(2);
    this.startDateTime =
        new DateTime(now.getYear(), now.getMonthOfYear(), now.getDayOfMonth(), now.getHourOfDay(), 30, 0, TZ);

    //create records, shift their timestamp by 1 minute
    DateTime recordDt = startDateTime;
    for (int i = 0; i < RECORD_SIZE; i++) {
      recordDt = recordDt.plusMinutes(1);
      recordTimestamps[i] = recordDt.getMillis();
    }

    // create dummy data partitioned by minutes
    State state = new State();
    state.setProp(TimeBasedAvroWriterPartitioner.WRITER_PARTITION_COLUMNS, PARTITION_COLUMN_NAME);
    state.setProp(ConfigurationKeys.WRITER_BUFFER_SIZE, ConfigurationKeys.DEFAULT_BUFFER_SIZE);
    state.setProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, ConfigurationKeys.LOCAL_FS_URI);
    state.setProp(ConfigurationKeys.WRITER_STAGING_DIR, STAGING_DIR);
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, OUTPUT_DIR);
    state.setProp(ConfigurationKeys.WRITER_FILE_PATH, SOURCE_ENTITY);
    state.setProp(ConfigurationKeys.WRITER_FILE_NAME, FILE_NAME);
    state.setProp(TimeBasedWriterPartitioner.WRITER_PARTITION_PATTERN, DATE_PATTERN);
    state.setProp(TimeBasedWriterPartitioner.WRITER_PARTITION_PREFIX, PREFIX);
    state.setProp(TimeBasedWriterPartitioner.WRITER_PARTITION_SUFFIX, SUFFIX);
    state.setProp(ConfigurationKeys.WRITER_PARTITIONER_CLASS, TimeBasedAvroWriterPartitioner.class.getName());

    DataWriterBuilder<Schema, GenericRecord> builder = new AvroDataWriterBuilder()
        .writeTo(Destination.of(Destination.DestinationType.HDFS, state))
        .writeInFormat(WriterOutputFormat.AVRO)
        .withWriterId("writer-1")
        .withSchema(this.schema)
        .withBranches(1).forBranch(0);

    this.writer = new PartitionedDataWriter<Schema, GenericRecord>(builder, state);

    GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(this.schema);
    for (int i = 0; i < RECORD_SIZE; i++) {
      genericRecordBuilder.set(PARTITION_COLUMN_NAME, recordTimestamps[i]);
      this.writer.writeEnvelope(new RecordEnvelope<>(genericRecordBuilder.build()));
    }

    this.writer.close();
    this.writer.commit();

  }

  @Test
  public void testReadPartitionsByMinute() throws IOException, DataRecordException {

    DatePartitionedAvroFileSource source = new DatePartitionedAvroFileSource();

    SourceState state = new SourceState();
    state.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, ConfigurationKeys.LOCAL_FS_URI);
    state.setProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY, SOURCE_ENTITY);
    state.setProp(ConfigurationKeys.SOURCE_FILEBASED_DATA_DIRECTORY, OUTPUT_DIR + Path.SEPARATOR + SOURCE_ENTITY);
    state.setProp(ConfigurationKeys.SOURCE_ENTITY, SOURCE_ENTITY);
    state.setProp(ConfigurationKeys.SOURCE_MAX_NUMBER_OF_PARTITIONS, 2);

    state.setProp("date.partitioned.source.partition.pattern", DATE_PATTERN);
    state.setProp("date.partitioned.source.min.watermark.value", DateTimeFormat.forPattern(DATE_PATTERN).print(
        this.startDateTime.minusMinutes(1)));
    state.setProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY, TableType.SNAPSHOT_ONLY);
    state.setProp("date.partitioned.source.partition.prefix", PREFIX);
    state.setProp("date.partitioned.source.partition.suffix", SUFFIX);

    //Read data partitioned by minutes, i.e each workunit is assigned records under the same YYYY/MM/dd/HH_mm directory
    List<WorkUnit> workunits = source.getWorkunits(state);

    Assert.assertEquals(workunits.size(), 4);
    verifyWorkUnits(workunits);
  }

  @Test
  public void testWorksNoPrefix() throws IOException, DataRecordException {
    DatePartitionedAvroFileSource source = new DatePartitionedAvroFileSource();

    SourceState state = new SourceState();
    state.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, ConfigurationKeys.LOCAL_FS_URI);
    state.setProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY, SOURCE_ENTITY);
    state.setProp(ConfigurationKeys.SOURCE_FILEBASED_DATA_DIRECTORY, OUTPUT_DIR + Path.SEPARATOR + SOURCE_ENTITY + Path.SEPARATOR + PREFIX);
    state.setProp(ConfigurationKeys.SOURCE_ENTITY, SOURCE_ENTITY);
    state.setProp(ConfigurationKeys.SOURCE_MAX_NUMBER_OF_PARTITIONS, 2);

    state.setProp("date.partitioned.source.partition.pattern", DATE_PATTERN);
    state.setProp("date.partitioned.source.min.watermark.value", DateTimeFormat.forPattern(DATE_PATTERN).print(
        this.startDateTime.minusMinutes(1)));
    state.setProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY, TableType.SNAPSHOT_ONLY);
    state.setProp("date.partitioned.source.partition.suffix", SUFFIX);

    //Read data partitioned by minutes, i.e each workunit is assigned records under the same YYYY/MM/dd/HH_mm directory
    List<WorkUnit> workunits = source.getWorkunits(state);

    Assert.assertEquals(workunits.size(), 4);
    verifyWorkUnits(workunits);
  }

  private void verifyWorkUnits(List<WorkUnit> workunits)
      throws IOException, DataRecordException {
    for (int i = 0; i < RECORD_SIZE; i++) {
      WorkUnit workUnit = ((MultiWorkUnit) workunits.get(i)).getWorkUnits().get(0);
      WorkUnitState wuState = new WorkUnitState(workunits.get(i), new State());
      wuState.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, ConfigurationKeys.LOCAL_FS_URI);
      wuState.setProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL,
          workUnit.getProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL));
      try (DatePartitionedAvroFileExtractor extractor = new DatePartitionedAvroFileExtractor(wuState);) {

        GenericRecord record = extractor.readRecord(null);
        Assert.assertEquals(recordTimestamps[i], record.get(PARTITION_COLUMN_NAME));
        Assert.assertEquals(recordTimestamps[i], workUnit.getPropAsLong(ConfigurationKeys.WORK_UNIT_DATE_PARTITION_KEY));
      }
    }
  }

  @AfterClass
  public void tearDown() throws IOException {
    this.writer.close();
    FileUtils.deleteDirectory(new File(TEST_ROOT_DIR));
  }

}
