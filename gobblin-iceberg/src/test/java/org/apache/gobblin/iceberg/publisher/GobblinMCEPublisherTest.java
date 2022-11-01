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

package org.apache.gobblin.iceberg.publisher;

import azkaban.jobExecutor.AbstractJob;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import java.io.InputStream;
import java.util.ArrayList;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.gobblin.hive.policy.HiveSnapshotRegistrationPolicy;
import org.apache.gobblin.iceberg.GobblinMCEProducer;
import org.apache.gobblin.metadata.GobblinMetadataChangeEvent;
import org.apache.gobblin.metadata.OperationType;
import org.apache.gobblin.metadata.SchemaSource;
import gobblin.configuration.WorkUnitState;
import java.io.File;
import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.writer.FsDataWriterBuilder;
import org.apache.gobblin.writer.GobblinOrcWriter;
import org.apache.gobblin.writer.PartitionedDataWriter;
import org.apache.gobblin.writer.partitioner.TimeBasedWriterPartitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Metrics;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;


public class GobblinMCEPublisherTest {

  Schema avroDataSchema = SchemaBuilder.record("test")
      .fields()
      .name("id")
      .type()
      .longType()
      .noDefault()
      .name("data")
      .type()
      .optional()
      .stringType()
      .endRecord();
  Schema _avroPartitionSchema;
  private String dbName = "hivedb";

  static File tmpDir;
  static File dataDir;
  static File dataFile;
  static File datasetDir;
  static Path orcFilePath;
  static String orcSchema;

  public static final List<GenericRecord> deserializeAvroRecords(Class clazz, Schema schema, String schemaPath)
      throws IOException {
    List<GenericRecord> records = new ArrayList<>();

    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);

    InputStream dataInputStream = clazz.getClassLoader().getResourceAsStream(schemaPath);
    Decoder decoder = DecoderFactory.get().jsonDecoder(schema, dataInputStream);
    GenericRecord recordContainer = reader.read(null, decoder);

    try {
      while (recordContainer != null) {
        records.add(recordContainer);
        recordContainer = reader.read(null, decoder);
      }
    } catch (IOException ioe) {
      dataInputStream.close();
    }
    return records;
  }
  @BeforeClass
  public void setUp() throws Exception {
    tmpDir = Files.createTempDir();
    datasetDir = new File(tmpDir, "/data/tracking/testTable");
    dataFile = new File(datasetDir, "/hourly/2020/03/17/08/data.avro");
    Files.createParentDirs(dataFile);
    dataDir = new File(dataFile.getParent());
    Assert.assertTrue(dataDir.exists());
    writeRecord();
    _avroPartitionSchema =
        SchemaBuilder.record("partitionTest").fields().name("ds").type().optional().stringType().endRecord();

    //Write ORC file for test
    Schema schema =
        new Schema.Parser().parse(this.getClass().getClassLoader().getResourceAsStream("publisherTest/schema.avsc"));
    orcSchema = schema.toString();
    List<GenericRecord> recordList = deserializeAvroRecords(this.getClass(), schema, "publisherTest/data.json");

    // Mock WriterBuilder, bunch of mocking behaviors to work-around precondition checks in writer builder
    FsDataWriterBuilder<Schema, GenericRecord> mockBuilder =
        (FsDataWriterBuilder<Schema, GenericRecord>) Mockito.mock(FsDataWriterBuilder.class);
    when(mockBuilder.getSchema()).thenReturn(schema);

    State dummyState = new WorkUnit();
    String stagingDir = new File(tmpDir, "/orc/staging").getAbsolutePath();
    String outputDir = new File(tmpDir, "/orc/output").getAbsolutePath();
    dummyState.setProp(ConfigurationKeys.WRITER_STAGING_DIR, stagingDir);
    dummyState.setProp(ConfigurationKeys.WRITER_FILE_PATH, "simple");
    dummyState.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, outputDir);
    dummyState.setProp(ConfigurationKeys.WRITER_STAGING_DIR, stagingDir);
    when(mockBuilder.getFileName(dummyState)).thenReturn("file.orc");
    orcFilePath = new Path(outputDir, "simple/file.orc");

    // Having a closer to manage the life-cycle of the writer object.
    // Will verify if scenarios like double-close could survive.
    Closer closer = Closer.create();
    GobblinOrcWriter orcWriter = closer.register(new GobblinOrcWriter(mockBuilder, dummyState));

    for (GenericRecord record : recordList) {
      orcWriter.write(record);
    }

    orcWriter.commit();
    orcWriter.close();

    // Verify ORC file contains correct records.
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Assert.assertTrue(fs.exists(orcFilePath));
  }

  @AfterClass
  public void cleanUp() throws Exception {
    FileUtils.forceDeleteOnExit(tmpDir);
  }

  @Test
  public void testPublishGMCEForAvro() throws IOException {
    GobblinMCEProducer producer = Mockito.mock(GobblinMCEProducer.class);
    Mockito.doCallRealMethod()
        .when(producer)
        .getGobblinMetadataChangeEvent(anyMap(), anyList(), anyList(), anyMap(), any(), any(), any());
    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        GobblinMetadataChangeEvent gmce =
            producer.getGobblinMetadataChangeEvent((Map<Path, Metrics>) args[0], null, null,
                (Map<String, String>) args[1], OperationType.add_files, SchemaSource.SCHEMAREGISTRY, null);
        Assert.assertEquals(gmce.getNewFiles().size(), 1);
        FileSystem fs = FileSystem.get(new Configuration());
        Assert.assertEquals(gmce.getNewFiles().get(0).getFilePath(),
            new Path(dataFile.getAbsolutePath()).makeQualified(fs.getUri(), new Path("/")).toString());
        return null;
      }
    }).when(producer).sendGMCE(anyMap(), anyList(), anyList(), anyMap(), any(), any());

    WorkUnitState state = new WorkUnitState();
    setGMCEPublisherStateForAvroFile(state);
    Mockito.doCallRealMethod().when(producer).setState(state);
    producer.setState(state);
    GobblinMCEPublisher publisher = new GobblinMCEPublisher(state, producer);
    publisher.publishData(Arrays.asList(state));
  }

  @Test
  public void testPublishGMCEForORC() throws IOException {
    GobblinMCEProducer producer = Mockito.mock(GobblinMCEProducer.class);
    Mockito.doCallRealMethod()
        .when(producer)
        .getGobblinMetadataChangeEvent(anyMap(), anyList(), anyList(), anyMap(), any(), any(), any());
    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        GobblinMetadataChangeEvent gmce =
            producer.getGobblinMetadataChangeEvent((Map<Path, Metrics>) args[0], null, null,
                (Map<String, String>) args[1], OperationType.add_files, SchemaSource.SCHEMAREGISTRY, null);
        Assert.assertEquals(gmce.getNewFiles().size(), 1);
        FileSystem fs = FileSystem.get(new Configuration());
        Charset charset = Charset.forName("UTF-8");
        CharsetEncoder encoder = charset.newEncoder();
        Assert.assertEquals(gmce.getNewFiles().get(0).getFilePath(),
            orcFilePath.makeQualified(fs.getUri(), new Path("/")).toString());
        Assert.assertEquals(gmce.getNewFiles().get(0).getFileMetrics().getLowerBounds().get(1).getValue(),
            encoder.encode(CharBuffer.wrap("Alyssa")));
        Assert.assertEquals(gmce.getNewFiles().get(0).getFileMetrics().getUpperBounds().get(1).getValue(),
            encoder.encode(CharBuffer.wrap("Bob")));
        return null;
      }
    }).when(producer).sendGMCE(anyMap(), anyList(), anyList(), anyMap(), any(), any());

    WorkUnitState state = new WorkUnitState();
    setGMCEPublisherStateForOrcFile(state);
    Mockito.doCallRealMethod().when(producer).setState(state);
    producer.setState(state);
    GobblinMCEPublisher publisher = new GobblinMCEPublisher(state, producer);
    publisher.publishData(Arrays.asList(state));
  }

  @Test (dependsOnMethods = {"testPublishGMCEForAvro"})
  public void testPublishGMCEWithoutFile() throws IOException {
    GobblinMCEProducer producer = Mockito.mock(GobblinMCEProducer.class);
    Mockito.doCallRealMethod()
        .when(producer)
        .getGobblinMetadataChangeEvent(anyMap(), anyList(), anyList(), anyMap(), any(), any(), any());
    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        GobblinMetadataChangeEvent gmce =
            producer.getGobblinMetadataChangeEvent((Map<Path, Metrics>) args[0], null, null,
                (Map<String, String>) args[1], OperationType.change_property, SchemaSource.NONE, null);
        Assert.assertEquals(gmce.getNewFiles().size(), 1);
        Assert.assertNull(gmce.getOldFiles());
        Assert.assertNull(gmce.getOldFilePrefixes());
        Assert.assertEquals(gmce.getOperationType(), OperationType.change_property);
        return null;
      }
    }).when(producer).sendGMCE(anyMap(), anyList(), anyList(), anyMap(), any(), any());

    WorkUnitState state = new WorkUnitState();
    setGMCEPublisherStateWithoutNewFile(state);
    Mockito.doCallRealMethod().when(producer).setState(state);
    producer.setState(state);
    GobblinMCEPublisher publisher = new GobblinMCEPublisher(state, producer);
    publisher.publishData(Arrays.asList(state));
  }

  private void setGMCEPublisherStateForOrcFile(WorkUnitState state) {
    state.setProp(GobblinMCEPublisher.NEW_FILES_LIST, orcFilePath.toString());
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY, "ORC");
    state.setProp(GobblinMCEPublisher.OFFSET_RANGE_KEY, "testTopic-1:0-1000");
    state.setProp(ConfigurationKeys.HIVE_REGISTRATION_POLICY,
        HiveSnapshotRegistrationPolicy.class.getCanonicalName());
    state.setProp(ConfigurationKeys.DATA_PUBLISHER_DATASET_DIR, datasetDir.toString());
    state.setProp(AbstractJob.JOB_ID, "testFlow");
    state.setProp(PartitionedDataWriter.WRITER_LATEST_SCHEMA, orcSchema);
  }

  private void setGMCEPublisherStateWithoutNewFile(WorkUnitState state) {
    //state.setProp(GobblinMCEPublisher.NEW_FILES_LIST, dataFile.toString());
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY, "AVRO");
    state.setProp(GobblinMCEPublisher.OFFSET_RANGE_KEY, "testTopic-1:0-1000");
    state.setProp(ConfigurationKeys.HIVE_REGISTRATION_POLICY,
        HiveSnapshotRegistrationPolicy.class.getCanonicalName());
    state.setProp(ConfigurationKeys.DATA_PUBLISHER_DATASET_DIR, datasetDir.toString());
    state.setProp(AbstractJob.JOB_ID, "testFlow");
    state.setProp(PartitionedDataWriter.WRITER_LATEST_SCHEMA, _avroPartitionSchema);
    state.setProp(TimeBasedWriterPartitioner.WRITER_PARTITION_PREFIX, "hourly");
  }

  private void setGMCEPublisherStateForAvroFile(WorkUnitState state) {
    state.setProp(GobblinMCEPublisher.NEW_FILES_LIST, dataFile.toString());
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY, "AVRO");
    state.setProp(GobblinMCEPublisher.OFFSET_RANGE_KEY, "testTopic-1:0-1000");
    state.setProp(ConfigurationKeys.HIVE_REGISTRATION_POLICY,
        HiveSnapshotRegistrationPolicy.class.getCanonicalName());
    state.setProp(ConfigurationKeys.DATA_PUBLISHER_DATASET_DIR, datasetDir.toString());
    state.setProp(AbstractJob.JOB_ID, "testFlow");
    state.setProp(PartitionedDataWriter.WRITER_LATEST_SCHEMA, _avroPartitionSchema);
  }

  private String writeRecord() throws IOException {
    GenericData.Record record = new GenericData.Record(avroDataSchema);
    record.put("id", 1L);
    record.put("data", "data");
    String path = dataFile.toString();
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>();
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(avroDataSchema, dataFile);
    dataFileWriter.append(record);
    dataFileWriter.close();
    return path;
  }
}
