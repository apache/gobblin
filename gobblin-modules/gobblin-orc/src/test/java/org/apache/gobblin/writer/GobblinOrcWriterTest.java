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

package org.apache.gobblin.writer;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.orc.OrcConf;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.ListColumnVector;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.io.Closer;
import com.google.common.io.Files;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.source.workunit.WorkUnit;

import static org.apache.gobblin.configuration.ConfigurationKeys.AVG_RECORD_SIZE;
import static org.apache.gobblin.writer.GenericRecordToOrcValueWriterTest.deserializeOrcRecords;
import static org.apache.gobblin.writer.GobblinOrcWriter.CONTAINER_JVM_MEMORY_OVERHEAD_MBS;
import static org.apache.gobblin.writer.GobblinOrcWriter.DEFAULT_RECORD_SIZE_SCALE_FACTOR;
import static org.apache.gobblin.writer.GobblinOrcWriter.ORC_WRITER_AUTO_TUNE_ENABLED;
import static org.apache.gobblin.writer.GobblinOrcWriter.RECORD_SIZE_SCALE_FACTOR;
import static org.mockito.Mockito.*;


/**
 * For running these tests in IDE, make sure all ORC libraries existed in the external library folder are specified
 * with "nohive" classifier if they do (orc-core)
 */
public class GobblinOrcWriterTest {

  public static final List<GenericRecord> deserializeAvroRecords(Class clazz, Schema schema, String schemaPath)
      throws IOException {
    List<GenericRecord> records = new ArrayList<>();

    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);

    InputStream dataInputStream = clazz.getClassLoader().getResourceAsStream(schemaPath);
    Decoder decoder = DecoderFactory.get().jsonDecoder(schema, dataInputStream);
    GenericRecord recordContainer = reader.read(null, decoder);
    ;
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

  @Test
  public void testAutoTuned() throws Exception {
    Closer closer = Closer.create();
    Schema schema =
        new Schema.Parser().parse(this.getClass().getClassLoader().getResourceAsStream("orc_writer_test/schema.avsc"));

    FsDataWriterBuilder<Schema, GenericRecord> mockBuilder =
        (FsDataWriterBuilder<Schema, GenericRecord>) Mockito.mock(FsDataWriterBuilder.class);
    when(mockBuilder.getSchema()).thenReturn(schema);
    State properties = new WorkUnit();
    String stagingDir = Files.createTempDir().getAbsolutePath();
    String outputDir = Files.createTempDir().getAbsolutePath();
    properties.setProp(ConfigurationKeys.WRITER_STAGING_DIR, stagingDir);
    properties.setProp(ConfigurationKeys.WRITER_FILE_PATH, "simple");
    properties.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, outputDir);
    when(mockBuilder.getFileName(properties)).thenReturn("file");

    properties.setProp(ORC_WRITER_AUTO_TUNE_ENABLED, true);
    properties.setProp(CONTAINER_JVM_MEMORY_OVERHEAD_MBS, 2048);
    closer.register(new GobblinOrcWriter(mockBuilder, properties));
    // Verify the side effect within the properties object.
    Assert.assertEquals(properties.getPropAsInt(OrcConf.ROWS_BETWEEN_CHECKS.name()),
        Math.round((4096 - 2048) * 0.5 * 1024 / 3) / (1024 * properties.getPropAsInt(RECORD_SIZE_SCALE_FACTOR, DEFAULT_RECORD_SIZE_SCALE_FACTOR)));

    // Will get to 5000
    properties.setProp(AVG_RECORD_SIZE, 10);
    closer.register(new GobblinOrcWriter(mockBuilder, properties));
    Assert.assertEquals(properties.getPropAsInt(OrcConf.ROWS_BETWEEN_CHECKS.name()), 5000);

    closer.close();
  }

  @Test
  public void testRowBatchDeepClean() throws Exception {
    Schema schema = new Schema.Parser().parse(
        this.getClass().getClassLoader().getResourceAsStream("orc_writer_list_test/schema.avsc"));
    List<GenericRecord> recordList = deserializeAvroRecords(this.getClass(), schema, "orc_writer_list_test/data.json");
    // Mock WriterBuilder, bunch of mocking behaviors to work-around precondition checks in writer builder
    FsDataWriterBuilder<Schema, GenericRecord> mockBuilder =
        (FsDataWriterBuilder<Schema, GenericRecord>) Mockito.mock(FsDataWriterBuilder.class);
    when(mockBuilder.getSchema()).thenReturn(schema);
    State dummyState = new WorkUnit();
    String stagingDir = Files.createTempDir().getAbsolutePath();
    String outputDir = Files.createTempDir().getAbsolutePath();
    dummyState.setProp(ConfigurationKeys.WRITER_STAGING_DIR, stagingDir);
    dummyState.setProp(ConfigurationKeys.WRITER_FILE_PATH, "simple");
    dummyState.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, outputDir);
    dummyState.setProp("orcWriter.deepCleanBatch", "true");
    when(mockBuilder.getFileName(dummyState)).thenReturn("file");

    Closer closer = Closer.create();

    GobblinOrcWriter orcWriter = closer.register(new GobblinOrcWriter(mockBuilder, dummyState));
    for (GenericRecord genericRecord : recordList) {
      orcWriter.write(genericRecord);
    }
    // Manual trigger flush
    orcWriter.flush();

    Assert.assertNull(((BytesColumnVector) ((ListColumnVector) orcWriter.rowBatch.cols[0]).child).vector);
    Assert.assertNull(((BytesColumnVector) orcWriter.rowBatch.cols[1]).vector);
  }

  /**
   * A basic unit for trivial writer correctness.
   * TODO: A detailed test suite of ORC-writer for different sorts of schema:
   */
  @Test
  public void testWrite() throws Exception {
    Schema schema =
        new Schema.Parser().parse(this.getClass().getClassLoader().getResourceAsStream("orc_writer_test/schema.avsc"));
    List<GenericRecord> recordList = deserializeAvroRecords(this.getClass(), schema, "orc_writer_test/data.json");

    // Mock WriterBuilder, bunch of mocking behaviors to work-around precondition checks in writer builder
    FsDataWriterBuilder<Schema, GenericRecord> mockBuilder =
        (FsDataWriterBuilder<Schema, GenericRecord>) Mockito.mock(FsDataWriterBuilder.class);
    when(mockBuilder.getSchema()).thenReturn(schema);

    State dummyState = new WorkUnit();
    String stagingDir = Files.createTempDir().getAbsolutePath();
    String outputDir = Files.createTempDir().getAbsolutePath();
    dummyState.setProp(ConfigurationKeys.WRITER_STAGING_DIR, stagingDir);
    dummyState.setProp(ConfigurationKeys.WRITER_FILE_PATH, "simple");
    dummyState.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, outputDir);
    when(mockBuilder.getFileName(dummyState)).thenReturn("file");
    Path outputFilePath = new Path(outputDir, "simple/file");

    // Having a closer to manage the life-cycle of the writer object.
    // Will verify if scenarios like double-close could survive.
    Closer closer = Closer.create();
    GobblinOrcWriter orcWriter = closer.register(new GobblinOrcWriter(mockBuilder, dummyState));

    // Create one more writer to test fail-case.
    GobblinOrcWriter orcFailWriter = new GobblinOrcWriter(mockBuilder, dummyState);

    for (GenericRecord record : recordList) {
      orcWriter.write(record);
      orcFailWriter.write(record);
    }

    // Not yet flushed or reaching default batch size, no records should have been materialized.
    Assert.assertEquals(orcWriter.recordsWritten(), 0);
    Assert.assertEquals(orcFailWriter.recordsWritten(), 0);

    // Try close, should catch relevant CloseBeforeFlushException
    try {
      orcFailWriter.close();
    } catch (CloseBeforeFlushException e) {
      Assert.assertEquals(e.datasetName, schema.getName());
    }

    orcWriter.commit();
    Assert.assertEquals(orcWriter.recordsWritten(), 2);

    // Verify ORC file contains correct records.
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Assert.assertTrue(fs.exists(outputFilePath));
    List<Writable> orcRecords = deserializeOrcRecords(outputFilePath, fs);
    Assert.assertEquals(orcRecords.size(), 2);

    // Double-close without protection of org.apache.gobblinGobblinOrcWriter#closed
    // leads to NPE within org.apache.orc.impl.PhysicalFsWriter.writeFileMetadata. Try removing protection condition
    // in close method implementation if want to verify.
    try {
      closer.close();
    } catch (NullPointerException npe) {
      Assert.fail();
    }
  }
}