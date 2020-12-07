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
package org.apache.gobblin.parquet.writer.test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import junit.framework.Assert;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.parquet.writer.ParquetRecordFormat;
import org.apache.gobblin.parquet.writer.ParquetWriterConfiguration;
import org.apache.gobblin.test.TestRecord;
import org.apache.gobblin.writer.DataWriter;
import org.apache.gobblin.writer.DataWriterBuilder;
import org.apache.gobblin.writer.Destination;
import org.apache.gobblin.writer.WriterOutputFormat;


/**
 * Base class for building version-specific tests for Parquet
 */
@Slf4j
public abstract class ParquetHdfsDataWriterTestBase {

  public ParquetHdfsDataWriterTestBase(TestConstantsBase testConstants)
  {
    this.testConstants = testConstants;
  }

  private final TestConstantsBase testConstants;
  private String filePath;
  private DataWriter writer;

  protected abstract DataWriterBuilder getDataWriterBuilder();

  public void setUp()
      throws Exception {
    // Making the staging and/or output dirs if necessary
    File stagingDir = new File(this.testConstants.TEST_STAGING_DIR);
    File outputDir = new File(this.testConstants.TEST_OUTPUT_DIR);
    if (!stagingDir.exists()) {
      boolean mkdirs = stagingDir.mkdirs();
      assert mkdirs;
    }
    if (!outputDir.exists()) {
      boolean mkdirs = outputDir.mkdirs();
      assert mkdirs;
    }
    this.filePath = getFilePath();
  }

  private String getFilePath() {
    return TestConstantsBase.TEST_EXTRACT_NAMESPACE.replaceAll("\\.", "/") + "/" + TestConstantsBase.TEST_EXTRACT_TABLE + "/"
        + TestConstantsBase.TEST_EXTRACT_ID + "_" + TestConstantsBase.TEST_EXTRACT_PULL_TYPE;
  }

  private State createStateWithConfig(ParquetRecordFormat format) {
    State properties = new State();
    properties.setProp(ConfigurationKeys.WRITER_BUFFER_SIZE, ConfigurationKeys.DEFAULT_BUFFER_SIZE);
    properties.setProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, TestConstantsBase.TEST_FS_URI);
    properties.setProp(ConfigurationKeys.WRITER_STAGING_DIR, TestConstantsBase.TEST_STAGING_DIR);
    properties.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, TestConstantsBase.TEST_OUTPUT_DIR);
    properties.setProp(ConfigurationKeys.WRITER_FILE_PATH, this.filePath);
    properties.setProp(ConfigurationKeys.WRITER_FILE_NAME, this.testConstants.getParquetTestFilename(format.name()));
    properties.setProp(ParquetWriterConfiguration.WRITER_PARQUET_DICTIONARY, true);
    properties.setProp(ParquetWriterConfiguration.WRITER_PARQUET_DICTIONARY_PAGE_SIZE, 1024);
    properties.setProp(ParquetWriterConfiguration.WRITER_PARQUET_PAGE_SIZE, 1024);
    properties.setProp(ParquetWriterConfiguration.WRITER_PARQUET_VALIDATE, true);
    properties.setProp(ParquetWriterConfiguration.WRITER_PARQUET_FORMAT, format.toString());
    properties.setProp(ConfigurationKeys.WRITER_CODEC_TYPE, "gzip");
    return properties;
  }


  protected abstract List<TestRecord> readParquetRecordsFromFile(File outputFile, ParquetRecordFormat format) throws IOException;


  public void testWrite()
      throws Exception {
    ParquetRecordFormat[] formats = ParquetRecordFormat.values();
    for (ParquetRecordFormat format : formats) {
      State formatSpecificProperties = createStateWithConfig(format);

      this.writer = getDataWriterBuilder()
          .writeTo(Destination.of(Destination.DestinationType.HDFS, formatSpecificProperties))
          .withWriterId(TestConstantsBase.TEST_WRITER_ID)
          .writeInFormat(WriterOutputFormat.PARQUET)
          .withSchema(getSchema(format))
          .build();

      for (int i=0; i < 2; ++i) {
        Object record = this.testConstants.getRecord(i, format);
        this.writer.write(record);
        Assert.assertEquals(i+1, this.writer.recordsWritten());
      }
      this.writer.close();
      this.writer.commit();

      String filePath = TestConstantsBase.TEST_OUTPUT_DIR + Path.SEPARATOR + this.filePath;
      File outputFile = new File(filePath, this.testConstants.getParquetTestFilename(format.name()));

      List<TestRecord> records = readParquetRecordsFromFile(outputFile, format);
      for (int i = 0; i < 2; ++i) {
        TestRecord resultRecord = records.get(i);
        log.debug("Testing {} record {}", i, resultRecord);
        Assert.assertEquals(TestConstantsBase.getPayloadValues()[i], resultRecord.getPayload());
        Assert.assertEquals(TestConstantsBase.getSequenceValues()[i], resultRecord.getSequence());
        Assert.assertEquals(TestConstantsBase.getPartitionValues()[i], resultRecord.getPartition());
      }
    }
  }

  protected abstract Object getSchema(ParquetRecordFormat format);

  public void tearDown()
      throws IOException {
    // Clean up the staging and/or output directories if necessary
    File testRootDir = new File(TestConstantsBase.TEST_ROOT_DIR);
    if (testRootDir.exists()) {
      FileUtil.fullyDelete(testRootDir);
    }
  }

}
