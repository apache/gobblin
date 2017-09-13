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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import parquet.example.data.Group;
import parquet.hadoop.ParquetReader;
import parquet.schema.MessageType;
import parquet.tools.read.SimpleReadSupport;
import parquet.tools.read.SimpleRecord;

@Test(groups = {"gobblin.writer"})
public class ParquetHdfsDataWriterTest {

  private MessageType schema;
  private String filePath;
  private DataWriter<Group> writer;
  private State properties;

  @BeforeMethod
  public void setUp()
      throws Exception {
    // Making the staging and/or output dirs if necessary
    File stagingDir = new File(TestConstants.TEST_STAGING_DIR);
    File outputDir = new File(TestConstants.TEST_OUTPUT_DIR);
    if (!stagingDir.exists()) {
      boolean mkdirs = stagingDir.mkdirs();
      assert mkdirs;
    }
    if (!outputDir.exists()) {
      boolean mkdirs = outputDir.mkdirs();
      assert mkdirs;
    }
    this.schema = TestConstants.PARQUET_SCHEMA;
    this.filePath = getFilePath();
    this.properties = createStateWithConfig();
    this.writer = getParquetDataWriterBuilder().build();
  }

  private String getFilePath() {
    return TestConstants.TEST_EXTRACT_NAMESPACE.replaceAll("\\.", "/") + "/" + TestConstants.TEST_EXTRACT_TABLE + "/"
        + TestConstants.TEST_EXTRACT_ID + "_" + TestConstants.TEST_EXTRACT_PULL_TYPE;
  }

  private State createStateWithConfig() {
    State properties = new State();
    properties.setProp(ConfigurationKeys.WRITER_BUFFER_SIZE, ConfigurationKeys.DEFAULT_BUFFER_SIZE);
    properties.setProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, TestConstants.TEST_FS_URI);
    properties.setProp(ConfigurationKeys.WRITER_STAGING_DIR, TestConstants.TEST_STAGING_DIR);
    properties.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, TestConstants.TEST_OUTPUT_DIR);
    properties.setProp(ConfigurationKeys.WRITER_FILE_PATH, this.filePath);
    properties.setProp(ConfigurationKeys.WRITER_FILE_NAME, TestConstants.PARQUET_TEST_FILENAME);
    properties.setProp(ConfigurationKeys.WRITER_PARQUET_DICTIONARY, true);
    properties.setProp(ConfigurationKeys.WRITER_PARQUET_DICTIONARY_PAGE_SIZE, 1024);
    properties.setProp(ConfigurationKeys.WRITER_PARQUET_PAGE_SIZE, 1024);
    properties.setProp(ConfigurationKeys.WRITER_PARQUET_VALIDATE, true);
    return properties;
  }

  private ParquetDataWriterBuilder getParquetDataWriterBuilder() {
    ParquetDataWriterBuilder writerBuilder = new ParquetDataWriterBuilder();
    writerBuilder.destination = Destination.of(Destination.DestinationType.HDFS, properties);
    writerBuilder.writerId = TestConstants.TEST_WRITER_ID;
    writerBuilder.schema = this.schema;
    writerBuilder.format = WriterOutputFormat.PARQUET;
    return writerBuilder;
  }

  private List<SimpleRecord> readParquetFiles(File outputFile)
      throws IOException {
    ParquetReader<SimpleRecord> reader = null;
    List<SimpleRecord> records = new ArrayList<>();
    try {
      reader = new ParquetReader<>(new Path(outputFile.toString()), new SimpleReadSupport());
      for (SimpleRecord value = reader.read(); value != null; value = reader.read()) {
        records.add(value);
      }
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (Exception ex) {
          System.out.println(ex.getMessage());
        }
      }
    }
    return records;
  }

  @Test
  public void testWrite()
      throws Exception {
    long firstWrite;
    long secondWrite;
    List<SimpleRecord> records;
    Group record1 = TestConstants.PARQUET_RECORD_1;
    Group record2 = TestConstants.PARQUET_RECORD_2;
    String filePath = TestConstants.TEST_OUTPUT_DIR + Path.SEPARATOR + this.filePath;
    File outputFile = new File(filePath, TestConstants.PARQUET_TEST_FILENAME);

    this.writer.write(record1);
    firstWrite = this.writer.recordsWritten();
    this.writer.write(record2);
    secondWrite = this.writer.recordsWritten();
    this.writer.close();
    this.writer.commit();
    records = readParquetFiles(outputFile);
    SimpleRecord resultRecord1 = records.get(0);
    SimpleRecord resultRecord2 = records.get(1);

    Assert.assertEquals(firstWrite, 1);
    Assert.assertEquals(secondWrite, 2);
    Assert.assertEquals(resultRecord1.getValues().get(0).getName(), "name");
    Assert.assertEquals(resultRecord1.getValues().get(0).getValue(), "tilak");
    Assert.assertEquals(resultRecord1.getValues().get(1).getName(), "age");
    Assert.assertEquals(resultRecord1.getValues().get(1).getValue(), 22);
    Assert.assertEquals(resultRecord2.getValues().get(0).getName(), "name");
    Assert.assertEquals(resultRecord2.getValues().get(0).getValue(), "other");
    Assert.assertEquals(resultRecord2.getValues().get(1).getName(), "age");
    Assert.assertEquals(resultRecord2.getValues().get(1).getValue(), 22);
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
}