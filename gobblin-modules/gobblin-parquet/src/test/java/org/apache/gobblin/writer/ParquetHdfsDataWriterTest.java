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
import java.util.Map;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import parquet.example.data.Group;
import parquet.example.data.simple.convert.GroupRecordConverter;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.api.InitContext;
import parquet.hadoop.api.ReadSupport;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

import static org.apache.gobblin.writer.ParquetDataWriterBuilder.WRITER_PARQUET_DICTIONARY;
import static org.apache.gobblin.writer.ParquetDataWriterBuilder.WRITER_PARQUET_DICTIONARY_PAGE_SIZE;
import static org.apache.gobblin.writer.ParquetDataWriterBuilder.WRITER_PARQUET_PAGE_SIZE;
import static org.apache.gobblin.writer.ParquetDataWriterBuilder.WRITER_PARQUET_VALIDATE;


@Test(groups = {"gobblin.writer"})
public class ParquetHdfsDataWriterTest {

  private MessageType schema;
  private String filePath;
  private ParquetHdfsDataWriter writer;
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
    this.writer = (ParquetHdfsDataWriter) getParquetDataWriterBuilder().build();
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
    properties.setProp(WRITER_PARQUET_DICTIONARY, true);
    properties.setProp(WRITER_PARQUET_DICTIONARY_PAGE_SIZE, 1024);
    properties.setProp(WRITER_PARQUET_PAGE_SIZE, 1024);
    properties.setProp(WRITER_PARQUET_VALIDATE, true);
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

  private List<Group> readParquetFiles(File outputFile)
      throws IOException {
    ParquetReader<Group> reader = null;
    List<Group> records = new ArrayList<>();
    try {
      reader = new ParquetReader<>(new Path(outputFile.toString()), new SimpleReadSupport());
      for (Group value = reader.read(); value != null; value = reader.read()) {
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
    List<Group> records;
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
    Group resultRecord1 = records.get(0);
    Group resultRecord2 = records.get(1);

    Assert.assertEquals(firstWrite, 1);
    Assert.assertEquals(secondWrite, 2);
    Assert.assertEquals(resultRecord1.getString("name", 0), "tilak");
    Assert.assertEquals(resultRecord1.getInteger("age", 0), 22);
    Assert.assertEquals(resultRecord2.getString("name", 0), "other");
    Assert.assertEquals(resultRecord2.getInteger("age", 0), 22);
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

  class SimpleReadSupport extends ReadSupport<Group> {
    @Override
    public RecordMaterializer<Group> prepareForRead(Configuration conf, Map<String, String> metaData,
        MessageType schema, ReadContext context) {
      return new GroupRecordConverter(schema);
    }

    @Override
    public ReadContext init(InitContext context) {
      return new ReadContext(context.getFileSchema());
    }
  }
}
