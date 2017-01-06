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

package gobblin.writer;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;


/**
 * Unit tests for {@link SimpleDataWriter}.
 *
 * @author akshay@nerdwallet.com
 */
@Test(groups = { "gobblin.writer" })
public class SimpleDataWriterTest {
  private String filePath;
  private final String schema = "";
  private final int newLine = "\n".getBytes()[0];
  private final State properties = new State();

  @BeforeMethod
  public void setUp() throws Exception {
    // Making the staging and/or output dirs if necessary
    File stagingDir = new File(TestConstants.TEST_STAGING_DIR);
    File outputDir = new File(TestConstants.TEST_OUTPUT_DIR);
    if (!stagingDir.exists()) {
      stagingDir.mkdirs();
    }
    if (!outputDir.exists()) {
      outputDir.mkdirs();
    }

    this.filePath = TestConstants.TEST_EXTRACT_NAMESPACE.replaceAll("\\.", "/") + "/" + TestConstants.TEST_EXTRACT_TABLE
        + "/" + TestConstants.TEST_EXTRACT_ID + "_" + TestConstants.TEST_EXTRACT_PULL_TYPE;

    properties.setProp(ConfigurationKeys.SIMPLE_WRITER_DELIMITER, "\n");
    properties.setProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, TestConstants.TEST_FS_URI);
    properties.setProp(ConfigurationKeys.WRITER_STAGING_DIR, TestConstants.TEST_STAGING_DIR);
    properties.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, TestConstants.TEST_OUTPUT_DIR);
    properties.setProp(ConfigurationKeys.WRITER_FILE_PATH, this.filePath);
    properties.setProp(ConfigurationKeys.WRITER_FILE_NAME, TestConstants.TEST_FILE_NAME);
    properties.setProp(ConfigurationKeys.SIMPLE_WRITER_PREPEND_SIZE, false);
  }

  /**
   * Test writing records without a delimiter and make sure it works.
   * @throws IOException
   */
  @Test
  public void testWriteBytesNoDelim() throws IOException {
    properties.setProp(ConfigurationKeys.SIMPLE_WRITER_DELIMITER, "");
    // Build a writer to write test records
    SimpleDataWriter writer = (SimpleDataWriter) new SimpleDataWriterBuilder()
        .writeTo(Destination.of(Destination.DestinationType.HDFS, properties)).writeInFormat(WriterOutputFormat.AVRO)
        .withWriterId(TestConstants.TEST_WRITER_ID).withSchema(this.schema).forBranch(0).build();
    byte[] rec1 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };
    byte[] rec2 = { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25 };
    byte[] rec3 = { 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45 };

    writer.write(rec1);
    writer.write(rec2);
    writer.write(rec3);

    writer.close();
    writer.commit();

    Assert.assertEquals(writer.recordsWritten(), 3);
    Assert.assertEquals(writer.bytesWritten(), rec1.length + rec2.length + rec3.length);

    File outputFile = new File(writer.getOutputFilePath());
    InputStream is = new FileInputStream(outputFile);
    int c, resNum = 0, resi = 0;
    byte[][] records = { rec1, rec2, rec3 };
    while ((c = is.read()) != -1) {
      if (resi >= records[resNum].length) {
        resNum++;
        resi = 0;
      }
      Assert.assertEquals(c, records[resNum][resi]);
      resi++;
    }
  }

  /**
   * Prepend the size to each record without delimiting the record. Each record
   * should be prepended by the size of that record and the bytes written should
   * include the prepended bytes.
   */
  @Test
  public void testPrependSizeWithoutDelimiter() throws IOException {
    properties.setProp(ConfigurationKeys.SIMPLE_WRITER_PREPEND_SIZE, true);
    properties.setProp(ConfigurationKeys.SIMPLE_WRITER_DELIMITER, "");
    SimpleDataWriter writer = (SimpleDataWriter) new SimpleDataWriterBuilder()
        .writeTo(Destination.of(Destination.DestinationType.HDFS, properties)).writeInFormat(WriterOutputFormat.AVRO)
        .withWriterId(TestConstants.TEST_WRITER_ID).withSchema(this.schema).forBranch(0).build();
    byte[] rec1 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };
    byte[] rec2 = { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25 };
    byte[] rec3 = { 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45 };
    byte[][] records = { rec1, rec2, rec3 };

    writer.write(rec1);
    writer.write(rec2);
    writer.write(rec3);

    writer.close();
    writer.commit();

    Assert.assertEquals(writer.recordsWritten(), 3);
    Assert.assertEquals(writer.bytesWritten(), rec1.length + rec2.length + rec3.length + (Long.SIZE / 8 * 3));

    File outputFile = new File(writer.getOutputFilePath());
    DataInputStream dis = new DataInputStream(new FileInputStream(outputFile));
    for (int i = 0; i < 3; i++) {
      long size = dis.readLong();
      Assert.assertEquals(size, records[i].length);
      for (int j = 0; j < size; j++) {
        Assert.assertEquals(dis.readByte(), records[i][j]);
      }
    }
  }

  /**
   * Use the simple data writer to write random bytes to a file and ensure
   * they are the same when read back.
   *
   * @throws IOException
   */
  @Test
  public void testWriteRandomBytes() throws IOException {
    // Build a writer to write test records
    SimpleDataWriter writer = (SimpleDataWriter) new SimpleDataWriterBuilder()
        .writeTo(Destination.of(Destination.DestinationType.HDFS, properties)).writeInFormat(WriterOutputFormat.AVRO)
        .withWriterId(TestConstants.TEST_WRITER_ID).withSchema(this.schema).forBranch(0).build();
    byte[] rec1 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };
    byte[] rec2 = { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25 };
    byte[] rec3 = { 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45 };

    writer.write(rec1);
    writer.write(rec2);
    writer.write(rec3);

    writer.close();
    writer.commit();

    Assert.assertEquals(writer.recordsWritten(), 3);
    Assert.assertEquals(writer.bytesWritten(), rec1.length + rec2.length + rec3.length + 3); // 3 bytes for newline character

    File outputFile = new File(writer.getOutputFilePath());
    InputStream is = new FileInputStream(outputFile);
    int c, resNum = 0, resi = 0;
    byte[][] records = { rec1, rec2, rec3 };
    while ((c = is.read()) != -1) {
      if (c != newLine) {
        Assert.assertEquals(c, records[resNum][resi]);
        resi++;
      } else {
        resNum++;
        resi = 0;
      }
    }
  }

  /**
   * Prepend the size to each record and delimit the record. Each record
   * should be prepended by the size of that record and the bytes written should
   * include the prepended bytes.
   */
  @Test
  public void testPrependSizeWithDelimiter() throws IOException {
    properties.setProp(ConfigurationKeys.SIMPLE_WRITER_PREPEND_SIZE, true);
    SimpleDataWriter writer = (SimpleDataWriter) new SimpleDataWriterBuilder()
        .writeTo(Destination.of(Destination.DestinationType.HDFS, properties)).writeInFormat(WriterOutputFormat.AVRO)
        .withWriterId(TestConstants.TEST_WRITER_ID).withSchema(this.schema).forBranch(0).build();
    byte[] rec1 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };
    byte[] rec2 = { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25 };
    byte[] rec3 = { 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45 };
    byte[][] records = { rec1, rec2, rec3 };

    writer.write(rec1);
    writer.write(rec2);
    writer.write(rec3);

    writer.close();
    writer.commit();

    Assert.assertEquals(writer.recordsWritten(), 3);
    Assert.assertEquals(writer.bytesWritten(), rec1.length + rec2.length + rec3.length + (Long.SIZE / 8 * 3) + 3);

    File outputFile = new File(writer.getOutputFilePath());
    DataInputStream dis = new DataInputStream(new FileInputStream(outputFile));
    for (int i = 0; i < 3; i++) {
      long size = dis.readLong();
      Assert.assertEquals(size, records[i].length + 1);
      for (int j = 0; j < size - 1; j++) {
        Assert.assertEquals(dis.readByte(), records[i][j]);
      }
      Assert.assertEquals(dis.readByte(), '\n');
    }
  }

  /**
   * Use the simple writer to write json entries to a file and ensure that
   * they are the same when read back.
   *
   * @throws IOException
   */
  @Test
  public void testWrite() throws IOException {
    SimpleDataWriter writer = (SimpleDataWriter) new SimpleDataWriterBuilder()
        .writeTo(Destination.of(Destination.DestinationType.HDFS, properties)).writeInFormat(WriterOutputFormat.AVRO)
        .withWriterId(TestConstants.TEST_WRITER_ID).withSchema(this.schema).forBranch(0).build();
    int totalBytes = 3; // 3 extra bytes for the newline character
    // Write all test records
    for (String record : TestConstants.JSON_RECORDS) {
      byte[] toWrite = record.getBytes();
      Assert.assertEquals(toWrite.length, record.length()); // ensure null byte does not get added to end
      writer.write(toWrite);
      totalBytes += toWrite.length;
    }

    writer.close();
    writer.commit();

    Assert.assertEquals(writer.recordsWritten(), 3);
    Assert.assertEquals(writer.bytesWritten(), totalBytes);

    File outputFile = new File(writer.getOutputFilePath());
    BufferedReader br = new BufferedReader(new FileReader(outputFile));
    String line;
    int lineNumber = 0;
    while ((line = br.readLine()) != null) {
      Assert.assertEquals(line, TestConstants.JSON_RECORDS[lineNumber]);
      lineNumber++;
    }
    br.close();
    Assert.assertEquals(lineNumber, 3);
  }

  /**
   * If the staging file exists, the simple data writer should overwrite its contents.
   *
   * @throws IOException
   */
  @Test
  public void testOverwriteExistingStagingFile() throws IOException {
    byte[] randomBytesStage = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };
    byte[] randomBytesWrite = { 11, 12, 13, 14, 15 };
    Path stagingFile = new Path(TestConstants.TEST_STAGING_DIR + Path.SEPARATOR + this.filePath,
        TestConstants.TEST_FILE_NAME + "." + TestConstants.TEST_WRITER_ID + "." + "tmp");
    Configuration conf = new Configuration();
    // Add all job configuration properties so they are picked up by Hadoop
    for (String key : properties.getPropertyNames()) {
      conf.set(key, properties.getProp(key));
    }
    FileSystem fs = FileSystem.get(URI.create(TestConstants.TEST_FS_URI), conf);

    OutputStream os = fs.create(stagingFile);
    os.write(randomBytesStage);
    os.flush();
    os.close();

    SimpleDataWriter writer = (SimpleDataWriter) new SimpleDataWriterBuilder()
        .writeTo(Destination.of(Destination.DestinationType.HDFS, properties)).writeInFormat(WriterOutputFormat.AVRO)
        .withWriterId(TestConstants.TEST_WRITER_ID).withSchema(this.schema).forBranch(0).build();

    writer.write(randomBytesWrite);
    writer.close();
    writer.commit();

    Assert.assertEquals(writer.recordsWritten(), 1);
    Assert.assertEquals(writer.bytesWritten(), randomBytesWrite.length + 1);

    File writeFile = new File(writer.getOutputFilePath());
    int c, i = 0;
    InputStream is = new FileInputStream(writeFile);
    while ((c = is.read()) != -1) {
      if (i == 5) {
        Assert.assertEquals(c, (byte) newLine); // the last byte should be newline
        i++;
        continue;
      }
      Assert.assertEquals(randomBytesWrite[i], c);
      i++;
    }
  }

  @AfterMethod
  public void tearDown() throws IOException {
    // Clean up the staging and/or output directories if necessary
    File testRootDir = new File(TestConstants.TEST_ROOT_DIR);
    if (testRootDir.exists()) {
      FileUtil.fullyDelete(testRootDir);
    }
  }
}
