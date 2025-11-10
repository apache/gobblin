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

package org.apache.gobblin.data.management.copy.iceberg;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.source.extractor.filebased.FileBasedHelperException;

/**
 * Tests for {@link IcebergFileStreamHelper}.
 */
public class IcebergFileStreamHelperTest {

  private File tempDir;
  private File testFile1;
  private File testFile2;
  private IcebergFileStreamHelper helper;
  private State state;

  @BeforeMethod
  public void setUp() throws Exception {
    // Create temp directory and test files
    tempDir = new File(System.getProperty("java.io.tmpdir"), "iceberg-helper-test-" + System.currentTimeMillis());
    tempDir.mkdirs();

    testFile1 = new File(tempDir, "data-file-1.parquet");
    try (FileOutputStream fos = new FileOutputStream(testFile1)) {
      fos.write("Test data for file 1".getBytes());
    }

    testFile2 = new File(tempDir, "data-file-2.parquet");
    try (FileOutputStream fos = new FileOutputStream(testFile2)) {
      fos.write("Test data for file 2 with more content".getBytes());
    }

    // Set up state with file paths
    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL,
      testFile1.getAbsolutePath() + "," + testFile2.getAbsolutePath());
    state = new State(properties);

    // Initialize helper
    helper = new IcebergFileStreamHelper(state);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    if (helper != null) {
      helper.close();
    }
    // Clean up temp files
    if (tempDir != null && tempDir.exists()) {
      deleteDirectory(tempDir);
    }
  }

  @Test
  public void testConnect() throws Exception {
    helper.connect();
    // If no exception thrown, connection succeeded
    Assert.assertTrue(true, "Connection should succeed");
  }

  @Test
  public void testListFiles() throws Exception {
    helper.connect();
    List<String> files = helper.ls("");

    Assert.assertNotNull(files, "File list should not be null");
    Assert.assertEquals(files.size(), 2, "Should return 2 files from configuration");
    Assert.assertTrue(files.contains(testFile1.getAbsolutePath()),
      "Should contain first test file");
    Assert.assertTrue(files.contains(testFile2.getAbsolutePath()),
      "Should contain second test file");
  }

  @Test
  public void testListFilesWithEmptyConfig() throws Exception {
    State emptyState = new State();
    emptyState.setProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL, "");
    IcebergFileStreamHelper emptyHelper = new IcebergFileStreamHelper(emptyState);

    try {
      emptyHelper.connect();
      List<String> files = emptyHelper.ls("");
      Assert.assertTrue(files.isEmpty(), "Should return empty list for empty configuration");
    } finally {
      emptyHelper.close();
    }
  }

  @Test
  public void testGetFileStream() throws Exception {
    helper.connect();

    // Test getting file stream
    InputStream is = helper.getFileStream(testFile1.getAbsolutePath());
    Assert.assertNotNull(is, "File stream should not be null");

    // Verify we can read from stream
    byte[] buffer = new byte[1024];
    int bytesRead = is.read(buffer);
    Assert.assertTrue(bytesRead > 0, "Should be able to read bytes from stream");

    String content = new String(buffer, 0, bytesRead);
    Assert.assertEquals(content, "Test data for file 1",
      "Stream content should match file content");

    is.close();
  }

  @Test(expectedExceptions = FileBasedHelperException.class)
  public void testGetFileStreamForNonExistentFile() throws Exception {
    helper.connect();

    // Test error handling for non-existent file
    helper.getFileStream("/non/existent/path/file.parquet");
  }

  @Test
  public void testGetFileSize() throws Exception {
    helper.connect();

    // Test getting file size
    long size1 = helper.getFileSize(testFile1.getAbsolutePath());
    long size2 = helper.getFileSize(testFile2.getAbsolutePath());

    Assert.assertEquals(size1, "Test data for file 1".getBytes().length,
      "File size should match actual file size");
    Assert.assertEquals(size2, "Test data for file 2 with more content".getBytes().length,
      "File size should match actual file size");
    Assert.assertTrue(size2 > size1, "Second file should be larger than first");
  }

  @Test(expectedExceptions = FileBasedHelperException.class)
  public void testGetFileSizeForNonExistentFile() throws Exception {
    helper.connect();

    // Test error handling for non-existent file
    helper.getFileSize("/non/existent/path/file.parquet");
  }

  @Test
  public void testGetFileMTime() throws Exception {
    helper.connect();

    // Test getting file modification time
    long mtime1 = helper.getFileMTime(testFile1.getAbsolutePath());
    long mtime2 = helper.getFileMTime(testFile2.getAbsolutePath());

    Assert.assertTrue(mtime1 > 0, "Modification time should be positive");
    Assert.assertTrue(mtime2 > 0, "Modification time should be positive");

    // mtime2 should be >= mtime1 since it was created after (or same millisecond)
    Assert.assertTrue(mtime2 >= mtime1,
      "Second file's mtime should be >= first file's mtime");
  }

  @Test(expectedExceptions = FileBasedHelperException.class)
  public void testGetFileMTimeForNonExistentFile() throws Exception {
    helper.connect();

    // Test error handling for non-existent file
    helper.getFileMTime("/non/existent/path/file.parquet");
  }


  @Test
  public void testHadoopConfigurationProperties() throws Exception {
    // Test that Hadoop configuration properties are properly propagated
    State stateWithHadoopProps = new State();
    stateWithHadoopProps.setProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL,
      testFile1.getAbsolutePath());

    // Add custom Hadoop properties
    String testFsProperty = "fs.custom.impl";
    String testFsValue = "org.example.CustomFileSystem";
    String testHadoopProperty = "hadoop.custom.setting";
    String testHadoopValue = "customValue";

    stateWithHadoopProps.setProp(testFsProperty, testFsValue);
    stateWithHadoopProps.setProp(testHadoopProperty, testHadoopValue);
    stateWithHadoopProps.setProp("not.hadoop.property", "shouldNotBeInConfig");

    IcebergFileStreamHelper helperWithProps = new IcebergFileStreamHelper(stateWithHadoopProps);

    try {
      helperWithProps.connect();

      // Verify the properties were set in the Hadoop Configuration via reflection
      java.lang.reflect.Field configField = IcebergFileStreamHelper.class.getDeclaredField("configuration");
      configField.setAccessible(true);
      org.apache.hadoop.conf.Configuration config =
        (org.apache.hadoop.conf.Configuration) configField.get(helperWithProps);

      Assert.assertEquals(config.get(testFsProperty), testFsValue,
        "fs.* property should be propagated to Hadoop configuration");
      Assert.assertEquals(config.get(testHadoopProperty), testHadoopValue,
        "hadoop.* property should be propagated to Hadoop configuration");
      Assert.assertNull(config.get("not.hadoop.property"),
        "Non-Hadoop properties should not be propagated");
    } finally {
      helperWithProps.close();
    }
  }

  @Test
  public void testClose() throws Exception {
    helper.connect();

    // Open a stream
    InputStream is = helper.getFileStream(testFile1.getAbsolutePath());
    Assert.assertNotNull(is);
    is.close();

    // Close helper
    helper.close();

    // After close, operations should fail
    try {
      helper.getFileStream(testFile1.getAbsolutePath());
    } catch (Exception e) {
      Assert.assertTrue(e instanceof FileBasedHelperException || e instanceof IOException,
        "Should throw appropriate exception after close");
    }
  }



  @Test
  public void testEmptyFile() throws Exception {
    // Create empty file
    File emptyFile = new File(tempDir, "empty.parquet");
    emptyFile.createNewFile();

    helper.connect();

    // Test edge case: empty file should have size 0
    long size = helper.getFileSize(emptyFile.getAbsolutePath());
    Assert.assertEquals(size, 0, "Empty file should have size 0");
  }

  @Test
  public void testCrossSchemeFileAccess() throws Exception {
    helper.connect();

    // Test that getFileSystemForPath correctly handles different schemes
    // Test 1: Local file path (no scheme)
    String localPath = testFile1.getAbsolutePath();
    InputStream localStream = helper.getFileStream(localPath);
    Assert.assertNotNull(localStream, "Should open stream for local file path");
    localStream.close();

    // Test 2: file:// scheme
    String fileScheme = "file://" + testFile1.getAbsolutePath();
    InputStream fileStream = helper.getFileStream(fileScheme);
    Assert.assertNotNull(fileStream, "Should open stream for file:// scheme");
    fileStream.close();
  }

  private void deleteDirectory(File directory) {
    File[] files = directory.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isDirectory()) {
          deleteDirectory(file);
        } else {
          file.delete();
        }
      }
    }
    directory.delete();
  }

}
