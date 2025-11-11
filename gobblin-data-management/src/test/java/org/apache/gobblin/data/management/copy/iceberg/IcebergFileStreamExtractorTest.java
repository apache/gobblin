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
import java.util.Iterator;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.copy.FileAwareInputStream;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;

/**
* Tests for {@link IcebergFileStreamExtractor}.
*/
public class IcebergFileStreamExtractorTest {

  private File tempDir;
  private File testFile;
  private IcebergFileStreamExtractor extractor;
  private WorkUnitState workUnitState;

  @BeforeMethod
  public void setUp() throws Exception {
    // Create temp directory and test file
    tempDir = new File(System.getProperty("java.io.tmpdir"), "iceberg-extractor-test-" + System.currentTimeMillis());
    tempDir.mkdirs();

    testFile = new File(tempDir, "test-data.parquet");
    try (FileOutputStream fos = new FileOutputStream(testFile)) {
      fos.write("Test data content for streaming".getBytes());
    }

    // Set up work unit state
    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL, testFile.getAbsolutePath());
    properties.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, tempDir.getAbsolutePath() + "/final");
    properties.setProperty(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, "file:///");
    properties.setProperty(ConfigurationKeys.WRITER_STAGING_DIR, tempDir.getAbsolutePath() + "/staging");
    properties.setProperty(ConfigurationKeys.WRITER_OUTPUT_DIR, tempDir.getAbsolutePath() + "/output");

    WorkUnit workUnit = WorkUnit.create(
      new Extract(Extract.TableType.SNAPSHOT_ONLY, "test_namespace", "test_table")
    );
    workUnitState = new WorkUnitState(workUnit, new org.apache.gobblin.configuration.State(properties));

    // Initialize extractor
    extractor = new IcebergFileStreamExtractor(workUnitState);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    if (extractor != null) {
      extractor.close();
    }
    // Clean up temp files
    if (tempDir != null && tempDir.exists()) {
      deleteDirectory(tempDir);
    }
  }

  @Test
  public void testGetSchema() throws Exception {
    String schema = extractor.getSchema();
    Assert.assertEquals(schema, "FileAwareInputStream",
        "Schema should be FileAwareInputStream for file streaming mode");
  }

  @Test
  public void testDownloadFile() throws Exception {
    // Test downloading a file
    Iterator<FileAwareInputStream> iterator = extractor.downloadFile(testFile.getAbsolutePath());

    Assert.assertTrue(iterator.hasNext(), "Should return at least one FileAwareInputStream");

    FileAwareInputStream fais = iterator.next();
    Assert.assertNotNull(fais, "FileAwareInputStream should not be null");
    Assert.assertNotNull(fais.getFile(), "CopyableFile should not be null");
    Assert.assertNotNull(fais.getInputStream(), "InputStream should not be null");

    // Verify no more items
    Assert.assertFalse(iterator.hasNext(), "Should only return one FileAwareInputStream per file");
  }

  @Test
  public void testDownloadFileWithCopyableFileMetadata() throws Exception {
    Iterator<FileAwareInputStream> iterator = extractor.downloadFile(testFile.getAbsolutePath());
    FileAwareInputStream fais = iterator.next();

    // Verify CopyableFile has correct metadata
    Assert.assertNotNull(fais.getFile().getOrigin(), "Origin FileStatus should be set");
    Assert.assertNotNull(fais.getFile().getDestination(), "Destination should be set");

    // Verify origin path matches source
    Assert.assertTrue(fais.getFile().getOrigin().getPath().toString().contains(testFile.getName()),
        "Origin path should contain test file name");

    // Verify destination path is under final dir
    String finalDir = workUnitState.getProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR);
    Assert.assertTrue(fais.getFile().getDestination().toString().contains(finalDir),
        "Destination should be under final dir");
    Assert.assertTrue(fais.getFile().getDestination().toString().contains(testFile.getName()),
        "Destination should contain test file name");
  }

  @Test
  public void testDownloadFileStreamIsReadable() throws Exception {
    Iterator<FileAwareInputStream> iterator = extractor.downloadFile(testFile.getAbsolutePath());
    FileAwareInputStream fais = iterator.next();

    // Verify we can read from the stream
    byte[] buffer = new byte[1024];
    int bytesRead = fais.getInputStream().read(buffer);
    Assert.assertTrue(bytesRead > 0, "Should be able to read bytes from stream");

    String content = new String(buffer, 0, bytesRead);
    Assert.assertTrue(content.contains("Test data content"), "Stream content should match file content");
  }

  @Test(expectedExceptions = IOException.class)
  public void testDownloadNonExistentFile() throws Exception {
    // Test error handling for non-existent file
    extractor.downloadFile("/non/existent/path/file.parquet");
  }

  @Test
  public void testDownloadMultipleFiles() throws Exception {
    // Create second test file
    File testFile2 = new File(tempDir, "test-data-2.parquet");
    try (FileOutputStream fos = new FileOutputStream(testFile2)) {
      fos.write("Second test file content".getBytes());
    }

    // Download first file
    Iterator<FileAwareInputStream> iterator1 = extractor.downloadFile(testFile.getAbsolutePath());
    Assert.assertTrue(iterator1.hasNext());
    FileAwareInputStream fais1 = iterator1.next();
    Assert.assertNotNull(fais1);

    // Close first extractor and create new one for second file (single file per extractor)
    extractor.close();
    extractor = new IcebergFileStreamExtractor(workUnitState);

    // Download second file
    Iterator<FileAwareInputStream> iterator2 = extractor.downloadFile(testFile2.getAbsolutePath());
    Assert.assertTrue(iterator2.hasNext());
    FileAwareInputStream fais2 = iterator2.next();
    Assert.assertNotNull(fais2);

    // Verify different files
    Assert.assertNotEquals(fais1.getFile().getOrigin().getPath().getName(),
    fais2.getFile().getOrigin().getPath().getName(),
        "Should process different files");
  }

  @Test
  public void testFileMetadataPreservation() throws Exception {
    Iterator<FileAwareInputStream> iterator = extractor.downloadFile(testFile.getAbsolutePath());
    FileAwareInputStream fais = iterator.next();

    // Verify origin file status is captured
    Assert.assertTrue(fais.getFile().getOrigin().isFile(),
        "Origin should be a file, not directory");
    Assert.assertTrue(fais.getFile().getOrigin().getLen() > 0,
        "Origin file size should be greater than 0");
  }

  @Test
  public void testPartitionAwareDestinationPath() throws Exception {
    // Test that partition path is correctly included in destination
    // Set up partition path mapping in work unit
    String partitionPath = "datepartition=2025-04-01";
    String fileToPartitionJson = String.format("{\"%s\":\"%s\"}", testFile.getAbsolutePath(), partitionPath);

    Properties propsWithPartition = new Properties();
    propsWithPartition.putAll(workUnitState.getProperties());
    propsWithPartition.setProperty(IcebergSource.ICEBERG_FILE_PARTITION_PATH, fileToPartitionJson);

    WorkUnit workUnit = WorkUnit.create(
        new Extract(Extract.TableType.SNAPSHOT_ONLY, "test_namespace", "test_table"));
    WorkUnitState wuStateWithPartition = new WorkUnitState(workUnit,
        new org.apache.gobblin.configuration.State(propsWithPartition));

    // Create new extractor with partition mapping
    IcebergFileStreamExtractor extractorWithPartition = new IcebergFileStreamExtractor(wuStateWithPartition);

    try {
      Iterator<FileAwareInputStream> iterator = extractorWithPartition.downloadFile(testFile.getAbsolutePath());
      FileAwareInputStream fais = iterator.next();

      // Verify destination includes partition path
      String destinationPath = fais.getFile().getDestination().toString();
      Assert.assertTrue(destinationPath.contains(partitionPath),
          "Destination should contain partition path: " + partitionPath);
      Assert.assertTrue(destinationPath.contains(testFile.getName()),
          "Destination should contain file name");

      // Verify path structure: <finalDir>/<partitionPath>/<filename>
      String expectedPathSubstring = partitionPath + "/" + testFile.getName();
      Assert.assertTrue(destinationPath.contains(expectedPathSubstring),
          "Destination should have structure: " + expectedPathSubstring);
    } finally {
      extractorWithPartition.close();
    }
  }

  @Test
  public void testMultiplePartitionAwareFiles() throws Exception {
    // Test multiple files with different partitions
    File testFile2 = new File(tempDir, "test-data-2.parquet");
    try (FileOutputStream fos = new FileOutputStream(testFile2)) {
      fos.write("Second partition data".getBytes());
    }

    // Set up partition mappings for both files
    String partition1 = "datepartition=2025-04-01";
    String partition2 = "datepartition=2025-04-02";
    String fileToPartitionJson = String.format("{\"%s\":\"%s\", \"%s\":\"%s\"}",
        testFile.getAbsolutePath(), partition1,
        testFile2.getAbsolutePath(), partition2);

    Properties propsWithPartitions = new Properties();
    propsWithPartitions.putAll(workUnitState.getProperties());
    propsWithPartitions.setProperty(IcebergSource.ICEBERG_FILE_PARTITION_PATH, fileToPartitionJson);

    WorkUnit workUnit = WorkUnit.create(
        new Extract(Extract.TableType.SNAPSHOT_ONLY, "test_namespace", "test_table"));
    WorkUnitState wuStateWithPartitions = new WorkUnitState(workUnit,
        new org.apache.gobblin.configuration.State(propsWithPartitions));

    IcebergFileStreamExtractor extractorWithPartitions = new IcebergFileStreamExtractor(wuStateWithPartitions);

    try {
      // Process first file
      Iterator<FileAwareInputStream> iterator1 = extractorWithPartitions.downloadFile(testFile.getAbsolutePath());
      FileAwareInputStream fais1 = iterator1.next();
      Assert.assertTrue(fais1.getFile().getDestination().toString().contains(partition1),
      "First file should map to partition1");

      // Close and create new extractor for second file
      extractorWithPartitions.close();
      extractorWithPartitions = new IcebergFileStreamExtractor(wuStateWithPartitions);

      // Process second file
      Iterator<FileAwareInputStream> iterator2 = extractorWithPartitions.downloadFile(testFile2.getAbsolutePath());
      FileAwareInputStream fais2 = iterator2.next();
      Assert.assertTrue(fais2.getFile().getDestination().toString().contains(partition2),
          "Second file should map to partition2");
    } finally {
      extractorWithPartitions.close();
    }
  }

  @Test
  public void testNoPartitionMappingFallback() throws Exception {
    // Test that when no partition mapping is provided, files go directly under finalDir
    // (extractor was already created without partition mapping in setUp)
    Iterator<FileAwareInputStream> iterator = extractor.downloadFile(testFile.getAbsolutePath());
    FileAwareInputStream fais = iterator.next();

    String destinationPath = fais.getFile().getDestination().toString();
    String finalDir = workUnitState.getProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR);

    // Should be: <finalDir>/<filename> (no partition subdirectory)
    String expectedPath = finalDir + "/" + testFile.getName();
    Assert.assertEquals(destinationPath, expectedPath,
        "Without partition mapping, destination should be directly under finalDir");
  }

  @Test(expectedExceptions = IOException.class)
  public void testMalformedPartitionJsonThrowsException() throws Exception {
    // Test that malformed JSON in partition path mapping throws a clear exception
    Properties propsWithMalformedJson = new Properties();
    propsWithMalformedJson.putAll(workUnitState.getProperties());
    propsWithMalformedJson.setProperty(IcebergSource.ICEBERG_FILE_PARTITION_PATH, "{invalid json missing quote");

    WorkUnit workUnit = WorkUnit.create(
        new Extract(Extract.TableType.SNAPSHOT_ONLY, "test_namespace", "test_table")
    );
    WorkUnitState wuStateWithMalformedJson = new WorkUnitState(workUnit,
        new org.apache.gobblin.configuration.State(propsWithMalformedJson));

    // Should throw IOException wrapping JsonSyntaxException with clear error message
    IcebergFileStreamExtractor testExtractor = null;
    try {
      testExtractor = new IcebergFileStreamExtractor(wuStateWithMalformedJson);
      Assert.fail("Should throw IOException for malformed partition JSON");
    } catch (IOException e) {
      // Verify error message is informative
      Assert.assertTrue(e.getMessage().contains("Failed to parse partition path mapping"),
          "Error message should indicate JSON parsing failure");
      Assert.assertTrue(e.getMessage().contains("invalid json missing quote"),
          "Error message should include the malformed JSON snippet");
      Assert.assertTrue(e.getCause() instanceof com.google.gson.JsonSyntaxException,
          "Root cause should be JsonSyntaxException");
      throw e; // Re-throw for @Test(expectedExceptions)
    } finally {
      if (testExtractor != null) {
        testExtractor.close();
      }
    }
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
