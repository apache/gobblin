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
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.io.Files;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.data.management.copy.CopySource;
import org.apache.gobblin.data.management.copy.entities.PrePublishStep;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;

/**
 * Unit tests for the delete step functionality in {@link IcebergSource}.
 * Tests the addDeleteStepIfNeeded method with various configurations.
 *
 * Note: The delete feature performs complete directory deletion (not individual file deletion).
 * When enabled, entire partition directories are deleted for a clean overwrite, regardless of
 * which files exist in the source.
 */
public class IcebergSourceDeleteStepTest {

  private File tempDir;
  private FileSystem localFs;
  private SourceState state;
  private IcebergSource source;

  @BeforeMethod
  public void setUp() throws Exception {
    // Create temporary directory for testing
    tempDir = Files.createTempDir();
    tempDir.deleteOnExit();

    // Get local filesystem
    Configuration conf = new Configuration();
    localFs = FileSystem.getLocal(conf);

    // Initialize source state
    state = new SourceState();
    state.setProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, "file:///");
    state.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, tempDir.getAbsolutePath());

    // Initialize IcebergSource
    source = new IcebergSource();
  }

  @AfterMethod
  public void tearDown() throws Exception {
    if (tempDir != null && tempDir.exists()) {
      deleteDirectory(tempDir);
    }
  }

  /**
   * Test: Delete is disabled - should not create delete work unit
   */
  @Test
  public void testDeleteDisabled() throws Exception {
    // Configure: delete disabled
    state.setProp(IcebergSource.DELETE_FILES_NOT_IN_SOURCE, false);

    List<WorkUnit> workUnits = Lists.newArrayList();
    Extract extract = new Extract(Extract.TableType.SNAPSHOT_ONLY, "test", "test");

    // Invoke private method using reflection
    invokeAddDeleteStepIfNeeded(workUnits, extract);

    // Assert: No delete work unit should be added
    Assert.assertEquals(workUnits.size(), 0, "No work unit should be added when delete is disabled");
  }

  /**
   * Test: Delete enabled but no work units - should not create delete work unit
   */
  @Test
  public void testDeleteEnabledButNoWorkUnits() throws Exception {
    // Configure: delete enabled
    state.setProp(IcebergSource.DELETE_FILES_NOT_IN_SOURCE, true);

    List<WorkUnit> workUnits = Lists.newArrayList(); // Empty work units
    Extract extract = new Extract(Extract.TableType.SNAPSHOT_ONLY, "test", "test");

    // Invoke private method using reflection
    invokeAddDeleteStepIfNeeded(workUnits, extract);

    // Assert: No delete work unit should be added
    Assert.assertEquals(workUnits.size(), 0, "No work unit should be added when there are no existing work units");
  }

  /**
   * Test: Delete enabled, filter disabled - should delete entire root directory
   */
  @Test
  public void testDeleteEntireRootDirectory() throws Exception {
    // Create test files in root directory
    createTestFile("file1.parquet");
    createTestFile("partition1/file2.parquet");
    createTestFile("partition2/file3.parquet");

    // Configure: delete enabled, filter disabled
    state.setProp(IcebergSource.DELETE_FILES_NOT_IN_SOURCE, true);
    state.setProp(IcebergSource.ICEBERG_FILTER_ENABLED, false);

    List<WorkUnit> workUnits = Lists.newArrayList();
    workUnits.add(createDummyWorkUnit()); // Add at least one work unit
    Extract extract = new Extract(Extract.TableType.SNAPSHOT_ONLY, "test", "test");

    // Invoke private method using reflection
    invokeAddDeleteStepIfNeeded(workUnits, extract);

    // Assert: One delete work unit should be added
    Assert.assertEquals(workUnits.size(), 2, "One delete work unit should be added");

    // Verify the delete work unit
    WorkUnit deleteWorkUnit = workUnits.get(1);
    Assert.assertEquals(deleteWorkUnit.getProp(ConfigurationKeys.DATASET_URN_KEY), "datasetUrn");
    Assert.assertEquals(deleteWorkUnit.getProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL), "");

    // Verify it's a PrePublishStep
    Class<?> entityClass = CopySource.getCopyEntityClass(deleteWorkUnit);
    Assert.assertEquals(entityClass, PrePublishStep.class);

    // Verify entire root directory is being deleted (not individual files)
    List<String> directoriesToDelete = getFilesToDeleteFromWorkUnit(deleteWorkUnit);
    Assert.assertEquals(directoriesToDelete.size(), 1, "Should delete 1 directory (root directory)");

    // Verify the root directory path is in the delete list
    assertContainsPartition(directoriesToDelete, tempDir.getAbsolutePath());
  }

  /**
   * Test: Delete enabled, filter enabled with single partition - should delete specific partition directory
   */
  @Test
  public void testDeleteSinglePartitionDirectory() throws Exception {
    // Create test files in partition directories
    createTestFile("datepartition=2025-10-11/file1.parquet");
    createTestFile("datepartition=2025-10-11/file2.parquet");
    createTestFile("datepartition=2025-10-10/file3.parquet");
    createTestFile("datepartition=2025-10-09/file4.parquet");

    // Configure: delete enabled, filter enabled for single partition
    state.setProp(IcebergSource.DELETE_FILES_NOT_IN_SOURCE, true);
    state.setProp(IcebergSource.ICEBERG_FILTER_ENABLED, true);
    state.setProp(IcebergSource.ICEBERG_PARTITION_KEY, "datepartition");
    state.setProp(IcebergSource.ICEBERG_PARTITION_VALUES, "2025-10-11");

    List<WorkUnit> workUnits = Lists.newArrayList();
    workUnits.add(createDummyWorkUnit());
    Extract extract = new Extract(Extract.TableType.SNAPSHOT_ONLY, "test", "test");

    // Invoke private method using reflection
    invokeAddDeleteStepIfNeeded(workUnits, extract);

    // Assert: One delete work unit should be added
    Assert.assertEquals(workUnits.size(), 2, "One delete work unit should be added");

    // Verify the delete work unit
    WorkUnit deleteWorkUnit = workUnits.get(1);
    Assert.assertEquals(deleteWorkUnit.getProp(ConfigurationKeys.DATASET_URN_KEY), "datasetUrn");

    // Verify correct partition directory is being deleted
    List<String> directoriesToDelete = getFilesToDeleteFromWorkUnit(deleteWorkUnit);
    Assert.assertEquals(directoriesToDelete.size(), 1, "Should delete 1 partition directory");

    // Verify only 2025-10-11 partition directory is included
    assertContainsPartition(directoriesToDelete, "datepartition=2025-10-11");

    // Verify other partition directories are NOT included
    assertNotContainsPartition(directoriesToDelete, "datepartition=2025-10-10");
    assertNotContainsPartition(directoriesToDelete, "datepartition=2025-10-09");
  }

  /**
   * Test: Delete enabled, filter enabled with multiple partitions (lookback) - should delete multiple partition directories
   */
  @Test
  public void testDeleteMultiplePartitionDirectories() throws Exception {
    // Create test files in partition directories
    createTestFile("datepartition=2025-10-11/file1.parquet");
    createTestFile("datepartition=2025-10-11/file2.parquet");
    createTestFile("datepartition=2025-10-10/file3.parquet");
    createTestFile("datepartition=2025-10-09/file4.parquet");
    createTestFile("datepartition=2025-10-08/file5.parquet"); // Should not be deleted

    // Configure: delete enabled, filter enabled with lookback (3 days)
    state.setProp(IcebergSource.DELETE_FILES_NOT_IN_SOURCE, true);
    state.setProp(IcebergSource.ICEBERG_FILTER_ENABLED, true);
    state.setProp(IcebergSource.ICEBERG_PARTITION_KEY, "datepartition");
    state.setProp(IcebergSource.ICEBERG_PARTITION_VALUES, "2025-10-11,2025-10-10,2025-10-09");

    List<WorkUnit> workUnits = Lists.newArrayList();
    workUnits.add(createDummyWorkUnit());
    Extract extract = new Extract(Extract.TableType.SNAPSHOT_ONLY, "test", "test");

    // Invoke private method using reflection
    invokeAddDeleteStepIfNeeded(workUnits, extract);

    // Assert: One delete work unit should be added
    Assert.assertEquals(workUnits.size(), 2, "One delete work unit should be added");

    // Verify the delete work unit contains correct partition directories
    WorkUnit deleteWorkUnit = workUnits.get(1);
    List<String> directoriesToDelete = getFilesToDeleteFromWorkUnit(deleteWorkUnit);

    // We expect 3 partition directories to be deleted
    Assert.assertEquals(directoriesToDelete.size(), 3, "Should delete 3 partition directories");

    // Verify correct partition directories are included
    assertContainsPartition(directoriesToDelete, "datepartition=2025-10-11");
    assertContainsPartition(directoriesToDelete, "datepartition=2025-10-10");
    assertContainsPartition(directoriesToDelete, "datepartition=2025-10-09");

    // Verify 2025-10-08 partition directory is NOT included
    assertNotContainsPartition(directoriesToDelete, "datepartition=2025-10-08");
  }

  /**
   * Test: Delete enabled with hourly partitions - should handle hourly suffix correctly
   */
  @Test
  public void testDeleteHourlyPartitions() throws Exception {
    // Create test files in hourly partition directories
    createTestFile("datepartition=2025-10-11-00/file1.parquet");
    createTestFile("datepartition=2025-10-11-00/file2.parquet");
    createTestFile("datepartition=2025-10-10-00/file3.parquet");

    // Configure: delete enabled with hourly partitions
    state.setProp(IcebergSource.DELETE_FILES_NOT_IN_SOURCE, true);
    state.setProp(IcebergSource.ICEBERG_FILTER_ENABLED, true);
    state.setProp(IcebergSource.ICEBERG_PARTITION_KEY, "datepartition");
    state.setProp(IcebergSource.ICEBERG_PARTITION_VALUES, "2025-10-11-00,2025-10-10-00");
    state.setProp(IcebergSource.ICEBERG_HOURLY_PARTITION_ENABLED, true);

    List<WorkUnit> workUnits = Lists.newArrayList();
    workUnits.add(createDummyWorkUnit());
    Extract extract = new Extract(Extract.TableType.SNAPSHOT_ONLY, "test", "test");

    // Invoke private method using reflection
    invokeAddDeleteStepIfNeeded(workUnits, extract);

    // Assert: One delete work unit should be added
    Assert.assertEquals(workUnits.size(), 2, "One delete work unit should be added for hourly partitions");

    // Verify correct hourly partition directories are being deleted
    WorkUnit deleteWorkUnit = workUnits.get(1);
    List<String> directoriesToDelete = getFilesToDeleteFromWorkUnit(deleteWorkUnit);
    Assert.assertEquals(directoriesToDelete.size(), 2, "Should delete 2 hourly partition directories");

    // Verify correct hourly partition directories are included
    assertContainsPartition(directoriesToDelete, "datepartition=2025-10-11-00");
    assertContainsPartition(directoriesToDelete, "datepartition=2025-10-10-00");
  }

  /**
   * Test: Delete enabled but target directory doesn't exist - should not create delete work unit
   */
  @Test
  public void testDeleteTargetDirectoryNotExists() throws Exception {
    // Configure: delete enabled but point to non-existent directory
    state.setProp(IcebergSource.DELETE_FILES_NOT_IN_SOURCE, true);
    state.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/non/existent/path");

    List<WorkUnit> workUnits = Lists.newArrayList();
    workUnits.add(createDummyWorkUnit());
    Extract extract = new Extract(Extract.TableType.SNAPSHOT_ONLY, "test", "test");

    // Invoke private method using reflection
    invokeAddDeleteStepIfNeeded(workUnits, extract);

    // Assert: No delete work unit should be added (only the dummy work unit remains)
    Assert.assertEquals(workUnits.size(), 1, "No delete work unit should be added when target doesn't exist");
  }

  /**
   * Test: Delete enabled but partition directory doesn't exist - should handle gracefully
   */
  @Test
  public void testDeletePartitionDirectoryNotExists() throws Exception {
    // Don't create any files - partition directories don't exist

    // Configure: delete enabled, filter enabled
    state.setProp(IcebergSource.DELETE_FILES_NOT_IN_SOURCE, true);
    state.setProp(IcebergSource.ICEBERG_FILTER_ENABLED, true);
    state.setProp(IcebergSource.ICEBERG_PARTITION_KEY, "datepartition");
    state.setProp(IcebergSource.ICEBERG_PARTITION_VALUES, "2025-10-11");

    List<WorkUnit> workUnits = Lists.newArrayList();
    workUnits.add(createDummyWorkUnit());
    Extract extract = new Extract(Extract.TableType.SNAPSHOT_ONLY, "test", "test");

    // Invoke private method using reflection
    invokeAddDeleteStepIfNeeded(workUnits, extract);

    // Assert: No delete work unit should be added when no files exist to delete
    Assert.assertEquals(workUnits.size(), 1, "No delete work unit should be added when partition directory doesn't exist");
  }

  /**
   * Test: Missing partition key in state - should handle gracefully
   */
  @Test
  public void testDeleteMissingPartitionKey() throws Exception {
    // Configure: delete enabled but missing partition key
    state.setProp(IcebergSource.DELETE_FILES_NOT_IN_SOURCE, true);
    state.setProp(IcebergSource.ICEBERG_FILTER_ENABLED, true);
    // Missing: ICEBERG_PARTITION_KEY and ICEBERG_PARTITION_VALUES

    List<WorkUnit> workUnits = Lists.newArrayList();
    workUnits.add(createDummyWorkUnit());
    Extract extract = new Extract(Extract.TableType.SNAPSHOT_ONLY, "test", "test");

    // Invoke private method using reflection
    invokeAddDeleteStepIfNeeded(workUnits, extract);

    // Assert: No delete work unit should be added when partition info is missing
    Assert.assertEquals(workUnits.size(), 1, "No delete work unit should be added when partition info is missing");
  }

  /**
   * Test: Missing DATA_PUBLISHER_FINAL_DIR - should handle gracefully and not create delete work unit
   */
  @Test
  public void testDeleteMissingDataPublisherFinalDir() throws Exception {
    // Configure: delete enabled but DATA_PUBLISHER_FINAL_DIR is not set
    state.setProp(IcebergSource.DELETE_FILES_NOT_IN_SOURCE, true);
    state.setProp(IcebergSource.ICEBERG_FILTER_ENABLED, true);
    state.setProp(IcebergSource.ICEBERG_PARTITION_KEY, "datepartition");
    state.setProp(IcebergSource.ICEBERG_PARTITION_VALUES, "2025-10-11");
    // Remove DATA_PUBLISHER_FINAL_DIR
    state.removeProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR);

    List<WorkUnit> workUnits = Lists.newArrayList();
    workUnits.add(createDummyWorkUnit());
    Extract extract = new Extract(Extract.TableType.SNAPSHOT_ONLY, "test", "test");

    // Invoke private method using reflection
    invokeAddDeleteStepIfNeeded(workUnits, extract);

    // Assert: No delete work unit should be added when DATA_PUBLISHER_FINAL_DIR is missing
    Assert.assertEquals(workUnits.size(), 1,
        "No delete work unit should be added when DATA_PUBLISHER_FINAL_DIR is not configured");
  }

  // ==================== Helper Methods ====================

  /**
   * Invoke the private addDeleteStepIfNeeded method using reflection
   */
  private void invokeAddDeleteStepIfNeeded(List<WorkUnit> workUnits, Extract extract) throws Exception {
    Method method = IcebergSource.class.getDeclaredMethod("addDeleteStepIfNeeded",
        SourceState.class,
        List.class,
        Extract.class,
        String.class);
    method.setAccessible(true);
    method.invoke(source, state, workUnits, extract, "datasetUrn");
  }

  /**
   * Create a test file in the temp directory
   */
  private void createTestFile(String relativePath) throws IOException {
    File file = new File(tempDir, relativePath);
    file.getParentFile().mkdirs();
    file.createNewFile();
  }

  /**
   * Create a dummy work unit for testing
   */
  private WorkUnit createDummyWorkUnit() {
    Extract extract = new Extract(Extract.TableType.SNAPSHOT_ONLY, "test", "test");
    WorkUnit workUnit = new WorkUnit(extract);
    workUnit.setProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL, "dummy.parquet");
    return workUnit;
  }

  /**
   * Extract file paths from DeleteFileCommitStep using reflection
   */
  private List<String> getFilesToDeleteFromWorkUnit(WorkUnit deleteWorkUnit) throws Exception {
    PrePublishStep prePublishStep = (PrePublishStep) CopySource.deserializeCopyEntity(deleteWorkUnit);
    Object deleteStep = prePublishStep.getStep();

    // Use reflection to access pathsToDelete field from DeleteFileCommitStep
    java.lang.reflect.Field field = deleteStep.getClass().getDeclaredField("pathsToDelete");
    field.setAccessible(true);

    @SuppressWarnings("unchecked")
    java.util.Collection<org.apache.hadoop.fs.FileStatus> pathsToDelete =
        (java.util.Collection<org.apache.hadoop.fs.FileStatus>) field.get(deleteStep);

    List<String> filePaths = Lists.newArrayList();
    for (org.apache.hadoop.fs.FileStatus status : pathsToDelete) {
      filePaths.add(status.getPath().toString());
    }
    return filePaths;
  }

  /**
   * Verify that a file path contains the expected partition directory
   */
  private void assertContainsPartition(List<String> filePaths, String partitionDir) {
    boolean found = false;
    for (String path : filePaths) {
      if (path.contains(partitionDir)) {
        found = true;
        break;
      }
    }
    Assert.assertTrue(found, "Expected to find partition directory: " + partitionDir + " in paths: " + filePaths);
  }

  /**
   * Verify that a file path does NOT contain the expected partition directory
   */
  private void assertNotContainsPartition(List<String> filePaths, String partitionDir) {
    for (String path : filePaths) {
      Assert.assertFalse(path.contains(partitionDir),
          "Should NOT find partition directory: " + partitionDir + " but found in: " + path);
    }
  }

  /**
   * Recursively delete a directory
   */
  private void deleteDirectory(File directory) {
    if (directory.isDirectory()) {
      File[] files = directory.listFiles();
      if (files != null) {
        for (File file : files) {
          deleteDirectory(file);
        }
      }
    }
    directory.delete();
  }
}
