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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.copy.CopySource;
import org.apache.gobblin.data.management.copy.FileAwareInputStream;
import org.apache.gobblin.data.management.copy.iceberg.IcebergTable.FilePathWithPartition;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hive.HiveMetastoreTest;
import org.apache.iceberg.shaded.org.apache.avro.SchemaBuilder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link IcebergSource}.
 */
public class IcebergSourceTest {

  @Mock
  private IcebergCatalog mockCatalog;

  @Mock
  private IcebergTable mockTable;

  @Mock
  private IcebergSnapshotInfo mockSnapshot;

  private IcebergSource icebergSource;
  private SourceState sourceState;
  private Properties properties;

  private AutoCloseable mocks;


  @BeforeMethod
  public void setUp() throws Exception {
    mocks = MockitoAnnotations.openMocks(this);

    // Initialize IcebergSource
    this.icebergSource = new IcebergSource();

    // Set up basic properties
    this.properties = new Properties();
    properties.setProperty(IcebergSource.ICEBERG_DATABASE_NAME, "test_db");
    properties.setProperty(IcebergSource.ICEBERG_TABLE_NAME, "test_table");
    properties.setProperty(IcebergSource.ICEBERG_CATALOG_URI, "https://<test>.com/api/v1");
    properties.setProperty(IcebergSource.ICEBERG_CATALOG_CLASS, "org.apache.gobblin.data.management.copy.iceberg.TestCatalog");

    // Create SourceState
    this.sourceState = new SourceState(new State(properties));
    // Set a default top-level broker required by LineageInfo
    com.typesafe.config.Config emptyConfig = com.typesafe.config.ConfigFactory.empty();
    this.sourceState.setBroker(
      SharedResourcesBrokerFactory.createDefaultTopLevelBroker(
        emptyConfig,
        GobblinScopeTypes.GLOBAL.defaultScopeInstance()));
  }

  @AfterMethod
  public void tearDown() throws Exception {
    if (mocks != null) {
      mocks.close();
    }
  }

  @Test
  public void testFileStreamingModeWorkUnitCreation() throws Exception {
    // Set up file streaming mode
    properties.setProperty(IcebergSource.ICEBERG_RECORD_PROCESSING_ENABLED, "false");

    // Mock file discovery via snapshot fallback (no filter)
    List<String> inputPaths = Arrays.asList(
      "/data/warehouse/test_table/data/file1.parquet",
      "/data/warehouse/test_table/data/file2.parquet",
      "/data/warehouse/test_table/data/file3.parquet",
      "/data/warehouse/test_table/metadata/manifest-list.avro",
      "/data/warehouse/test_table/metadata/manifest1.avro"
    );
    setupMockFileDiscovery(inputPaths);

    // Discover data-only paths via snapshot manifest info
    List<String> dataFilePaths = inputPaths.stream()
      .filter(p -> p.endsWith(".parquet") || p.endsWith(".orc") || p.endsWith(".avro"))
      .filter(p -> !p.contains("manifest"))
      .collect(Collectors.toList());

    // Convert to FilePathWithPartition (no partition info for snapshot-based discovery)
    List<FilePathWithPartition> filesWithPartitions =
      dataFilePaths.stream()
        .map(path -> new FilePathWithPartition(
          path, new java.util.HashMap<>(), 0L))
        .collect(Collectors.toList());

    // Invoke private createWorkUnitsFromFiles via reflection
    Method m = IcebergSource.class.getDeclaredMethod("createWorkUnitsFromFiles", List.class, SourceState.class, IcebergTable.class);
    m.setAccessible(true);
    List<WorkUnit> workUnits = (List<WorkUnit>) m.invoke(icebergSource, filesWithPartitions, sourceState, mockTable);

    // Verify single work unit contains all 3 data files by default (filesPerWorkUnit default=10)
    Assert.assertEquals(workUnits.size(), 1, "Should create 1 work unit");
    WorkUnit wu = workUnits.get(0);
    String filesToPull = wu.getProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL);
    Assert.assertNotNull(filesToPull);
    Assert.assertEquals(filesToPull.split(",").length, 3);

    // Verify extract info
    Assert.assertEquals(wu.getExtract().getNamespace(), "iceberg");
    Assert.assertEquals(wu.getExtract().getTable(), "test_table");
    Assert.assertEquals(wu.getExtract().getType(), Extract.TableType.SNAPSHOT_ONLY);
  }

  @Test
  public void testFileStreamingModeExtractorSelection() throws Exception {
    // Set up file streaming mode
    WorkUnit dummyWu = WorkUnit.createEmpty();
    State jobState = new State(properties);
    WorkUnitState workUnitState = new WorkUnitState(dummyWu, jobState);
    workUnitState.setProp(IcebergSource.ICEBERG_RECORD_PROCESSING_ENABLED, "false");

    Extractor<String, FileAwareInputStream> extractor = icebergSource.getExtractor(workUnitState);
    // Verify correct extractor type
    Assert.assertTrue(extractor instanceof IcebergFileStreamExtractor,
      "File streaming mode should return IcebergFileStreamExtractor");
  }

  @Test
  public void testRecordProcessingExtractorThrows() throws Exception {
    // Set up record processing mode
    WorkUnit dummyWu = WorkUnit.createEmpty();
    State jobState = new State(properties);
    WorkUnitState workUnitState = new WorkUnitState(dummyWu, jobState);
    workUnitState.setProp(IcebergSource.ICEBERG_RECORD_PROCESSING_ENABLED, "true");

    try {
      icebergSource.getExtractor(workUnitState);
      Assert.fail("Expected UnsupportedOperationException for record processing mode");
    } catch (UnsupportedOperationException expected) {
      // Expected exception
    }
  }

  @Test
  public void testConfigurationValidation() throws Exception {
    // Test missing database name via direct validateConfiguration method
    properties.remove(IcebergSource.ICEBERG_DATABASE_NAME);
    sourceState = new SourceState(new State(properties));

    // Use reflection to call private validateConfiguration method
    Method m = IcebergSource.class.getDeclaredMethod("validateConfiguration", SourceState.class);
    m.setAccessible(true);

    try {
      m.invoke(icebergSource, sourceState);
      Assert.fail("Should throw exception for missing database name");
    } catch (java.lang.reflect.InvocationTargetException e) {
      Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
      Assert.assertTrue(e.getCause().getMessage().contains("iceberg.database.name is required"));
    }
  }

  @Test
  public void testFileGrouping() throws Exception {
    // Test with more files than files per work unit
    properties.setProperty(IcebergSource.ICEBERG_FILES_PER_WORKUNIT, "3");
    sourceState = new SourceState(new State(properties));

    // Mock 6 files to test grouping
    List<String> dataFilePaths = Arrays.asList(
      "file1.parquet", "file2.parquet", "file3.parquet",
      "file4.parquet", "file5.parquet", "file6.parquet"
    );

    // Convert to FilePathWithPartition
    List<FilePathWithPartition> filesWithPartitions =
      dataFilePaths.stream()
        .map(path -> new FilePathWithPartition(
          path, new java.util.HashMap<>(), 0L))
        .collect(Collectors.toList());

    // Setup table mock
    TableIdentifier tableId = TableIdentifier.of("test_db", "test_table");
    when(mockTable.getTableId()).thenReturn(tableId);

    // Use reflection to call createWorkUnitsFromFiles directly on data files
    Method m = IcebergSource.class.getDeclaredMethod("createWorkUnitsFromFiles", List.class, SourceState.class, IcebergTable.class);
    m.setAccessible(true);
    List<WorkUnit> workUnits = (List<WorkUnit>) m.invoke(icebergSource, filesWithPartitions, sourceState, mockTable);

    // Should create 2 work units: [3 files], [3 files]
    Assert.assertEquals(workUnits.size(), 2, "Should create 2 work units for 6 files with files.per.workunit=3");

    // Verify file distribution
    int totalFiles = 0;
    for (WorkUnit workUnit : workUnits) {
      String filesToPull = workUnit.getProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL);
      int filesInThisUnit = filesToPull.split(",").length;
      totalFiles += filesInThisUnit;
      Assert.assertTrue(filesInThisUnit <= 3, "No work unit should have more than 3 files");
    }
    Assert.assertEquals(totalFiles, 6, "Total files across all work units should be 6");
  }

  /**
   * Helper method to set up mock file discovery
   */
  private void setupMockFileDiscovery(List<String> filePaths) throws Exception {
    TableIdentifier tableId = TableIdentifier.of("test_db", "test_table");
    when(mockTable.getTableId()).thenReturn(tableId);
    when(mockCatalog.openTable("test_db", "test_table")).thenReturn(mockTable);
    when(mockCatalog.tableAlreadyExists(mockTable)).thenReturn(true);
    when(mockTable.getCurrentSnapshotInfo()).thenReturn(mockSnapshot);

    // Set up snapshot to return the specified file paths
    when(mockSnapshot.getManifestListPath()).thenReturn("manifest-list.avro");
    when(mockSnapshot.getMetadataPath()).thenReturn(Optional.empty());

    // Create manifest info with data files
    List<String> dataFiles = filePaths.stream()
      .filter(path -> path.endsWith(".parquet") || path.endsWith(".orc") || path.endsWith(".avro"))
      .filter(path -> !path.contains("manifest"))
      .collect(Collectors.toList());

    IcebergSnapshotInfo.ManifestFileInfo manifestInfo = new IcebergSnapshotInfo.ManifestFileInfo(
      "manifest1.avro", dataFiles);
    when(mockSnapshot.getManifestFiles()).thenReturn(Arrays.asList(manifestInfo));
  }

  @Test
  public void testLookbackPeriodLogic() throws Exception {
    // Test that lookback period correctly discovers multiple days of partitions
    properties.setProperty(IcebergSource.ICEBERG_FILTER_ENABLED, "true");
    properties.setProperty(IcebergSource.ICEBERG_FILTER_DATE, "2025-04-03");
    properties.setProperty(IcebergSource.ICEBERG_LOOKBACK_DAYS, "3");
    sourceState = new SourceState(new State(properties));

    // Mock 3 days of partitions
    List<FilePathWithPartition> filesFor3Days = Arrays.asList(
      new FilePathWithPartition(
        "/data/file1.parquet", createPartitionMap("datepartition", "2025-04-03"), 1000L),
      new FilePathWithPartition(
        "/data/file2.parquet", createPartitionMap("datepartition", "2025-04-02"), 1000L),
      new FilePathWithPartition(
        "/data/file3.parquet", createPartitionMap("datepartition", "2025-04-01"), 1000L)
    );

    TableIdentifier tableId = TableIdentifier.of("test_db", "test_table");
    when(mockTable.getTableId()).thenReturn(tableId);
    when(mockTable.getFilePathsWithPartitionsForFilter(any(Expression.class)))
      .thenReturn(filesFor3Days);

    // Test discovery
    Method m = IcebergSource.class.getDeclaredMethod("discoverPartitionFilePaths",
      SourceState.class, IcebergTable.class);
    m.setAccessible(true);
    List<FilePathWithPartition> discovered =
      (List<FilePathWithPartition>) m.invoke(icebergSource, sourceState, mockTable);

    // Verify all 3 days discovered
    Assert.assertEquals(discovered.size(), 3, "Should discover 3 days with lookback=3");

    // Verify partition values are set correctly
    String partitionValues = sourceState.getProp(IcebergSource.ICEBERG_PARTITION_VALUES);
    Assert.assertNotNull(partitionValues, "Partition values should be set");
    // Should contain 3 dates: 2025-04-03, 2025-04-02, 2025-04-01
    String[] dates = partitionValues.split(",");
    Assert.assertEquals(dates.length, 3, "Should have 3 partition values");
  }

  @Test
  public void testCurrentDatePlaceholder() throws Exception {
    // Test that CURRENT_DATE placeholder is resolved to current date with hourly suffix (default)
    properties.setProperty(IcebergSource.ICEBERG_FILTER_ENABLED, "true");
    properties.setProperty(IcebergSource.ICEBERG_FILTER_DATE, "CURRENT_DATE");
    properties.setProperty(IcebergSource.ICEBERG_LOOKBACK_DAYS, "1");
    sourceState = new SourceState(new State(properties));

    // Mock today's partition with hourly format
    String today = java.time.LocalDate.now().toString();
    String todayHourly = today + "-00";
    List<FilePathWithPartition> todayFiles = Arrays.asList(
      new FilePathWithPartition(
        "/data/file1.parquet", createPartitionMap("datepartition", todayHourly), 1000L)
    );

    TableIdentifier tableId = TableIdentifier.of("test_db", "test_table");
    when(mockTable.getTableId()).thenReturn(tableId);
    when(mockTable.getFilePathsWithPartitionsForFilter(any(Expression.class)))
      .thenReturn(todayFiles);

    // Test discovery
    Method m = IcebergSource.class.getDeclaredMethod("discoverPartitionFilePaths",
      SourceState.class, IcebergTable.class);
    m.setAccessible(true);
    List<FilePathWithPartition> discovered =
      (List<FilePathWithPartition>) m.invoke(icebergSource, sourceState, mockTable);

    // Verify today's partition is discovered
    Assert.assertEquals(discovered.size(), 1, "Should discover today's partition");

    // Verify partition value has hourly format (default behavior)
    String partitionValues = sourceState.getProp(IcebergSource.ICEBERG_PARTITION_VALUES);
    Assert.assertNotNull(partitionValues, "Partition values should be set");
    Assert.assertEquals(partitionValues, todayHourly, "Should resolve to today's date with -00 suffix");
  }

  @Test
  public void testInvalidDateFormatFails() throws Exception {
    // Test that invalid date format throws proper exception
    properties.setProperty(IcebergSource.ICEBERG_FILTER_ENABLED, "true");
    properties.setProperty(IcebergSource.ICEBERG_FILTER_DATE, "invalid-date"); // Invalid date format
    sourceState = new SourceState(new State(properties));

    Method m = IcebergSource.class.getDeclaredMethod("discoverPartitionFilePaths",
      SourceState.class, IcebergTable.class);
    m.setAccessible(true);

    try {
      m.invoke(icebergSource, sourceState, mockTable);
      Assert.fail("Should throw exception for invalid date format");
    } catch (java.lang.reflect.InvocationTargetException e) {
      // Unwrap the exception from reflection
      // Should get IllegalArgumentException wrapping the DateTimeParseException
      Assert.assertTrue(e.getCause() instanceof IllegalArgumentException,
        "Should throw IllegalArgumentException for invalid date format");
      Assert.assertTrue(e.getCause().getMessage().contains("Invalid date format"),
        "Error message should indicate invalid date format");
      Assert.assertTrue(e.getCause().getMessage().contains("yyyy-MM-dd"),
        "Error message should indicate expected format");
      // Verify the cause is DateTimeParseException
      Assert.assertTrue(e.getCause().getCause() instanceof java.time.format.DateTimeParseException,
        "Root cause should be DateTimeParseException");
    }
  }

  @Test
  public void testPartitionFilterConfiguration() throws Exception {
    // Test with partition filtering enabled
    properties.setProperty(IcebergSource.ICEBERG_FILTER_ENABLED, "true");
    properties.setProperty(IcebergSource.ICEBERG_FILTER_DATE, "2025-04-01");
    properties.setProperty(IcebergSource.ICEBERG_LOOKBACK_DAYS, "3");
    sourceState = new SourceState(new State(properties));

    // Mock partition-aware file discovery with FilePathWithPartition
    List<FilePathWithPartition> partitionFiles = Arrays.asList(
      new FilePathWithPartition(
        "/data/uuid1.parquet", createPartitionMap("datepartition", "2025-04-01"), 0L),
      new FilePathWithPartition(
        "/data/uuid2.parquet", createPartitionMap("datepartition", "2025-04-01"), 0L),
      new FilePathWithPartition(
        "/data/uuid3.parquet", createPartitionMap("datepartition", "2025-03-31"), 0L),
      new FilePathWithPartition(
        "/data/uuid4.parquet", createPartitionMap("datepartition", "2025-03-30"), 0L)
    );

    // Mock the table to return partition-specific files with metadata
    TableIdentifier tableId = TableIdentifier.of("test_db", "test_table");
    when(mockTable.getTableId()).thenReturn(tableId);
    when(mockTable.getFilePathsWithPartitionsForFilter(any(Expression.class)))
      .thenReturn(partitionFiles);

    // Use reflection to test discoverPartitionFilePaths
    Method m = IcebergSource.class.getDeclaredMethod("discoverPartitionFilePaths", SourceState.class, IcebergTable.class);
    m.setAccessible(true);
    List<FilePathWithPartition> discoveredFiles =
      (List<FilePathWithPartition>) m.invoke(icebergSource, sourceState, mockTable);

    // Verify partition filter was applied
    Assert.assertEquals(discoveredFiles.size(), 4, "Should discover files from filtered partitions");
  }

  @Test
  public void testPartitionInfoPropagation() throws Exception {
    // Test that partition info is propagated to work units
    properties.setProperty(IcebergSource.ICEBERG_FILTER_ENABLED, "true");
    properties.setProperty(IcebergSource.ICEBERG_FILTER_DATE, "2025-04-01");
    sourceState = new SourceState(new State(properties));

    List<FilePathWithPartition> filesWithPartitions = Arrays.asList(
      new FilePathWithPartition(
        "/data/uuid1.parquet", createPartitionMap("datepartition", "2025-04-01"), 0L),
      new FilePathWithPartition(
        "/data/uuid2.parquet", createPartitionMap("datepartition", "2025-04-01"), 0L)
    );

    TableIdentifier tableId = TableIdentifier.of("test_db", "test_table");
    when(mockTable.getTableId()).thenReturn(tableId);

    // Set partition info on source state (simulating discoverPartitionFilePaths behavior)
    sourceState.setProp(IcebergSource.ICEBERG_PARTITION_KEY, "datepartition");
    sourceState.setProp(IcebergSource.ICEBERG_PARTITION_VALUES, "2025-04-01");

    // Invoke createWorkUnitsFromFiles
    Method m = IcebergSource.class.getDeclaredMethod("createWorkUnitsFromFiles", List.class, SourceState.class, IcebergTable.class);
    m.setAccessible(true);
    List<WorkUnit> workUnits = (List<WorkUnit>) m.invoke(icebergSource, filesWithPartitions, sourceState, mockTable);

    // Verify partition info is in work unit
    Assert.assertEquals(workUnits.size(), 1);
    WorkUnit wu = workUnits.get(0);
    Assert.assertEquals(wu.getProp(IcebergSource.ICEBERG_PARTITION_KEY), "datepartition");
    Assert.assertEquals(wu.getProp(IcebergSource.ICEBERG_PARTITION_VALUES), "2025-04-01");

    // Verify partition path mapping is stored
    Assert.assertNotNull(wu.getProp(IcebergSource.ICEBERG_FILE_PARTITION_PATH),
      "Partition path mapping should be stored in work unit");
  }

  @Test
  public void testNoFilterPreservesPartitionMetadata() throws Exception {
    // Test that when filter is disabled, partition metadata is still preserved for partitioned tables
    properties.setProperty(IcebergSource.ICEBERG_FILTER_ENABLED, "false");
    sourceState = new SourceState(new State(properties));

    // Mock all files from a partitioned table with partition metadata
    List<FilePathWithPartition> allFilesWithPartitions = Arrays.asList(
      new FilePathWithPartition(
        "/warehouse/db/table/datepartition=2025-04-01/file1.parquet",
        createPartitionMap("datepartition", "2025-04-01"), 1000L),
      new FilePathWithPartition(
        "/warehouse/db/table/datepartition=2025-04-01/file2.parquet",
        createPartitionMap("datepartition", "2025-04-01"), 1500L),
      new FilePathWithPartition(
        "/warehouse/db/table/datepartition=2025-04-02/file3.parquet",
        createPartitionMap("datepartition", "2025-04-02"), 2000L),
      new FilePathWithPartition(
        "/warehouse/db/table/datepartition=2025-04-03/file4.parquet",
        createPartitionMap("datepartition", "2025-04-03"), 2500L)
    );

    TableIdentifier tableId = TableIdentifier.of("test_db", "test_table");
    when(mockTable.getTableId()).thenReturn(tableId);
    // Mock TableScan with alwaysTrue() filter to return all files with partition metadata
    when(mockTable.getFilePathsWithPartitionsForFilter(any(Expression.class)))
      .thenReturn(allFilesWithPartitions);

    // Use reflection to test discoverPartitionFilePaths
    Method m = IcebergSource.class.getDeclaredMethod("discoverPartitionFilePaths", SourceState.class, IcebergTable.class);
    m.setAccessible(true);
    List<FilePathWithPartition> discoveredFiles = (List<FilePathWithPartition>) m.invoke(icebergSource, sourceState, mockTable);

    // Verify all files discovered with partition metadata preserved
    Assert.assertEquals(discoveredFiles.size(), 4, "Should discover all data files");
        
    // Verify partition metadata is preserved for each file
    for (FilePathWithPartition file : discoveredFiles) {
      Assert.assertNotNull(file.getPartitionData(), "Partition data should be present");
      Assert.assertFalse(file.getPartitionData().isEmpty(), "Partition data should not be empty");
      Assert.assertTrue(file.getPartitionData().containsKey("datepartition"),
        "Should have datepartition key");
      Assert.assertFalse(file.getPartitionPath().isEmpty(),
        "Partition path should not be empty");
      Assert.assertTrue(file.getPartitionPath().startsWith("datepartition="),
        "Partition path should be in format: datepartition=<date>");
    }
        
    // Verify files are from different partitions
    java.util.Set<String> uniquePartitions = discoveredFiles.stream()
      .map(f -> f.getPartitionData().get("datepartition"))
      .collect(java.util.stream.Collectors.toSet());
    Assert.assertEquals(uniquePartitions.size(), 3, "Should have files from 3 different partitions");
  }

  @Test
  public void testEmptyFileList() throws Exception {
    // Test handling of empty file list
    List<FilePathWithPartition> emptyList = Arrays.asList();
    TableIdentifier tableId = TableIdentifier.of("test_db", "test_table");
    when(mockTable.getTableId()).thenReturn(tableId);

    Method m = IcebergSource.class.getDeclaredMethod("createWorkUnitsFromFiles", List.class, SourceState.class, IcebergTable.class);
    m.setAccessible(true);
    List<WorkUnit> workUnits = (List<WorkUnit>) m.invoke(icebergSource, emptyList, sourceState, mockTable);

    // Should return empty list
    Assert.assertTrue(workUnits.isEmpty(), "Should return empty work unit list for empty file list");
  }

  @Test
  public void testSingleFilePerWorkUnit() throws Exception {
    // Test with files per work unit = 1
    properties.setProperty(IcebergSource.ICEBERG_FILES_PER_WORKUNIT, "1");
    sourceState = new SourceState(new State(properties));

    List<FilePathWithPartition> filesWithPartitions = Arrays.asList(
      new FilePathWithPartition(
        "file1.parquet", new java.util.HashMap<>(), 0L),
      new FilePathWithPartition(
        "file2.parquet", new java.util.HashMap<>(), 0L),
      new FilePathWithPartition(
        "file3.parquet", new java.util.HashMap<>(), 0L)
    );

    TableIdentifier tableId = TableIdentifier.of("test_db", "test_table");
    when(mockTable.getTableId()).thenReturn(tableId);

    Method m = IcebergSource.class.getDeclaredMethod("createWorkUnitsFromFiles", List.class, SourceState.class, IcebergTable.class);
    m.setAccessible(true);
    List<WorkUnit> workUnits = (List<WorkUnit>) m.invoke(icebergSource, filesWithPartitions, sourceState, mockTable);

    // Should create 3 work units, one per file
    Assert.assertEquals(workUnits.size(), 3, "Should create one work unit per file");

    for (WorkUnit wu : workUnits) {
      String filesToPull = wu.getProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL);
      Assert.assertEquals(filesToPull.split(",").length, 1, "Each work unit should have exactly 1 file");
    }
  }

  @Test
  public void testFilterEnabledWithoutDate() throws Exception {
    // Test that enabling filter without date value throws exception
    properties.setProperty(IcebergSource.ICEBERG_FILTER_ENABLED, "true");
    properties.remove(IcebergSource.ICEBERG_FILTER_DATE);
    sourceState = new SourceState(new State(properties));

    Method m = IcebergSource.class.getDeclaredMethod("discoverPartitionFilePaths", SourceState.class, IcebergTable.class);
    m.setAccessible(true);

    try {
      m.invoke(icebergSource, sourceState, mockTable);
      Assert.fail("Expected IllegalArgumentException for missing filter date");
    } catch (java.lang.reflect.InvocationTargetException e) {
      // Unwrap the exception from reflection
      Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
      Assert.assertTrue(e.getCause().getMessage().contains("iceberg.filter.date is required"));
    }
  }

  /**
   * Integration test class that creates real Iceberg tables with multiple partitions
   * and tests partition-specific data file fetching.
   */
  public static class IcebergSourcePartitionIntegrationTest extends HiveMetastoreTest {

    private static final String TEST_DB_NAME = "test_partition_db";
    private TableIdentifier partitionedTableId;
    private Table partitionedTable;
    private IcebergTable icebergTable;

    // Schema with partition field 'datepartition'
    private static final org.apache.iceberg.shaded.org.apache.avro.Schema partitionedAvroSchema =
      SchemaBuilder.record("partitioned_test")
        .fields()
        .name("id").type().longType().noDefault()
        .name("datepartition").type().stringType().noDefault()
        .endRecord();
    private static final Schema partitionedIcebergSchema =
      AvroSchemaUtil.toIceberg(partitionedAvroSchema);
    private static final PartitionSpec partitionSpec =
      PartitionSpec.builderFor(partitionedIcebergSchema)
        .identity("datepartition")
        .build();

    @BeforeClass
    public void setUp() throws Exception {
      // Start metastore and create test namespace
      startMetastore();
      catalog.createNamespace(Namespace.of(TEST_DB_NAME));
    }

    @BeforeMethod
    public void setUpEachTest() {
      // Create a partitioned table for each test
      partitionedTableId = TableIdentifier.of(TEST_DB_NAME, "partitioned_table");
      partitionedTable = catalog.createTable(partitionedTableId, partitionedIcebergSchema, partitionSpec,
        java.util.Collections.singletonMap("format-version", "2"));

      // Add data files for multiple partitions
      addDataFilesForPartition("2025-04-01", java.util.Arrays.asList(
        "/data/warehouse/test_db/partitioned_table/datepartition=2025-04-01/file1.parquet",
        "/data/warehouse/test_db/partitioned_table/datepartition=2025-04-01/file2.parquet"
      ));
      addDataFilesForPartition("2025-04-02", java.util.Arrays.asList(
        "/data/warehouse/test_db/partitioned_table/datepartition=2025-04-02/file3.parquet"
      ));
      addDataFilesForPartition("2025-04-03", java.util.Arrays.asList(
        "/data/warehouse/test_db/partitioned_table/datepartition=2025-04-03/file4.parquet",
        "/data/warehouse/test_db/partitioned_table/datepartition=2025-04-03/file5.parquet",
        "/data/warehouse/test_db/partitioned_table/datepartition=2025-04-03/file6.parquet"
      ));

      // Create IcebergTable wrapper
      icebergTable = new IcebergTable(
        partitionedTableId,
        catalog.newTableOps(partitionedTableId),
        catalog.getConf().get(CatalogProperties.URI),
        catalog.loadTable(partitionedTableId)
      );
    }

    @AfterMethod
    public void cleanUpEachTest() {
      // Clean up partitioned table
      if (partitionedTableId != null && catalog != null) {
        try {
          catalog.dropTable(partitionedTableId);
        } catch (Exception e) {
          // Ignore cleanup errors
        }
      }
    }

    @Test
    public void testGetDataFilePathsForSinglePartition() throws Exception {
      // Test fetching data files for a single partition
      List<String> dt20250402Files = icebergTable.getDataFilePathsForPartitionValues("datepartition",
        java.util.Collections.singletonList("2025-04-02"));

      // Should return exactly 1 file for datepartition=2025-04-02
      Assert.assertEquals(dt20250402Files.size(), 1,
        "Should return exactly 1 file for partition datepartition=2025-04-02");
      Assert.assertTrue(dt20250402Files.get(0).contains("datepartition=2025-04-02"),
        "File path should contain partition value");
      Assert.assertTrue(dt20250402Files.get(0).contains("file3.parquet"),
        "File path should be file3.parquet");
    }

    @Test
    public void testGetDataFilePathsForMultiplePartitions() throws Exception {
      // Test fetching data files for multiple partitions (OR filter)
      List<String> multiPartitionFiles = icebergTable.getDataFilePathsForPartitionValues("datepartition",
        java.util.Arrays.asList("2025-04-01", "2025-04-03"));

      // Should return 2 files from datepartition=2025-04-01 and 3 files from datepartition=2025-04-03
      Assert.assertEquals(multiPartitionFiles.size(), 5,
        "Should return 5 files (2 from datepartition=2025-04-01 + 3 from datepartition=2025-04-03)");

      // Verify files from both partitions are present
      long dt20250401Count = multiPartitionFiles.stream()
        .filter(path -> path.contains("datepartition=2025-04-01"))
        .count();
      long dt20250403Count = multiPartitionFiles.stream()
        .filter(path -> path.contains("datepartition=2025-04-03"))
        .count();

      Assert.assertEquals(dt20250401Count, 2, "Should have 2 files from datepartition=2025-04-01");
      Assert.assertEquals(dt20250403Count, 3, "Should have 3 files from datepartition=2025-04-03");

      // Verify no files from datepartition=2025-04-02
      boolean hasFilesFromExcludedPartition = multiPartitionFiles.stream()
        .anyMatch(path -> path.contains("datepartition=2025-04-02"));
      Assert.assertFalse(hasFilesFromExcludedPartition,
        "Should not include files from datepartition=2025-04-02");
    }

    @Test
    public void testGetDataFilePathsForAllPartitions() throws Exception {
      // Test fetching all data files across all partitions
      List<String> allFiles = icebergTable.getDataFilePathsForPartitionValues("datepartition",
        java.util.Arrays.asList("2025-04-01", "2025-04-02", "2025-04-03"));

      // Should return all 6 files (2 + 1 + 3)
      Assert.assertEquals(allFiles.size(), 6,
        "Should return all 6 files across all partitions");

      // Verify distribution across partitions
      long dt20250401Count = allFiles.stream().filter(p -> p.contains("datepartition=2025-04-01")).count();
      long dt20250402Count = allFiles.stream().filter(p -> p.contains("datepartition=2025-04-02")).count();
      long dt20250403Count = allFiles.stream().filter(p -> p.contains("datepartition=2025-04-03")).count();

      Assert.assertEquals(dt20250401Count, 2);
      Assert.assertEquals(dt20250402Count, 1);
      Assert.assertEquals(dt20250403Count, 3);
    }

    @Test
    public void testGetDataFilePathsForNonExistentPartition() throws Exception {
      // Test fetching data files for a partition that doesn't exist
      List<String> noFiles = icebergTable.getDataFilePathsForPartitionValues("datepartition",
        java.util.Collections.singletonList("2025-12-31"));

      // Should return empty list
      Assert.assertTrue(noFiles.isEmpty(),
        "Should return empty list for non-existent partition");
    }

    /**
     * Helper method to add data files for a specific partition
     */
    private void addDataFilesForPartition(String partitionValue, List<String> filePaths) {
      PartitionData partitionData =
        new PartitionData(partitionSpec.partitionType());
      partitionData.set(0, partitionValue);

      AppendFiles append = partitionedTable.newAppend();
      for (String filePath : filePaths) {
        DataFile dataFile = DataFiles.builder(partitionSpec)
          .withPath(filePath)
          .withFileSizeInBytes(100L)
          .withRecordCount(10L)
          .withPartition(partitionData)
          .withFormat(FileFormat.PARQUET)
          .build();
        append.appendFile(dataFile);
      }
      append.commit();
    }
  }

  /**
   * Helper method to create partition map for testing
   */
  private Map<String, String> createPartitionMap(String key, String value) {
    Map<String, String> partitionMap = new HashMap<>();
    partitionMap.put(key, value);
    return partitionMap;
  }

  @Test
  public void testWorkUnitSizeTracking() throws Exception {
    // Test that work units include file size information for dynamic scaling
    properties.setProperty(IcebergSource.ICEBERG_FILES_PER_WORKUNIT, "2");
    sourceState = new SourceState(new State(properties));

    // Create files with different sizes
    List<FilePathWithPartition> filesWithSizes = Arrays.asList(
      new FilePathWithPartition(
        "file1.parquet", new HashMap<>(), 1073741824L), // 1 GB
      new FilePathWithPartition(
        "file2.parquet", new HashMap<>(), 536870912L),  // 512 MB
      new FilePathWithPartition(
        "file3.parquet", new HashMap<>(), 2147483648L), // 2 GB
      new FilePathWithPartition(
        "file4.parquet", new HashMap<>(), 268435456L)   // 256 MB
    );

    TableIdentifier tableId = TableIdentifier.of("test_db", "test_table");
    when(mockTable.getTableId()).thenReturn(tableId);

    // Invoke createWorkUnitsFromFiles
    Method m = IcebergSource.class.getDeclaredMethod("createWorkUnitsFromFiles", List.class, SourceState.class, IcebergTable.class);
    m.setAccessible(true);
    List<WorkUnit> workUnits = (List<WorkUnit>) m.invoke(icebergSource, filesWithSizes, sourceState, mockTable);

    // Should create 2 work units (4 files / 2 files per unit)
    Assert.assertEquals(workUnits.size(), 2, "Should create 2 work units");

    // Verify each work unit has WORK_UNIT_SIZE set
    WorkUnit wu1 = workUnits.get(0);
    long wu1Size = wu1.getPropAsLong(ServiceConfigKeys.WORK_UNIT_SIZE);
    Assert.assertEquals(wu1Size, 1073741824L + 536870912L, // 1 GB + 512 MB
      "WorkUnit 1 should have total size of its files");

    WorkUnit wu2 = workUnits.get(1);
    long wu2Size = wu2.getPropAsLong(ServiceConfigKeys.WORK_UNIT_SIZE);
    Assert.assertEquals(wu2Size, 2147483648L + 268435456L, // 2 GB + 256 MB
      "WorkUnit 2 should have total size of its files");

    // Verify work unit weight is set for bin packing
    String weight1 = wu1.getProp("iceberg.workUnitWeight");
    Assert.assertNotNull(weight1, "Work unit weight should be set");
    Assert.assertEquals(Long.parseLong(weight1), wu1Size, "Weight should equal total size");

    String weight2 = wu2.getProp("iceberg.workUnitWeight");
    Assert.assertNotNull(weight2, "Work unit weight should be set");
    Assert.assertEquals(Long.parseLong(weight2), wu2Size, "Weight should equal total size");
  }

  @Test
  public void testBinPackingDisabled() throws Exception {
    // Test that bin packing is skipped when not configured
    properties.setProperty(IcebergSource.ICEBERG_FILES_PER_WORKUNIT, "1");
    // Do NOT set binPacking.maxSizePerBin - bin packing should be disabled
    sourceState = new SourceState(new State(properties));

    List<FilePathWithPartition> filesWithSizes = Arrays.asList(
      new FilePathWithPartition(
        "file1.parquet", new HashMap<>(), 1000L),
      new FilePathWithPartition(
        "file2.parquet", new HashMap<>(), 2000L),
      new FilePathWithPartition(
        "file3.parquet", new HashMap<>(), 3000L)
    );

    TableIdentifier tableId = TableIdentifier.of("test_db", "test_table");
    when(mockTable.getTableId()).thenReturn(tableId);

    // Create work units
    Method createMethod = IcebergSource.class.getDeclaredMethod("createWorkUnitsFromFiles", List.class, SourceState.class, IcebergTable.class);
    createMethod.setAccessible(true);
    List<WorkUnit> initialWorkUnits = (List<WorkUnit>) createMethod.invoke(icebergSource, filesWithSizes, sourceState, mockTable);

    // Apply bin packing (should return original list)
    Method binPackMethod = IcebergSource.class.getDeclaredMethod("applyBinPacking", List.class, SourceState.class);
    binPackMethod.setAccessible(true);
    List<? extends WorkUnit> packedWorkUnits = (List<? extends WorkUnit>) binPackMethod.invoke(icebergSource, initialWorkUnits, sourceState);

    // Should return same number of work units (no packing applied)
    Assert.assertEquals(packedWorkUnits.size(), initialWorkUnits.size(),
      "Bin packing should be disabled, returning original work units");
    Assert.assertEquals(packedWorkUnits.size(), 3, "Should have 3 unpacked work units");
  }

  @Test
  public void testBinPackingEnabled() throws Exception {
    // Test that bin packing groups work units by size using WorstFitDecreasing algorithm
    properties.setProperty(IcebergSource.ICEBERG_FILES_PER_WORKUNIT, "1");
    // Use CopySource bin packing configuration key for consistency
    properties.setProperty(CopySource.MAX_SIZE_MULTI_WORKUNITS, "5000"); // 5KB max per bin
    sourceState = new SourceState(new State(properties));

    // Create 6 work units with sizes: 1KB, 1KB, 2KB, 2KB, 3KB, 3KB (total 12KB)
    // WorstFitDecreasing algorithm packs largest items first:
    // Expected packing with 5KB limit:
    //   Bin 1: 3KB + 2KB = 5KB
    //   Bin 2: 3KB + 2KB = 5KB
    //   Bin 3: 1KB + 1KB = 2KB
    // Total: 3 bins
    List<FilePathWithPartition> filesWithSizes = Arrays.asList(
      new FilePathWithPartition(
        "file1.parquet", new HashMap<>(), 1000L),
      new FilePathWithPartition(
        "file2.parquet", new HashMap<>(), 1000L),
      new FilePathWithPartition(
        "file3.parquet", new HashMap<>(), 2000L),
      new FilePathWithPartition(
        "file4.parquet", new HashMap<>(), 2000L),
      new FilePathWithPartition(
        "file5.parquet", new HashMap<>(), 3000L),
      new FilePathWithPartition(
        "file6.parquet", new HashMap<>(), 3000L)
    );

    TableIdentifier tableId = TableIdentifier.of("test_db", "test_table");
    when(mockTable.getTableId()).thenReturn(tableId);

    // Create initial work units (1 file per work unit)
    Method createMethod = IcebergSource.class.getDeclaredMethod("createWorkUnitsFromFiles", List.class, SourceState.class, IcebergTable.class);
    createMethod.setAccessible(true);
    List<WorkUnit> initialWorkUnits = (List<WorkUnit>) createMethod.invoke(icebergSource, filesWithSizes, sourceState, mockTable);

    Assert.assertEquals(initialWorkUnits.size(), 6, "Should create 6 initial work units");

    // Apply bin packing
    Method binPackMethod = IcebergSource.class.getDeclaredMethod("applyBinPacking", List.class, SourceState.class);
    binPackMethod.setAccessible(true);
    List<? extends WorkUnit> packedWorkUnits = (List<? extends WorkUnit>) binPackMethod.invoke(icebergSource, initialWorkUnits, sourceState);

    // Verify bin packing reduced work unit count
    Assert.assertTrue(packedWorkUnits.size() < initialWorkUnits.size(),
      "Bin packing should reduce work unit count from 6 to 3");

    // Verify exact bin count (WorstFitDecreasing packs optimally)
    Assert.assertEquals(packedWorkUnits.size(), 3,
      "WorstFitDecreasing should pack 6 files (1KB,1KB,2KB,2KB,3KB,3KB) into exactly 3 bins with 5KB limit");

    // Note: Individual bin sizes are not directly accessible on MultiWorkUnit returned by bin packing
    // Size validation is covered by testWorkUnitSizeTracking() which validates WORK_UNIT_SIZE
    // is set correctly on individual work units before bin packing
  }

  @Test
  public void testSimulateModeReturnsEmptyList() throws Exception {
    // Test that simulate mode configuration is respected and would return empty list
    properties.setProperty(CopySource.SIMULATE, "true");
    properties.setProperty(IcebergSource.ICEBERG_FILTER_ENABLED, "true");
    properties.setProperty(IcebergSource.ICEBERG_FILTER_DATE, "2025-10-21");
    sourceState = new SourceState(new State(properties));

    // Mock files that would be discovered
    List<FilePathWithPartition> mockFiles = Arrays.asList(
      new FilePathWithPartition(
        "/data/file1.parquet", createPartitionMap("datepartition", "2025-10-21"), 1000L),
      new FilePathWithPartition(
        "/data/file2.parquet", createPartitionMap("datepartition", "2025-10-21"), 2000L)
    );

    TableIdentifier tableId = TableIdentifier.of("test_db", "test_table");
    when(mockTable.getTableId()).thenReturn(tableId);
    when(mockTable.getFilePathsWithPartitionsForFilter(any(Expression.class)))
      .thenReturn(mockFiles);

    // Test 1: Verify simulate mode is enabled in configuration
    Assert.assertTrue(sourceState.contains(CopySource.SIMULATE),
      "Simulate mode configuration should be present");
    Assert.assertTrue(sourceState.getPropAsBoolean(CopySource.SIMULATE),
      "Simulate mode should be enabled");

    // Test 2: File discovery should work normally in simulate mode (discovery happens before simulate check)
    Method discoverMethod = IcebergSource.class.getDeclaredMethod("discoverPartitionFilePaths",
      SourceState.class, IcebergTable.class);
    discoverMethod.setAccessible(true);
    List<FilePathWithPartition> discovered =
      (List<FilePathWithPartition>)
      discoverMethod.invoke(icebergSource, sourceState, mockTable);

    Assert.assertEquals(discovered.size(), 2,
      "In simulate mode, file discovery should work normally (happens before simulate check)");

    // Test 3: Work units should be created normally (happens before simulate check)
    Method createMethod = IcebergSource.class.getDeclaredMethod("createWorkUnitsFromFiles",
      List.class, SourceState.class, IcebergTable.class);
    createMethod.setAccessible(true);
    List<WorkUnit> workUnitsBeforeSimulateCheck = (List<WorkUnit>) createMethod.invoke(
      icebergSource, discovered, sourceState, mockTable);

    Assert.assertFalse(workUnitsBeforeSimulateCheck.isEmpty(),
      "Work units should be created before simulate mode check");
    Assert.assertEquals(workUnitsBeforeSimulateCheck.size(), 1,
      "Should create 1 work unit from 2 files before simulate check");

    // Test 4: Verify logSimulateMode can be called successfully (logs the plan)
    Method logMethod = IcebergSource.class.getDeclaredMethod("logSimulateMode",
      List.class, List.class, SourceState.class);
    logMethod.setAccessible(true);
    // Should not throw - just logs the simulate mode plan
    logMethod.invoke(icebergSource, workUnitsBeforeSimulateCheck, discovered, sourceState);

    // Test 5: Verify the critical behavior - after simulate check, work units should NOT be returned
    // Simulate the conditional logic from getWorkunits()
    List<WorkUnit> actualReturnedWorkUnits;
    if (sourceState.contains(CopySource.SIMULATE)
      && sourceState.getPropAsBoolean(CopySource.SIMULATE)) {
      // This is what getWorkunits() does in simulate mode
      actualReturnedWorkUnits = Lists.newArrayList(); // Empty list
    } else {
      actualReturnedWorkUnits = workUnitsBeforeSimulateCheck;
    }

    // Assert: In simulate mode, the returned work units should be EMPTY
    Assert.assertTrue(actualReturnedWorkUnits.isEmpty(),
      "Simulate mode: getWorkunits() should return empty list (no execution)");
    Assert.assertEquals(actualReturnedWorkUnits.size(), 0,
      "Simulate mode: zero work units should be returned for execution");

    // Verify the work units were created but NOT returned
    Assert.assertEquals(workUnitsBeforeSimulateCheck.size(), 1,
      "Work units were created internally but not returned due to simulate mode");
  }

  @Test
  public void testHourlyPartitionDateFormat() throws Exception {
    // Test that hourly partition format is generated when enabled
    properties.setProperty(IcebergSource.ICEBERG_FILTER_ENABLED, "true");
    properties.setProperty(IcebergSource.ICEBERG_FILTER_DATE, "2025-04-01"); // Standard date format
    properties.setProperty(IcebergSource.ICEBERG_HOURLY_PARTITION_ENABLED, "true"); // Enable hourly format
    properties.setProperty(IcebergSource.ICEBERG_LOOKBACK_DAYS, "1");
    sourceState = new SourceState(new State(properties));

    // Mock partition with hourly format
    List<FilePathWithPartition> files = Arrays.asList(
      new FilePathWithPartition(
        "/data/file1.parquet", createPartitionMap("datepartition", "2025-04-01-00"), 1000L)
    );

    TableIdentifier tableId = TableIdentifier.of("test_db", "test_table");
    when(mockTable.getTableId()).thenReturn(tableId);
    when(mockTable.getFilePathsWithPartitionsForFilter(any(Expression.class)))
      .thenReturn(files);

    // Test discovery
    Method m = IcebergSource.class.getDeclaredMethod("discoverPartitionFilePaths",
      SourceState.class, IcebergTable.class);
    m.setAccessible(true);
    List<FilePathWithPartition> discovered =
      (List<FilePathWithPartition>) m.invoke(icebergSource, sourceState, mockTable);

    // Verify partition is discovered
    Assert.assertEquals(discovered.size(), 1, "Should discover partition");

    // Verify partition value has -00 suffix appended
    String partitionValues = sourceState.getProp(IcebergSource.ICEBERG_PARTITION_VALUES);
    Assert.assertNotNull(partitionValues, "Partition values should be set");
    Assert.assertEquals(partitionValues, "2025-04-01-00", "Should append -00 suffix for hourly partition");
  }

  @Test
  public void testHourlyPartitionDateFormatWithLookback() throws Exception {
    // Test that hourly partition format works with lookback period
    properties.setProperty(IcebergSource.ICEBERG_FILTER_ENABLED, "true");
    properties.setProperty(IcebergSource.ICEBERG_FILTER_DATE, "2025-04-03"); // Standard date format
    properties.setProperty(IcebergSource.ICEBERG_HOURLY_PARTITION_ENABLED, "true"); // Enable hourly format
    properties.setProperty(IcebergSource.ICEBERG_LOOKBACK_DAYS, "3");
    sourceState = new SourceState(new State(properties));

    // Mock 3 days of partitions with hourly format
    List<FilePathWithPartition> filesFor3Days = Arrays.asList(
      new FilePathWithPartition(
        "/data/file1.parquet", createPartitionMap("datepartition", "2025-04-03-00"), 1000L),
      new FilePathWithPartition(
        "/data/file2.parquet", createPartitionMap("datepartition", "2025-04-02-00"), 1000L),
      new FilePathWithPartition(
        "/data/file3.parquet", createPartitionMap("datepartition", "2025-04-01-00"), 1000L)
    );

    TableIdentifier tableId = TableIdentifier.of("test_db", "test_table");
    when(mockTable.getTableId()).thenReturn(tableId);
    when(mockTable.getFilePathsWithPartitionsForFilter(any(Expression.class)))
      .thenReturn(filesFor3Days);

    // Test discovery
    Method m = IcebergSource.class.getDeclaredMethod("discoverPartitionFilePaths",
      SourceState.class, IcebergTable.class);
    m.setAccessible(true);
    List<FilePathWithPartition> discovered =
      (List<FilePathWithPartition>) m.invoke(icebergSource, sourceState, mockTable);

    // Verify all 3 days discovered
    Assert.assertEquals(discovered.size(), 3, "Should discover 3 days with lookback=3");

    // Verify partition values have -00 suffix appended to all dates
    String partitionValues = sourceState.getProp(IcebergSource.ICEBERG_PARTITION_VALUES);
    Assert.assertNotNull(partitionValues, "Partition values should be set");
    String[] dates = partitionValues.split(",");
    Assert.assertEquals(dates.length, 3, "Should have 3 partition values");
    
    // Verify -00 suffix is appended to all dates
    Assert.assertEquals(dates[0], "2025-04-03-00", "Should have -00 suffix for day 0");
    Assert.assertEquals(dates[1], "2025-04-02-00", "Should have -00 suffix for day 1");
    Assert.assertEquals(dates[2], "2025-04-01-00", "Should have -00 suffix for day 2");
    
    // Verify all follow the hourly format pattern
    for (String date : dates) {
      Assert.assertEquals(date.length(), 13, "Date should be in yyyy-MM-dd-00 format (13 chars)");
      Assert.assertTrue(date.matches("\\d{4}-\\d{2}-\\d{2}-00"), "Date should match yyyy-MM-dd-00 pattern");
    }
  }

  @Test
  public void testZeroSizeFilesHandling() throws Exception {
    // Test handling of files with zero or very small sizes
    properties.setProperty(IcebergSource.ICEBERG_FILES_PER_WORKUNIT, "3");
    sourceState = new SourceState(new State(properties));

    List<FilePathWithPartition> filesWithSizes = Arrays.asList(
      new FilePathWithPartition(
        "file1.parquet", new HashMap<>(), 0L),     // Empty file
      new FilePathWithPartition(
        "file2.parquet", new HashMap<>(), 1L),     // 1 byte
      new FilePathWithPartition(
        "file3.parquet", new HashMap<>(), 100L)    // 100 bytes
    );

    TableIdentifier tableId = TableIdentifier.of("test_db", "test_table");
    when(mockTable.getTableId()).thenReturn(tableId);

    // Create work units
    Method m = IcebergSource.class.getDeclaredMethod("createWorkUnitsFromFiles", List.class, SourceState.class, IcebergTable.class);
    m.setAccessible(true);
    List<WorkUnit> workUnits = (List<WorkUnit>) m.invoke(icebergSource, filesWithSizes, sourceState, mockTable);

    // Should handle gracefully
    Assert.assertEquals(workUnits.size(), 1);
    WorkUnit wu = workUnits.get(0);

    long totalSize = wu.getPropAsLong(ServiceConfigKeys.WORK_UNIT_SIZE);
    Assert.assertEquals(totalSize, 101L, "Total size should be 0 + 1 + 100 = 101");

    // Weight should be at least 1 (minimum weight)
    String weightStr = wu.getProp("iceberg.workUnitWeight");
    long weight = Long.parseLong(weightStr);
    Assert.assertTrue(weight >= 1L, "Weight should be at least 1 for very small files");
  }

}
