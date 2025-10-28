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

import java.io.IOException;
import java.net.URI;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.base.Optional;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.copy.FileAwareInputStream;
import org.apache.gobblin.dataset.DatasetConstants;
import org.apache.gobblin.dataset.DatasetDescriptor;
import org.apache.gobblin.metrics.event.lineage.LineageInfo;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.filebased.FileBasedHelperException;
import org.apache.gobblin.source.extractor.filebased.FileBasedSource;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.source.workunit.WorkUnitWeighter;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.binpacking.FieldWeighter;
import org.apache.gobblin.util.binpacking.WorstFitDecreasingBinPacking;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

/**
 * Unified Iceberg source that supports partition-based data copying from Iceberg tables.
 *
 * This source reads job configuration, applies date partition filters with optional lookback period,
 * and uses Iceberg's TableScan API to enumerate data files for specific partitions. It groups files
 * into work units for parallel processing.
 *
 * <pre>
 * # Basic configuration
 * source.class=org.apache.gobblin.data.management.copy.iceberg.IcebergSource
 * iceberg.database.name=db1
 * iceberg.table.name=table1
 * iceberg.catalog.uri=https://openhouse.com/catalog
 *
 * # Partition filtering with lookback
 * iceberg.filter.enabled=true
 * iceberg.filter.expr=datepartition=2025-04-01
 * iceberg.lookback.days=3
 * </pre>
 */
@Slf4j
public class IcebergSource extends FileBasedSource<String, FileAwareInputStream> {

    public static final String ICEBERG_DATABASE_NAME = "iceberg.database.name";
    public static final String ICEBERG_TABLE_NAME = "iceberg.table.name";
    public static final String ICEBERG_CATALOG_URI = "iceberg.catalog.uri";
    public static final String ICEBERG_CATALOG_CLASS = "iceberg.catalog.class";
    public static final String DEFAULT_ICEBERG_CATALOG_CLASS = "org.apache.gobblin.data.management.copy.iceberg.IcebergHiveCatalog";
    public static final String ICEBERG_RECORD_PROCESSING_ENABLED = "iceberg.record.processing.enabled";
    public static final boolean DEFAULT_RECORD_PROCESSING_ENABLED = false;
    public static final String ICEBERG_FILES_PER_WORKUNIT = "iceberg.files.per.workunit";
    public static final int DEFAULT_FILES_PER_WORKUNIT = 10;
    public static final String ICEBERG_FILTER_ENABLED = "iceberg.filter.enabled";
    public static final String ICEBERG_FILTER_EXPR = "iceberg.filter.expr"; // e.g., datepartition=2025-04-01
    public static final String ICEBERG_LOOKBACK_DAYS = "iceberg.lookback.days";
    public static final int DEFAULT_LOOKBACK_DAYS = 1;
    public static final String ICEBERG_DATE_PARTITION_KEY = "datepartition"; // date partition key name
    public static final String ICEBERG_PARTITION_KEY = "iceberg.partition.key";
    public static final String ICEBERG_PARTITION_VALUES = "iceberg.partition.values";
    public static final String ICEBERG_FILE_PARTITION_PATH = "iceberg.file.partition.path";
    public static final String ICEBERG_SIMULATE = "iceberg.simulate";
    public static final String ICEBERG_MAX_SIZE_MULTI_WORKUNITS = "iceberg.binPacking.maxSizePerBin";
    public static final String ICEBERG_MAX_WORK_UNITS_PER_BIN = "iceberg.binPacking.maxWorkUnitsPerBin";
    private static final String WORK_UNIT_WEIGHT = "iceberg.workUnitWeight";

    private Optional<LineageInfo> lineageInfo;
    private final WorkUnitWeighter weighter = new FieldWeighter(WORK_UNIT_WEIGHT);

    /**
     * Initialize file system helper based on mode (streaming vs record processing)
     */
    @Override
    public void initFileSystemHelper(State state) throws FileBasedHelperException {
        // For file streaming mode, we use IcebergFileStreamHelper
        // For record processing mode, we'll use a different helper (future implementation)
        boolean recordProcessingEnabled = state.getPropAsBoolean(
            ICEBERG_RECORD_PROCESSING_ENABLED, DEFAULT_RECORD_PROCESSING_ENABLED);

        if (recordProcessingEnabled) {
            // Future: Initialize helper for record processing
            throw new UnsupportedOperationException("Record processing mode not yet implemented. " +
                "This will be added when SQL/Data Lake destinations are required.");
        } else {
            // Initialize helper for file streaming - now implements TimestampAwareFileBasedHelper
            this.fsHelper = new IcebergFileStreamHelper(state);
            this.fsHelper.connect();
        }
    }

    /**
     * Get work units by discovering files from Iceberg table
     * @param state is the source state
     * @return List<WorkUnit> list of work units
     */
    @Override
    public List<WorkUnit> getWorkunits(SourceState state) {
        this.lineageInfo = LineageInfo.getLineageInfo(state.getBroker());

        try {
            initFileSystemHelper(state);

            validateConfiguration(state);

            IcebergCatalog catalog = createCatalog(state);
            String database = state.getProp(ICEBERG_DATABASE_NAME);
            String table = state.getProp(ICEBERG_TABLE_NAME);

            IcebergTable icebergTable = catalog.openTable(database, table);
            Preconditions.checkArgument(catalog.tableAlreadyExists(icebergTable),
                String.format("OpenHouse table not found: %s.%s", database, table));

            List<IcebergTable.FilePathWithPartition> filesWithPartitions = discoverPartitionFilePaths(state, icebergTable);
            log.info("Discovered {} files from Iceberg table {}.{}", filesWithPartitions.size(), database, table);

            // Create work units from discovered files
            List<WorkUnit> workUnits = createWorkUnitsFromFiles(filesWithPartitions, state, icebergTable);

            // Handle simulate mode - log what would be copied without executing
            if (state.contains(ICEBERG_SIMULATE) && state.getPropAsBoolean(ICEBERG_SIMULATE)) {
                log.info("Simulate mode enabled. Will not execute the copy.");
                logSimulateMode(workUnits, filesWithPartitions, state);
                return Lists.newArrayList();
            }

            // Apply bin packing to work units if configured
            List<? extends WorkUnit> packedWorkUnits = applyBinPacking(workUnits, state);
            log.info("Work unit creation complete. Initial work units: {}, packed work units: {}",
                workUnits.size(), packedWorkUnits.size());

            return Lists.newArrayList(packedWorkUnits);

        } catch (Exception e) {
            log.error("Failed to create work units for Iceberg table", e);
            throw new RuntimeException("Failed to create work units", e);
        }
    }

    /**
     * Get extractor based on mode (streaming vs record processing)
     *
     * @param state a {@link org.apache.gobblin.configuration.WorkUnitState} carrying properties needed by the returned {@link Extractor}
     * @return
     * @throws IOException
     */
    @Override
    public Extractor<String, FileAwareInputStream> getExtractor(WorkUnitState state) throws IOException {
        boolean recordProcessingEnabled = state.getPropAsBoolean(
            ICEBERG_RECORD_PROCESSING_ENABLED, DEFAULT_RECORD_PROCESSING_ENABLED);

        if (recordProcessingEnabled) {
            // Return record processing extractor
            throw new UnsupportedOperationException("Record processing mode not yet implemented.");
        } else {
            // Return file streaming extractor
            return new IcebergFileStreamExtractor(state);
        }
    }

    /**
     * Discover partition data files using Iceberg TableScan API with optional lookback for date partitions.
     *
     * <p>This method supports two modes:
     * <ol>
     *   <li><b>Full table scan</b>: When {@code iceberg.filter.enabled=false}, returns all data files from current snapshot</li>
     *   <li><b>Partition filter</b>: When {@code iceberg.filter.enabled=true}, uses Iceberg TableScan with partition
     *       filter and applies lookback period for date partitions</li>
     * </ol>
     *
     * <p>For date partitions (partition key = {@value #ICEBERG_DATE_PARTITION_KEY}), the lookback period allows copying data for the last N days.
     * For example, with {@code iceberg.filter.expr=datepartition=2025-04-03} and {@code iceberg.lookback.days=3},
     * this will discover files for partitions: datepartition=2025-04-03, datepartition=2025-04-02, datepartition=2025-04-01
     *
     * @param state source state containing filter configuration
     * @param icebergTable the Iceberg table to scan
     * @return list of file paths with partition metadata matching the filter criteria
     * @throws IOException if table scan or file discovery fails
     */
    private List<IcebergTable.FilePathWithPartition> discoverPartitionFilePaths(SourceState state, IcebergTable icebergTable) throws IOException {
        boolean filterEnabled = state.getPropAsBoolean(ICEBERG_FILTER_ENABLED, true);

        if (!filterEnabled) {
            log.info("Partition filter disabled, discovering all data files from current snapshot");
            IcebergSnapshotInfo snapshot = icebergTable.getCurrentSnapshotInfo();
            List<IcebergTable.FilePathWithPartition> result = Lists.newArrayList();
            for (IcebergSnapshotInfo.ManifestFileInfo mfi : snapshot.getManifestFiles()) {
                for (String filePath : mfi.getListedFilePaths()) {
                    result.add(new IcebergTable.FilePathWithPartition(filePath, Maps.newHashMap()));
                }
            }
            log.info("Discovered {} data files from snapshot", result.size());
            return result;
        }

        // Parse filter expression
        String expr = state.getProp(ICEBERG_FILTER_EXPR);
        Preconditions.checkArgument(!StringUtils.isBlank(expr),
            "iceberg.filter.expr is required when iceberg.filter.enabled=true");
        String[] parts = expr.split("=", 2);
        Preconditions.checkArgument(parts.length == 2,
            "Invalid iceberg.filter.expr. Expected key=value, got: %s", expr);
        String key = parts[0].trim();
        String value = parts[1].trim();

        // Apply lookback period for date partitions
        // lookbackDays=1 (default) means copy only the specified date
        // lookbackDays=3 means copy specified date + 2 previous days (total 3 days)
        int lookbackDays = state.getPropAsInt(ICEBERG_LOOKBACK_DAYS, DEFAULT_LOOKBACK_DAYS);
        List<String> values = Lists.newArrayList();

        if (ICEBERG_DATE_PARTITION_KEY.equals(key) && lookbackDays >= 1) {
            log.info("Applying lookback period of {} days for date partition: {}", lookbackDays, value);
            LocalDate start = LocalDate.parse(value);
            for (int i = 0; i < lookbackDays; i++) {
                String partitionValue = start.minusDays(i).toString();
                values.add(partitionValue);
                log.debug("Including partition: {}={}", ICEBERG_DATE_PARTITION_KEY, partitionValue);
            }
        } else {
            log.error("Partition key is not correct or lookbackDays < 1, skipping lookback. Input: {}={}, expected: {}=<date>",
                key, value, ICEBERG_DATE_PARTITION_KEY);
            throw new IllegalArgumentException(String.format(
                "Only date partition filter with lookback period is supported. Expected partition key: '%s', got: '%s'",
                ICEBERG_DATE_PARTITION_KEY, key));
        }

        // Store partition info on state for downstream use (extractor, destination path mapping)
        state.setProp(ICEBERG_PARTITION_KEY, key);
        state.setProp(ICEBERG_PARTITION_VALUES, String.join(",", values));

        // Use Iceberg TableScan API to get only data files (parquet/orc/avro) for specified partitions
        // TableScan.planFiles() returns DataFiles only - no manifest files or metadata files
        log.info("Executing TableScan with filter: {}={}", key, values);
        Expression icebergExpr = null;
        for (String val : values) {
            Expression e = Expressions.equal(key, val);
            icebergExpr = (icebergExpr == null) ? e : Expressions.or(icebergExpr, e);
        }

        List<IcebergTable.FilePathWithPartition> filesWithPartitions = icebergTable.getFilePathsWithPartitionsForFilter(icebergExpr);
        log.info("Discovered {} data files for partitions: {}", filesWithPartitions.size(), values);

        return filesWithPartitions;
    }

    /**
     * Create work units from discovered file paths by grouping them for parallel processing.
     *
     * Files are grouped into work units based on {@code iceberg.files.per.workunit} configuration.
     * Each work unit contains metadata about the files to process.
     *
     * @param filesWithPartitions list of file paths with partition metadata to process
     * @param state source state containing job configuration
     * @param table the Iceberg table being copied
     * @return list of work units ready for parallel execution
     */
    private List<WorkUnit> createWorkUnitsFromFiles(List<IcebergTable.FilePathWithPartition> filesWithPartitions, SourceState state, IcebergTable table) {
        List<WorkUnit> workUnits = Lists.newArrayList();

        if (filesWithPartitions.isEmpty()) {
            log.warn("No files discovered for table {}.{}, returning empty work unit list",
                state.getProp(ICEBERG_DATABASE_NAME), state.getProp(ICEBERG_TABLE_NAME));
            return workUnits;
        }

        String nameSpace = state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY, "iceberg");
        String tableName = table.getTableId().name();
        Extract extract = new Extract(Extract.TableType.SNAPSHOT_ONLY, nameSpace, tableName);

        int filesPerWorkUnit = state.getPropAsInt(ICEBERG_FILES_PER_WORKUNIT, DEFAULT_FILES_PER_WORKUNIT);
        List<List<IcebergTable.FilePathWithPartition>> groups = Lists.partition(filesWithPartitions, Math.max(1, filesPerWorkUnit));
        log.info("Grouping {} files into {} work units ({} files per work unit)",
            filesWithPartitions.size(), groups.size(), filesPerWorkUnit);

        for (int i = 0; i < groups.size(); i++) {
            List<IcebergTable.FilePathWithPartition> group = groups.get(i);
            WorkUnit workUnit = new WorkUnit(extract);

            // Store data file paths and their partition metadata separately
            // Note: Only data files (parquet/orc/avro) are included, no Iceberg metadata files
            List<String> filePaths = Lists.newArrayList();
            Map<String, String> fileToPartitionPath = Maps.newHashMap();
            long totalSize = 0L;

            for (IcebergTable.FilePathWithPartition fileWithPartition : group) {
                String filePath = fileWithPartition.getFilePath();
                filePaths.add(filePath);
                // Store partition path for each file
                fileToPartitionPath.put(filePath, fileWithPartition.getPartitionPath());
                // Accumulate file sizes for work unit weight
                totalSize += fileWithPartition.getFileSize();
            }

            workUnit.setProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL, String.join(",", filePaths));

            // Store partition path mapping as JSON for extractor to use
            workUnit.setProp(ICEBERG_FILE_PARTITION_PATH, new com.google.gson.Gson().toJson(fileToPartitionPath));

            // Set work unit size for dynamic scaling (instead of just file count)
            workUnit.setProp(ServiceConfigKeys.WORK_UNIT_SIZE, totalSize);

            // Set work unit weight for bin packing
            setWorkUnitWeight(workUnit, totalSize);

            // Carry partition info to extractor for destination path mapping
            if (state.contains(ICEBERG_PARTITION_KEY)) {
                workUnit.setProp(ICEBERG_PARTITION_KEY, state.getProp(ICEBERG_PARTITION_KEY));
            }
            if (state.contains(ICEBERG_PARTITION_VALUES)) {
                workUnit.setProp(ICEBERG_PARTITION_VALUES, state.getProp(ICEBERG_PARTITION_VALUES));
            }

            // Add lineage information for data governance and tracking
            addLineageSourceInfo(state, workUnit, table);
            workUnits.add(workUnit);

            log.debug("Created work unit {} with {} files, total size: {} bytes", i, group.size(), totalSize);
        }

        return workUnits;
    }

    /**
     * Create catalog using existing IcebergDatasetFinder logic
     */
    private IcebergCatalog createCatalog(SourceState state) throws IOException {
        String catalogPrefix = "iceberg.catalog.";
        Map<String, String> catalogProperties = buildMapFromPrefixChildren(state.getProperties(), catalogPrefix);

        Configuration configuration = HadoopUtils.getConfFromProperties(state.getProperties());
        String catalogClassName = catalogProperties.getOrDefault("class", DEFAULT_ICEBERG_CATALOG_CLASS);

        return IcebergCatalogFactory.create(catalogClassName, catalogProperties, configuration);
    }

    /**
     * Build map of properties with given prefix
     *
     */
    private Map<String, String> buildMapFromPrefixChildren(Properties properties, String configPrefix) {
        Map<String, String> catalogProperties = Maps.newHashMap();

        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = (String) entry.getKey();
            if (key.startsWith(configPrefix)) {
                String relativeKey = key.substring(configPrefix.length());
                catalogProperties.put(relativeKey, (String) entry.getValue());
            }
        }

        String catalogUri = catalogProperties.get("uri");
        Preconditions.checkNotNull(catalogUri, "Catalog URI is required");

        return catalogProperties;
    }

    /**
     * Add lineage information
     */
    private void addLineageSourceInfo(SourceState sourceState, WorkUnit workUnit, IcebergTable table) {
        if (this.lineageInfo != null && this.lineageInfo.isPresent()) {
            String catalogUri = sourceState.getProp(ICEBERG_CATALOG_URI);
            String database = sourceState.getProp(ICEBERG_DATABASE_NAME);
            String tableName = sourceState.getProp(ICEBERG_TABLE_NAME);

            DatasetDescriptor source = new DatasetDescriptor(
                DatasetConstants.PLATFORM_ICEBERG,
                URI.create(catalogUri),
                database + "." + tableName
            );

            source.addMetadata("catalog.uri", catalogUri);
            source.addMetadata("table.location", getTableLocation(table));

            this.lineageInfo.get().setSource(source, workUnit);
        }
    }

    /**
     * Get table location from Iceberg table metadata
     * @param table the Iceberg table
     * @return table location or "unknown" if not available
     */
    private String getTableLocation(IcebergTable table) {
        try {
            return table.accessTableMetadata().location();
        } catch (Exception e) {
            return "unknown";
        }
    }

    /**
     * Validate required configuration properties
     */
    private void validateConfiguration(SourceState state) {
        String database = state.getProp(ICEBERG_DATABASE_NAME);
        String table = state.getProp(ICEBERG_TABLE_NAME);
        String catalogUri = state.getProp(ICEBERG_CATALOG_URI);

        if (StringUtils.isBlank(database)) {
            throw new IllegalArgumentException("iceberg.database.name is required");
        }
        if (StringUtils.isBlank(table)) {
            throw new IllegalArgumentException("iceberg.table.name is required");
        }
        if (StringUtils.isBlank(catalogUri)) {
            throw new IllegalArgumentException("iceberg.catalog.uri is required");
        }
    }

    /**
     * Set work unit weight for bin packing based on total file size.
     * Ensures a minimum weight to prevent skew in bin packing.
     *
     * @param workUnit the work unit to set weight on
     * @param totalSize total size of files in bytes
     */
    private void setWorkUnitWeight(WorkUnit workUnit, long totalSize) {
        long weight = Math.max(totalSize, 1L);
        workUnit.setProp(WORK_UNIT_WEIGHT, Long.toString(weight));
    }

    /**
     * Apply bin packing to work units if configured.
     * Groups work units into bins based on size constraints for better resource utilization.
     *
     * @param workUnits initial list of work units
     * @param state source state containing bin packing configuration
     * @return packed work units (or original if bin packing not configured)
     */
    private List<? extends WorkUnit> applyBinPacking(List<WorkUnit> workUnits, SourceState state) {
        long maxSizePerBin = state.getPropAsLong(ICEBERG_MAX_SIZE_MULTI_WORKUNITS, 0);

        if (maxSizePerBin <= 0) {
            log.debug("Bin packing disabled (maxSizePerBin={}), returning original work units", maxSizePerBin);
            return workUnits;
        }

        long maxWorkUnitsPerBin = state.getPropAsLong(ICEBERG_MAX_WORK_UNITS_PER_BIN, 50);
        log.info("Applying bin packing: maxSizePerBin={} bytes, maxWorkUnitsPerBin={}",
            maxSizePerBin, maxWorkUnitsPerBin);

        List<? extends WorkUnit> packedWorkUnits = new WorstFitDecreasingBinPacking(maxSizePerBin)
            .pack(workUnits, this.weighter);

        log.info("Bin packing complete. Initial work units: {}, packed work units: {}, max weight per bin: {}, max work units per bin: {}",
            workUnits.size(), packedWorkUnits.size(), maxSizePerBin, maxWorkUnitsPerBin);

        return packedWorkUnits;
    }

    /**
     * Log simulate mode information - what would be copied without executing.
     * Provides detailed information about files, partitions, and sizes for dry-run validation.
     *
     * @param workUnits work units that would be executed
     * @param filesWithPartitions discovered files with partition metadata
     * @param state source state containing job configuration
     */
    private void logSimulateMode(List<WorkUnit> workUnits,
                                  List<IcebergTable.FilePathWithPartition> filesWithPartitions,
                                  SourceState state) {
        String database = state.getProp(ICEBERG_DATABASE_NAME);
        String table = state.getProp(ICEBERG_TABLE_NAME);

        String separator = StringUtils.repeat("=", 80);
        String dashSeparator = StringUtils.repeat("-", 80);

        log.info(separator);
        log.info("SIMULATE MODE: Iceberg Table Copy Plan");
        log.info(separator);
        log.info("Source Table: {}.{}", database, table);
        log.info("Total Files Discovered: {}", filesWithPartitions.size());
        log.info("Total Work Units: {}", workUnits.size());

        // Calculate total size
        long totalSize = 0L;
        Map<String, Long> partitionSizes = Maps.newLinkedHashMap();

        for (IcebergTable.FilePathWithPartition fileWithPartition : filesWithPartitions) {
            long fileSize = fileWithPartition.getFileSize();
            totalSize += fileSize;

            String partitionPath = fileWithPartition.getPartitionPath();
            if (!partitionPath.isEmpty()) {
                partitionSizes.put(partitionPath, partitionSizes.getOrDefault(partitionPath, 0L) + fileSize);
            }
        }

        log.info("Total Data Size: {} bytes ({} MB)", totalSize, totalSize / (1024 * 1024));

        if (!partitionSizes.isEmpty()) {
            log.info(dashSeparator);
            log.info("Partition Breakdown:");
            for (Map.Entry<String, Long> entry : partitionSizes.entrySet()) {
                long sizeInMB = entry.getValue() / (1024 * 1024);
                log.info("  Partition: {} -> {} bytes ({} MB)", entry.getKey(), entry.getValue(), sizeInMB);
            }
        }

        log.info(dashSeparator);
        log.info("Work Unit Distribution:");
        for (int i = 0; i < Math.min(workUnits.size(), 10); i++) {
            WorkUnit wu = workUnits.get(i);
            String filesToPull = wu.getProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL, "");
            int fileCount = filesToPull.isEmpty() ? 0 : filesToPull.split(",").length;
            long wuSize = wu.getPropAsLong(ServiceConfigKeys.WORK_UNIT_SIZE, 0L);
            log.info("  WorkUnit[{}]: {} files, {} bytes ({} MB)", i, fileCount, wuSize, wuSize / (1024 * 1024));
        }

        if (workUnits.size() > 10) {
            log.info("  ... and {} more work units", workUnits.size() - 10);
        }

        log.info(separator);
        log.info("Simulate mode: No data will be copied. Set iceberg.simulate=false to execute.");
        log.info(separator);
    }

}
