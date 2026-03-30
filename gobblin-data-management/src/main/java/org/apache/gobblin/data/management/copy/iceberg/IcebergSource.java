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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
import org.apache.gobblin.data.management.copy.CopySource;
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

import org.apache.iceberg.expressions.Expressions;

import static org.apache.gobblin.configuration.ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR;
import static org.apache.gobblin.data.management.copy.CopySource.SERIALIZED_COPYABLE_DATASET;

import org.apache.gobblin.data.management.copy.entities.PrePublishStep;
import org.apache.gobblin.util.commit.DeleteFileCommitStep;
import org.apache.gobblin.commit.CommitStep;

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
 * iceberg.catalog.uri=ICEBERG_CATALOG_URI
 *
 * # Partition filtering with lookback - Static date
 * iceberg.filter.enabled=true
 * iceberg.partition.column=datepartition         # Optional, defaults to "datepartition"
 * iceberg.filter.date=2025-04-01                 # Input date in the format specified by iceberg.partition.value.datetime.format
 * iceberg.lookback.days=3
 *
 * # Partition filtering - Scheduled flows (dynamic date)
 * iceberg.filter.enabled=true
 * iceberg.filter.date=CURRENT_DATE               # Resolved to current date at runtime
 * iceberg.lookback.days=3
 *
 * # --- Recommended: configurable partition value format ---
 * # iceberg.partition.value.datetime.format is a DateTimeFormatter pattern applied to the output
 * # partition value used in the filter expression.
 * # When CURRENT_DATE is used, the reference datetime is LocalDateTime.now(), so a pattern
 * # with HH will embed the current hour automatically — no separate hour config needed.
 * # When set, it supersedes iceberg.hourly.partition.enabled.
 *
 * # Standard hourly partitions (yyyy-MM-dd-HH) — CURRENT_DATE picks up live hour
 * iceberg.partition.value.datetime.format=yyyy-MM-dd-HH   # → "2025-04-01-14" (current hour)
 *
 * # Reversed-date hourly partitions (dd-MM-yyyy-HH)
 * iceberg.partition.value.datetime.format=dd-MM-yyyy-HH   # → "01-04-2025-00"
 *
 * # Compact daily partitions (no separator)
 * iceberg.partition.value.datetime.format=yyyyMMdd         # → "20250401"
 *
 * # Daily partitions (standard)
 * iceberg.partition.value.datetime.format=yyyy-MM-dd       # → "2025-04-01"
 *
 * # --- Legacy: hourly flag (backward compat, used when iceberg.partition.value.datetime.format is absent) ---
 * iceberg.hourly.partition.enabled=true           # default true → appends -HH suffix
 * iceberg.hourly.partition.enabled=false          # for plain yyyy-MM-dd partitions
 *
 * # Copy all data (no filtering)
 * iceberg.filter.enabled=false
 * # No filter.date needed - will copy all partitions from current snapshot
 *
 * # Bin packing for better resource utilization
 * gobblin.copy.binPacking.maxSizePerBin=1000000000  # 1GB per bin
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
  public static final String ICEBERG_FILTER_ENABLED = "iceberg.filter.enabled";
  public static final String ICEBERG_FILTER_DATE = "iceberg.filter.date"; // Date value (e.g., 2025-04-01 or CURRENT_DATE)
  public static final String ICEBERG_LOOKBACK_DAYS = "iceberg.lookback.days";
  public static final int DEFAULT_LOOKBACK_DAYS = 1;
  public static final String ICEBERG_LOOKBACK_HOURS = "iceberg.lookback.hours"; // hourly lookback; when > 0 takes precedence over lookback.days
  public static final int DEFAULT_LOOKBACK_HOURS = 0; // 0 = disabled
  public static final String ICEBERG_PARTITION_COLUMN = "iceberg.partition.column"; // configurable partition column name
  public static final String DEFAULT_DATE_PARTITION_COLUMN = "datepartition"; // default date partition column name
  public static final String CURRENT_DATE_PLACEHOLDER = "CURRENT_DATE"; // placeholder for current date
  public static final String ICEBERG_PARTITION_KEY = "iceberg.partition.key";
  public static final String ICEBERG_PARTITION_VALUES = "iceberg.partition.values";
  public static final String ICEBERG_FILE_PARTITION_PATH = "iceberg.file.partition.path";
  public static final String ICEBERG_HOURLY_PARTITION_ENABLED = "iceberg.hourly.partition.enabled";
  public static final boolean DEFAULT_HOURLY_PARTITION_ENABLED = true;
  /**
   * Optional {@link DateTimeFormatter} pattern controlling how the partition value is rendered.
   *
   * <p>When {@code iceberg.filter.date=CURRENT_DATE} the reference datetime is
   * {@link java.time.LocalDateTime#now()}, so a pattern that includes {@code HH} will embed
   * the current clock-hour automatically — no separate hour config is needed.
   * For a specific date (e.g. {@code 2025-04-03}), the time defaults to midnight (00:00).
   *
   * <p>Examples:
   * <ul>
   *   <li>{@code yyyy-MM-dd}      → {@code 2025-04-01}      (daily, no hour)</li>
   *   <li>{@code yyyy-MM-dd-HH}   → {@code 2025-04-01-14}   (hourly; CURRENT_DATE uses live hour)</li>
   *   <li>{@code dd-MM-yyyy-HH}   → {@code 01-04-2025-00}   (reversed-date hourly)</li>
   *   <li>{@code yyyyMMdd}        → {@code 20250401}         (compact daily)</li>
   * </ul>
   *
   * <p>When this property is set it supersedes {@code iceberg.hourly.partition.enabled}.
   * When absent the legacy {@code iceberg.hourly.partition.enabled} behaviour is preserved
   * for backward compatibility.
   */
  public static final String ICEBERG_PARTITION_VALUE_DATETIME_FORMAT = "iceberg.partition.value.datetime.format";
  private static final String WORK_UNIT_WEIGHT = "iceberg.workUnitWeight";

  // Delete configuration - similar to RecursiveCopyableDataset
  public static final String DELETE_FILES_NOT_IN_SOURCE = "iceberg.copy.delete";
  public static final boolean DEFAULT_DELETE_FILES_NOT_IN_SOURCE = true;

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

      List<IcebergTable.FilePathWithPartition> filesWithPartitions = discoverPartitionFilePaths(state, icebergTable);
      log.info("Discovered {} files from Iceberg table {}.{}", filesWithPartitions.size(), database, table);

      // Create work units from discovered files
      List<WorkUnit> workUnits = createWorkUnitsFromFiles(filesWithPartitions, state, icebergTable);

      // Handle simulate mode - log what would be copied without executing
      if (state.contains(CopySource.SIMULATE) && state.getPropAsBoolean(CopySource.SIMULATE)) {
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
   * <p>This method supports three modes:
   * <ol>
   * <li><b>Full table scan (copy all data)</b>: Set {@code iceberg.filter.enabled=false}.
   * Returns all data files from current snapshot. Use this for one-time full copies or backfills.</li>
   * <li><b>Static date partition filter</b>: Set {@code iceberg.filter.enabled=true} with a specific date
   * (e.g., {@code iceberg.filter.date=2025-04-01}). Use this for ad-hoc historical data copies.</li>
   * <li><b>Dynamic date partition filter</b>: Set {@code iceberg.filter.enabled=true} with
   * {@code iceberg.filter.date=CURRENT_DATE}. The {@value #CURRENT_DATE_PLACEHOLDER} placeholder
   * is resolved to the current date at runtime. Use this for daily scheduled flows.</li>
   * </ol>
   *
   * <p>The partition column name is configurable via {@code iceberg.partition.column}
   * (defaults to {@value #DEFAULT_DATE_PARTITION_COLUMN}). The date value is specified separately via
   * {@code iceberg.filter.date} in standard format ({@code yyyy-MM-dd}).
   *
   * <p><b>Partition Value Format:</b> Both the input date ({@code iceberg.filter.date}) and the output
   * partition value use the pattern specified by {@code iceberg.partition.value.datetime.format}
   * (a standard {@link java.time.format.DateTimeFormatter} pattern).  Use {@code CURRENT_DATE} as the
   * date value to resolve the reference datetime to {@link java.time.LocalDateTime#now()} automatically,
   * embedding the current hour when the pattern includes {@code HH}. Examples:
   * <ul>
   *   <li>{@code yyyy-MM-dd-HH} with date {@code 2025-04-01-05} → {@code 2025-04-01-05}</li>
   *   <li>{@code dd-MM-yyyy-HH} with date {@code 01-04-2025-00} → {@code 01-04-2025-00}</li>
   *   <li>{@code yyyyMMdd}      with date {@code 20250401}       → {@code 20250401} (compact daily)</li>
   * </ul>
   * When {@code iceberg.partition.value.datetime.format} is set it supersedes
   * {@code iceberg.hourly.partition.enabled}. When absent, the legacy
   * {@code iceberg.hourly.partition.enabled} behaviour is preserved for backward compatibility.
   *
   * <p><b>Configuration Examples:</b>
   * <ul>
   * <li>Standard daily: {@code iceberg.partition.value.datetime.format=yyyy-MM-dd, iceberg.filter.date=2025-04-03,
   *     iceberg.lookback.days=3} → partitions: {@code 2025-04-03, 2025-04-02, 2025-04-01}</li>
   * <li>Reversed-date hourly: {@code iceberg.partition.value.datetime.format=dd-MM-yyyy-HH,
   *     iceberg.filter.date=CURRENT_DATE} → {@code 03-04-2025-14, 02-04-2025-14, 01-04-2025-14}</li>
   * <li>Dynamic daily: {@code iceberg.filter.date=CURRENT_DATE, iceberg.lookback.days=1}
   *     → today's partition only (resolved at runtime)</li>
   * </ul>
   *
   * @param state source state containing filter configuration
   * @param icebergTable the Iceberg table to scan
   * @return list of file paths with partition metadata matching the filter criteria
   * @throws IOException if table scan or file discovery fails
   */
  private List<IcebergTable.FilePathWithPartition> discoverPartitionFilePaths(SourceState state, IcebergTable icebergTable) throws IOException {
    boolean filterEnabled = state.getPropAsBoolean(ICEBERG_FILTER_ENABLED, true);

    if (!filterEnabled) {
      log.info("Partition filter disabled, discovering all data files with partition metadata from current snapshot");
      // Use TableScan without filter to get all files with partition metadata preserved
      // This ensures partition structure is maintained even for full table copies
      List<IcebergTable.FilePathWithPartition> result = icebergTable.getFilePathsWithPartitionsForFilter(Expressions.alwaysTrue());
      log.info("Discovered {} data files from current snapshot with partition metadata", result.size());
      return result;
    }

    String datePartitionColumn = state.getProp(ICEBERG_PARTITION_COLUMN, DEFAULT_DATE_PARTITION_COLUMN);

    String dateValue = state.getProp(ICEBERG_FILTER_DATE);
    Preconditions.checkArgument(!StringUtils.isBlank(dateValue),
      "iceberg.filter.date is required when iceberg.filter.enabled=true");

    // Resolve the DateTimeFormatter used to render each partition value.
    // resolvePartitionFormatter normalises both the new iceberg.partition.value.datetime.format
    // path and the legacy iceberg.hourly.partition.enabled path into a single formatter.
    DateTimeFormatter partitionFormatter = resolvePartitionFormatter(state);

    // Resolve the reference datetime for the filter.
    // CURRENT_DATE uses LocalDateTime.now() so a formatter pattern that includes HH will
    // embed the current clock-hour automatically.  For a specific date (yyyy-MM-dd) the time
    // defaults to midnight (00:00).
    LocalDateTime startDateTime;
    if (CURRENT_DATE_PLACEHOLDER.equalsIgnoreCase(dateValue)) {
      startDateTime = LocalDateTime.now();
      log.info("Resolved {} placeholder to current datetime: {}", CURRENT_DATE_PLACEHOLDER, startDateTime);
    } else {
      // When iceberg.partition.value.datetime.format is explicitly set, the input date must match
      // that pattern (consistent input/output format).  Legacy path keeps accepting yyyy-MM-dd for
      // backward compatibility.
      boolean isCustomFormat = state.contains(ICEBERG_PARTITION_VALUE_DATETIME_FORMAT);
      String patternForError = isCustomFormat
          ? state.getProp(ICEBERG_PARTITION_VALUE_DATETIME_FORMAT)
          : (state.getPropAsBoolean(ICEBERG_HOURLY_PARTITION_ENABLED, DEFAULT_HOURLY_PARTITION_ENABLED)
              ? "yyyy-MM-dd-HH" : "yyyy-MM-dd");
      try {
        if (isCustomFormat) {
          try {
            startDateTime = LocalDateTime.parse(dateValue, partitionFormatter);
          } catch (java.time.format.DateTimeParseException ex) {
            // Format may be date-only (e.g. yyyyMMdd) — fall back to LocalDate + midnight
            startDateTime = LocalDate.parse(dateValue, partitionFormatter).atStartOfDay();
          }
        } else {
          startDateTime = LocalDate.parse(dateValue).atStartOfDay();
        }
      } catch (java.time.format.DateTimeParseException e) {
        String errorMsg = String.format(
          "Invalid date format for '%s': '%s'. Expected format matching pattern '%s'. Error: %s",
          ICEBERG_FILTER_DATE, dateValue, patternForError, e.getMessage());
        log.error(errorMsg);
        throw new IllegalArgumentException(errorMsg, e);
      }
    }

    // Delegate partition value list + OR expression to IcebergPartitionFilterGenerator.
    // When iceberg.lookback.hours > 0 it takes precedence over iceberg.lookback.days.
    int lookbackHours = state.getPropAsInt(ICEBERG_LOOKBACK_HOURS, DEFAULT_LOOKBACK_HOURS);
    int lookbackDays  = state.getPropAsInt(ICEBERG_LOOKBACK_DAYS, DEFAULT_LOOKBACK_DAYS);

    if (lookbackHours > 0) {
      Preconditions.checkArgument(lookbackHours <= 48,
          "iceberg.lookback.hours must be <= 48 (2 days), got: %s", lookbackHours);
    }

    IcebergPartitionFilterGenerator.FilterResult filterResult;
    if (lookbackHours > 0) {
      if (state.contains(ICEBERG_LOOKBACK_DAYS)) {
        log.warn("Both {} ({}) and {} ({}) are set; {} takes precedence",
          ICEBERG_LOOKBACK_HOURS, lookbackHours, ICEBERG_LOOKBACK_DAYS, lookbackDays,
          ICEBERG_LOOKBACK_HOURS);
      }
      log.info("Hourly lookback: {} hours for column '{}' starting at {}",
        lookbackHours, datePartitionColumn, startDateTime);
      filterResult = IcebergPartitionFilterGenerator.forHours(
        datePartitionColumn, startDateTime, lookbackHours, partitionFormatter);
    } else {
      Preconditions.checkArgument(lookbackDays >= 1,
        "iceberg.lookback.days must be >= 1, got: %s", lookbackDays);
      log.info("Daily lookback: {} day(s) for column '{}' starting at {}",
        lookbackDays, datePartitionColumn, startDateTime);
      filterResult = IcebergPartitionFilterGenerator.forDays(
        datePartitionColumn, startDateTime, lookbackDays, partitionFormatter);
    }

    // Store partition info on state for downstream use (extractor, destination path mapping).
    state.setProp(ICEBERG_PARTITION_KEY, datePartitionColumn);
    state.setProp(ICEBERG_PARTITION_VALUES, String.join(",", filterResult.getPartitionValues()));

    log.info("Executing TableScan with filter: {}={}",
      datePartitionColumn, filterResult.getPartitionValues());
    List<IcebergTable.FilePathWithPartition> filesWithPartitions =
      icebergTable.getFilePathsWithPartitionsForFilter(filterResult.getFilterExpression());
    log.info("Discovered {} data files for partitions: {}",
      filesWithPartitions.size(), filterResult.getPartitionValues());

    return filesWithPartitions;
  }

  /**
   * Resolve the {@link DateTimeFormatter} used to render each partition value string.
   *
   * <p>Resolution order:
   * <ol>
   *   <li>If {@code iceberg.partition.value.datetime.format} is set, parse it as a
   *       {@link DateTimeFormatter} pattern and return it.  This path supersedes the
   *       legacy hourly flag.</li>
   *   <li>Otherwise fall back to the legacy {@code iceberg.hourly.partition.enabled} flag:
   *       {@code true} (default) → {@code yyyy-MM-dd-HH}; {@code false} → {@code yyyy-MM-dd}.</li>
   * </ol>
   *
   * @param state source state containing configuration properties
   * @return resolved {@link DateTimeFormatter}
   * @throws IllegalArgumentException if {@code iceberg.partition.value.datetime.format} contains an
   *                                  invalid pattern
   */
  private DateTimeFormatter resolvePartitionFormatter(SourceState state) {
    String valueFormat = state.getProp(ICEBERG_PARTITION_VALUE_DATETIME_FORMAT);
    if (valueFormat != null) {
      try {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(valueFormat);
        log.info("Using configurable partition value datetime format='{}'", valueFormat);
        return formatter;
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
          String.format("Invalid %s pattern '%s': %s",
            ICEBERG_PARTITION_VALUE_DATETIME_FORMAT, valueFormat, e.getMessage()), e);
      }
    }
    // Legacy path: iceberg.hourly.partition.enabled drives the pattern.
    boolean isHourlyPartition = state.getPropAsBoolean(ICEBERG_HOURLY_PARTITION_ENABLED, DEFAULT_HOURLY_PARTITION_ENABLED);
    String pattern = isHourlyPartition ? "yyyy-MM-dd-HH" : "yyyy-MM-dd";
    log.info("Using legacy partition mode: hourlyEnabled={}, pattern='{}'", isHourlyPartition, pattern);
    return DateTimeFormatter.ofPattern(pattern);
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
  private List<WorkUnit> createWorkUnitsFromFiles(
      List<IcebergTable.FilePathWithPartition> filesWithPartitions, SourceState state, IcebergTable table) throws IOException {
    List<WorkUnit> workUnits = Lists.newArrayList();

    if (filesWithPartitions.isEmpty()) {
      log.warn("No files discovered for table {}.{}, returning empty work unit list",
        state.getProp(ICEBERG_DATABASE_NAME), state.getProp(ICEBERG_TABLE_NAME));
      return workUnits;
    }

    String nameSpace = state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY, "iceberg");
    String tableName = table.getTableId().name();
    Extract extract = new Extract(Extract.TableType.SNAPSHOT_ONLY, nameSpace, tableName);

    String datasetUrn = table.getTableId().toString();
    long totalSize = 0L;

    for (IcebergTable.FilePathWithPartition fileWithPartition : filesWithPartitions) {
      WorkUnit workUnit = new WorkUnit(extract);
      String filePath = fileWithPartition.getFilePath();
      totalSize += fileWithPartition.getFileSize();

      // Store partition path for each file
      Map<String, String> fileToPartitionPath = Maps.newHashMap();
      fileToPartitionPath.put(filePath, fileWithPartition.getPartitionPath());
      workUnit.setProp(ConfigurationKeys.DATASET_URN_KEY, datasetUrn);

      workUnit.setProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL, filePath);
      // Serialized copyable dataset / file is not required during work unit generation step.
      // Copyable file is created during process work unit step (IcebergFileStreamExtractor)
      workUnit.setProp(SERIALIZED_COPYABLE_DATASET, "{}");

      // Store partition path mapping as JSON for extractor to use
      workUnit.setProp(ICEBERG_FILE_PARTITION_PATH, new com.google.gson.Gson().toJson(fileToPartitionPath));

      // Set work unit size for dynamic scaling (instead of just file count)
      workUnit.setProp(ServiceConfigKeys.WORK_UNIT_SIZE, fileWithPartition.getFileSize());

      // Set work unit weight for bin packing
      setWorkUnitWeight(workUnit, fileWithPartition.getFileSize());

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
    }
    log.info("Created {} work unit(s), total size: {} bytes", workUnits.size(), totalSize);

    // Add delete step to overwrite partitions
    addDeleteStepIfNeeded(state, workUnits, extract, datasetUrn);

    return workUnits;
  }

  /**
   * Creates a PrePublishStep with DeleteFileCommitStep to delete ALL files in impacted directories.
   * This enables complete partition rewrites - all existing files in target partitions are deleted
   * before new files are copied from source.
   *
   * Execution Order:
   * 1. Source Phase (this method): Identifies directories to delete and creates PrePublishStep
   * 2. Task Execution: Files are copied from source to staging directory
   * 3. Publisher Phase - PrePublishStep: Deletes ALL files from target directories (BEFORE rename)
   * 4. Publisher Phase - Rename: Moves files from staging to final target location
   *
   * Behavior:
   * 1. If filter is enabled (iceberg.filter.enabled=true): Deletes ALL files in specific partition
   *    directories based on source partition values. For example, if source has partitions 2025-10-11,
   *    2025-10-10, 2025-10-09, it will delete ALL files in those partition directories in target.
   * 2. If filter is disabled (iceberg.filter.enabled=false): Deletes ALL files in the entire root directory.
   * 3. No file comparison - this is a complete rewrite of the impacted directories.
   */
  private void addDeleteStepIfNeeded(SourceState state, List<WorkUnit> workUnits, Extract extract, String datasetUrn) throws IOException {
    boolean deleteEnabled = state.getPropAsBoolean(DELETE_FILES_NOT_IN_SOURCE, DEFAULT_DELETE_FILES_NOT_IN_SOURCE);
    if (!deleteEnabled || workUnits.isEmpty()) {
      log.info("Delete not enabled or no work units created, skipping delete step");
      return;
    }

    // Get target filesystem and directory
    String targetRootDir = state.getProp(DATA_PUBLISHER_FINAL_DIR);
    if (targetRootDir == null) {
      log.warn("DATA_PUBLISHER_FINAL_DIR not configured, cannot determine directories to delete");
      return;
    }

    try {
      FileSystem targetFs = HadoopUtils.getWriterFileSystem(state, 1, 0);
      Path targetRootPath = new Path(targetRootDir);

      if (!targetFs.exists(targetRootPath)) {
        log.info("Target directory {} does not exist, no directories to delete", targetRootPath);
        return;
      }

      // Determine which directories to delete based on filter configuration
      List<Path> directoriesToDelete = Lists.newArrayList();
      boolean filterEnabled = state.getPropAsBoolean(ICEBERG_FILTER_ENABLED, true);

      if (!filterEnabled) {
        // No filter: Delete entire root directory to rewrite all data
        log.info("Filter disabled - will delete entire root directory: {}", targetRootPath);
        directoriesToDelete.add(targetRootPath);
      } else {
        // Filter enabled: Delete only specific partition directories
        String partitionColumn = state.getProp(ICEBERG_PARTITION_KEY);
        String partitionValuesStr = state.getProp(ICEBERG_PARTITION_VALUES);

        if (partitionColumn == null || partitionValuesStr == null) {
          log.warn("Partition key or values not found in state, cannot determine partition directories to delete");
          return;
        }

        // Parse partition values (comma-separated list from lookback calculation)
        // These values already include hourly suffix if applicable (e.g., "2025-10-11-00,2025-10-10-00,2025-10-09-00")
        String[] values = partitionValuesStr.split(",");
        log.info("Filter enabled - will delete {} partition directories for {}={}",
            values.length, partitionColumn, partitionValuesStr);

        // Collect partition directories to delete
        for (String value : values) {
          String trimmedValue = value.trim();
          // Construct partition directory path: targetRoot/partitionColumn=value/
          // Example: /root/datepartition=2025-10-11-00/
          Path partitionDir = new Path(targetRootPath, partitionColumn + "=" + trimmedValue);

          if (targetFs.exists(partitionDir)) {
            log.info("Found partition directory to delete: {}", partitionDir);
            directoriesToDelete.add(partitionDir);
          } else {
            log.info("Partition directory does not exist in target: {}", partitionDir);
          }
        }
      }

      if (directoriesToDelete.isEmpty()) {
        log.info("No directories to delete in target directory {}", targetRootPath);
        return;
      }

      // Delete directories (and all their contents) for complete overwrite
      // DeleteFileCommitStep will recursively delete all files within these directories
      log.info("Will delete {} for complete overwrite", directoriesToDelete.size());

      // Log directories to be deleted
      for (Path dir : directoriesToDelete) {
        log.info("Will delete directory: {}", dir);
      }

      // Create DeleteFileCommitStep to delete directories recursively
      // Note: deleteEmptyDirs is not needed since we're deleting entire directories
      CommitStep deleteStep = DeleteFileCommitStep.fromPaths(targetFs, directoriesToDelete, state.getProperties());

      // Create a dedicated work unit for the delete step
      WorkUnit deleteWorkUnit = new WorkUnit(extract);
      deleteWorkUnit.addAll(state);

      // Set properties so extractor knows this is a delete-only work unit (no files to copy)
      deleteWorkUnit.setProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL, "");
      deleteWorkUnit.setProp(SERIALIZED_COPYABLE_DATASET, "{}");
      deleteWorkUnit.setProp(ConfigurationKeys.DATASET_URN_KEY, datasetUrn);
      deleteWorkUnit.setProp(ICEBERG_FILE_PARTITION_PATH, "{}");
      setWorkUnitWeight(deleteWorkUnit, 0);

      // Use PrePublishStep to delete BEFORE copying new files
      PrePublishStep prePublishStep = new PrePublishStep(datasetUrn, Maps.newHashMap(), deleteStep, 0);

      // Serialize the PrePublishStep as a CopyEntity
      CopySource.serializeCopyEntity(deleteWorkUnit, prePublishStep);
      workUnits.add(deleteWorkUnit);

      log.info("Added PrePublishStep with DeleteFileCommitStep to work units");

    } catch (Exception e) {
      log.error("Failed to create delete step", e);
      throw new IOException("Failed to create delete step", e);
    }
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
      source.addMetadata(DatasetConstants.FS_URI, sourceState.getProp(ConfigurationKeys.FS_URI_KEY));

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
    long maxSizePerBin = state.getPropAsLong(CopySource.MAX_SIZE_MULTI_WORKUNITS, 0);

    if (maxSizePerBin <= 0) {
      log.info("Bin packing disabled (maxSizePerBin={}), returning original work units", maxSizePerBin);
      return workUnits;
    }

    log.info("Applying bin packing with maxSizePerBin={} bytes", maxSizePerBin);

    List<? extends WorkUnit> packedWorkUnits = new WorstFitDecreasingBinPacking(maxSizePerBin)
      .pack(workUnits, this.weighter);

    log.info("Bin packing complete. Initial work units: {}, packed work units: {}, max size per bin: {} bytes",
      workUnits.size(), packedWorkUnits.size(), maxSizePerBin);

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
