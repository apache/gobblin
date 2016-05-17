/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.data.management.copy.hive;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import com.google.gson.Gson;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import gobblin.commit.CommitStep;
import gobblin.configuration.State;
import gobblin.data.management.copy.CopyConfiguration;
import gobblin.data.management.copy.CopyEntity;
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.entities.PostPublishStep;
import gobblin.data.management.copy.entities.PrePublishStep;
import gobblin.data.management.copy.hive.avro.HiveAvroCopyEntityHelper;
import gobblin.data.management.partition.FileSet;
import gobblin.hive.HiveMetastoreClientPool;
import gobblin.hive.HiveRegProps;
import gobblin.hive.HiveRegisterStep;
import gobblin.hive.PartitionDeregisterStep;
import gobblin.hive.TableDeregisterStep;
import gobblin.hive.metastore.HiveMetaStoreUtils;
import gobblin.hive.spec.HiveSpec;
import gobblin.hive.spec.SimpleHiveSpec;
import gobblin.metrics.event.EventSubmitter;
import gobblin.metrics.event.MultiTimingEvent;
import gobblin.util.PathUtils;
import gobblin.util.commit.DeleteFileCommitStep;
import gobblin.util.reflection.GobblinConstructorUtils;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Singular;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;


/**
 * Creates {@link CopyEntity}s for copying a Hive table.
 */
@Slf4j
@Getter
public class HiveCopyEntityHelper {

  /**
   * Specifies a root path for the data in a table. All files containing table data will be placed under this directory.
   * <p>
   *   Does some token replacement in the input path. For example, if the table myTable is in DB myDatabase:
   *   /data/$DB/$TABLE -> /data/myDatabase/myTable.
   *   /data/$TABLE -> /data/myTable
   *   /data -> /data/myTable
   * </p>
   *
   * See javadoc for {@link #getCopyEntities} for further explanation.
   */
  public static final String COPY_TARGET_TABLE_ROOT =
      HiveDatasetFinder.HIVE_DATASET_PREFIX + ".copy.target.table.root";
  
  /**
   * These two options, in pair, specify the output location of the data files on copy
   * {@link #COPY_TARGET_TABLE_PREFIX_TOBE_REPLACED} specified the prefix of the path (without Scheme and Authority ) to be replaced
   * {@link #COPY_TARGET_TABLE_PREFIX_REPLACEMENT} specified the replacement of {@link #COPY_TARGET_TABLE_PREFIX_TOBE_REPLACED}
   * <p>
   * for example, if the data file is $sourceFs/data/databases/DB/Table/Snapshot/part-00000.avro , 
   * {@link #COPY_TARGET_TABLE_PREFIX_TOBE_REPLACED} is /data/databases
   * {@link #COPY_TARGET_TABLE_PREFIX_REPLACEMENT} is /data/databases/_parallel
   * 
   * then, the output location for that file will be
   * $targetFs/data/databases/_parallel/DB/Table/Snapshot/part-00000.avro
   * </p>
   */
  public static final String COPY_TARGET_TABLE_PREFIX_TOBE_REPLACED =
	      HiveDatasetFinder.HIVE_DATASET_PREFIX + ".copy.target.table.prefixToBeReplaced";
  public static final String COPY_TARGET_TABLE_PREFIX_REPLACEMENT =
	      HiveDatasetFinder.HIVE_DATASET_PREFIX + ".copy.target.table.prefixReplacement";
  
  /**
   * Specifies that, on copy, data files for this table should all be relocated to a single directory per partition.
   * See javadoc for {@link #getCopyEntities} for further explanation.
   */
  public static final String RELOCATE_DATA_FILES_KEY =
      HiveDatasetFinder.HIVE_DATASET_PREFIX + ".copy.relocate.data.files";
  public static final String DEFAULT_RELOCATE_DATA_FILES = Boolean.toString(false);
  public static final String EXISTING_ENTITY_POLICY_KEY = HiveDatasetFinder.HIVE_DATASET_PREFIX +
      ".existing.entity.conflict.policy";
  public static final String DEFAULT_EXISTING_ENTITY_POLICY = ExistingEntityPolicy.ABORT.name();
  /** Target metastore URI */
  public static final String TARGET_METASTORE_URI_KEY = HiveDatasetFinder.HIVE_DATASET_PREFIX + ".copy.target.metastore.uri";
  /** Target database name */
  public static final String TARGET_DATABASE_KEY = HiveDatasetFinder.HIVE_DATASET_PREFIX + ".copy.target.database";

  /** A filter to select partitions to copy */
  public static final String COPY_PARTITIONS_FILTER_CONSTANT =
      HiveDatasetFinder.HIVE_DATASET_PREFIX + ".copy.partition.filter.constant";
  /** Use an implementation of {@link PartitionFilterGenerator} to dynamically create partition filter. The value should
   * be the name of the implementation to use. */
  public static final String COPY_PARTITION_FILTER_GENERATOR =
      HiveDatasetFinder.HIVE_DATASET_PREFIX + ".copy.partition.filter.generator";
  /** A predicate applied to each partition before any file listing.
   * If the predicate returns true, the partition will be skipped. */
  public static final String FAST_PARTITION_SKIP_PREDICATE =
      HiveDatasetFinder.HIVE_DATASET_PREFIX + ".copy.fast.partition.skip.predicate";
  /** Method for deleting files on deregister. One of {@link DeregisterFileDeleteMethod}. */
  public static final String DELETE_FILES_ON_DEREGISTER =
      HiveDatasetFinder.HIVE_DATASET_PREFIX + ".copy.deregister.fileDeleteMethod";
  public static final DeregisterFileDeleteMethod DEFAULT_DEREGISTER_DELETE_METHOD = DeregisterFileDeleteMethod.NO_DELETE;

  private static final String databaseToken = "$DB";
  private static final String tableToken = "$TABLE";

  private static final Gson gson = new Gson();

  private static final String source_client = "source_client";
  private static final String target_client = "target_client";
  public static final String GOBBLIN_DISTCP = "gobblin-distcp";

  public static class Stages {
    public static final String EXISTING_PARTITION = "ExistingPartition";
    public static final String PARTITION_SKIP_PREDICATE = "PartitionSkipPredicate";
    public static final String CREATE_LOCATIONS = "CreateLocations";
    public static final String FULL_PATH_DIFF = "FullPathDiff";
    public static final String CREATE_DELETE_UNITS = "CreateDeleteUnits";
    public static final String CREATE_COPY_UNITS = "CreateCopyUnits";
    public static final String SOURCE_PATH_LISTING = "SourcePathListing";
    public static final String TARGET_EXISTING_PATH_LISTING = "TargetExistingPathListing";
    public static final String DESIRED_PATHS_LISTING = "DesiredPathsListing";
    public static final String PATH_DIFF = "PathDiff";
    public static final String COMPUTE_DELETE_PATHS = "ComputeDeletePaths";
    public static final String GET_TABLES = "GetTables";
    public static final String COMPUTE_TARGETS = "ComputeTargets";
  }

  private final long startTime;

  private final HiveDataset dataset;
  private final CopyConfiguration configuration;
  private final FileSystem targetFs;

  private final Optional<Path> targetTableRoot;
  private final Optional<Path> targetTablePrefixTobeReplaced;
  private final Optional<Path> targetTablePrefixReplacement;
  private final boolean relocateDataFiles;
  private final HiveMetastoreClientPool targetClientPool;
  private final String targetDatabase;
  private final HiveRegProps hiveRegProps;
  private Optional<Table> existingTargetTable;
  private final Table targetTable;
  private final Optional<String> targetURI;
  private final ExistingEntityPolicy existingEntityPolicy;
  private final Optional<String> partitionFilter;
  private final Optional<Predicate<PartitionCopy>> fastPartitionSkip;
  
  private final DeregisterFileDeleteMethod deleteMethod;

  private final Optional<CommitStep> tableRegistrationStep;
  private final Map<List<String>, Partition> sourcePartitions;
  private final Map<List<String>, Partition> targetPartitions;

  private final EventSubmitter eventSubmitter;

  /**
   * Defines what should be done for partitions that exist in the target but are not compatible with the source.
   */
  public enum ExistingEntityPolicy {
    /** Deregister target partition, delete its files, and create a new partition with correct values. */
    REPLACE_PARTITIONS,
    /** Deregister target table, do NOT delete its files, and create a new table with correct values. */
    REPLACE_TABLE,
    /** Abort copying of conflict table. */
    ABORT
  }

  public enum DeregisterFileDeleteMethod {
    /** Delete the files pointed at by the input format. */
    INPUT_FORMAT,
    /** Delete all files at the partition location recursively. */
    RECURSIVE,
    /** Don't delete files, just deregister partition. */
    NO_DELETE
  }

  /**
   * A container for the differences between desired and existing files.
   */
  @Builder
  @ToString
  protected static class DiffPathSet {
    /** Desired files that don't exist on target */
    @Singular(value = "copyFile") Collection<FileStatus> filesToCopy;
    /** Files in target that are not desired */
    @Singular(value = "deleteFile") Collection<Path> pathsToDelete;
  }

  /**
   * Represents a source {@link FileStatus} and a {@link Path} destination.
   */
  @Data
  private static class SourceAndDestination {
    private final FileStatus source;
    private final Path destination;
  }

  HiveCopyEntityHelper(HiveDataset dataset, CopyConfiguration configuration, FileSystem targetFs) throws IOException {

    try (Closer closer = Closer.create()) {
      log.info("Finding copy entities for table " + dataset.table.getCompleteName());

      this.eventSubmitter = new EventSubmitter.Builder(dataset.getMetricContext(), "hive.dataset.copy").build();
      MultiTimingEvent multiTimer = closer.register(new MultiTimingEvent(this.eventSubmitter, "HiveCopySetup", true));

      this.startTime = System.currentTimeMillis();

      this.dataset = dataset;
      this.configuration = configuration;
      this.targetFs = targetFs;

      this.relocateDataFiles =
          Boolean.valueOf(this.dataset.getProperties().getProperty(RELOCATE_DATA_FILES_KEY, DEFAULT_RELOCATE_DATA_FILES));
      this.targetTableRoot = this.dataset.getProperties().containsKey(COPY_TARGET_TABLE_ROOT) ? Optional.of(
          resolvePath(this.dataset.getProperties().getProperty(COPY_TARGET_TABLE_ROOT), this.dataset.table.getDbName(), this.dataset.table.getTableName())) : Optional.<Path>absent();

      
      this.targetTablePrefixTobeReplaced = this.dataset.getProperties().containsKey(COPY_TARGET_TABLE_PREFIX_TOBE_REPLACED) ? 
    		  Optional.of( new Path(this.dataset.getProperties().getProperty(COPY_TARGET_TABLE_PREFIX_TOBE_REPLACED))) : Optional.<Path>absent();

      this.targetTablePrefixReplacement = this.dataset.getProperties().containsKey(COPY_TARGET_TABLE_PREFIX_REPLACEMENT) ?
    		  Optional.of( new Path(this.dataset.getProperties().getProperty(COPY_TARGET_TABLE_PREFIX_REPLACEMENT))) : Optional.<Path>absent();

      this.hiveRegProps = new HiveRegProps(new State(this.dataset.getProperties()));
      this.targetURI = Optional.fromNullable(this.dataset.getProperties().getProperty(TARGET_METASTORE_URI_KEY));
      this.targetClientPool = HiveMetastoreClientPool.get(this.dataset.getProperties(), this.targetURI);
      this.targetDatabase = Optional.fromNullable(this.dataset.getProperties().getProperty(TARGET_DATABASE_KEY)).
          or(this.dataset.table.getDbName());
      this.existingEntityPolicy = ExistingEntityPolicy.valueOf(
          this.dataset.getProperties().getProperty(EXISTING_ENTITY_POLICY_KEY, DEFAULT_EXISTING_ENTITY_POLICY).toUpperCase());
      
      this.deleteMethod = this.dataset.getProperties().containsKey(DELETE_FILES_ON_DEREGISTER)
          ? DeregisterFileDeleteMethod.valueOf(this.dataset.getProperties().getProperty(DELETE_FILES_ON_DEREGISTER).toUpperCase())
          : DEFAULT_DEREGISTER_DELETE_METHOD;

      if (this.dataset.getProperties().containsKey(COPY_PARTITION_FILTER_GENERATOR)) {
        try {
          PartitionFilterGenerator generator = GobblinConstructorUtils.invokeFirstConstructor(
              (Class<PartitionFilterGenerator>) Class.forName(this.dataset.getProperties().getProperty(COPY_PARTITION_FILTER_GENERATOR)),
              Lists.<Object>newArrayList(this.dataset.getProperties()), Lists.newArrayList());
          this.partitionFilter = Optional.of(generator.getFilter(this.dataset));
          log.info(String.format("Dynamic partition filter for table %s: %s.", this.dataset.table.getCompleteName(),
              this.partitionFilter.get()));
        } catch (ReflectiveOperationException roe) {
          throw new IOException(roe);
        }
      } else {
        this.partitionFilter = Optional.fromNullable(this.dataset.getProperties().getProperty(COPY_PARTITIONS_FILTER_CONSTANT));
      }

      try {
        this.fastPartitionSkip = this.dataset.getProperties().containsKey(FAST_PARTITION_SKIP_PREDICATE) ? Optional.of(
            GobblinConstructorUtils.invokeFirstConstructor((Class<Predicate<PartitionCopy>>) Class.forName(this.dataset.getProperties().getProperty(FAST_PARTITION_SKIP_PREDICATE)),
                Lists.<Object>newArrayList(this), Lists.newArrayList())) : Optional.<Predicate<PartitionCopy>>absent();
      } catch (ReflectiveOperationException roe) {
        closer.close();
        throw new IOException(roe);
      }

      Map<String, HiveMetastoreClientPool> namedPools =
          ImmutableMap.of(source_client, this.dataset.clientPool, target_client, this.targetClientPool);

      multiTimer.nextStage(Stages.GET_TABLES);
      try (HiveMetastoreClientPool.MultiClient multiClient = HiveMetastoreClientPool.safeGetClients(namedPools)) {

        if (multiClient.getClient(target_client).tableExists(targetDatabase, this.dataset.table.getTableName())) {
          this.existingTargetTable =
              Optional.of(new Table(multiClient.getClient(target_client).getTable(targetDatabase, this.dataset.table.getTableName())));
        } else {
          this.existingTargetTable = Optional.absent();
        }

        Path targetPath = getTargetLocation(dataset.fs, this.targetFs, dataset.table.getDataLocation(), Optional.<Partition>absent());
        this.targetTable = getTargetTable(this.dataset.table, targetPath);
        HiveSpec tableHiveSpec = new SimpleHiveSpec.Builder<>(targetPath).
            withTable(HiveMetaStoreUtils.getHiveTable(this.targetTable.getTTable())).build();
        CommitStep tableRegistrationStep = new HiveRegisterStep(targetURI, tableHiveSpec, this.hiveRegProps);

        this.tableRegistrationStep = Optional.of(tableRegistrationStep);

        if (this.existingTargetTable.isPresent() && this.existingTargetTable.get().isPartitioned()) {
          checkPartitionedTableCompatibility(this.targetTable, this.existingTargetTable.get()); 
        }

        if (HiveUtils.isPartitioned(this.dataset.table)) {
          this.sourcePartitions =
              HiveUtils.getPartitionsMap(multiClient.getClient(source_client), this.dataset.table, this.partitionFilter);
          this.targetPartitions = this.existingTargetTable.isPresent() ? HiveUtils.getPartitionsMap(multiClient.getClient(target_client),
              this.existingTargetTable.get(), this.partitionFilter) : Maps.<List<String>, Partition>newHashMap();
        } else {
          this.sourcePartitions = Maps.newHashMap();
          this.targetPartitions = Maps.newHashMap();
        }
      } catch (TException te) {
        closer.close();
        throw new IOException("Failed to generate work units for table " + dataset.table.getCompleteName(), te);
      }
    }
  }

  /**
   * Finds all files read by the table and generates {@link CopyEntity}s for duplicating the table. The semantics are as follows:
   * 1. Find all valid {@link org.apache.hadoop.hive.metastore.api.StorageDescriptor}. If the table is partitioned, the
   *    {@link org.apache.hadoop.hive.metastore.api.StorageDescriptor} of the base
   *    table will be ignored, and we will instead process the {@link org.apache.hadoop.hive.metastore.api.StorageDescriptor} of each partition.
   * 2. For each {@link org.apache.hadoop.hive.metastore.api.StorageDescriptor} find all files referred by it.
   * 3. Generate a {@link CopyableFile} for each file referred by a {@link org.apache.hadoop.hive.metastore.api.StorageDescriptor}.
   * 4. If the table is partitioned, create a file set for each partition.
   * 5. Create work units for registering, deregistering partitions / tables, and deleting unnecessary files in the target.
   *
   * <p>
   *   The target locations of data files for this table depend on the values of the resolved table root (e.g.
   *   the value of {@link #COPY_TARGET_TABLE_ROOT} with tokens replaced) and {@link #RELOCATE_DATA_FILES_KEY}:
   *   * if {@link #RELOCATE_DATA_FILES_KEY} is true, then origin file /path/to/file/myFile will be written to
   *     /resolved/table/root/<partition>/myFile
   *   * otherwise, if the resolved table root is defined (e.g. {@link #COPY_TARGET_TABLE_ROOT} is defined in the
   *     properties), we define:
   *     origin_table_root := the deepest non glob ancestor of table.getSc().getLocation() iff getLocation() points to
   *                           a single glob. (e.g. /path/to/*&#47;files -> /path/to). If getLocation() contains none
   *                           or multiple globs, job will fail.
   *     relative_path := path of the file relative to origin_table_root. If the path of the file is not a descendant
   *                      of origin_table_root, job will fail.
   *     target_path := /resolved/table/root/relative/path
   *     This mode is useful when moving a table with a complicated directory structure to a different base directory.
   *   * if {@link #COPY_TARGET_TABLE_ROOT} is not defined in the properties, then the target is identical to the origin
   *     path.
   * </p>
   */
  Iterator<FileSet<CopyEntity>> getCopyEntities() throws IOException {
    if (HiveUtils.isPartitioned(this.dataset.table)) {
      return new PartitionIterator(this.sourcePartitions);
    } else {
      FileSet<CopyEntity> fileSet = new FileSet.Builder<>(this.dataset.table.getCompleteName(), this.dataset).
          add(getCopyEntitiesForUnpartitionedTable()).build();
      return Iterators.singletonIterator(fileSet);
    }
  }

  /**
   * An iterator producing a {@link FileSet} of {@link CopyEntity} for each partition in this table. The files
   * are not scanned or the {@link FileSet} materialized until {@link #next} is called.
   */
  private class PartitionIterator implements Iterator<FileSet<CopyEntity>> {

    private final Iterator<Map.Entry<List<String>, Partition>> partitionIterator;

    public PartitionIterator(Map<List<String>, Partition> partitionMap) {
      this.partitionIterator = partitionMap.entrySet().iterator();
    }

    @Override
    public boolean hasNext() {
      return this.partitionIterator.hasNext() || !targetPartitions.isEmpty();
    }

    @Override
    public FileSet<CopyEntity> next() {
      if (this.partitionIterator.hasNext()) {
        Map.Entry<List<String>, Partition> partitionEntry = partitionIterator.next();
        List<CopyEntity> copyEntities = Lists.newArrayList();
        try {
          copyEntities = new PartitionCopy(partitionEntry.getValue(), dataset.properties).getCopyEntities();
        } catch (IOException ioe) {
          log.error("Could not generate work units to copy partition " + partitionEntry.getValue().getCompleteName(),
              ioe);
        }
        targetPartitions.remove(partitionEntry.getKey());
        return new FileSet.Builder<>(partitionEntry.getValue().getCompleteName(), dataset).add(copyEntities).build();
      } else if (!targetPartitions.isEmpty()) {
        List<CopyEntity> deregisterCopyEntities = Lists.newArrayList();
        int priority = 1;
        String deregisterFileSet = "deregister";
        for (Map.Entry<List<String>, Partition> partitionEntry : targetPartitions.entrySet()) {
          try {
            priority = addPartitionDeregisterSteps(deregisterCopyEntities, deregisterFileSet, priority, targetTable,
                partitionEntry.getValue());
          } catch (IOException ioe) {
            log.error("Could not create work unit to deregister partition " + partitionEntry.getValue().getCompleteName());
          }
        }
        targetPartitions.clear();
        return new FileSet.Builder<>(deregisterFileSet, dataset).add(deregisterCopyEntities).build();
      } else {
        throw new NoSuchElementException();
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Takes a path with tokens {@link #databaseToken} or {@link #tableToken} and replaces these tokens with the actual
   * database names and table name. For example, if db is myDatabase, table is myTable, then /data/$DB/$TABLE will be
   * resolved to /data/myDatabase/myTable.
   */
  protected static Path resolvePath(String pattern, String database, String table) {
    pattern = pattern.replace(databaseToken, database);
    if (pattern.contains(tableToken)) {
      pattern = pattern.replace(tableToken, table);
      return new Path(pattern);
    } else {
      return new Path(pattern, table);
    }
  }

  private Table getTargetTable(Table originTable, Path targetLocation) throws IOException {
    try {
      Table targetTable = originTable.copy();

      targetTable.setDbName(this.targetDatabase);
      targetTable.setDataLocation(targetLocation);
      /*
       * Need to set the table owner as the flow executor
       */
      targetTable.setOwner(UserGroupInformation.getCurrentUser().getShortUserName());
      targetTable.getTTable().putToParameters(HiveDataset.REGISTERER, GOBBLIN_DISTCP);
      targetTable.getTTable().putToParameters(HiveDataset.REGISTRATION_GENERATION_TIME_MILLIS, Long.toString(this.startTime));

      HiveAvroCopyEntityHelper.updateTableAttributesIfAvro(targetTable, this);

      return targetTable;
    } catch (HiveException he) {
      throw new IOException(he);
    }
  }

  private Partition getTargetPartition(Partition originPartition, Path targetLocation) throws IOException {
    try {
      Partition targetPartition = new Partition(this.targetTable, originPartition.getTPartition().deepCopy());
      targetPartition.getTable().setDbName(this.targetDatabase);
      targetPartition.getTPartition().setDbName(this.targetDatabase);
      targetPartition.getTPartition().putToParameters(HiveDataset.REGISTERER, GOBBLIN_DISTCP);
      targetPartition.getTPartition().putToParameters(HiveDataset.REGISTRATION_GENERATION_TIME_MILLIS, Long.toString(this.startTime));
      targetPartition.setLocation(targetLocation.toString());
      return targetPartition;
    } catch (HiveException he) {
      throw new IOException(he);
    }
  }

  /**
   * Creates {@link CopyEntity}s for a partition.
   */
  @Getter
  public class PartitionCopy {

    private final Partition partition;
    private final Properties properties;
    private Optional<Partition> existingTargetPartition;
    private final EventSubmitter eventSubmitter;

    public PartitionCopy(Partition partition, Properties properties) {
      this.partition = partition;
      this.properties = properties;
      this.existingTargetPartition = Optional.fromNullable(targetPartitions.get(this.partition.getValues()));
      this.eventSubmitter = new EventSubmitter.Builder(dataset.getMetricContext(), "hive.dataset.copy").
          addMetadata("Partition", this.partition.getName()).build();
    }

    private List<CopyEntity> getCopyEntities()
        throws IOException {

      try (Closer closer = Closer.create()) {
        log.info("Getting copy entities for partition " + this.partition.getCompleteName());
        MultiTimingEvent multiTimer = closer.register(new MultiTimingEvent(this.eventSubmitter, "PartitionCopy", true));

        int stepPriority = 0;
        String fileSet = gson.toJson(this.partition.getValues());

        List<CopyEntity> copyEntities = Lists.newArrayList();

        stepPriority = addSharedSteps(copyEntities, fileSet, stepPriority);

        multiTimer.nextStage(Stages.COMPUTE_TARGETS);
        Path targetPath = getTargetLocation(dataset.fs, targetFs, this.partition.getDataLocation(), Optional.of(this.partition));
        Partition targetPartition = getTargetPartition(this.partition, targetPath);

        multiTimer.nextStage(Stages.EXISTING_PARTITION);
        if (this.existingTargetPartition.isPresent()) {
          targetPartitions.remove(partition.getValues());
          try {
            checkPartitionCompatibility(targetPartition, existingTargetPartition.get());
          } catch (IOException ioe) {
            if (existingEntityPolicy != ExistingEntityPolicy.REPLACE_PARTITIONS) {
              log.error("Source and target partitions are not compatible. Aborting copy of partition " + this.partition,
                  ioe);
              return Lists.newArrayList();
            }
            log.warn("Source and target partitions are not compatible. Will override target partition.", ioe);
            stepPriority = addPartitionDeregisterSteps(copyEntities, fileSet, stepPriority, targetTable, existingTargetPartition.get());
            existingTargetPartition = Optional.absent();
          }
        }

        multiTimer.nextStage(Stages.PARTITION_SKIP_PREDICATE);
        if (fastPartitionSkip.isPresent() && fastPartitionSkip.get().apply(this)) {
          log.info(String.format("Skipping copy of partition %s due to fast partition skip predicate.", this.partition.getCompleteName()));
          return Lists.newArrayList();
        }

        HiveSpec partitionHiveSpec = new SimpleHiveSpec.Builder<>(targetPath).
            withTable(HiveMetaStoreUtils.getHiveTable(targetTable.getTTable())).
            withPartition(Optional.of(HiveMetaStoreUtils.getHivePartition(targetPartition.getTPartition()))).build();
        HiveRegisterStep register = new HiveRegisterStep(targetURI, partitionHiveSpec, hiveRegProps);
        copyEntities.add(new PostPublishStep(fileSet, Maps.<String, Object>newHashMap(), register, stepPriority++));

        multiTimer.nextStage(Stages.CREATE_LOCATIONS);
        HiveLocationDescriptor sourceLocation =
            HiveLocationDescriptor.forPartition(this.partition, dataset.fs, properties);
        HiveLocationDescriptor desiredTargetLocation =
            HiveLocationDescriptor.forPartition(targetPartition, targetFs, properties);
        Optional<HiveLocationDescriptor> existingTargetLocation = existingTargetPartition.isPresent() ? Optional.of(
            HiveLocationDescriptor.forPartition(existingTargetPartition.get(), targetFs, properties)) : Optional.<HiveLocationDescriptor>absent();

        multiTimer.nextStage(Stages.FULL_PATH_DIFF);
        DiffPathSet diffPathSet = fullPathDiff(sourceLocation, desiredTargetLocation, existingTargetLocation, Optional.<Partition>absent(),
            multiTimer, HiveCopyEntityHelper.this);

        multiTimer.nextStage(Stages.CREATE_DELETE_UNITS);
        if (diffPathSet.pathsToDelete.size() > 0) {
          DeleteFileCommitStep deleteStep = DeleteFileCommitStep.fromPaths(targetFs, diffPathSet.pathsToDelete, dataset.properties);
          copyEntities.add(new PrePublishStep(fileSet, Maps.<String, Object>newHashMap(), deleteStep, stepPriority++));
        }

        multiTimer.nextStage(Stages.CREATE_COPY_UNITS);
        for (CopyableFile.Builder builder : getCopyableFilesFromPaths(diffPathSet.filesToCopy, configuration, Optional.of(partition))) {
          copyEntities.add(builder.fileSet(fileSet).checksum(new byte[0]).build());
        }

        return copyEntities;
      }
    }
  }

  private int addPartitionDeregisterSteps(List<CopyEntity> copyEntities, String fileSet, int initialPriority,
      Table table, Partition partition) throws IOException {

    int stepPriority = initialPriority;
    Collection<Path> partitionPaths = Lists.newArrayList();

    if (this.deleteMethod == DeregisterFileDeleteMethod.RECURSIVE) {
      partitionPaths = Lists.newArrayList(partition.getDataLocation());
    } else if (this.deleteMethod == DeregisterFileDeleteMethod.INPUT_FORMAT) {
      InputFormat<?, ?> inputFormat = HiveUtils.getInputFormat(partition.getTPartition().getSd());

      HiveLocationDescriptor targetLocation =
          new HiveLocationDescriptor(partition.getDataLocation(), inputFormat, this.targetFs, this.dataset.getProperties());

      partitionPaths = targetLocation.getPaths().keySet();
    } else if (this.deleteMethod == DeregisterFileDeleteMethod.NO_DELETE) {
      partitionPaths = Lists.newArrayList();
    }

    if (!partitionPaths.isEmpty()) {
      DeleteFileCommitStep deletePaths =
          DeleteFileCommitStep.fromPaths(targetFs, partitionPaths, this.dataset.getProperties(), table.getDataLocation());
      copyEntities.add(new PrePublishStep(fileSet, Maps.<String, Object>newHashMap(), deletePaths, stepPriority++));
    }

    PartitionDeregisterStep deregister =
        new PartitionDeregisterStep(table.getTTable(), partition.getTPartition(), targetURI, hiveRegProps);
    copyEntities.add(new PrePublishStep(fileSet, Maps.<String, Object>newHashMap(), deregister, stepPriority++));
    return stepPriority;
  }
  
  @VisibleForTesting
  protected int addTableDeregisterSteps(List<CopyEntity> copyEntities, String fileSet, int initialPriority,
      Table table) throws IOException {

    int stepPriority = initialPriority;
    Collection<Path> tablePaths = Lists.newArrayList();

    switch (this.getDeleteMethod()) {
      case RECURSIVE:
        tablePaths = Lists.newArrayList(table.getDataLocation());
        break;
      case INPUT_FORMAT:
        InputFormat<?, ?> inputFormat = HiveUtils.getInputFormat(table.getSd());

        HiveLocationDescriptor targetLocation =
            new HiveLocationDescriptor(table.getDataLocation(), inputFormat, this.getTargetFs(), this.getDataset().getProperties());

        tablePaths = targetLocation.getPaths().keySet();
        break;
      case NO_DELETE: 
        tablePaths = Lists.newArrayList();
        break;
      default:
        tablePaths = Lists.newArrayList();
    }

    if (!tablePaths.isEmpty()) {
      DeleteFileCommitStep deletePaths =
          DeleteFileCommitStep.fromPaths(this.getTargetFs(), tablePaths, this.getDataset().getProperties(), table.getDataLocation());
      copyEntities.add(new PrePublishStep(fileSet, Maps.<String, Object>newHashMap(), deletePaths, stepPriority++));
    }

    TableDeregisterStep deregister =
        new TableDeregisterStep(table.getTTable(), this.getTargetURI(), this.getHiveRegProps());
    copyEntities.add(new PrePublishStep(fileSet, Maps.<String, Object>newHashMap(), deregister, stepPriority++));
    return stepPriority;
  }

  private int addSharedSteps(List<CopyEntity> copyEntities, String fileSet, int initialPriority) {
    int priority = initialPriority;
    if (this.tableRegistrationStep.isPresent()) {
      copyEntities.add(new PrePublishStep(fileSet, Maps.<String, Object>newHashMap(), this.tableRegistrationStep.get(),
          priority++));
    }
    return priority;
  }

  // Suppress warnings for "stepPriority++" in the PrePublishStep constructor, as stepPriority may be used later
  @SuppressFBWarnings("DLS_DEAD_LOCAL_STORE")
  private List<CopyEntity> getCopyEntitiesForUnpartitionedTable() throws IOException {
    MultiTimingEvent multiTimer = new MultiTimingEvent(this.eventSubmitter, "TableCopy", true);

    int stepPriority = 0;
    String fileSet = this.dataset.table.getTableName();
    List<CopyEntity> copyEntities = Lists.newArrayList();

    if (this.existingTargetTable.isPresent()) {
      if (!this.targetTable.getDataLocation().equals(this.existingTargetTable.get().getDataLocation())) {
        if (existingEntityPolicy != ExistingEntityPolicy.REPLACE_TABLE) {
          log.error("Source and target table are not compatible. Aborting copy of table " + this.targetTable,
              new HiveTableLocationNotMatchException(this.targetTable.getDataLocation(), this.existingTargetTable.get().getDataLocation()));
          return Lists.newArrayList();
        }
        
        log.warn("Source and target table are not compatible. Will override target table " + this.existingTargetTable.get().getDataLocation());
        stepPriority = addTableDeregisterSteps(copyEntities, fileSet, stepPriority, targetTable);
        this.existingTargetTable = Optional.absent();
      }
    }
    
    stepPriority = addSharedSteps(copyEntities, fileSet, stepPriority);

    HiveLocationDescriptor sourceLocation = HiveLocationDescriptor.forTable(this.dataset.table, this.dataset.fs, this.dataset.getProperties());
    HiveLocationDescriptor desiredTargetLocation = HiveLocationDescriptor.forTable(this.targetTable, this.targetFs, this.dataset.getProperties());
    Optional<HiveLocationDescriptor> existingTargetLocation = this.existingTargetTable.isPresent() ?
        Optional.of(HiveLocationDescriptor.forTable(this.existingTargetTable.get(), this.targetFs, this.dataset.getProperties())) :
        Optional.<HiveLocationDescriptor>absent();

    DiffPathSet diffPathSet = fullPathDiff(sourceLocation, desiredTargetLocation, existingTargetLocation,
        Optional.<Partition>absent(), multiTimer, this);

    // Could used to delete files for the existing snapshot
    DeleteFileCommitStep deleteStep = DeleteFileCommitStep.fromPaths(targetFs, diffPathSet.pathsToDelete,
        this.dataset.getProperties());
    copyEntities.add(new PrePublishStep(fileSet, Maps.<String, Object>newHashMap(), deleteStep, stepPriority++));

    for (CopyableFile.Builder builder : getCopyableFilesFromPaths(diffPathSet.filesToCopy,
        configuration, Optional.<Partition>absent())) {
      copyEntities.add(builder.build());
    }

    multiTimer.close();
    return copyEntities;
  }

  /**
   * Compares three entities to figure out which files should be copied and which files should be deleted in the target
   * file system.
   * @param sourceLocation Represents the source table or partition.
   * @param desiredTargetLocation Represents the new desired table or partition.
   * @param currentTargetLocation Represents the corresponding existing table or partition in the target hcat if it exists.
   * @param partition If present, contains partition information.
   * @return A {@link DiffPathSet} with data on files to copy and delete.
   * @throws IOException if the copy of this table / partition should be aborted.
   */
  @VisibleForTesting
  protected static DiffPathSet fullPathDiff(HiveLocationDescriptor sourceLocation, HiveLocationDescriptor desiredTargetLocation,
      Optional<HiveLocationDescriptor> currentTargetLocation, Optional<Partition> partition, MultiTimingEvent multiTimer,
      HiveCopyEntityHelper helper)
      throws IOException {

    DiffPathSet.DiffPathSetBuilder builder = DiffPathSet.builder();

    multiTimer.nextStage(Stages.SOURCE_PATH_LISTING);
    // These are the paths at the source
    Map<Path, FileStatus> sourcePaths = sourceLocation.getPaths();

    multiTimer.nextStage(Stages.TARGET_EXISTING_PATH_LISTING);
    // These are the paths that the existing target table / partition uses now
    Map<Path, FileStatus> targetExistingPaths = currentTargetLocation.isPresent() ? currentTargetLocation.get().getPaths() :
        Maps.<Path, FileStatus>newHashMap();

    multiTimer.nextStage(Stages.DESIRED_PATHS_LISTING);
    // These are the paths that exist at the destination and the new table / partition would pick up
    Map<Path, FileStatus> desiredTargetExistingPaths;
    try {
      desiredTargetExistingPaths = desiredTargetLocation.getPaths();
    } catch (InvalidInputException ioe) {
      // Thrown if inputFormat cannot find location in target. Since location doesn't exist, this set is empty.
      desiredTargetExistingPaths = Maps.newHashMap();
    }

    multiTimer.nextStage(Stages.PATH_DIFF);
    for (FileStatus sourcePath : sourcePaths.values()) {
      // For each source path
      Path newPath = helper.getTargetPath(sourcePath.getPath(), desiredTargetLocation.getFileSystem(), partition, true);
      boolean shouldCopy = true;
      if (desiredTargetExistingPaths.containsKey(newPath)) {
        // If the file exists at the destination, check whether it should be replaced, if not, no need to copy
        FileStatus existingTargetStatus = desiredTargetExistingPaths.get(newPath);
        if (!helper.shouldReplaceFile(existingTargetStatus, sourcePath)) {
          shouldCopy = false;
        }
      }
      if (shouldCopy) {
        builder.copyFile(sourcePath);
      } else {
        // if not copying, we want to keep the file in the target
        // at the end of this loop, all files in targetExistingPaths will be marked for deletion, so remove this file
        targetExistingPaths.remove(newPath);
        desiredTargetExistingPaths.remove(newPath);
      }
    }

    multiTimer.nextStage(Stages.COMPUTE_DELETE_PATHS);
    // At this point, targetExistingPaths contains paths managed by target partition / table, but that we don't want
    // delete them
    for (Path delete : targetExistingPaths.keySet()) {
      builder.deleteFile(delete);
      desiredTargetExistingPaths.remove(delete);
    }

    // Now desiredTargetExistingPaths contains paths that we don't want, but which are not managed by the existing
    // table / partition. We shouldn't delete them (they're not managed by Hive), and we don't want to pick them up
    // in the new table / partition, so if there are any leftover files, abort copying this table / partition.
    if (desiredTargetExistingPaths.size() > 0) {
      throw new IOException(String.format("New table / partition would pick up existing, undesired files in target file system. "
          + "%s, files %s.", partition.isPresent() ? partition.get().getCompleteName() : helper.dataset.table.getCompleteName(),
          Arrays.toString(desiredTargetExistingPaths.keySet().toArray())));
    }

    return builder.build();
  }

  private boolean shouldReplaceFile(FileStatus referencePath, FileStatus replacementFile) {
    return replacementFile.getLen() != referencePath.getLen() ||
        referencePath.getModificationTime() < replacementFile.getModificationTime();
  }

  private void checkPartitionedTableCompatibility(Table desiredTargetTable, Table existingTargetTable) throws IOException {
    if (!desiredTargetTable.getDataLocation().equals(existingTargetTable.getDataLocation())) {
      throw new HiveTableLocationNotMatchException(desiredTargetTable.getDataLocation(), 
          existingTargetTable.getDataLocation());
    }

    if (HiveUtils.isPartitioned(desiredTargetTable) != HiveUtils.isPartitioned(existingTargetTable)) {
      throw new IOException(String.format(
          "%s: Desired target table %s partitioned, existing target table %s partitioned. Tables are incompatible.",
          this.dataset.tableIdentifier, HiveUtils.isPartitioned(desiredTargetTable) ? "is" : "is not", HiveUtils
              .isPartitioned(existingTargetTable) ? "is" : "is not"));
    }
    if (desiredTargetTable.isPartitioned() && !desiredTargetTable.getPartitionKeys().equals(existingTargetTable.getPartitionKeys())) {
      throw new IOException(String.format("%s: Desired target table has partition keys %s, existing target table has partition  keys %s. "
              + "Tables are incompatible.",
          this.dataset.tableIdentifier, gson.toJson(desiredTargetTable.getPartitionKeys()), gson.toJson(existingTargetTable.getPartitionKeys())));
    }
  }

  private void checkPartitionCompatibility(Partition desiredTargetPartition, Partition existingTargetPartition)
      throws IOException {
    if (!desiredTargetPartition.getDataLocation().equals(existingTargetPartition.getDataLocation())) {
      throw new IOException(
          String.format("Desired target location %s and already registered target location %s do not agree.",
              desiredTargetPartition.getDataLocation(), existingTargetPartition.getDataLocation()));
    }
  }

  /**
   * Get builders for a {@link CopyableFile} for each file referred to by a {@link org.apache.hadoop.hive.metastore.api.StorageDescriptor}.
   */
  private List<CopyableFile.Builder> getCopyableFilesFromPaths(Collection<FileStatus> paths,
      CopyConfiguration configuration, Optional<Partition> partition)
      throws IOException {
    List<CopyableFile.Builder> builders = Lists.newArrayList();
    List<SourceAndDestination> dataFiles = Lists.newArrayList();

    Configuration hadoopConfiguration = new Configuration();
    FileSystem actualSourceFs = null;
    String referenceScheme = null;
    String referenceAuthority = null;

    for (FileStatus status : paths) {
      dataFiles.add(new SourceAndDestination(status, getTargetPath(status.getPath(), this.targetFs, partition, true)));
    }

    for (SourceAndDestination sourceAndDestination : dataFiles) {

      URI uri = sourceAndDestination.getSource().getPath().toUri();
      if (actualSourceFs == null || !StringUtils.equals(referenceScheme, uri.getScheme())
          || !StringUtils.equals(referenceAuthority, uri.getAuthority())) {
        actualSourceFs = sourceAndDestination.getSource().getPath().getFileSystem(hadoopConfiguration);
        referenceScheme = uri.getScheme();
        referenceAuthority = uri.getAuthority();
      }

      builders.add(CopyableFile.fromOriginAndDestination(actualSourceFs, sourceAndDestination.getSource(),
          sourceAndDestination.getDestination(), configuration));
    }

    return builders;
  }

  /**
   * Compute the target location for a Hive location.
   * @param sourceFs Source {@link FileSystem}.
   * @param path source {@link Path} in Hive location.
   * @param partition partition these paths correspond to.
   * @return transformed location in the target.
   * @throws IOException if cannot generate a single target location.
   */
  private Path getTargetLocation(FileSystem sourceFs, FileSystem targetFs, Path path, Optional<Partition> partition)
      throws IOException {
    return getTargetPath(path, targetFs, partition, false);
  }

  /**
   * Compute the target {@link Path} for a file or directory referred by this table.
   * @param sourcePath Source path to be transformed.
   * @param targetFs target {@link FileSystem}
   * @param partition partition this file belongs to.
   * @param isConcreteFile true if this is a path to an existing file in HDFS.
   */
  public Path getTargetPath(Path sourcePath, FileSystem targetFs, Optional<Partition> partition, boolean isConcreteFile) {
    if (this.relocateDataFiles) {
      Preconditions.checkArgument(this.targetTableRoot.isPresent(), "Must define %s to relocate data files.",
          COPY_TARGET_TABLE_ROOT);
      Path path = this.targetTableRoot.get();
      if (partition.isPresent()) {
        addPartitionToPath(path, partition.get());
      }
      if (!isConcreteFile) {
        return targetFs.makeQualified(path);
      } else {
        return targetFs.makeQualified(new Path(path, sourcePath.getName()));
      }
    }
    
    // both prefixs must be present as the same time
    // can not used with option {@link #COPY_TARGET_TABLE_ROOT}
    if (this.targetTablePrefixTobeReplaced.isPresent() || this.targetTablePrefixReplacement.isPresent()){
      Preconditions.checkState(this.targetTablePrefixTobeReplaced.isPresent(), 
          String.format("Must specify both %s option and %s option together", COPY_TARGET_TABLE_PREFIX_TOBE_REPLACED, COPY_TARGET_TABLE_PREFIX_REPLACEMENT));
      Preconditions.checkState(this.targetTablePrefixReplacement.isPresent(), 
          String.format("Must specify both %s option and %s option together", COPY_TARGET_TABLE_PREFIX_TOBE_REPLACED, COPY_TARGET_TABLE_PREFIX_REPLACEMENT));
      
      Preconditions.checkState(!this.targetTableRoot.isPresent(),
          String.format("Can not specify the option %s with option %s ", COPY_TARGET_TABLE_ROOT, COPY_TARGET_TABLE_PREFIX_REPLACEMENT));
      
      Path targetPathWithoutSchemeAndAuthority = replacedPrefix(sourcePath, this.targetTablePrefixTobeReplaced.get(), this.targetTablePrefixReplacement.get());
      return targetFs.makeQualified(targetPathWithoutSchemeAndAuthority);
    }
    else if (this.targetTableRoot.isPresent()){
      Preconditions.checkArgument(this.targetTableRoot.isPresent(), "Must define %s to relocate data files.",
          COPY_TARGET_TABLE_ROOT);
      Path path = this.targetTableRoot.get();
      if (partition.isPresent()) {
        addPartitionToPath(path, partition.get());
      }
      if (!isConcreteFile) {
        return targetFs.makeQualified(path);
      } else {
        return targetFs.makeQualified(new Path(path, sourcePath.getName()));
      }
    }
    else {
      return targetFs.makeQualified(PathUtils.getPathWithoutSchemeAndAuthority(sourcePath));
    }
  }
  
  protected static Path replacedPrefix(Path sourcePath, Path prefixTobeReplaced, Path prefixReplacement){
    Path sourcePathWithoutSchemeAndAuthority = PathUtils.getPathWithoutSchemeAndAuthority(sourcePath);
    Preconditions.checkArgument(PathUtils.isAncestor(prefixTobeReplaced, sourcePathWithoutSchemeAndAuthority),
        "When replacing prefix, all locations must be descendants of the prefix. "
            + "The prefix: %s, file location: %s.", prefixTobeReplaced, sourcePathWithoutSchemeAndAuthority);
    Path relativePath = PathUtils.relativizePath(sourcePathWithoutSchemeAndAuthority, prefixTobeReplaced);
    Path result = new Path(prefixReplacement, relativePath);
    return result;
  }

  private Path addPartitionToPath(Path path, Partition partition) {
    for (String partitionValue : partition.getValues()) {
      path = new Path(path, partitionValue);
    }
    return path;
  }

  public FileSystem getTargetFileSystem(){
    return this.targetFs;
  }
}
