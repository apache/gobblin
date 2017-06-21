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

package gobblin.data.management.copy.hive;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;

import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.ConfigFactory;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import com.google.gson.Gson;
import com.typesafe.config.Config;

import gobblin.commit.CommitStep;
import gobblin.configuration.State;
import gobblin.util.ClassAliasResolver;
import gobblin.data.management.copy.CopyConfiguration;
import gobblin.data.management.copy.CopyEntity;
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.OwnerAndPermission;
import gobblin.data.management.copy.entities.PostPublishStep;
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
import gobblin.util.request_allocation.PushDownRequestor;

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

  public static final String EXISTING_ENTITY_POLICY_KEY =
      HiveDatasetFinder.HIVE_DATASET_PREFIX + ".existing.entity.conflict.policy";
  public static final String DEFAULT_EXISTING_ENTITY_POLICY = ExistingEntityPolicy.ABORT.name();

  public static final String UNMANAGED_DATA_POLICY_KEY =
      HiveDatasetFinder.HIVE_DATASET_PREFIX + ".unmanaged.data.conflict.policy";
  public static final String DEFAULT_UNMANAGED_DATA_POLICY = UnmanagedDataPolicy.ABORT.name();

  /** Target metastore URI */
  public static final String TARGET_METASTORE_URI_KEY =
      HiveDatasetFinder.HIVE_DATASET_PREFIX + ".copy.target.metastore.uri";
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

  /** A predicate applied to non partition table before any file listing.
   * If the predicate returns true, the table will be skipped. */
  public static final String FAST_TABLE_SKIP_PREDICATE =
      HiveDatasetFinder.HIVE_DATASET_PREFIX + ".copy.fast.table.skip.predicate";

  /** Method for deleting files on deregister. One of {@link DeregisterFileDeleteMethod}. */
  public static final String DELETE_FILES_ON_DEREGISTER =
      HiveDatasetFinder.HIVE_DATASET_PREFIX + ".copy.deregister.fileDeleteMethod";
  public static final DeregisterFileDeleteMethod DEFAULT_DEREGISTER_DELETE_METHOD =
      DeregisterFileDeleteMethod.NO_DELETE;

  /**
   * Config key to specify if {@link IMetaStoreClient }'s filtering method {@link listPartitionsByFilter} is not enough
   * for filtering out specific partitions.
   * For example, if you specify "Path" as the filter type and "Hourly" as the filtering condition,
   * partitions with Path containing '/Hourly/' will be kept.
   */
  public static final String HIVE_PARTITION_EXTENDED_FILTER_TYPE = HiveDatasetFinder.HIVE_DATASET_PREFIX + ".extendedFilterType";

  static final Gson gson = new Gson();

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

  private final HiveMetastoreClientPool targetClientPool;
  private final String targetDatabase;
  private final HiveRegProps hiveRegProps;
  private Optional<Table> existingTargetTable;
  private final Table targetTable;
  private final Optional<String> targetURI;
  private final ExistingEntityPolicy existingEntityPolicy;
  private final UnmanagedDataPolicy unmanagedDataPolicy;
  private final Optional<String> partitionFilter;
  private Optional<? extends HivePartitionExtendedFilter> hivePartitionExtendedFilter;
  private final Optional<Predicate<HivePartitionFileSet>> fastPartitionSkip;
  private final Optional<Predicate<HiveCopyEntityHelper>> fastTableSkip;

  private final DeregisterFileDeleteMethod deleteMethod;

  private final Optional<CommitStep> tableRegistrationStep;
  private final Map<List<String>, Partition> sourcePartitions;
  private final Map<List<String>, Partition> targetPartitions;

  private final EventSubmitter eventSubmitter;
  @Getter
  protected final HiveTargetPathHelper targetPathHelper;

  /**
   * Defines what should be done for partitions that exist in the target but are not compatible with the source.
   */
  public enum ExistingEntityPolicy {
    /** Deregister target partition, delete its files, and create a new partition with correct values. */
    REPLACE_PARTITIONS,
    /** Deregister target table, do NOT delete its files, and create a new table with correct values. */
    REPLACE_TABLE,
    /** Keep the target table as registered while updating the file location */
    UPDATE_TABLE,
    /** Abort copying of conflict table. */
    ABORT
  }

  /**
   * Defines what should be done for data that is not managed by the existing target table / partition.
   */
  public enum UnmanagedDataPolicy {
    /** Delete any data that is not managed by the existing target table / partition. */
    DELETE_UNMANAGED_DATA,
    /** Abort copying of conflict table / partition. */
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
    @Singular(value = "copyFile")
    Collection<FileStatus> filesToCopy;
    /** Files in target that are not desired */
    @Singular(value = "deleteFile")
    Collection<Path> pathsToDelete;
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

      this.targetPathHelper = new HiveTargetPathHelper(this.dataset);
      this.hiveRegProps = new HiveRegProps(new State(this.dataset.getProperties()));
      this.targetURI = Optional.fromNullable(this.dataset.getProperties().getProperty(TARGET_METASTORE_URI_KEY));
      this.targetClientPool = HiveMetastoreClientPool.get(this.dataset.getProperties(), this.targetURI);
      this.targetDatabase = Optional.fromNullable(this.dataset.getProperties().getProperty(TARGET_DATABASE_KEY))
          .or(this.dataset.table.getDbName());
      this.existingEntityPolicy = ExistingEntityPolicy.valueOf(this.dataset.getProperties()
          .getProperty(EXISTING_ENTITY_POLICY_KEY, DEFAULT_EXISTING_ENTITY_POLICY).toUpperCase());
      this.unmanagedDataPolicy = UnmanagedDataPolicy.valueOf(
          this.dataset.getProperties().getProperty(UNMANAGED_DATA_POLICY_KEY, DEFAULT_UNMANAGED_DATA_POLICY)
              .toUpperCase());

      this.deleteMethod = this.dataset.getProperties().containsKey(DELETE_FILES_ON_DEREGISTER)
          ? DeregisterFileDeleteMethod
          .valueOf(this.dataset.getProperties().getProperty(DELETE_FILES_ON_DEREGISTER).toUpperCase())
          : DEFAULT_DEREGISTER_DELETE_METHOD;

      if (this.dataset.getProperties().containsKey(COPY_PARTITION_FILTER_GENERATOR)) {
        try {
          PartitionFilterGenerator generator = GobblinConstructorUtils.invokeFirstConstructor(
              (Class<PartitionFilterGenerator>) Class
                  .forName(this.dataset.getProperties().getProperty(COPY_PARTITION_FILTER_GENERATOR)),
              Lists.<Object> newArrayList(this.dataset.getProperties()), Lists.newArrayList());
          this.partitionFilter = Optional.of(generator.getFilter(this.dataset));
          log.info(String.format("Dynamic partition filter for table %s: %s.", this.dataset.table.getCompleteName(),
              this.partitionFilter.get()));
        } catch (ReflectiveOperationException roe) {
          throw new IOException(roe);
        }
      } else {
        this.partitionFilter =
            Optional.fromNullable(this.dataset.getProperties().getProperty(COPY_PARTITIONS_FILTER_CONSTANT));
      }

      // Initialize extended partition filter
      if ( this.dataset.getProperties().containsKey(HIVE_PARTITION_EXTENDED_FILTER_TYPE)){
        String filterType = dataset.getProperties().getProperty(HIVE_PARTITION_EXTENDED_FILTER_TYPE);
        try {
          Config config = ConfigFactory.parseProperties(this.dataset.getProperties());
          this.hivePartitionExtendedFilter =
              Optional.of(new ClassAliasResolver<>(HivePartitionExtendedFilterFactory.class).resolveClass(filterType).newInstance().createFilter(config));
        } catch (ReflectiveOperationException roe) {
          log.error("Error: Could not find filter with alias " + filterType);
          closer.close();
          throw new IOException(roe);
        }
      }
      else {
        this.hivePartitionExtendedFilter = Optional.absent();
      }

      try {
        this.fastPartitionSkip = this.dataset.getProperties().containsKey(FAST_PARTITION_SKIP_PREDICATE)
            ? Optional.of(GobblinConstructorUtils.invokeFirstConstructor(
            (Class<Predicate<HivePartitionFileSet>>) Class
                .forName(this.dataset.getProperties().getProperty(FAST_PARTITION_SKIP_PREDICATE)),
            Lists.<Object> newArrayList(this), Lists.newArrayList()))
            : Optional.<Predicate<HivePartitionFileSet>> absent();

        this.fastTableSkip = this.dataset.getProperties().containsKey(FAST_TABLE_SKIP_PREDICATE)
            ? Optional.of(GobblinConstructorUtils.invokeFirstConstructor(
            (Class<Predicate<HiveCopyEntityHelper>>) Class
                .forName(this.dataset.getProperties().getProperty(FAST_TABLE_SKIP_PREDICATE)),
            Lists.newArrayList()))
            : Optional.<Predicate<HiveCopyEntityHelper>> absent();

      } catch (ReflectiveOperationException roe) {
        closer.close();
        throw new IOException(roe);
      }

      Map<String, HiveMetastoreClientPool> namedPools =
          ImmutableMap.of(source_client, this.dataset.clientPool, target_client, this.targetClientPool);

      multiTimer.nextStage(Stages.GET_TABLES);
      try (HiveMetastoreClientPool.MultiClient multiClient = HiveMetastoreClientPool.safeGetClients(namedPools)) {

        if (multiClient.getClient(target_client).tableExists(this.targetDatabase, this.dataset.table.getTableName())) {
          this.existingTargetTable = Optional.of(new Table(
              multiClient.getClient(target_client).getTable(this.targetDatabase, this.dataset.table.getTableName())));
        } else {
          this.existingTargetTable = Optional.absent();
        }

        // Constructing CommitStep object for table registration
        Path targetPath = getTargetLocation(dataset.fs, this.targetFs, dataset.table.getDataLocation(),
            Optional.<Partition> absent());
        this.targetTable = getTargetTable(this.dataset.table, targetPath);
        HiveSpec tableHiveSpec = new SimpleHiveSpec.Builder<>(targetPath)
            .withTable(HiveMetaStoreUtils.getHiveTable(this.targetTable.getTTable())).build();

        CommitStep tableRegistrationStep =
            new HiveRegisterStep(this.targetURI, tableHiveSpec, this.hiveRegProps);
        this.tableRegistrationStep = Optional.of(tableRegistrationStep);

        if (this.existingTargetTable.isPresent() && this.existingTargetTable.get().isPartitioned()) {
          checkPartitionedTableCompatibility(this.targetTable, this.existingTargetTable.get());
        }
        if (HiveUtils.isPartitioned(this.dataset.table)) {
          this.sourcePartitions = HiveUtils.getPartitionsMap(multiClient.getClient(source_client), this.dataset.table,
              this.partitionFilter, this.hivePartitionExtendedFilter);
          HiveAvroCopyEntityHelper.updatePartitionAttributesIfAvro(this.targetTable, this.sourcePartitions, this);

          // Note: this must be mutable, so we copy the map
          this.targetPartitions =
              this.existingTargetTable.isPresent() ? Maps.newHashMap(
                  HiveUtils.getPartitionsMap(multiClient.getClient(target_client),
                      this.existingTargetTable.get(), this.partitionFilter, this.hivePartitionExtendedFilter))
                  : Maps.<List<String>, Partition> newHashMap();
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
   * See {@link #getCopyEntities(CopyConfiguration, Comparator, PushDownRequestor)}. This method does not pushdown any prioritizer.
   */
  Iterator<FileSet<CopyEntity>> getCopyEntities(CopyConfiguration configuration) throws IOException {
    return getCopyEntities(configuration, null, null);
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
   * For computation of target locations see {@link HiveTargetPathHelper#getTargetPath}
   */
  Iterator<FileSet<CopyEntity>> getCopyEntities(CopyConfiguration configuration, Comparator<FileSet<CopyEntity>> prioritizer,
      PushDownRequestor<FileSet<CopyEntity>> requestor) throws IOException {
    if (HiveUtils.isPartitioned(this.dataset.table)) {
      return new PartitionIterator(this.sourcePartitions, configuration, prioritizer, requestor);
    } else {
      FileSet<CopyEntity> fileSet = new UnpartitionedTableFileSet(this.dataset.table.getCompleteName(), this.dataset, this);
      return Iterators.singletonIterator(fileSet);
    }
  }

  /**
   * An iterator producing a {@link FileSet} of {@link CopyEntity} for each partition in this table. The files
   * are not scanned or the {@link FileSet} materialized until {@link #next} is called.
   */
  private class PartitionIterator implements Iterator<FileSet<CopyEntity>> {

    static final String DEREGISTER_FILE_SET = "deregister";

    private final List<FileSet<CopyEntity>> allFileSets;
    private final Iterator<FileSet<CopyEntity>> fileSetIterator;

    public PartitionIterator(Map<List<String>, Partition> partitionMap, CopyConfiguration configuration,
        Comparator<FileSet<CopyEntity>> prioritizer, PushDownRequestor<FileSet<CopyEntity>> requestor) {
      this.allFileSets = generateAllFileSets(partitionMap);
      for (FileSet<CopyEntity> fileSet : this.allFileSets) {
        fileSet.setRequestor(requestor);
      }
      if (prioritizer != null) {
        Collections.sort(this.allFileSets, prioritizer);
      }
      this.fileSetIterator = this.allFileSets.iterator();
    }

    @Override
    public boolean hasNext() {
      return this.fileSetIterator.hasNext();
    }

    @Override
    public FileSet<CopyEntity> next() {
      return this.fileSetIterator.next();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    private List<FileSet<CopyEntity>> generateAllFileSets(Map<List<String>, Partition> partitionMap) {
      List<FileSet<CopyEntity>> fileSets = Lists.newArrayList();
      for (Map.Entry<List<String>, Partition> partition : partitionMap.entrySet()) {
        fileSets.add(fileSetForPartition(partition.getValue()));
        HiveCopyEntityHelper.this.targetPartitions.remove(partition.getKey());
      }
      if (!HiveCopyEntityHelper.this.targetPartitions.isEmpty()) {
        fileSets.add(new HivePartitionsDeregisterFileSet(
            HiveCopyEntityHelper.this.dataset.getTable().getCompleteName() + DEREGISTER_FILE_SET,
            HiveCopyEntityHelper.this.dataset, HiveCopyEntityHelper.this.targetPartitions.values(), HiveCopyEntityHelper.this));
      }
      return fileSets;
    }

    private FileSet<CopyEntity> fileSetForPartition(final Partition partition) {
      return new HivePartitionFileSet(HiveCopyEntityHelper.this, partition, HiveCopyEntityHelper.this.dataset.getProperties());
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
      targetTable.getTTable().putToParameters(HiveDataset.REGISTRATION_GENERATION_TIME_MILLIS,
          Long.toString(this.startTime));
      targetTable.getTTable().unsetCreateTime();

      HiveAvroCopyEntityHelper.updateTableAttributesIfAvro(targetTable, this);

      return targetTable;
    } catch (HiveException he) {
      throw new IOException(he);
    }
  }

  int addPartitionDeregisterSteps(List<CopyEntity> copyEntities, String fileSet, int initialPriority,
      Table table, Partition partition) throws IOException {

    int stepPriority = initialPriority;
    Collection<Path> partitionPaths = Lists.newArrayList();

    if (this.deleteMethod == DeregisterFileDeleteMethod.RECURSIVE) {
      partitionPaths = Lists.newArrayList(partition.getDataLocation());
    } else if (this.deleteMethod == DeregisterFileDeleteMethod.INPUT_FORMAT) {
      InputFormat<?, ?> inputFormat = HiveUtils.getInputFormat(partition.getTPartition().getSd());

      HiveLocationDescriptor targetLocation = new HiveLocationDescriptor(partition.getDataLocation(), inputFormat,
          this.targetFs, this.dataset.getProperties());

      partitionPaths = targetLocation.getPaths().keySet();
    } else if (this.deleteMethod == DeregisterFileDeleteMethod.NO_DELETE) {
      partitionPaths = Lists.newArrayList();
    }

    if (!partitionPaths.isEmpty()) {
      DeleteFileCommitStep deletePaths = DeleteFileCommitStep.fromPaths(this.targetFs, partitionPaths,
          this.dataset.getProperties(), table.getDataLocation());
      copyEntities.add(new PostPublishStep(fileSet, Maps.<String, String> newHashMap(), deletePaths, stepPriority++));
    }

    PartitionDeregisterStep deregister =
        new PartitionDeregisterStep(table.getTTable(), partition.getTPartition(), this.targetURI, this.hiveRegProps);
    copyEntities.add(new PostPublishStep(fileSet, Maps.<String, String> newHashMap(), deregister, stepPriority++));
    return stepPriority;
  }

  @VisibleForTesting
  protected int addTableDeregisterSteps(List<CopyEntity> copyEntities, String fileSet, int initialPriority, Table table)
      throws IOException {

    int stepPriority = initialPriority;
    Collection<Path> tablePaths = Lists.newArrayList();

    switch (this.getDeleteMethod()) {
      case RECURSIVE:
        tablePaths = Lists.newArrayList(table.getDataLocation());
        break;
      case INPUT_FORMAT:
        InputFormat<?, ?> inputFormat = HiveUtils.getInputFormat(table.getSd());

        HiveLocationDescriptor targetLocation = new HiveLocationDescriptor(table.getDataLocation(), inputFormat,
            this.getTargetFs(), this.getDataset().getProperties());

        tablePaths = targetLocation.getPaths().keySet();
        break;
      case NO_DELETE:
        tablePaths = Lists.newArrayList();
        break;
      default:
        tablePaths = Lists.newArrayList();
    }

    if (!tablePaths.isEmpty()) {
      DeleteFileCommitStep deletePaths = DeleteFileCommitStep.fromPaths(this.getTargetFs(), tablePaths,
          this.getDataset().getProperties(), table.getDataLocation());
      copyEntities.add(new PostPublishStep(fileSet, Maps.<String, String> newHashMap(), deletePaths, stepPriority++));
    }

    TableDeregisterStep deregister =
        new TableDeregisterStep(table.getTTable(), this.getTargetURI(), this.getHiveRegProps());
    copyEntities.add(new PostPublishStep(fileSet, Maps.<String, String> newHashMap(), deregister, stepPriority++));
    return stepPriority;
  }

  int addSharedSteps(List<CopyEntity> copyEntities, String fileSet, int initialPriority) {
    int priority = initialPriority;
    if (this.tableRegistrationStep.isPresent()) {
      copyEntities.add(new PostPublishStep(fileSet, Maps.<String, String> newHashMap(), this.tableRegistrationStep.get(),
          priority++));
    }
    return priority;
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
  protected static DiffPathSet fullPathDiff(HiveLocationDescriptor sourceLocation,
      HiveLocationDescriptor desiredTargetLocation, Optional<HiveLocationDescriptor> currentTargetLocation,
      Optional<Partition> partition, MultiTimingEvent multiTimer, HiveCopyEntityHelper helper) throws IOException {

    DiffPathSet.DiffPathSetBuilder builder = DiffPathSet.builder();

    multiTimer.nextStage(Stages.SOURCE_PATH_LISTING);
    // These are the paths at the source
    Map<Path, FileStatus> sourcePaths = sourceLocation.getPaths();

    multiTimer.nextStage(Stages.TARGET_EXISTING_PATH_LISTING);
    // These are the paths that the existing target table / partition uses now
    Map<Path, FileStatus> targetExistingPaths = currentTargetLocation.isPresent()
        ? currentTargetLocation.get().getPaths() : Maps.<Path, FileStatus> newHashMap();

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
      Path newPath = helper.getTargetPathHelper().getTargetPath(sourcePath.getPath(), desiredTargetLocation.getFileSystem(), partition, true);
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
    // table / partition.
    // Ideally, we shouldn't delete them (they're not managed by Hive), and we don't want to pick
    // them up in the new table / partition, so if there are any leftover files, we should abort copying
    // this table / partition.
    if (desiredTargetExistingPaths.size() > 0 && helper.getUnmanagedDataPolicy() != UnmanagedDataPolicy.DELETE_UNMANAGED_DATA) {
      throw new IOException(String.format(
          "New table / partition would pick up existing, undesired files in target file system. " + "%s, files %s.",
          partition.isPresent() ? partition.get().getCompleteName() : helper.getDataset().getTable().getCompleteName(),
          Arrays.toString(desiredTargetExistingPaths.keySet().toArray())));
    }
    // Unless, the policy requires us to delete such un-managed files - in which case: we will add the leftover files
    // to the deletion list.
    else if (desiredTargetExistingPaths.size() > 0) {
      for (Path delete : desiredTargetExistingPaths.keySet()) {
        builder.deleteFile(delete);
      }
      log.warn(String.format("Un-managed files detected in target file system, however deleting them "
              + "because of the policy: %s Files to be deleted are: %s", UnmanagedDataPolicy.DELETE_UNMANAGED_DATA,
          StringUtils.join(desiredTargetExistingPaths.keySet(), ",")));
    }

    return builder.build();
  }

  private static boolean shouldReplaceFile(FileStatus referencePath, FileStatus replacementFile) {
    return replacementFile.getLen() != referencePath.getLen()
        || referencePath.getModificationTime() < replacementFile.getModificationTime();
  }

  private void checkPartitionedTableCompatibility(Table desiredTargetTable, Table existingTargetTable)
      throws IOException {
    if (!desiredTargetTable.getDataLocation().equals(existingTargetTable.getDataLocation())) {
      throw new HiveTableLocationNotMatchException(desiredTargetTable.getDataLocation(),
          existingTargetTable.getDataLocation());
    }

    if (HiveUtils.isPartitioned(desiredTargetTable) != HiveUtils.isPartitioned(existingTargetTable)) {
      throw new IOException(String.format(
          "%s: Desired target table %s partitioned, existing target table %s partitioned. Tables are incompatible.",
          this.dataset.tableIdentifier, HiveUtils.isPartitioned(desiredTargetTable) ? "is" : "is not",
          HiveUtils.isPartitioned(existingTargetTable) ? "is" : "is not"));
    }
    if (desiredTargetTable.isPartitioned()
        && !desiredTargetTable.getPartitionKeys().equals(existingTargetTable.getPartitionKeys())) {
      throw new IOException(String.format(
          "%s: Desired target table has partition keys %s, existing target table has partition keys %s. "
              + "Tables are incompatible.",
          this.dataset.tableIdentifier, gson.toJson(desiredTargetTable.getPartitionKeys()),
          gson.toJson(existingTargetTable.getPartitionKeys())));
    }
  }

  /**
   * Get builders for a {@link CopyableFile} for each file referred to by a {@link org.apache.hadoop.hive.metastore.api.StorageDescriptor}.
   */
  List<CopyableFile.Builder> getCopyableFilesFromPaths(Collection<FileStatus> paths,
      CopyConfiguration configuration, Optional<Partition> partition) throws IOException {
    List<CopyableFile.Builder> builders = Lists.newArrayList();
    List<SourceAndDestination> dataFiles = Lists.newArrayList();

    Configuration hadoopConfiguration = new Configuration();
    FileSystem actualSourceFs = null;
    String referenceScheme = null;
    String referenceAuthority = null;

    for (FileStatus status : paths) {
      dataFiles.add(new SourceAndDestination(status, getTargetPathHelper().getTargetPath(status.getPath(), this.targetFs, partition, true)));
    }

    for (SourceAndDestination sourceAndDestination : dataFiles) {

      URI uri = sourceAndDestination.getSource().getPath().toUri();
      if (actualSourceFs == null || !StringUtils.equals(referenceScheme, uri.getScheme())
          || !StringUtils.equals(referenceAuthority, uri.getAuthority())) {
        actualSourceFs = sourceAndDestination.getSource().getPath().getFileSystem(hadoopConfiguration);
        referenceScheme = uri.getScheme();
        referenceAuthority = uri.getAuthority();
      }

      if (!this.dataset.getTableRootPath().isPresent()) {
        // The logic for computing ancestor owner and permissions for hive copies depends on tables having a non-glob
        // location. Currently, this restriction is also imposed by Hive, so this is not a problem. If this ever changes
        // on the Hive side, and we try to copy a table with a glob location, this logic will have to change.
        throw new IOException(String.format("Table %s does not have a concrete table root path.",
            this.dataset.getTable().getCompleteName()));
      }
      List<OwnerAndPermission> ancestorOwnerAndPermission =
          CopyableFile.resolveReplicatedOwnerAndPermissionsRecursively(actualSourceFs,
              sourceAndDestination.getSource().getPath().getParent(), this.dataset.getTableRootPath().get().getParent(), configuration);

      builders.add(CopyableFile.fromOriginAndDestination(actualSourceFs, sourceAndDestination.getSource(),
          sourceAndDestination.getDestination(), configuration).
          ancestorsOwnerAndPermission(ancestorOwnerAndPermission));
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
  Path getTargetLocation(FileSystem sourceFs, FileSystem targetFs, Path path, Optional<Partition> partition)
      throws IOException {
    return getTargetPathHelper().getTargetPath(path, targetFs, partition, false);
  }

  protected static Path replacedPrefix(Path sourcePath, Path prefixTobeReplaced, Path prefixReplacement) {
    Path sourcePathWithoutSchemeAndAuthority = PathUtils.getPathWithoutSchemeAndAuthority(sourcePath);
    Preconditions.checkArgument(PathUtils.isAncestor(prefixTobeReplaced, sourcePathWithoutSchemeAndAuthority),
        "When replacing prefix, all locations must be descendants of the prefix. "
            + "The prefix: %s, file location: %s.",
        prefixTobeReplaced, sourcePathWithoutSchemeAndAuthority);
    Path relativePath = PathUtils.relativizePath(sourcePathWithoutSchemeAndAuthority, prefixTobeReplaced);
    Path result = new Path(prefixReplacement, relativePath);
    return result;
  }

  public FileSystem getTargetFileSystem() {
    return this.targetFs;
  }
}
