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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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
import org.apache.thrift.TException;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;

import gobblin.commit.CommitStep;
import gobblin.configuration.State;
import gobblin.data.management.copy.CopyConfiguration;
import gobblin.data.management.copy.CopyEntity;
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.RecursivePathFinder;
import gobblin.data.management.copy.entities.PostPublishStep;
import gobblin.data.management.copy.entities.PrePublishStep;
import gobblin.data.management.copy.hive.avro.HiveAvroCopyEntityHelper;
import gobblin.hive.HiveMetastoreClientPool;
import gobblin.hive.HiveRegProps;
import gobblin.hive.HiveRegisterStep;
import gobblin.hive.PartitionDeregisterStep;
import gobblin.hive.metastore.HiveMetaStoreUtils;
import gobblin.hive.spec.HiveSpec;
import gobblin.hive.spec.SimpleHiveSpec;
import gobblin.util.AutoReturnableObject;
import gobblin.util.PathUtils;
import gobblin.util.commit.DeleteFileCommitStep;


/**
 * Creates {@link CopyEntity}s for copying a Hive table.
 */
@Slf4j
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
  
  private static final String databaseToken = "$DB";
  private static final String tableToken = "$TABLE";

  private static final Gson gson = new Gson();

  private static final String source_client = "source_client";
  private static final String target_client = "target_client";

  private final HiveDataset dataset;
  private final CopyConfiguration configuration;
  private final FileSystem targetFs;

  private final Optional<Path> targetTableRoot;
  private final boolean relocateDataFiles;
  private final HiveMetastoreClientPool targetClientPool;
  private final String targetDatabase;
  private final HiveRegProps hiveRegProps;
  private final Optional<Table> existingTargetTable;
  private final Table targetTable;
  private final Optional<String> targetURI;
  private final ExistingEntityPolicy existingEntityPolicy;

  private final Optional<CommitStep> tableRegistrationStep;
  private final Map<List<String>, Partition> sourcePartitions;
  private final Map<List<String>, Partition> targetPartitions;

  /**
   * Defines what should be done for partitions that exist in the target but are not compatible with the source.
   */
  public enum ExistingEntityPolicy {
    /** Deregister target partition, delete its files, and create a new partition with correct values. */
    REPLACE_PARTITIONS,
    /** Abort copying of conflict table. */
    ABORT
  }

  /**
   * A container for the differences between desired and existing files.
   */
  @Builder
  private static class DiffPathSet {
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

    log.info("Finding copy entities for table " + dataset.table.getCompleteName());

    this.dataset = dataset;
    this.configuration = configuration;
    this.targetFs = targetFs;

    this.relocateDataFiles =
        Boolean.valueOf(this.dataset.properties.getProperty(RELOCATE_DATA_FILES_KEY, DEFAULT_RELOCATE_DATA_FILES));
    this.targetTableRoot = this.dataset.properties.containsKey(COPY_TARGET_TABLE_ROOT) ?
        Optional.of(resolvePath(this.dataset.properties.getProperty(COPY_TARGET_TABLE_ROOT),
            this.dataset.table.getDbName(), this.dataset.table.getTableName())) : Optional.<Path>absent();

    this.hiveRegProps = new HiveRegProps(new State(this.dataset.properties));
    this.targetURI = Optional.fromNullable(this.dataset.properties.getProperty(TARGET_METASTORE_URI_KEY));
    this.targetClientPool = HiveMetastoreClientPool.get(this.dataset.properties, this.targetURI);
    this.targetDatabase = Optional.fromNullable(this.dataset.properties.getProperty(TARGET_DATABASE_KEY)).
        or(this.dataset.table.getDbName());
    this.existingEntityPolicy = ExistingEntityPolicy.valueOf(
        this.dataset.properties.getProperty(EXISTING_ENTITY_POLICY_KEY, DEFAULT_EXISTING_ENTITY_POLICY).toUpperCase());

    Map<String, HiveMetastoreClientPool> namedPools =
        ImmutableMap.of(source_client, this.dataset.clientPool, target_client, this.targetClientPool);

    try(HiveMetastoreClientPool.MultiClient multiClient = HiveMetastoreClientPool.safeGetClients(namedPools)) {

      if (multiClient.getClient(target_client).tableExists(targetDatabase, this.dataset.table.getTableName())) {
        this.existingTargetTable =
            Optional.of(new Table(multiClient.getClient(target_client).getTable(targetDatabase,
                this.dataset.table.getTableName())));
      } else {
        this.existingTargetTable = Optional.absent();
      }

      if (this.existingTargetTable.isPresent()) {
        checkTableCompatibility(this.dataset.table, this.existingTargetTable.get());
        this.targetTable = this.existingTargetTable.get();
        this.tableRegistrationStep = Optional.absent();
      } else {
        Path targetPath = getTargetLocation(dataset.fs, this.targetFs, dataset.table.getDataLocation(),
            Optional.<Partition>absent());
        this.targetTable = getTargetTable(this.dataset.table, targetPath);

        HiveSpec tableHiveSpec = new SimpleHiveSpec.Builder<>(targetPath).
                withTable(HiveMetaStoreUtils.getHiveTable(this.targetTable.getTTable())).build();
        CommitStep tableRegistrationStep = new HiveRegisterStep(targetURI, tableHiveSpec, this.hiveRegProps);

        this.tableRegistrationStep = Optional.of(tableRegistrationStep);
      }

      if (HiveUtils.isPartitioned(this.dataset.table)) {
        this.sourcePartitions = HiveUtils.getPartitionsMap(multiClient.getClient(source_client), this.dataset.table);
        this.targetPartitions = this.existingTargetTable.isPresent() ?
            HiveUtils.getPartitionsMap(multiClient.getClient(target_client), this.existingTargetTable.get()) :
            Maps.<List<String>, Partition>newHashMap();
      } else {
        this.sourcePartitions = Maps.newHashMap();
        this.targetPartitions = Maps.newHashMap();
      }

    } catch (TException te) {
      throw new IOException("Failed to generate work units for table " + dataset.table.getCompleteName(), te);
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
  Collection<CopyEntity> getCopyEntities() throws IOException {
    List<CopyEntity> copyableFiles = Lists.newArrayList();

    if (HiveUtils.isPartitioned(this.dataset.table)) {
      for (Map.Entry<List<String>, Partition> partitionEntry : sourcePartitions.entrySet()) {
        try {
          copyableFiles.addAll(new PartitionCopy(partitionEntry.getValue(), this.dataset.properties).getCopyEntities());
        } catch (IOException ioe) {
          log.error("Could not generate work units to copy partition " + partitionEntry.getValue().getCompleteName(), ioe);
        }
        targetPartitions.remove(partitionEntry.getKey());
      }

      // Partitions that don't exist in source
      String deregisterFileSet = "deregister";
      int priority = 1;
      for (Map.Entry<List<String>, Partition> partitionEntry : targetPartitions.entrySet()) {
        priority = addDeregisterSteps(copyableFiles, deregisterFileSet, priority, this.targetTable,
            partitionEntry.getValue());
      }

    } else {
      copyableFiles.addAll(getCopyEntitiesForUnpartitionedTable());
    }
    return copyableFiles;
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
      
      HiveAvroCopyEntityHelper.updateAvroTableAttributes(targetTable, this);

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
      targetPartition.setLocation(targetLocation.toString());
      return targetPartition;
    } catch (HiveException he) {
      throw new IOException(he);
    }
  }

  /**
   * Creates {@link CopyEntity}s for a partition.
   */
  @AllArgsConstructor
  private class PartitionCopy {

    private final Partition partition;
    private final Properties properties;

    private List<CopyEntity> getCopyEntities()
        throws IOException {

      log.info("Getting copy entities for partition " + this.partition.getCompleteName());

      int stepPriority = 0;
      String fileSet = gson.toJson(this.partition.getValues());

      List<CopyEntity> copyEntities = Lists.newArrayList();

      stepPriority = addSharedSteps(copyEntities, fileSet, stepPriority);


      Optional<Partition> existingTargetPartition = Optional.fromNullable(targetPartitions.get(partition.getValues()));
      Partition targetPartition;

      if (existingTargetPartition.isPresent()) {
        targetPartitions.remove(partition.getValues());
        targetPartition = existingTargetPartition.get();
        try {
          checkPartitionCompatibility(partition, existingTargetPartition.get());
        } catch (IOException ioe) {
          if (existingEntityPolicy != ExistingEntityPolicy.REPLACE_PARTITIONS) {
            log.error("Source and target partitions are not compatible. Aborting copy of partition " + this.partition);
            return Lists.newArrayList();
          }
          log.warn("Source and target partitions are not compatible. Will override target partition.");
          stepPriority = addDeregisterSteps(copyEntities, fileSet, stepPriority, targetTable, existingTargetPartition.get());
          existingTargetPartition = Optional.absent();
        }
      } else {
        Path targetPath = getTargetLocation(dataset.fs, targetFs, this.partition.getDataLocation(),
            Optional.of(this.partition));
        targetPartition = getTargetPartition(this.partition, targetPath);

        HiveSpec partitionHiveSpec = new SimpleHiveSpec.Builder<>(targetPath).
            withTable(HiveMetaStoreUtils.getHiveTable(targetTable.getTTable())).
            withPartition(Optional.of(HiveMetaStoreUtils.getHivePartition(targetPartition.getTPartition()))).build();

        HiveRegisterStep register = new HiveRegisterStep(targetURI, partitionHiveSpec, hiveRegProps);
        copyEntities.add(new PostPublishStep(fileSet, Maps.<String, Object>newHashMap(), register, stepPriority++));
      }

      HiveLocationDescriptor sourceLocation = HiveLocationDescriptor.forPartition(this.partition, dataset.fs, properties);
      HiveLocationDescriptor desiredTargetLocation = HiveLocationDescriptor.forPartition(targetPartition, targetFs, properties);
      Optional<HiveLocationDescriptor> existingTargetLocation = existingTargetPartition.isPresent() ?
          Optional.of(HiveLocationDescriptor.forPartition(existingTargetPartition.get(), targetFs, properties)) :
          Optional.<HiveLocationDescriptor>absent();

      DiffPathSet diffPathSet = fullPathDiff(sourceLocation, desiredTargetLocation, existingTargetLocation,
          Optional.<Partition>absent());

      if (diffPathSet.pathsToDelete.size() > 0) {
        DeleteFileCommitStep deleteStep = DeleteFileCommitStep.fromPaths(targetFs, diffPathSet.pathsToDelete,
            dataset.properties);
        copyEntities.add(new PrePublishStep(fileSet, Maps.<String, Object>newHashMap(), deleteStep, stepPriority++));
      }

      for (CopyableFile.Builder builder : getCopyableFilesFromPaths(diffPathSet.filesToCopy,
          configuration, Optional.of(partition))) {
        copyEntities.add(builder.fileSet(fileSet).build());
      }

      return copyEntities;
    }
  }

  private int addDeregisterSteps(List<CopyEntity> copyEntities, String fileSet, int initialPriority,
      Table table, Partition partition) throws IOException {

    int stepPriority = initialPriority;

    InputFormat<?, ?> inputFormat = HiveUtils.getInputFormat(partition.getTPartition().getSd());
    Collection<Path> partitionPaths = HiveUtils.getPaths(inputFormat, partition.getDataLocation());
    DeleteFileCommitStep deletePaths = DeleteFileCommitStep.fromPaths(targetFs, partitionPaths, this.dataset.properties);
    copyEntities.add(new PrePublishStep(fileSet, Maps.<String, Object>newHashMap(), deletePaths, stepPriority++));

    PartitionDeregisterStep deregister =
        new PartitionDeregisterStep(table.getTTable(), partition.getTPartition(), targetURI, hiveRegProps);
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

  private List<CopyEntity> getCopyEntitiesForUnpartitionedTable() throws IOException {

    int stepPriority = 0;
    String fileSet = this.dataset.table.getTableName();
    List<CopyEntity> copyEntities = Lists.newArrayList();

    stepPriority = addSharedSteps(copyEntities, fileSet, stepPriority);

    HiveLocationDescriptor sourceLocation = HiveLocationDescriptor.forTable(this.dataset.table, this.dataset.fs, this.dataset.properties);
    HiveLocationDescriptor desiredTargetLocation = HiveLocationDescriptor.forTable(this.targetTable, this.targetFs, this.dataset.properties);
    Optional<HiveLocationDescriptor> existingTargetLocation = this.existingTargetTable.isPresent() ?
        Optional.of(HiveLocationDescriptor.forTable(this.existingTargetTable.get(), this.targetFs, this.dataset.properties)) :
        Optional.<HiveLocationDescriptor>absent();

    DiffPathSet diffPathSet = fullPathDiff(sourceLocation, desiredTargetLocation, existingTargetLocation,
        Optional.<Partition>absent());

    DeleteFileCommitStep deleteStep = DeleteFileCommitStep.fromPaths(targetFs, diffPathSet.pathsToDelete,
        this.dataset.properties);
    copyEntities.add(new PrePublishStep(fileSet, Maps.<String, Object>newHashMap(), deleteStep, stepPriority++));

    for (CopyableFile.Builder builder : getCopyableFilesFromPaths(diffPathSet.filesToCopy,
        configuration, Optional.<Partition>absent())) {
      copyEntities.add(builder.build());
    }

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
  private DiffPathSet fullPathDiff(HiveLocationDescriptor sourceLocation, HiveLocationDescriptor desiredTargetLocation,
      Optional<HiveLocationDescriptor> currentTargetLocation, Optional<Partition> partition) throws IOException {

    DiffPathSet.DiffPathSetBuilder builder = DiffPathSet.builder();

    // These are the paths at the source
    Set<Path> sourcePaths = sourceLocation.getPaths();
    // These are the paths that the existing target table / partition uses now
    Set<Path> targetExistingPaths = currentTargetLocation.isPresent() ? currentTargetLocation.get().getPaths() :
        Sets.<Path>newHashSet();
    // These are the paths that exist at the destination and the new table / partition would pick up
    Set<Path> desiredTargetExistingPaths;
    try {
      desiredTargetExistingPaths = desiredTargetLocation.getPaths();
    } catch (InvalidInputException ioe) {
      // Thrown if inputFormat cannot find location in target. Since location doesn't exist, this set is empty.
      desiredTargetExistingPaths = Sets.newHashSet();
    }

    for (Path sourcePath : sourcePaths) {
      // For each source path
      Path newPath = getTargetPath(sourcePath, desiredTargetLocation.getFileSystem(), partition, true);
      boolean shouldCopy = true;
      FileStatus originStatus = sourceLocation.getFileSystem().getFileStatus(sourcePath);
      if (desiredTargetExistingPaths.contains(newPath)) {
        // If the file exists at the destination, check whether it should be replaced, if not, no need to copy
        FileStatus existingTargetStatus = desiredTargetLocation.getFileSystem().getFileStatus(newPath);
        if (!shouldReplaceFile(existingTargetStatus, originStatus)) {
          shouldCopy = false;
        }
      }
      if (shouldCopy) {
        builder.copyFile(originStatus);
      } else {
        // if not copying, we want to keep the file in the target
        // at the end of this loop, all files in targetExistingPaths will be marked for deletion, so remove this file
        targetExistingPaths.remove(newPath);
        desiredTargetExistingPaths.remove(newPath);
      }
    }

    // At this point, targetExistingPaths contains paths managed by target partition / table, but that we don't want
    // delete them
    for (Path delete : targetExistingPaths) {
      builder.deleteFile(delete);
      desiredTargetExistingPaths.remove(delete);
    }

    // Now desiredTargetExistingPaths contains paths that we don't want, but which are not managed by the existing
    // table / partition. We shouldn't delete them (they're not managed by Hive), and we don't want to pick them up
    // in the new table / partition, so if there are any leftover files, abort copying this table / partition.
    if (desiredTargetExistingPaths.size() > 0) {
      throw new IOException(String.format("New table / partition would pick up existing, undesired files in target file system. "
          + "%s, files %s.", partition.isPresent() ? partition.get().getCompleteName() : dataset.table.getCompleteName(),
          Arrays.toString(desiredTargetExistingPaths.toArray())));
    }

    return builder.build();
  }

  private boolean shouldReplaceFile(FileStatus referencePath, FileStatus replacementFile) {
    return replacementFile.getLen() != referencePath.getLen() ||
        referencePath.getModificationTime() < replacementFile.getModificationTime();
  }

  private void checkTableCompatibility(Table sourceTable, Table targetTable) throws IOException {

    Path targetLocation = getTargetLocation(this.dataset.fs, this.targetFs, sourceTable.getDataLocation(),
        Optional.<Partition>absent());
    if (!targetLocation.equals(targetTable.getDataLocation())) {
      throw new IOException(
          String.format("Computed target location %s and already registered target location %s do not agree.",
              targetLocation, targetTable.getDataLocation()));
    }

    if (HiveUtils.isPartitioned(sourceTable) != HiveUtils.isPartitioned(targetTable)) {
      throw new IOException(String.format(
          "%s: Source table %s partitioned, target table %s partitioned. Tables are incompatible.",
          this.dataset.tableIdentifier, HiveUtils.isPartitioned(sourceTable) ? "is" : "is not", HiveUtils
              .isPartitioned(targetTable) ? "is" : "is not"));
    }
    if (sourceTable.isPartitioned() && !sourceTable.getPartitionKeys().equals(targetTable.getPartitionKeys())) {
      throw new IOException(String.format("%s: Source table has partition keys %s, target table has partition  keys %s. "
              + "Tables are incompatible.",
          this.dataset.tableIdentifier, gson.toJson(sourceTable.getPartitionKeys()), gson.toJson(targetTable.getPartitionKeys())));
    }
  }

  private void checkPartitionCompatibility(Partition sourcePartition, Partition targetPartition) throws IOException {
    Path targetLocation = getTargetLocation(this.dataset.fs,
        this.targetFs, sourcePartition.getDataLocation(), Optional.<Partition>absent());
    if (!targetLocation.equals(targetPartition.getDataLocation())) {
      throw new IOException(
          String.format("Computed target location %s and already registered target location %s do not agree.",
              targetLocation, targetPartition.getDataLocation()));
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

    for (FileStatus status : paths) {
      dataFiles.add(new SourceAndDestination(status, getTargetPath(status.getPath(), this.targetFs, partition, true)));
    }

    for (SourceAndDestination sourceAndDestination : dataFiles) {
      FileSystem actualSourceFs = sourceAndDestination.getSource().getPath().getFileSystem(new Configuration());

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

    if (this.targetTableRoot.isPresent()) {
      Preconditions.checkArgument(this.dataset.tableRootPath.isPresent(),
          "Cannot move paths to a new root unless table has exactly one location.");
      Preconditions.checkArgument(PathUtils.isAncestor(this.dataset.tableRootPath.get(), sourcePath),
          "When moving paths to a new root, all locations must be descendants of the table root location. "
              + "Table root location: %s, file location: %s.", this.dataset.tableRootPath, sourcePath);

      Path relativePath = PathUtils.relativizePath(sourcePath, this.dataset.tableRootPath.get());
      return targetFs.makeQualified(new Path(this.targetTableRoot.get(), relativePath));
    } else {
      return targetFs.makeQualified(PathUtils.getPathWithoutSchemeAndAuthority(sourcePath));
    }

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
