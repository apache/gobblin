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
import java.util.Collection;
import java.util.List;
import java.util.Map;
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
import org.apache.thrift.TException;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;

import gobblin.commit.CommitStep;
import gobblin.configuration.State;
import gobblin.data.management.copy.CopyConfiguration;
import gobblin.data.management.copy.CopyEntity;
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.entities.PostPublishStep;
import gobblin.data.management.copy.entities.PrePublishStep;
import gobblin.hive.HiveMetastoreClientPool;
import gobblin.hive.HiveRegProps;
import gobblin.hive.HiveRegisterStep;
import gobblin.hive.HiveRegisterUtils;
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
class HiveCopyEntityHelper {

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
    REPLACE,
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

    try(AutoReturnableObject<IMetaStoreClient> sourceClient = this.dataset.clientPool.getClient();
        AutoReturnableObject<IMetaStoreClient> targetClient = this.targetClientPool.getClient()) {

      if (targetClient.get().tableExists(targetDatabase, this.dataset.table.getTableName())) {
        this.existingTargetTable =
            Optional.of(new Table(targetClient.get().getTable(targetDatabase, this.dataset.table.getTableName())));
      } else {
        this.existingTargetTable = Optional.absent();
      }

      if (this.existingTargetTable.isPresent()) {
        checkTableCompatibility(this.dataset.table, this.existingTargetTable.get());
        this.targetTable = this.existingTargetTable.get();
        this.tableRegistrationStep = Optional.absent();
      } else {
        Path targetPath = getTargetLocation(dataset.fs, this.targetFs, dataset.tableLocations, Optional.<Partition>absent());
        this.targetTable = getTargetTable(this.dataset.table, targetPath);

        HiveSpec tableHiveSpec = new SimpleHiveSpec.Builder<>(targetPath).
                withTable(HiveMetaStoreUtils.getHiveTable(this.targetTable.getTTable())).build();
        CommitStep tableRegistrationStep = new HiveRegisterStep(targetURI, tableHiveSpec, this.hiveRegProps);

        this.tableRegistrationStep = Optional.of(tableRegistrationStep);
      }

      if (HiveDataset.isPartitioned(this.dataset.table)) {
        this.sourcePartitions = HiveDataset.getPartitions(sourceClient, this.dataset.table);
        this.targetPartitions = this.existingTargetTable.isPresent() ?
            HiveDataset.getPartitions(targetClient, this.existingTargetTable.get()) : Maps.<List<String>, Partition>newHashMap();
      } else {
        this.sourcePartitions = Maps.newHashMap();
        this.targetPartitions = Maps.newHashMap();
      }

    } catch (TException te) {
      throw new IOException("Failed to generate work units.", te);
    }
  }

  /**
   * Finds all files read by the table and generates CopyableFiles. The semantics are as follows:
   * 1. Find all valid {@link org.apache.hadoop.hive.metastore.api.StorageDescriptor}. If the table is partitioned, the
   *    {@link org.apache.hadoop.hive.metastore.api.StorageDescriptor} of the base
   *    table will be ignored, and we will instead process the {@link org.apache.hadoop.hive.metastore.api.StorageDescriptor} of each partition.
   * 2. For each {@link org.apache.hadoop.hive.metastore.api.StorageDescriptor} find all files referred by it.
   * 3. Generate a {@link CopyableFile} for each file referred by a {@link org.apache.hadoop.hive.metastore.api.StorageDescriptor}.
   * 4. If the table is partitioned, create a file set for each partition.
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

    if (HiveDataset.isPartitioned(this.dataset.table)) {
      for (Map.Entry<List<String>, Partition> partitionEntry : sourcePartitions.entrySet()) {
        copyableFiles.addAll(new PartitionCopy(partitionEntry.getValue()).getCopyEntities());
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
      return targetTable;
    } catch (HiveException he) {
      throw new IOException(he);
    }
  }

  private Partition getTargetPartition(Partition originPartition, Path targetLocation) throws IOException {
    try {
      Partition targetPartition = new Partition(this.targetTable, originPartition.getTPartition().deepCopy());
      targetPartition.getTable().setDbName(this.targetDatabase);
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

    private List<CopyEntity> getCopyEntities()
        throws IOException {

      int stepPriority = 0;
      String fileSet = gson.toJson(this.partition.getValues());

      List<CopyEntity> copyEntities = Lists.newArrayList();

      stepPriority = addSharedSteps(copyEntities, fileSet, stepPriority);

      InputFormat<?, ?> inputFormat = HiveDataset.getInputFormat(this.partition.getTPartition().getSd());

      Optional<Partition> targetPartition = Optional.fromNullable(targetPartitions.get(partition.getValues()));

      if (targetPartition.isPresent()) {
        targetPartitions.remove(partition.getValues());
        try {
          checkPartitionCompatibility(partition, targetPartition.get());
        } catch (IOException ioe) {
          if (existingEntityPolicy == ExistingEntityPolicy.ABORT) {
            log.error("Source and target partitions are not compatible. Aborting copy of partition " + this.partition);
            return Lists.newArrayList();
          }
          log.warn("Source and target partitions are not compatible. Will override target partition.");
          stepPriority = addDeregisterSteps(copyEntities, fileSet, stepPriority, targetTable, targetPartition.get());
          targetPartition = Optional.absent();
        }
      }

      dataset.fs.getFileStatus(new Path("/user/ketl_dev"));

      Collection<Path> sourcePaths = HiveDataset.getPaths(inputFormat, partition.getLocation());
      Collection<Path> targetExistingPaths = targetPartition.isPresent() ?
          HiveDataset.getPaths(inputFormat, targetPartition.get().getLocation()) : Sets.<Path>newHashSet();

      DiffPathSet diffPathSet =
          diffSourceAndTargetPaths(sourcePaths, targetExistingPaths, Optional.of(partition), targetFs);

      DeleteFileCommitStep deleteStep = DeleteFileCommitStep.fromPaths(targetFs, diffPathSet.pathsToDelete, dataset.properties);
      copyEntities.add(new PrePublishStep(fileSet, Maps.<String, Object>newHashMap(), deleteStep, stepPriority++));

      for (CopyableFile.Builder builder : getCopyableFilesFromPaths(diffPathSet.filesToCopy,
          configuration, Optional.of(partition))) {
        copyEntities.add(builder.fileSet(fileSet).build());
      }

      if (!targetPartition.isPresent()) {
        Path targetPath = getTargetLocation(dataset.fs, targetFs,
            HiveDataset.parseLocationIntoPaths(this.partition.getDataLocation()), Optional.of(this.partition));
        Partition newTargetPartition = getTargetPartition(this.partition, targetPath);

        HiveSpec partitionHiveSpec = new SimpleHiveSpec.Builder<>(targetPath).
            withTable(HiveMetaStoreUtils.getHiveTable(targetTable.getTTable())).
            withPartition(Optional.of(HiveMetaStoreUtils.getHivePartition(newTargetPartition.getTPartition()))).build();

        HiveRegisterStep register = new HiveRegisterStep(targetURI, partitionHiveSpec, hiveRegProps);
        copyEntities.add(new PostPublishStep(fileSet, Maps.<String, Object>newHashMap(), register, stepPriority++));

      }

      return copyEntities;
    }
  }

  private int addDeregisterSteps(List<CopyEntity> copyEntities, String fileSet, int initialPriority,
      Table table, Partition partition) throws IOException {

    int stepPriority = initialPriority;

    InputFormat<?, ?> inputFormat = HiveDataset.getInputFormat(partition.getTPartition().getSd());
    Collection<Path> partitionPaths = HiveDataset.getPaths(inputFormat, partition.getLocation());
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

    InputFormat<?, ?> inputFormat = HiveDataset.getInputFormat(this.dataset.table.getSd());

    Collection<Path> sourcePaths = HiveDataset.getPaths(inputFormat, this.dataset.table.getSd().getLocation());
    Collection<Path> targetExistingPaths = this.existingTargetTable.isPresent() ?
        HiveDataset.getPaths(inputFormat, this.existingTargetTable.get().getSd().getLocation()) : Sets.<Path>newHashSet();

    DiffPathSet diffPathSet = diffSourceAndTargetPaths(sourcePaths, targetExistingPaths,
        Optional.<Partition>absent(), targetFs);

    DeleteFileCommitStep deleteStep = DeleteFileCommitStep.fromPaths(targetFs, diffPathSet.pathsToDelete,
        this.dataset.properties);
    copyEntities.add(new PrePublishStep(fileSet, Maps.<String, Object>newHashMap(), deleteStep, stepPriority++));

    for (CopyableFile.Builder builder : getCopyableFilesFromPaths(diffPathSet.filesToCopy,
        configuration, Optional.<Partition>absent())) {
      copyEntities.add(builder.build());
    }

    return copyEntities;
  }

  private DiffPathSet diffSourceAndTargetPaths(Collection<Path> sourcePaths, Collection<Path> targetExistingPaths,
      Optional<Partition> partition, FileSystem targetFs) throws IOException {
    DiffPathSet.DiffPathSetBuilder builder = DiffPathSet.builder();

    Set<Path> targetPathSet = Sets.newHashSet(targetExistingPaths);

    for (Path sourcePath : sourcePaths) {
      FileStatus status = this.dataset.fs.getFileStatus(sourcePath);
      Path targetPath = targetFs.makeQualified(getTargetPath(status, this.targetFs, partition));
      if (targetPathSet.contains(targetPath)) {
        FileStatus targetStatus = targetFs.getFileStatus(targetPath);
        if (targetStatus.getLen() != status.getLen() || targetStatus.getModificationTime() < status.getModificationTime()) {
          // We must replace target file
          builder.deleteFile(targetPath);
          builder.copyFile(status);
        }
        targetPathSet.remove(targetPath);
      } else {
        builder.copyFile(status);
      }
    }

    // All leftover paths in target are not in source, should be deleted.
    for (Path deletePath : targetPathSet) {
      builder.deleteFile(deletePath);
    }

    return builder.build();
  }

  private void checkTableCompatibility(Table sourceTable, Table targetTable) throws IOException {

    Path targetLocation = getTargetLocation(this.dataset.fs, this.targetFs,
        HiveDataset.parseLocationIntoPaths(sourceTable.getDataLocation()), Optional.<Partition>absent());
    if (targetLocation != targetTable.getDataLocation()) {
      throw new IOException(
          String.format("Computed target location %s and already registered target location %s do not agree.",
              targetLocation, targetTable.getDataLocation()));
    }

    if (HiveDataset.isPartitioned(sourceTable) != HiveDataset.isPartitioned(targetTable)) {
      throw new IOException(String.format(
          "%s: Source table %s partitioned, target table %s partitioned. Tables are incompatible.",
          this.dataset.tableIdentifier, HiveDataset.isPartitioned(sourceTable) ? "is" : "is not", HiveDataset.isPartitioned(
              targetTable) ? "is" : "is not"));
    }
    if (sourceTable.isPartitioned() && sourceTable.getPartitionKeys() != targetTable.getPartitionKeys()) {
      throw new IOException(String.format("%s: Source table has partition keys %s, target table has partition  keys %s. "
              + "Tables are incompatible.",
          this.dataset.tableIdentifier, gson.toJson(sourceTable.getPartitionKeys()), gson.toJson(targetTable.getPartitionKeys())));
    }
  }

  private void checkPartitionCompatibility(Partition sourcePartition, Partition targetPartition) throws IOException {
    Path targetLocation = getTargetLocation(this.dataset.fs,
        this.targetFs, HiveDataset.parseLocationIntoPaths(sourcePartition.getDataLocation()), Optional.<Partition>absent());
    if (targetLocation != targetPartition.getDataLocation()) {
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
      dataFiles.add(new SourceAndDestination(status, getTargetPath(status, this.targetFs, partition)));
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
   * @param paths source {@link Path}s in Hive location.
   * @param partition partition these paths correspond to.
   * @return transformed location in the target.
   * @throws IOException if cannot generate a single target location.
   */
  private Path getTargetLocation(FileSystem sourceFs, FileSystem targetFs, Collection<Path> paths, Optional<Partition> partition)
      throws IOException {
    Set<Path> targetPaths = Sets.newHashSet();
    for (Path path : paths) {
      targetPaths.add(getTargetPath(sourceFs.getFileStatus(path), targetFs, partition));
    }
    if (targetPaths.size() != 1) {
      throw new IOException("Can only register exactly one path. Trying to register " + HiveDataset.joiner.join(targetPaths));
    }
    return targetPaths.iterator().next();
  }

  /**
   * Compute the target {@link Path} for a file or directory referred by this table.
   */
  private Path getTargetPath(FileStatus status, FileSystem targetFs, Optional<Partition> partition) {

    if (this.relocateDataFiles) {
      Preconditions.checkArgument(this.targetTableRoot.isPresent(), "Must define %s to relocate data files.",
          COPY_TARGET_TABLE_ROOT);
      Path path = this.targetTableRoot.get();
      if (partition.isPresent()) {
        addPartitionToPath(path, partition.get());
      }
      if (status.isDir()) {
        return targetFs.makeQualified(path);
      } else {
        return targetFs.makeQualified(new Path(path, status.getPath().getName()));
      }
    }

    if (this.targetTableRoot.isPresent()) {
      Preconditions.checkArgument(this.dataset.tableRootPath.isPresent(),
          "Cannot move paths to a new root unless table has exactly one location.");
      Preconditions.checkArgument(PathUtils.isAncestor(this.dataset.tableRootPath.get(), status.getPath()),
          "When moving paths to a new root, all locations must be descendants of the table root location. "
              + "Table root location: %s, file location: %s.", this.dataset.tableRootPath, status);

      Path relativePath = PathUtils.relativizePath(status.getPath(), this.dataset.tableRootPath.get());
      return targetFs.makeQualified(new Path(this.targetTableRoot.get(), relativePath));
    } else {
      return targetFs.makeQualified(PathUtils.getPathWithoutSchemeAndAuthority(status.getPath()));
    }

  }

  private Path addPartitionToPath(Path path, Partition partition) {
    for (String partitionValue : partition.getValues()) {
      path = new Path(path, partitionValue);
    }
    return path;
  }
}
