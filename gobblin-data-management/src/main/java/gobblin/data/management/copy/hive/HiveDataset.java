/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
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

import lombok.Builder;
import lombok.Data;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.thrift.TException;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;

import gobblin.annotation.Alpha;
import gobblin.data.management.copy.CopyConfiguration;
import gobblin.data.management.copy.CopyEntity;
import gobblin.data.management.copy.CopyableDataset;
import gobblin.data.management.copy.CopyableFile;
import gobblin.hive.HiveMetastoreClientPool;
import gobblin.hive.spec.HiveSpec;
import gobblin.hive.spec.SimpleHiveSpec;
import gobblin.util.AutoReturnableObject;
import gobblin.util.PathUtils;


/**
 * Hive dataset implementing {@link CopyableDataset}.
 */
@Slf4j
@Alpha
public class HiveDataset implements CopyableDataset {

  /**
   * Specifies a root path for the data in a table. All files containing table data will be placed under this directory.
   * <p>
   *   Does some token replacement in the input path. For example, if the table myTable is in DB myDatabase:
   *   /data/$DB/$TABLE -> /data/myDatabase/myTable.
   *   /data/$TABLE -> /data/myTable
   *   /data -> /data/myTable
   * </p>
   *
   * See javadoc for {@link #getCopyableFiles} for further explanation.
   */
  public static final String COPY_TARGET_TABLE_ROOT =
      HiveDatasetFinder.HIVE_DATASET_PREFIX + ".copy.target.table.root";
  /**
   * Specifies that, on copy, data files for this table should all be relocated to a single directory per partition.
   * See javadoc for {@link #getCopyableFiles} for further explanation.
   */
  public static final String RELOCATE_DATA_FILES_KEY =
      HiveDatasetFinder.HIVE_DATASET_PREFIX + ".copy.relocate.data.files";
  public static final String DEFAULT_RELOCATE_DATA_FILES = Boolean.toString(false);

  public static final String TARGET_METASTORE_URI_KEY = HiveDatasetFinder.HIVE_DATASET_PREFIX + ".copy.target.metastore.uri";
  public static final String TARGET_DATABASE_KEY = HiveDatasetFinder.HIVE_DATASET_PREFIX + ".copy.target.database";

  private static final Gson gson = new Gson();
  private static final String databaseToken = "$DB";
  private static final String tableToken = "$TABLE";

  private final Properties properties;
  private final FileSystem fs;
  private final HiveMetastoreClientPool clientPool;
  private final Table table;
  private final Splitter splitter = Splitter.on(",").omitEmptyStrings();

  private final List<Path> tableLocations;
  // Only set if table has exactly one location
  private final Optional<Path> tableRootPath;
  private final Optional<Path> targetTableRoot;
  private final boolean relocateDataFiles;

  private final String tableIdentifier;


  public HiveDataset(FileSystem fs, HiveMetastoreClientPool clientPool, Table table, Properties properties) throws IOException {
    this.fs = fs;
    this.clientPool = clientPool;
    this.table = table;
    this.properties = properties;

    this.tableLocations = parseLocationIntoPaths(this.table.getSd().getLocation());

    this.tableRootPath = this.tableLocations.size() == 1 ?
        Optional.of(PathUtils.deepestNonGlobPath(this.tableLocations.get(0))) : Optional.<Path>absent();

    this.relocateDataFiles =
        Boolean.valueOf(properties.getProperty(RELOCATE_DATA_FILES_KEY, DEFAULT_RELOCATE_DATA_FILES));
    this.targetTableRoot = properties.containsKey(COPY_TARGET_TABLE_ROOT) ?
        Optional.of(resolvePath(properties.getProperty(COPY_TARGET_TABLE_ROOT))) : Optional.<Path>absent();

    this.tableIdentifier = this.table.getDbName() + "." + this.table.getTableName();
  }

  /**
   * Takes a path with tokens {@link #databaseToken} or {@link #tableToken} and replaces these tokens with the actual
   * database names and table name. For example, if db is myDatabase, table is myTable, then /data/$DB/$TABLE will be
   * resolved to /data/myDatabase/myTable.
   */
  private Path resolvePath(String pattern) {
    pattern = pattern.replaceAll(databaseToken, this.table.getDbName());
    if (pattern.contains(tableToken)) {
      pattern = pattern.replaceAll(tableToken, this.table.getTableName());
      return new Path(pattern);
    } else {
      return new Path(pattern, this.table.getTableName());
    }
  }

  /**
   * Parse a location string from a {@link StorageDescriptor} into an actual list of globs.
   */
  private List<Path> parseLocationIntoPaths(String sdLocation) {
    List<Path> locations = Lists.newArrayList();

    if (sdLocation == null) {
      return locations;
    }

    for (String location : this.splitter.split(sdLocation)) {
      locations.add(new Path(location));
    }
    return locations;
  }

  /**
   * Finds all files read by the table and generates CopyableFiles. The semantics are as follows:
   * 1. Find all valid {@link StorageDescriptor}. If the table is partitioned, the {@link StorageDescriptor} of the base
   *    table will be ignored, and we will instead process the {@link StorageDescriptor} of each partition.
   * 2. For each {@link StorageDescriptor} find all files referred by it.
   * 3. Generate a {@link CopyableFile} for each file referred by a {@link StorageDescriptor}.
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
  @Override public Collection<CopyEntity> getCopyableFiles(FileSystem targetFs, CopyConfiguration configuration)
      throws IOException {

    List<CopyEntity> copyableFiles = Lists.newArrayList();

    HiveMetastoreClientPool targetClientPool = HiveMetastoreClientPool.get(this.properties,
        Optional.fromNullable(properties.getProperty(TARGET_METASTORE_URI_KEY)));
    String targetDatabase = Optional.fromNullable(this.properties.getProperty(TARGET_DATABASE_KEY)).or(this.table.getDbName());

    try(AutoReturnableObject<IMetaStoreClient> sourceClient = this.clientPool.getClient();
        AutoReturnableObject<IMetaStoreClient> targetClient = targetClientPool.getClient()) {

      Optional<Table> targetTable = Optional.absent();
      if (targetClient.get().tableExists(targetDatabase, this.table.getTableName())) {
        targetTable = Optional.of(new Table(targetClient.get().getTable(targetDatabase, this.table.getTableName())));
      }

      if (targetTable.isPresent()) {
        try {
          checkTableCompatibility(this.table, targetTable.get());
        } catch (IOException ioe) {
          log.error("Source and target table are not compatible. Will not copy table.");
          return Lists.newArrayList();
        }
      } else {
        HiveSpec hiveSpec = new SimpleHiveSpec.Builder(this.table.getDataLocation()).withTable(this.table.getTTable()).
            build();
        // TODO: add hive registration step
      }

      if (isPartitioned(this.table)) {

        Map<List<String>, Partition> sourcePartitions = getPartitions(sourceClient, this.table);
        Map<List<String>, Partition> targetPartitions = targetTable.isPresent() ?
            getPartitions(targetClient, targetTable.get()) : Maps.<List<String>, Partition>newHashMap();

        for (Map.Entry<List<String>, Partition> partitionEntry : sourcePartitions.entrySet()) {
          copyableFiles.addAll(getCopyEntitiesForPartition(targetFs, targetTable, partitionEntry.getValue(),
              targetPartitions, configuration));
        }

        // Partitions that don't exist in source
        for (Map.Entry<List<String>, Partition> partitionEntry : targetPartitions.entrySet()) {
          // TODO: add partition deregistration step
        }

      } else {
        copyableFiles.addAll(getCopyEntitiesForUnpartitionedTable(targetFs, targetTable, configuration));
      }
    } catch (TException te) {
      throw new IOException("Hive Error", te);
    }
    return copyableFiles;
  }

  private List<CopyEntity> getCopyEntitiesForPartition(FileSystem targetFs, Optional<Table> targetTable,
      Partition partition, Map<List<String>, Partition> targetPartitions,
      CopyConfiguration configuration) throws IOException {

    List<CopyEntity> copyEntities = Lists.newArrayList();

    InputFormat<?, ?> inputFormat = getInputFormat(partition.getTPartition().getSd());

    Optional<Partition> targetPartition = Optional.fromNullable(targetPartitions.get(partition.getValues()));

    if (targetPartition.isPresent()) {
      targetPartitions.remove(partition.getValues());
      try {
        checkPartitionCompatibility(partition, targetPartition.get());
      } catch (IOException ioe) {
        log.warn("Source and target partitions are not compatible. Will override target partition.");
        // TODO: add partition deregistration step
        targetPartition = Optional.absent();
      }
    }

    Collection<Path> sourcePaths = getPaths(inputFormat, partition.getLocation());
    Collection<Path> targetExistingPaths = targetPartition.isPresent() ?
        getPaths(inputFormat, targetPartition.get().getLocation()) : Sets.<Path>newHashSet();

    DiffPathSet diffPathSet =
        diffSourceAndTargetPaths(sourcePaths, targetExistingPaths, Optional.of(partition), targetFs);

    // TODO: create delete work units for files that must be deleted on target

    for (CopyableFile.Builder builder : getCopyableFilesFromPaths(diffPathSet.filesToCopy,
        configuration, Optional.of(partition))) {
      copyEntities.add(builder.fileSet(gson.toJson(partition.getValues())).build());
    }

    if (!targetPartition.isPresent()) {
      HiveSpec partitionHiveSpec = new SimpleHiveSpec.Builder(this.table.getDataLocation()).
          withTable(this.table.getTTable()).withPartition(Optional.of(partition.getTPartition())).build();
      // TODO: add partition registration step
    }

    return copyEntities;

  }

  private List<CopyEntity> getCopyEntitiesForUnpartitionedTable(FileSystem targetFs,
      Optional<Table> targetTable, CopyConfiguration configuration) throws IOException {

    List<CopyEntity> copyEntities = Lists.newArrayList();

    InputFormat<?, ?> inputFormat = getInputFormat(this.table.getSd());

    Collection<Path> sourcePaths = getPaths(inputFormat, this.table.getSd().getLocation());
    Collection<Path> targetExistingPaths = targetTable.isPresent() ?
        getPaths(inputFormat, targetTable.get().getSd().getLocation()) : Sets.<Path>newHashSet();

    DiffPathSet diffPathSet = diffSourceAndTargetPaths(sourcePaths, targetExistingPaths,
        Optional.<Partition>absent(), targetFs);

    // TODO: create delete work units for files that must be deleted on target

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
      FileStatus status = this.fs.getFileStatus(sourcePath);
      Path targetPath = targetFs.makeQualified(getTargetPath(status, partition));
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

  @Builder
  private static class DiffPathSet {
    @Singular(value = "copyFile") Collection<FileStatus> filesToCopy;
    @Singular(value = "deleteFile") Collection<Path> pathsToDelete;
  }

  private Map<List<String>, Partition> getPartitions(AutoReturnableObject<IMetaStoreClient> client, Table table)
      throws IOException {
    try {
      Map<List<String>, Partition> partitions = Maps.newHashMap();
      for (org.apache.hadoop.hive.metastore.api.Partition p : client.get()
          .listPartitions(table.getDbName(), table.getTableName(), (short) -1)) {
        Partition partition = new Partition(this.table, p);
        partitions.put(partition.getValues(), partition);
      }
      return partitions;
    } catch (TException | HiveException te) {
      throw new IOException("Hive Error", te);
    }
  }

  private InputFormat<?, ?> getInputFormat(StorageDescriptor sd) throws IOException {
    try {
      InputFormat<?, ?> inputFormat = ConstructorUtils.invokeConstructor(
          (Class<? extends InputFormat>) Class.forName(sd.getInputFormat()));
      if (inputFormat instanceof JobConfigurable) {
        ((JobConfigurable) inputFormat).configure(new JobConf());
      }
      return inputFormat;
    } catch (ReflectiveOperationException re) {
      throw new IOException("Failed to instantiate input format.", re);
    }
  }

  private void checkTableCompatibility(Table sourceTable, Table targetTable) throws IOException {

    if (isPartitioned(sourceTable) != isPartitioned(targetTable)) {
      throw new IOException(String.format(
          "%s: Source table %s partitioned, target table %s partitioned. Tables are incompatible.",
          this.tableIdentifier, isPartitioned(sourceTable) ? "is" : "is not", isPartitioned(targetTable) ? "is" : "is not"));
    }
    if (sourceTable.isPartitioned() && sourceTable.getPartitionKeys() != targetTable.getPartitionKeys()) {
      throw new IOException(String.format("%s: Source table has partition keys %s, target table has partition  keys %s. "
              + "Tables are incompatible.",
          this.tableIdentifier, gson.toJson(sourceTable.getPartitionKeys()), gson.toJson(targetTable.getPartitionKeys())));
    }
  }

  private void checkPartitionCompatibility(Partition sourcePartition, Partition targetPartition) throws IOException {

  }

  /**
   * Get paths from a Hive location using the provided input format.
   */
  private Collection<Path> getPaths(InputFormat<?, ?> inputFormat, String location) throws IOException {
    JobConf jobConf = new JobConf();

    Set<Path> paths = Sets.newHashSet();

    FileInputFormat.addInputPaths(jobConf, location);
    InputSplit[] splits = inputFormat.getSplits(jobConf, 1000);
    for (InputSplit split : splits) {
      if (!(split instanceof FileSplit)) {
        throw new IOException("Not a file split.");
      }
      FileSplit fileSplit = (FileSplit) split;
      paths.add(fileSplit.getPath());
    }

    return paths;
  }

  /**
   * Get builders for a {@link CopyableFile} for each file referred to by a {@link StorageDescriptor}.
   */
  private List<CopyableFile.Builder> getCopyableFilesFromPaths(Collection<FileStatus> paths,
      CopyConfiguration configuration, Optional<Partition> partition)
      throws IOException {
    List<CopyableFile.Builder> builders = Lists.newArrayList();
    List<SourceAndDestination> dataFiles = Lists.newArrayList();

    for (FileStatus status : paths) {
      dataFiles.add(new SourceAndDestination(status, getTargetPath(status, partition)));
    }

    for (SourceAndDestination sourceAndDestination : dataFiles) {
      FileSystem actualSourceFs = sourceAndDestination.getSource().getPath().getFileSystem(new Configuration());

      builders.add(CopyableFile.fromOriginAndDestination(actualSourceFs, sourceAndDestination.getSource(),
              sourceAndDestination.getDestination(), configuration));
    }

    return builders;
  }

  /**
   * Represents a source {@link FileStatus} and a {@link Path} destination.
   */
  @Data
  private static class SourceAndDestination {
    private final FileStatus source;
    private final Path destination;
  }

  public static boolean isPartitioned(Table table) {
    return table.isPartitioned();
  }

  @Override public String datasetURN() {
    return this.table.getDbName() + "." + this.table.getTableName();
  }

  /**
   * Compute the target {@link Path} for a file or directory referred by this table.
   */
  private Path getTargetPath(FileStatus status, Optional<Partition> partition) {

    if (this.relocateDataFiles) {
      Preconditions.checkArgument(this.targetTableRoot.isPresent(), "Must define %s to relocate data files.",
          COPY_TARGET_TABLE_ROOT);
      Path path = this.targetTableRoot.get();
      if (partition.isPresent()) {
        addPartitionToPath(path, partition.get());
      }
      if (status.isDir()) {
        return path;
      } else {
        return new Path(path, status.getPath().getName());
      }
    }

    if (this.targetTableRoot.isPresent()) {
      Preconditions.checkArgument(this.tableRootPath.isPresent(),
          "Cannot move paths to a new root unless table has exactly one location.");
      Preconditions.checkArgument(PathUtils.isAncestor(this.tableRootPath.get(), status.getPath()),
          "When moving paths to a new root, all locations must be descendants of the table root location. "
              + "Table root location: %s, file location: %s.", this.tableRootPath, status);

      Path relativePath = PathUtils.relativizePath(status.getPath(), this.tableRootPath.get());
      return new Path(this.targetTableRoot.get(), relativePath);
    } else {
      return PathUtils.getPathWithoutSchemeAndAuthority(status.getPath());
    }

  }

  private Path addPartitionToPath(Path path, Partition partition) {
    for (String partitionValue : partition.getValues()) {
      path = new Path(path, partitionValue);
    }
    return path;
  }

}
