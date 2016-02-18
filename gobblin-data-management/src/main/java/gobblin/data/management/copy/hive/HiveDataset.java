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

import lombok.Data;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.gson.Gson;

import gobblin.data.management.copy.CopyConfiguration;
import gobblin.data.management.copy.CopyableDataset;
import gobblin.data.management.copy.CopyableFile;
import gobblin.util.AutoReturnableObject;
import gobblin.util.PathUtils;
import gobblin.util.filters.HiddenFilter;


/**
 * Hive dataset implementing {@link CopyableDataset}.
 */
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

  private static final Gson gson = new Gson();
  private static final String databaseToken = "$DB";
  private static final String tableToken = "$TABLE";

  private final Properties properties;
  private final FileSystem fs;
  private final GenericObjectPool<IMetaStoreClient> clientPool;
  private final Table table;
  private final Splitter splitter = Splitter.on(",").omitEmptyStrings();

  private final List<Path> tableLocations;
  // Only set if table has exactly one location
  private final Optional<Path> tableRootPath;
  private final Optional<Path> targetTableRoot;
  private final boolean relocateDataFiles;

  public HiveDataset(FileSystem fs, GenericObjectPool<IMetaStoreClient> clientPool, Table table, Properties properties) {
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
  @Override public Collection<CopyableFile> getCopyableFiles(FileSystem targetFs, CopyConfiguration configuration)
      throws IOException {

    List<CopyableFile> copyableFiles = Lists.newArrayList();

    try(AutoReturnableObject<IMetaStoreClient> client = new AutoReturnableObject<>(this.clientPool)) {
      if (isPartitioned()) {
        List<Partition> partitions = client.get().listPartitions(this.table.getDbName(), this.table.getTableName(),
            Short.MAX_VALUE);
        for (Partition partition : partitions) {

          StorageDescriptor storage = partition.getSd();

          for (CopyableFile.Builder builder : getCopyableFilesFromStorageDescriptor(storage, configuration,
              Optional.of(partition))) {
            copyableFiles.add(builder.fileSet(gson.toJson(partition.getValues())).build());
          }

        }
      } else {
        for (CopyableFile.Builder builder : getCopyableFilesFromStorageDescriptor(this.table.getSd(), configuration,
            Optional.<Partition>absent())) {
          copyableFiles.add(builder.build());
        }
      }
    } catch (TException te) {
      throw new IOException(te);
    }
    return copyableFiles;
  }

  /**
   * Get builders for a {@link CopyableFile} for each file referred to by a {@link StorageDescriptor}.
   */
  private List<CopyableFile.Builder> getCopyableFilesFromStorageDescriptor(StorageDescriptor storageDescriptor,
      CopyConfiguration configuration, Optional<Partition> partition)
      throws IOException {
    List<CopyableFile.Builder> builders = Lists.newArrayList();
    List<SourceAndDestination> dataFiles = Lists.newArrayList();

    for (Path path : parseLocationIntoPaths(storageDescriptor.getLocation())) {

      FileStatus[] statuses = this.fs.globStatus(path);
      if (statuses == null) {
        continue;
      }

      for (FileStatus status : statuses) {

        if (status.isDir()) {
          for (FileStatus dataFile : this.fs.listStatus(status.getPath(), new HiddenFilter())) {
            if (!dataFile.isDir()) {
              dataFiles.add(new SourceAndDestination(dataFile, getTargetPath(dataFile, partition)));
            }
          }
        } else {
          dataFiles.add(new SourceAndDestination(status, getTargetPath(status, partition)));
        }

      }

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

  public boolean isPartitioned() {
    return this.table.isSetPartitionKeys() && this.table.getPartitionKeys().size() > 0;
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
              + "Table root location: %s, ");

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
