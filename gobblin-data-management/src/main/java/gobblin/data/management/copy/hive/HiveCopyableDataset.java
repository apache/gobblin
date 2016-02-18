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
 * Hive Copiable dataset.
 */
public class HiveCopyableDataset implements CopyableDataset {

  public static final String COPY_TARGET_TABLE_ROOT =
      HiveDatasetFinder.HIVE_DATASET_PREFIX + ".copy.target.table.root";
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

  public HiveCopyableDataset(FileSystem fs, GenericObjectPool<IMetaStoreClient> clientPool, Table table,
      Properties properties) {
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

  private Path resolvePath(String pattern) {
    pattern = pattern.replaceAll(databaseToken, this.table.getDbName());
    if (pattern.contains(tableToken)) {
      pattern = pattern.replaceAll(tableToken, this.table.getTableName());
      return new Path(pattern);
    } else {
      return new Path(pattern, this.table.getTableName());
    }
  }

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
