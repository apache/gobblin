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
import java.util.Properties;
import java.util.Set;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.mapred.InputFormat;

import gobblin.data.management.copy.RecursivePathFinder;
import gobblin.util.PathUtils;

/**
 * Contains data for a Hive location as well as additional data if {@link #HIVE_DATASET_COPY_ADDITIONAL_PATHS_RECURSIVELY_ENABLED} set to true.
 */
@Data
@Slf4j
class HiveLocationDescriptor {
  public static final String HIVE_DATASET_COPY_ADDITIONAL_PATHS_RECURSIVELY_ENABLED =
      HiveDatasetFinder.HIVE_DATASET_PREFIX + ".copy.additional.paths.recursively.enabled";
  public static final String HIVE_LOCATION_LISTING_METHOD =
      HiveDatasetFinder.HIVE_DATASET_PREFIX + ".copy.location.listing.method";
  public static final String SKIP_HIDDEN_PATHS =
      HiveDatasetFinder.HIVE_DATASET_PREFIX + ".copy.locations.listing.skipHiddenPaths";
  public static final String DEFAULT_SKIP_HIDDEN_PATHS = Boolean.toString(false);
  public static final String DEFAULT_HIVE_LOCATION_LISTING_METHOD = PathFindingMethod.INPUT_FORMAT.name();

  public enum PathFindingMethod {
    INPUT_FORMAT, RECURSIVE
  }

  protected final Path location;
  protected final InputFormat<?, ?> inputFormat;
  protected final FileSystem fileSystem;
  protected final Properties properties;

  public Set<Path> getPaths() throws IOException {

    PathFindingMethod pathFindingMethod = PathFindingMethod.valueOf(
        this.properties.getProperty(HIVE_LOCATION_LISTING_METHOD, DEFAULT_HIVE_LOCATION_LISTING_METHOD).toUpperCase());

    if (pathFindingMethod == PathFindingMethod.INPUT_FORMAT) {

      Set<Path> result = HiveUtils.getPaths(this.inputFormat, this.location);

      boolean useHiveLocationDescriptorWithAdditionalData =
          Boolean.valueOf(this.properties.getProperty(HIVE_DATASET_COPY_ADDITIONAL_PATHS_RECURSIVELY_ENABLED, "false"));

      if (useHiveLocationDescriptorWithAdditionalData) {
        if (PathUtils.isGlob(this.location)) {
          throw new IOException("can not get additional data for glob pattern path " + this.location);
        }
        RecursivePathFinder finder = new RecursivePathFinder(this.fileSystem, this.location, this.properties);
        result.addAll(finder.getPaths(false));
      }

      return result;
    } else if (pathFindingMethod == PathFindingMethod.RECURSIVE) {
      if (PathUtils.isGlob(this.location)) {
        throw new IOException("Cannot use recursive listing for globbed locations.");
      }
      boolean skipHiddenPaths =  Boolean.parseBoolean(this.properties.getProperty(SKIP_HIDDEN_PATHS, DEFAULT_SKIP_HIDDEN_PATHS));
      RecursivePathFinder finder = new RecursivePathFinder(this.fileSystem, this.location, this.properties);
      return finder.getPaths(skipHiddenPaths);
    } else {
      throw new IOException("Hive location listing method not recognized: " + pathFindingMethod);
    }
  }

  public static HiveLocationDescriptor forTable(Table table, FileSystem fs, Properties properties) throws IOException {
    return new HiveLocationDescriptor(table.getDataLocation(), HiveUtils.getInputFormat(table.getTTable().getSd()), fs, properties);
  }

  public static HiveLocationDescriptor forPartition(Partition partition, FileSystem fs, Properties properties) throws IOException {
    return new HiveLocationDescriptor(partition.getDataLocation(),
        HiveUtils.getInputFormat(partition.getTPartition().getSd()), fs, properties);
  }

}
