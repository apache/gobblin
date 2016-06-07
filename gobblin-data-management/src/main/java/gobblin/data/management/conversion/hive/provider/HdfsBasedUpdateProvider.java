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
package gobblin.data.management.conversion.hive.provider;

import java.io.IOException;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

import gobblin.hive.HivePartition;
import gobblin.hive.HiveTable;


/**
 * Uses the file modification time of the data location of a {@link HiveTable} or {@link HivePartition} on HDFS
 */
@Slf4j
@AllArgsConstructor
public class HdfsBasedUpdateProvider implements HiveUnitUpdateProvider {

  private FileSystem fs;

  /**
   * Get the update time of a {@link Partition}
   *
   * @return the update time if available, 0 otherwise
   *
   * {@inheritDoc}
   * @see HiveUnitUpdateProvider#getUpdateTime(org.apache.hadoop.hive.ql.metadata.Partition)
   */
  @Override
  public long getUpdateTime(Partition partition) {

    try {
      return getUpdateTime(partition.getDataLocation());
    } catch (IOException e) {
      log.warn(
          String.format("Failed to get update time for %s. Will return update time as 0", partition.getCompleteName()),
          e);
    }
    return 0;
  }

  /**
   * Get the update time of a {@link Table}
   * @return the update time if available, 0 otherwise
   *
   * {@inheritDoc}
   * @see HiveUnitUpdateProvider#getUpdateTime(org.apache.hadoop.hive.ql.metadata.Table)
   */
  @Override
  public long getUpdateTime(Table table) {
    try {
      return getUpdateTime(table.getDataLocation());
    } catch (IOException e) {
      log.warn(
          String.format("Failed to get update time for %s. Will return update time as 0", table.getCompleteName()), e);
    }
    return 0;
  }

  private long getUpdateTime(Path path) throws IOException {

    if (this.fs.exists(path)) {
      return this.fs.getFileStatus(path).getModificationTime();
    }
    return 0;
  }
}
