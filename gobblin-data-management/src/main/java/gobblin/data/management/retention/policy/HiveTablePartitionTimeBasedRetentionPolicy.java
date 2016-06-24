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

package gobblin.data.management.retention.policy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import gobblin.data.management.version.DatasetVersion;
import gobblin.data.management.version.HiveTablePartitionVersion;
import gobblin.util.Either;


/**
 * List deletable {@link HiveTablePartitionVersion}s by checking if the modification timestamp of the data location older than currentTime minus {@link #retention}.
 * This can be overridden by getting the time information from other ways, e.g., the datetime pattern along the path.
 */
public class HiveTablePartitionTimeBasedRetentionPolicy implements RetentionPolicy<HiveTablePartitionVersion> {

  private final Duration retention;
  private final FileSystem fs;

  public HiveTablePartitionTimeBasedRetentionPolicy(Properties props, FileSystem fs) {
    this.retention = Duration.standardMinutes(Long.parseLong(props
        .getProperty(TimeBasedRetentionPolicy.RETENTION_MINUTES_KEY,
            TimeBasedRetentionPolicy.RETENTION_MINUTES_DEFAULT)));
    this.fs = fs;
  }

  @Override
  public Class<? extends DatasetVersion> versionClass() {
    return HiveTablePartitionVersion.class;
  }

  @Override
  public Collection<HiveTablePartitionVersion> listDeletableVersions(List<HiveTablePartitionVersion> allVersions) {
    List<HiveTablePartitionVersion> deletableVersions = new ArrayList<>();
    for (HiveTablePartitionVersion hiveTablePartitionVersion : allVersions) {
      Either<Table, Partition> version = hiveTablePartitionVersion.getVersion();
      try {
        if (this.shouldDeleteTableOrPartition(version)) {
          deletableVersions.add(hiveTablePartitionVersion);
        }
      } catch (IOException e) {
        throw new RuntimeException("Unable to determine deletable versions", e);
      }
    }
    return deletableVersions;
  }

  public boolean shouldDeleteTableOrPartition(Either<Table, Partition> tableOrPartition)
      throws IOException {
    Path dataLoc =
        tableOrPartition instanceof Either.Left ? ((Table) ((Either.Left) tableOrPartition).getLeft()).getDataLocation()
            : ((Partition) ((Either.Right) tableOrPartition).getRight()).getDataLocation();
    if (this.fs.exists(dataLoc) && new DateTime(this.fs.getFileStatus(dataLoc).getModificationTime()).plus(retention)
        .isBeforeNow()) {
      return true;
    }
    return false;
  }
}
