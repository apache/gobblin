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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

import gobblin.data.management.version.DatasetVersion;
import gobblin.data.management.version.HiveTablePartitionVersion;
import gobblin.util.Either;

import lombok.AllArgsConstructor;


/**
 * List deletable {@link HiveTablePartitionVersion}s by checking if the data location pointed by {@link Table} or {@link Partition} exists.
 */
@AllArgsConstructor
public class HiveTablePartitionInvalidDataLocRetentionPolicy implements RetentionPolicy<HiveTablePartitionVersion> {
  private final FileSystem fs;

  @Override
  public Class<? extends DatasetVersion> versionClass() {
    return HiveTablePartitionVersion.class;
  }

  @Override
  public Collection<HiveTablePartitionVersion> listDeletableVersions(List<HiveTablePartitionVersion> allVersions) {
    List<HiveTablePartitionVersion> deletableVersions = new ArrayList<>();
    for (HiveTablePartitionVersion hiveTablePartitionVersion : allVersions) {
      Either<Table, Partition> version = hiveTablePartitionVersion.getVersion();
      Path dataLoc = version instanceof Either.Left ? ((Table) ((Either.Left) version).getLeft()).getDataLocation()
          : ((Partition) ((Either.Right) version).getRight()).getDataLocation();
      try {
        if (!this.fs.exists(dataLoc)) {
          deletableVersions.add(hiveTablePartitionVersion);
        }
      } catch (IOException e) {
        throw new RuntimeException("Unable to determine deletable versions", e);
      }
    }
    return deletableVersions;
  }
}
