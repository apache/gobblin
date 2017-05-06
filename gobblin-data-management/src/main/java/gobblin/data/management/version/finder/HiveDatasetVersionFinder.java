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

package gobblin.data.management.version.finder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import gobblin.data.management.copy.hive.HiveDataset;
import gobblin.data.management.copy.hive.HiveDatasetFinder;
import gobblin.data.management.copy.hive.HiveUtils;
import gobblin.data.management.version.DatasetVersion;
import gobblin.data.management.version.HiveTablePartitionVersion;
import gobblin.dataset.Dataset;
import gobblin.util.Either;


/**
 * Find {@link HiveTablePartitionVersion}s for {@link HiveDataset}. If the hive table is partitioned, then versions will
 * be partitions. Otherwise, it is a single version using table itself.
 */
public class HiveDatasetVersionFinder extends HiveDatasetFinder implements VersionFinder<HiveTablePartitionVersion> {
  public HiveDatasetVersionFinder(FileSystem fs, Properties properties)
      throws IOException {
    super(fs, properties);
  }

  @Override
  public Class<? extends DatasetVersion> versionClass() {
    return HiveTablePartitionVersion.class;
  }

  @Override
  public Collection<HiveTablePartitionVersion> findDatasetVersions(Dataset dataset)
      throws IOException {
    Preconditions.checkArgument(dataset instanceof HiveDataset);
    List<HiveTablePartitionVersion> hiveDatasetVersions = new ArrayList<>();
    HiveDataset hiveDataset = (HiveDataset) dataset;

    if (!hiveDataset.getTable().isPartitioned()) {
      hiveDatasetVersions.add(new HiveTablePartitionVersion(Either.<Table, Partition>left(hiveDataset.getTable())));
    } else {
      for (Partition partition : HiveUtils
          .getPartitions(hiveDataset.getClientPool().getClient().get(), hiveDataset.getTable(),
              Optional.<String>absent())) {
        hiveDatasetVersions.add(new HiveTablePartitionVersion(Either.<Table, Partition>right(partition)));
      }
    }

    return hiveDatasetVersions;
  }
}
