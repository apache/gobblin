/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gobblin.data.management.version.finder;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.Partition;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.gobblin.data.management.copy.hive.HiveDataset;
import org.apache.gobblin.data.management.copy.hive.HiveUtils;
import org.apache.gobblin.data.management.version.DatasetVersion;
import org.apache.gobblin.data.management.version.HiveDatasetVersion;
import org.apache.gobblin.dataset.Dataset;
import org.apache.gobblin.util.AutoReturnableObject;


/**
 * An abstract {@link VersionFinder} to create {@link HiveDatasetVersion}s for all {@link Partition}s of a {@link HiveDataset}.
 * Calls {@link #getDatasetVersion(Partition)} for every {@link Partition} found.
 */
@Slf4j
public abstract class AbstractHiveDatasetVersionFinder implements VersionFinder<HiveDatasetVersion> {

  @Override
  public Class<? extends DatasetVersion> versionClass() {
    return HiveDatasetVersion.class;
  }

  /**
   * Create {@link HiveDatasetVersion}s for all {@link Partition}s of a {@link HiveDataset}.
   * Calls {@link #getDatasetVersion(Partition)} for every {@link Partition} found.
   * <p>
   * Note: If an exception occurs while processing a partition, that partition will be ignored in the returned collection
   * </p>
   *
   * @throws IllegalArgumentException if <code>dataset</code> is not a {@link HiveDataset}. Or if {@link HiveDataset#getTable()}
   * is not partitioned.
   */
  @Override
  public Collection<HiveDatasetVersion> findDatasetVersions(Dataset dataset) throws IOException {
    if (!(dataset instanceof HiveDataset)) {
      throw new IllegalArgumentException("HiveDatasetVersionFinder is only compatible with HiveDataset");
    }
    final HiveDataset hiveDataset = (HiveDataset) dataset;

    if (!HiveUtils.isPartitioned(hiveDataset.getTable())) {
      throw new IllegalArgumentException("HiveDatasetVersionFinder is only compatible with partitioned hive tables");
    }

    try (AutoReturnableObject<IMetaStoreClient> client = hiveDataset.getClientPool().getClient()) {

      List<Partition> partitions = HiveUtils.getPartitions(client.get(), hiveDataset.getTable(), Optional.<String> absent());
      return Lists.newArrayList(Iterables.filter(Iterables.transform(partitions, new Function<Partition, HiveDatasetVersion>() {

        @Override
        public HiveDatasetVersion apply(Partition partition) {
          try {
            return getDatasetVersion(partition);
          } catch (Throwable e) {
            log.warn(String.format("Failed to get DatasetVersion %s. Skipping.", partition.getCompleteName()), e);
            return null;
          }
        }
      }), Predicates.notNull()));
    }
  }

  /**
   *
   * Create a {@link HiveDatasetVersion} for the {@link Partition}
   * @param partition for which a {@link HiveDatasetVersion} is created
   */
  protected abstract HiveDatasetVersion getDatasetVersion(Partition partition);

}
