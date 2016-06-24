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

package gobblin.data.management.retention.dataset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.thrift.TException;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import gobblin.data.management.copy.hive.HiveDataset;
import gobblin.data.management.retention.policy.CombineRetentionPolicy;
import gobblin.data.management.retention.policy.HiveTablePartitionInvalidDataLocRetentionPolicy;
import gobblin.data.management.retention.policy.HiveTablePartitionTimeBasedRetentionPolicy;
import gobblin.data.management.retention.policy.RetentionPolicy;
import gobblin.data.management.version.HiveTablePartitionVersion;
import gobblin.data.management.version.finder.HiveDatasetVersionFinder;
import gobblin.data.management.version.finder.VersionFinder;
import gobblin.hive.HiveMetastoreClientPool;
import gobblin.util.AutoReturnableObject;
import gobblin.util.Either;

import lombok.extern.slf4j.Slf4j;


/**
 * Extends {@link HiveDataset} and also is cleanable. It uses {@link CombineRetentionPolicy} of {@link HiveTablePartitionTimeBasedRetentionPolicy}
 * and {@link HiveTablePartitionInvalidDataLocRetentionPolicy} to decide the deletable versions.
 */
@Slf4j
public class HiveCleanableDataset extends HiveDataset implements CleanableDataset {
  private final VersionFinder<HiveTablePartitionVersion> versionFinder;
  private final CombineRetentionPolicy<HiveTablePartitionVersion> retentionPolicy;
  private final boolean simulate;

  public HiveCleanableDataset(FileSystem fs, HiveMetastoreClientPool clientPool, Table table, Properties properties)
      throws IOException {
    super(fs, clientPool, table, properties);
    this.versionFinder = new HiveDatasetVersionFinder(fs, properties);
    List<RetentionPolicy<HiveTablePartitionVersion>> policies = Lists
        .newArrayList(new HiveTablePartitionInvalidDataLocRetentionPolicy(fs),
            new HiveTablePartitionTimeBasedRetentionPolicy(properties, fs));
    this.retentionPolicy = new CombineRetentionPolicy<HiveTablePartitionVersion>(policies,
        CombineRetentionPolicy.DeletableCombineOperation.UNION);
    this.simulate = Boolean.valueOf(properties
        .getProperty(MultiVersionCleanableDatasetBase.SIMULATE_KEY, MultiVersionCleanableDatasetBase.SIMULATE_DEFAULT));
  }

  @Override
  public void clean()
      throws IOException {
    Iterable<Either<Table, Partition>> deletableTableOrPartitions = Iterables.transform(
        this.retentionPolicy.listDeletableVersions(new ArrayList<>(this.versionFinder.findDatasetVersions(this))),
        new Function<HiveTablePartitionVersion, Either<Table, Partition>>() {
          @Override
          public Either<Table, Partition> apply(HiveTablePartitionVersion input) {
            return input.getVersion();
          }
        });
    for (Either<Table, Partition> tableOrPartitionToDelete : deletableTableOrPartitions) {
      this.cleanImpl(tableOrPartitionToDelete);
    }
  }

  protected void cleanImpl(Either<Table, Partition> tableOrPartition)
      throws IOException {
    try (AutoReturnableObject<IMetaStoreClient> client = this.clientPool.getClient()) {
      if (tableOrPartition instanceof Either.Right) {
        Partition paritionToDrop = ((Partition) ((Either.Right) tableOrPartition).getRight());
        if (this.simulate) {
          log.info("Simulate mode: will delete partition " + paritionToDrop.getValues() + " for table " + paritionToDrop
              .getTable().getTableName());
        }

        client.get().dropPartition(this.table.getDbName(), this.table.getTableName(), paritionToDrop.getValues(), true);
      } else {
        Table tableToDrop = ((Table) ((Either.Left) tableOrPartition).getLeft());
        if (this.simulate) {
          log.info("Simulate mode: will delete table " + tableToDrop.getTableName());
        }
        client.get().dropTable(tableToDrop.getDbName(), tableToDrop.getTableName(), true, true);
      }
    } catch (NoSuchObjectException e) {
      // Partition or table does not exist. Nothing to do
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Path datasetRoot() {
    return this.tableRootPath.isPresent() ? tableRootPath.get() : null;
  }
}
