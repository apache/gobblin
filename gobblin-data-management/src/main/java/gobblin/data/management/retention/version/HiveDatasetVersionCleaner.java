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
package gobblin.data.management.retention.version;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.thrift.TException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import gobblin.data.management.retention.dataset.CleanableDataset;
import gobblin.data.management.retention.dataset.CleanableHiveDataset;
import gobblin.data.management.version.DatasetVersion;
import gobblin.data.management.version.HiveDatasetVersion;
import gobblin.util.AutoReturnableObject;
import gobblin.util.ConfigUtils;


/**
 * An abstraction for cleaning a {@link HiveDatasetVersionCleaner} of a {@link CleanableHiveDataset}.
 */
@Slf4j
public class HiveDatasetVersionCleaner extends VersionCleaner {

  public static final String REPLACEMENT_HIVE_DB_NAME_KEY = "hive.replacementHiveDbName";
  public static final String REPLACEMENT_HIVE_TABLE_NAME_KEY = "hive.replacementHiveTableName";
  public static final String SHOULD_REPLACE_PARTITION_KEY = "hive.shouldReplacePartition";

  private final CleanableHiveDataset cleanableHiveDataset;
  private final HiveDatasetVersion hiveDatasetVersion;

  private final Optional<String> replacementDbName;
  private final Optional<String> replacementTableName;

  public HiveDatasetVersionCleaner(DatasetVersion datasetVersion, CleanableDataset cleanableDataset) {
    super(datasetVersion, cleanableDataset);

    Preconditions.checkArgument(cleanableDataset instanceof CleanableHiveDataset, String.format("%s only supports %s, "
        + "found: %s", this.getClass(), CleanableHiveDataset.class, cleanableDataset.getClass()));
    Preconditions.checkArgument(datasetVersion instanceof HiveDatasetVersion, String.format("%s only supports %s, "
        + "found: %s", this.getClass(), HiveDatasetVersionCleaner.class, datasetVersion.getClass()));

    this.cleanableHiveDataset = (CleanableHiveDataset) cleanableDataset;
    this.hiveDatasetVersion = (HiveDatasetVersion) datasetVersion;

    // For post cleanup activity:
    // Get db / table name from which partition has to be replaced-in for the target partition being deleted.
    this.replacementDbName = Optional.fromNullable(ConfigUtils.getString(cleanableHiveDataset.getDatasetConfig(), REPLACEMENT_HIVE_DB_NAME_KEY, null));
    this.replacementTableName = Optional.fromNullable(ConfigUtils.getString(cleanableHiveDataset.getDatasetConfig(), REPLACEMENT_HIVE_TABLE_NAME_KEY, null));
  }

  @Override
  public void preCleanAction() throws IOException {
    // no-op
  }

  @Override
  public void clean() throws IOException {

    // Possible empty directories to clean for this partition (version)
    Set<Path> possiblyEmptyDirectories = new HashSet<>();

    try (AutoReturnableObject<IMetaStoreClient> client = cleanableHiveDataset.getClientPool().getClient()) {
      Partition partition = hiveDatasetVersion.getPartition();
      try {
        if (!cleanableHiveDataset.isSimulate()) {
          client.get().dropPartition(partition.getTable().getDbName(), partition.getTable().getTableName(), partition.getValues(), false);
          log.info("Successfully dropped partition " + partition.getCompleteName());
        } else {
          log.info("Simulating drop partition " + partition.getCompleteName());
        }
        if (cleanableHiveDataset.isShouldDeleteData()) {
          cleanableHiveDataset.getFsCleanableHelper().clean(hiveDatasetVersion, possiblyEmptyDirectories);
        }
      } catch (TException | IOException e) {
        log.warn(String.format("Failed to completely delete partition %s.", partition.getCompleteName()), e);
        throw new IOException(e);
      }
    }
    cleanableHiveDataset.getFsCleanableHelper().cleanEmptyDirectories(possiblyEmptyDirectories, cleanableHiveDataset);
  }

  @Override
  public void postCleanAction() throws IOException {
    // As a post-cleanup activity, Hive dataset version cleaner supports swapping-in a different partition.
    // So, if configured, swap-in the other partition.
    boolean shouldReplacePartition = shouldReplacePartition(cleanableHiveDataset.getDatasetConfig(),
        hiveDatasetVersion.getPartition().getTable().getDbName(), hiveDatasetVersion.getPartition().getTable().getTableName(),
        this.replacementDbName, this.replacementTableName);
    // Replace the partition being dropped with a replacement partition from another table (if configured)
    // This is required for cases such as when we want to replace-in a different storage format partition in
    // .. a hybrid table. Eg. Replace ORC partition with Avro or vice-versa
    if (shouldReplacePartition) {
      try (AutoReturnableObject<IMetaStoreClient> client = cleanableHiveDataset.getClientPool().getClient()) {
        org.apache.hadoop.hive.metastore.api.Partition sourcePartition = client.get().getPartition(
            this.replacementDbName.get(),
            this.replacementTableName.get(),
            hiveDatasetVersion.getPartition().getValues());
        org.apache.hadoop.hive.metastore.api.Partition replacementPartition = new org.apache.hadoop.hive.metastore.api.Partition(
            hiveDatasetVersion.getPartition().getValues(),
            hiveDatasetVersion.getPartition().getTable().getDbName(),
            hiveDatasetVersion.getPartition().getTable().getTableName(),
            sourcePartition.getCreateTime(),
            sourcePartition.getLastAccessTime(),
            sourcePartition.getSd(),
            sourcePartition.getParameters());
        if (!cleanableHiveDataset.isSimulate()) {
          client.get().add_partition(replacementPartition);
          log.info("Successfully swapped partition " + replacementPartition);
        } else {
          log.info("Simulating swap partition " + replacementPartition);
        }
      } catch (TException e) {
        log.warn(String.format("Failed to swap-in replacement partition for partition being deleted: %s",
            hiveDatasetVersion.getPartition().getCompleteName()), e);
        throw new IOException(e);
      }
    }
  }

  /***
   * Determine if a partition should be replaced-in from another table for a partition being deleted from
   * the current table.
   *
   * @param config                          Config to get check if partition replacement is enabled.
   * @param replacedPartitionDbName         Database name for the table from where partition is being deleted.
   * @param replacedPartitionTableName      Table name from where partition is being deleted.
   * @param replacementPartitionDbName      Database name from where the partition should be registered.
   * @param replacementPartitionTableName   Table name from where the partition should be registered.
   * @return  True if partition should be replaced-in from another table.
   */
  @VisibleForTesting
  protected static boolean shouldReplacePartition(Config config,
      String replacedPartitionDbName, String replacedPartitionTableName,
      Optional<String> replacementPartitionDbName, Optional<String> replacementPartitionTableName) {
    // If disabled explicitly, rest does not matters
    boolean shouldReplacePartition = ConfigUtils.getBoolean(config, SHOULD_REPLACE_PARTITION_KEY, false);

    // If any of the replacement DB name or replacement Table name is missing, then do not replace partition
    if (!replacementPartitionDbName.isPresent() || !replacementPartitionTableName.isPresent()) {
      return false;
    }
    // If not disabled explicitly, check if source db / table are same as the replacement partition's db / table
    // .. if so, do not try replacement.
    else {
      return shouldReplacePartition
          && !(replacedPartitionDbName.equalsIgnoreCase(replacementPartitionDbName.get()) &&
          replacedPartitionTableName.equalsIgnoreCase(replacementPartitionTableName.get()));
    }
  }
}
