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

package org.apache.gobblin.data.management.retention.dataset;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;

import org.apache.gobblin.data.management.retention.policy.RetentionPolicy;
import org.apache.gobblin.data.management.version.FileSystemDatasetVersion;
import org.apache.gobblin.data.management.version.finder.VersionFinder;
import org.apache.gobblin.dataset.FileSystemDataset;


/**
 * Implementation of a {@link CleanableDataset} that uses a
 * {@link org.apache.gobblin.data.management.retention.version.finder.VersionFinder} to find dataset versions, a
 * {@link org.apache.gobblin.data.management.retention.policy.RetentionPolicy} to figure out deletable versions, and then deletes
 * those files and newly empty parent directories.
 *
 * <p>
 *   Concrete subclasses should implement {@link #getVersionFinder} and {@link #getRetentionPolicy}.
 * </p>
 *
 * <p>
 * Datasets are directories in the filesystem containing data files organized in version-like directory structures.
 * Example datasets:
 * </p>
 *
 * <p>
 *   For snapshot based datasets, with the directory structure:
 *   <pre>
 *   /path/to/table/
 *      snapshot1/
 *          dataFiles...
 *      snapshot2/
 *          dataFiles...
 *   </pre>
 *   each of snapshot1 and snapshot2 are dataset versions.
 * </p>
 *
 * <p>
 *   For tracking datasets, with the directory structure:
 *   <pre>
 *   /path/to/tracking/data/
 *      2015/
 *          06/
 *              01/
 *                  dataFiles...
 *              02/
 *                  dataFiles...
 *   </pre>
 *   each of 2015/06/01 and 2015/06/02 are dataset versions.
 * </p>
 *
 * <p>
 *  {@link CleanableDatasetBase} uses a {@link org.apache.gobblin.data.management.version.finder.DatasetVersionFinder} to find all
 *  subdirectories that are versions of this dataset. After that, for each dataset, it uses a
 *  {@link org.apache.gobblin.data.management.retention.policy.RetentionPolicy} to decide which versions of the dataset should be
 *  deleted. For each version deleted, if {@link #deleteEmptyDirectories} it will also look at all parent directories
 *  and delete directories that are now empty, up to but not including the dataset root.
 * </p>
 *
 * @param <T> type of {@link org.apache.gobblin.data.management.retention.version.DatasetVersion} supported by this
 *           {@link CleanableDataset}.
 */
public abstract class CleanableDatasetBase<T extends FileSystemDatasetVersion>
    extends MultiVersionCleanableDatasetBase<T> implements CleanableDataset, FileSystemDataset {

  /**
   * Get {@link org.apache.gobblin.data.management.retention.version.finder.VersionFinder} to use.
   */
  public abstract VersionFinder<? extends T> getVersionFinder();

  /**
   * Get {@link org.apache.gobblin.data.management.retention.policy.RetentionPolicy} to use.
   */
  public abstract RetentionPolicy<T> getRetentionPolicy();

  public CleanableDatasetBase(final FileSystem fs, final Properties props, Config config, Logger log)
      throws IOException {
    super(fs, props, config, log);
  }

  public CleanableDatasetBase(final FileSystem fs, final Properties props, Logger log) throws IOException {
    super(fs, props, log);
  }

  public CleanableDatasetBase(FileSystem fs, Properties properties, boolean simulate, boolean skipTrash,
      boolean deleteEmptyDirectories, boolean deleteAsOwner, boolean isDatasetBlacklisted, Logger log)
      throws IOException {
    super(fs, properties, simulate, skipTrash, deleteEmptyDirectories, deleteAsOwner, isDatasetBlacklisted, log);
  }

  public CleanableDatasetBase(FileSystem fs, Properties properties, boolean simulate, boolean skipTrash,
      boolean deleteEmptyDirectories, boolean deleteAsOwner, Logger log) throws IOException {
    super(fs, properties, simulate, skipTrash, deleteEmptyDirectories, deleteAsOwner,
        Boolean.parseBoolean(IS_DATASET_BLACKLISTED_DEFAULT), log);
  }

  @Override
  public List<VersionFinderAndPolicy<T>> getVersionFindersAndPolicies() {
    return ImmutableList
        .<VersionFinderAndPolicy<T>> of(new VersionFinderAndPolicy<>(getRetentionPolicy(), getVersionFinder()));
  }
}
