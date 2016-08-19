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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import lombok.AllArgsConstructor;
import lombok.Getter;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.data.management.policy.EmbeddedRetentionSelectionPolicy;
import gobblin.data.management.policy.VersionSelectionPolicy;
import gobblin.data.management.retention.policy.RetentionPolicy;
import gobblin.data.management.trash.ProxiedTrash;
import gobblin.data.management.version.FileSystemDatasetVersion;
import gobblin.data.management.version.finder.VersionFinder;
import gobblin.dataset.FileSystemDataset;
import gobblin.util.ConfigUtils;


/**
 * {@link MultiVersionCleanableDatasetBase#getVersionFindersAndPolicies()}
 * Implementation of a {@link CleanableDataset} that may have several types of version and selctionPolicies.
 * <ul>
 * <li>{@link MultiVersionCleanableDatasetBase#getVersionFindersAndPolicies()} gets a list {@link VersionFinderAndPolicy}s
 * <li>Each {@link VersionFinderAndPolicy} contains a {@link VersionFinder} and a {@link VersionSelectionPolicy}.
 * <li>The {@link MultiVersionCleanableDatasetBase#clean()} method finds all the {@link FileSystemDatasetVersion}s for a
 * {@link VersionFinderAndPolicy#versionFinder} and gets the deletable {@link FileSystemDatasetVersion}s by applying
 * {@link VersionFinderAndPolicy#versionSelectionPolicy}. These deletable version are deleted  and then deletes empty parent directories.
 * </ul>
 * For each different types of versions with uses a
 * {@link gobblin.data.management.retention.version.finder.VersionFinder} to find dataset versions, a
 * {@link gobblin.data.management.retention.policy.RetentionPolicy} to figure out deletable versions,
 *
 * <p>
 *   Concrete subclasses should implement {@link #getVersionFindersAndPolicies()}
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
 * @param <T> type of {@link FileSystemDatasetVersion} supported by this {@link CleanableDataset}.
 */
public abstract class MultiVersionCleanableDatasetBase<T extends FileSystemDatasetVersion>
    implements CleanableDataset, FileSystemDataset {

  /**
   * @deprecated in favor of {@link FsCleanableHelper}
   */
  @Deprecated
  public static final String CONFIGURATION_KEY_PREFIX = FsCleanableHelper.CONFIGURATION_KEY_PREFIX;

  /**
   * @deprecated in favor of {@link FsCleanableHelper}
   */
  @Deprecated
  public static final String SIMULATE_KEY = FsCleanableHelper.SIMULATE_KEY;
  public static final String SIMULATE_DEFAULT = FsCleanableHelper.SIMULATE_DEFAULT;

  /**
   * @deprecated in favor of {@link FsCleanableHelper}
   */
  @Deprecated
  public static final String SKIP_TRASH_KEY = FsCleanableHelper.SKIP_TRASH_KEY;
  public static final String SKIP_TRASH_DEFAULT = FsCleanableHelper.SKIP_TRASH_DEFAULT;

  /**
   * @deprecated in favor of {@link FsCleanableHelper}
   */
  @Deprecated
  public static final String DELETE_EMPTY_DIRECTORIES_KEY = FsCleanableHelper.DELETE_EMPTY_DIRECTORIES_KEY;
  public static final String DELETE_EMPTY_DIRECTORIES_DEFAULT = FsCleanableHelper.DELETE_EMPTY_DIRECTORIES_DEFAULT;

  /**
   * @deprecated in favor of {@link FsCleanableHelper}
   */
  @Deprecated
  public static final String DELETE_AS_OWNER_KEY = FsCleanableHelper.DELETE_AS_OWNER_KEY;
  public static final String DELETE_AS_OWNER_DEFAULT = FsCleanableHelper.DELETE_AS_OWNER_DEFAULT;

  public static final String IS_DATASET_BLACKLISTED_KEY = CONFIGURATION_KEY_PREFIX + "dataset.is.blacklisted";
  public static final String IS_DATASET_BLACKLISTED_DEFAULT = Boolean.toString(false);

  protected final FileSystem fs;

  /**
   * @deprecated in favor of {@link FsCleanableHelper}
   */
  @Deprecated
  protected final ProxiedTrash trash;

  @Getter
  @VisibleForTesting
  protected final boolean isDatasetBlacklisted;

  private final FsCleanableHelper fsCleanableHelper;

  protected final Logger log;

  /**
   * @deprecated in favor of {@link FsCleanableHelper}
   */
  @Deprecated
  protected final boolean simulate;
  /**
   * @deprecated in favor of {@link FsCleanableHelper}
   */
  @Deprecated
  protected final boolean skipTrash;
  /**
   * @deprecated in favor of {@link FsCleanableHelper}
   */
  @Deprecated
  protected final boolean deleteEmptyDirectories;
  /**
   * @deprecated in favor of {@link FsCleanableHelper}
   */
  @Deprecated
  protected final boolean deleteAsOwner;
  /**
   * Get {@link gobblin.data.management.retention.policy.RetentionPolicy} to use.
   */
  public abstract List<VersionFinderAndPolicy<T>> getVersionFindersAndPolicies();

  public MultiVersionCleanableDatasetBase(final FileSystem fs, final Properties props, Config config, Logger log)
      throws IOException {
    this(fs, props, Boolean.valueOf(props.getProperty(SIMULATE_KEY, SIMULATE_DEFAULT)),
        Boolean.valueOf(props.getProperty(SKIP_TRASH_KEY, SKIP_TRASH_DEFAULT)),
        Boolean.valueOf(props.getProperty(DELETE_EMPTY_DIRECTORIES_KEY, DELETE_EMPTY_DIRECTORIES_DEFAULT)),
        Boolean.valueOf(props.getProperty(DELETE_AS_OWNER_KEY, DELETE_AS_OWNER_DEFAULT)),
        ConfigUtils.getBoolean(config, IS_DATASET_BLACKLISTED_KEY, Boolean.valueOf(IS_DATASET_BLACKLISTED_DEFAULT)), log);
  }

  public MultiVersionCleanableDatasetBase(final FileSystem fs, final Properties props, Logger log) throws IOException {
    // This constructor is used by retention jobs configured through job configs and do not use dataset configs from config store.
    // IS_DATASET_BLACKLISTED_KEY is only available with dataset config. Hence set IS_DATASET_BLACKLISTED_KEY to default
    // ...false for jobs running with job configs
    this(fs, props, ConfigFactory.parseMap(ImmutableMap.<String, String> of(IS_DATASET_BLACKLISTED_KEY,
        IS_DATASET_BLACKLISTED_DEFAULT)), log);
  }

  /**
   * Constructor for {@link MultiVersionCleanableDatasetBase}.
   * @param fs {@link org.apache.hadoop.fs.FileSystem} where files are located.
   * @param properties {@link java.util.Properties} for object.
   * @param simulate whether to simulate deletes.
   * @param skipTrash if true, delete files and directories immediately.
   * @param deleteEmptyDirectories if true, newly empty parent directories will be deleted.
   * @param deleteAsOwner if true, all deletions will be executed as the owner of the file / directory.
   * @param log logger to use.
   * @param isDatasetBlacklisted if true, clean will be skipped for this dataset
   *
   * @throws IOException
   */
  public MultiVersionCleanableDatasetBase(FileSystem fs, Properties properties, boolean simulate, boolean skipTrash,
      boolean deleteEmptyDirectories, boolean deleteAsOwner, boolean isDatasetBlacklisted, Logger log)
      throws IOException {
    this.log = log;
    this.fsCleanableHelper = new FsCleanableHelper(fs, properties, simulate, skipTrash, deleteEmptyDirectories, deleteAsOwner, log);
    this.fs = fs;
    this.simulate = simulate;
    this.skipTrash = skipTrash;
    this.deleteEmptyDirectories = deleteEmptyDirectories;
    this.trash = this.fsCleanableHelper.getTrash();
    this.deleteAsOwner = deleteAsOwner;
    this.isDatasetBlacklisted = isDatasetBlacklisted;


  }

  public MultiVersionCleanableDatasetBase(FileSystem fs, Properties properties, boolean simulate, boolean skipTrash,
      boolean deleteEmptyDirectories, boolean deleteAsOwner, Logger log) throws IOException {
    this(fs, properties, simulate, skipTrash, deleteEmptyDirectories, deleteAsOwner,
        Boolean.parseBoolean(IS_DATASET_BLACKLISTED_DEFAULT), log);
  }

  /**
   * Perform the cleanup of old / deprecated dataset versions. See {@link gobblin.data.management.retention.DatasetCleanerNew}
   * javadoc for more information.
   * @throws java.io.IOException
   */
  @Override
  public void clean() throws IOException {

    if (this.isDatasetBlacklisted) {
      this.log.info("Dataset blacklisted. Cleanup skipped for " + datasetRoot());
      return;
    }

    for (VersionFinderAndPolicy<T> versionFinderAndPolicy : getVersionFindersAndPolicies()) {

      VersionSelectionPolicy<T> selectionPolicy = versionFinderAndPolicy.getVersionSelectionPolicy();
      VersionFinder<? extends T> versionFinder = versionFinderAndPolicy.getVersionFinder();

      if (!selectionPolicy.versionClass().isAssignableFrom(versionFinder.versionClass())) {
        throw new IOException("Incompatible dataset version classes.");
      }

      this.log.info(String.format("Cleaning dataset %s. Using version finder %s and policy %s", this,
          versionFinder.getClass().getName(), selectionPolicy.getClass().getName()));

      List<T> versions = Lists.newArrayList(versionFinder.findDatasetVersions(this));

      if (versions.isEmpty()) {
        this.log.warn("No dataset version can be found. Ignoring.");
        continue;
      }

      Collections.sort(versions, Collections.reverseOrder());

      Collection<T> deletableVersions = selectionPolicy.listSelectedVersions(versions);

      cleanImpl(deletableVersions);
    }

  }

  protected void cleanImpl(Collection<T> deletableVersions) throws IOException {
    this.fsCleanableHelper.clean(deletableVersions, this);
  }

  @Override
  public String toString() {
    return datasetRoot().toString();
  }

  @Override
  public String datasetURN() {
    return this.datasetRoot().toString();
  }

  @AllArgsConstructor
  @Getter
  public static class VersionFinderAndPolicy<T extends FileSystemDatasetVersion> {
    VersionSelectionPolicy<T> versionSelectionPolicy;
    VersionFinder<? extends T> versionFinder;

    public VersionFinderAndPolicy(RetentionPolicy<T> retentionPolicy, VersionFinder<? extends T> versionFinder) {
      this(new EmbeddedRetentionSelectionPolicy<>(retentionPolicy), versionFinder);
    }
  }
}
