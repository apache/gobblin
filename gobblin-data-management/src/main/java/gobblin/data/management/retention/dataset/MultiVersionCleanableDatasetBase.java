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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import lombok.AllArgsConstructor;
import lombok.Getter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.data.management.policy.EmbeddedRetentionSelectionPolicy;
import gobblin.data.management.policy.VersionSelectionPolicy;
import gobblin.data.management.retention.policy.RetentionPolicy;
import gobblin.data.management.trash.ProxiedTrash;
import gobblin.data.management.trash.TrashFactory;
import gobblin.data.management.version.FileSystemDatasetVersion;
import gobblin.data.management.version.finder.VersionFinder;
import gobblin.dataset.FileSystemDataset;
import gobblin.util.PathUtils;


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
public abstract class MultiVersionCleanableDatasetBase<T extends FileSystemDatasetVersion> implements CleanableDataset, FileSystemDataset {

  public static final String CONFIGURATION_KEY_PREFIX = "gobblin.retention.";
  public static final String SIMULATE_KEY = CONFIGURATION_KEY_PREFIX + "simulate";
  public static final String SIMULATE_DEFAULT = Boolean.toString(false);
  public static final String SKIP_TRASH_KEY = CONFIGURATION_KEY_PREFIX + "skip.trash";
  public static final String SKIP_TRASH_DEFAULT = Boolean.toString(false);
  public static final String DELETE_EMPTY_DIRECTORIES_KEY = CONFIGURATION_KEY_PREFIX + "delete.empty.directories";
  public static final String IS_DATASET_BLACKLISTED_KEY = CONFIGURATION_KEY_PREFIX + "dataset.is.blacklisted";

  public static final String DELETE_EMPTY_DIRECTORIES_DEFAULT = Boolean.toString(true);
  public static final String DELETE_AS_OWNER_KEY = CONFIGURATION_KEY_PREFIX + "delete.as.owner";
  public static final String DELETE_AS_OWNER_DEFAULT = Boolean.toString(true);
  public static final String IS_DATASET_BLACKLISTED_DEFAULT = Boolean.toString(false);

  protected final FileSystem fs;
  protected final ProxiedTrash trash;
  protected final boolean simulate;
  protected final boolean skipTrash;
  protected final boolean deleteEmptyDirectories;
  protected final boolean deleteAsOwner;
  protected final boolean isDatasetBlacklisted;

  protected final Logger log;

  /**
   * Get {@link gobblin.data.management.retention.policy.RetentionPolicy} to use.
   */
  public abstract List<VersionFinderAndPolicy<T>> getVersionFindersAndPolicies();

  public MultiVersionCleanableDatasetBase(final FileSystem fs, final Properties props, Config config, Logger log)
      throws IOException {
    this(fs, props, Boolean.valueOf(props.getProperty(SIMULATE_KEY, SIMULATE_DEFAULT)), Boolean.valueOf(props
        .getProperty(SKIP_TRASH_KEY, SKIP_TRASH_DEFAULT)), Boolean.valueOf(props.getProperty(
        DELETE_EMPTY_DIRECTORIES_KEY, DELETE_EMPTY_DIRECTORIES_DEFAULT)), Boolean.valueOf(props.getProperty(
        DELETE_AS_OWNER_KEY, DELETE_AS_OWNER_DEFAULT)), config.getBoolean(IS_DATASET_BLACKLISTED_KEY), log);
  }

  public MultiVersionCleanableDatasetBase(final FileSystem fs, final Properties props, Logger log) throws IOException {
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
      boolean deleteEmptyDirectories, boolean deleteAsOwner, boolean isDatasetBlacklisted, Logger log) throws IOException {
    this.log = log;
    this.fs = fs;
    this.simulate = simulate;
    this.skipTrash = skipTrash;
    this.deleteEmptyDirectories = deleteEmptyDirectories;
    Properties thisProperties = new Properties();
    thisProperties.putAll(properties);
    if (this.simulate) {
      thisProperties.setProperty(TrashFactory.SIMULATE, Boolean.toString(true));
    }
    if (this.skipTrash) {
      thisProperties.setProperty(TrashFactory.SKIP_TRASH, Boolean.toString(true));
    }
    this.trash = TrashFactory.createProxiedTrash(this.fs, thisProperties);
    this.deleteAsOwner = deleteAsOwner;
    this.isDatasetBlacklisted = isDatasetBlacklisted;
  }

  public MultiVersionCleanableDatasetBase(FileSystem fs, Properties properties, boolean simulate, boolean skipTrash,
      boolean deleteEmptyDirectories, boolean deleteAsOwner, Logger log) throws IOException {
    this(fs, properties, simulate, skipTrash, deleteEmptyDirectories, deleteAsOwner, Boolean
        .parseBoolean(IS_DATASET_BLACKLISTED_DEFAULT), log);
  }

  /**
   * Perform the cleanup of old / deprecated dataset versions. See {@link gobblin.data.management.retention.DatasetCleanerNew}
   * javadoc for more information.
   * @throws java.io.IOException
   */
  @Override
  public void clean() throws IOException {

    if (this.isDatasetBlacklisted) {
      log.info("Dataset blacklisted. Cleanup skipped for " + datasetRoot());
      return;
    }

    for (VersionFinderAndPolicy<T> versionFinderAndPolicy : getVersionFindersAndPolicies()) {

      VersionSelectionPolicy<T> selectionPolicy = versionFinderAndPolicy.getVersionSelectionPolicy();
      VersionFinder<? extends T> versionFinder = versionFinderAndPolicy.getVersionFinder();

      if (!selectionPolicy.versionClass().isAssignableFrom(versionFinder.versionClass())) {
        throw new IOException("Incompatible dataset version classes.");
      }

      this.log.info(String.format("Cleaning dataset %s. Using version finder %s and policy %s", this, versionFinder
          .getClass().getName(), selectionPolicy.getClass().getName()));

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
    if (deletableVersions.isEmpty()) {
      this.log.warn("No deletable dataset version can be found. Ignoring.");
      return;
    }

    Set<Path> possiblyEmptyDirectories = new HashSet<Path>();

    for (FileSystemDatasetVersion versionToDelete : deletableVersions) {
      this.log.info("Deleting dataset version " + versionToDelete);

      Set<Path> pathsToDelete = versionToDelete.getPaths();
      this.log.info("Deleting paths: " + Arrays.toString(pathsToDelete.toArray()));

      boolean deletedAllPaths = true;

      for (Path path : pathsToDelete) {

        boolean successfullyDeleted =
            this.deleteAsOwner ? this.trash.moveToTrashAsOwner(path) : this.trash.moveToTrash(path);

        if (successfullyDeleted) {
          possiblyEmptyDirectories.add(path.getParent());
        } else {
          this.log.error("Failed to delete path " + path + " in dataset version " + versionToDelete);
          deletedAllPaths = false;
        }
      }

      if (!deletedAllPaths) {
        this.log.error("Failed to delete some paths in dataset version " + versionToDelete);
      }

    }

    if (this.deleteEmptyDirectories) {
      for (Path parentDirectory : possiblyEmptyDirectories) {
        deleteEmptyParentDirectories(datasetRoot(), parentDirectory);
      }
    }
  }

  private void deleteEmptyParentDirectories(Path datasetRoot, Path parent) throws IOException {
    if (PathUtils.isAncestor(datasetRoot, parent)
        && !PathUtils.getPathWithoutSchemeAndAuthority(datasetRoot).equals(PathUtils.getPathWithoutSchemeAndAuthority(parent))
        && this.fs.listStatus(parent).length == 0) {
      if (!this.fs.delete(parent, false)) {
        log.warn("Failed to delete empty directory " + parent);
      } else {
        log.info("Deleted empty directory " + parent);
      }
      deleteEmptyParentDirectories(datasetRoot, parent.getParent());
    }
  }

  @Override
  public String toString() {
    return datasetRoot().toString();
  }

  @Override public String datasetURN() {
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
