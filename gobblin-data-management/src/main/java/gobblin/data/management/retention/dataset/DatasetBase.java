/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;

import com.google.common.collect.Lists;

import gobblin.data.management.retention.DatasetCleaner;
import gobblin.data.management.retention.policy.RetentionPolicy;
import gobblin.data.management.retention.version.DatasetVersion;
import gobblin.data.management.retention.version.finder.VersionFinder;
import gobblin.data.management.trash.ProxiedTrash;
import gobblin.data.management.trash.TrashFactory;


/**
 * Implementation of a {@link gobblin.data.management.retention.dataset.Dataset} that uses a
 * {@link gobblin.data.management.retention.version.finder.VersionFinder} to find dataset versions, a
 * {@link gobblin.data.management.retention.policy.RetentionPolicy} to figure out deletable versions, and then deletes
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
 *  {@link DatasetBase} uses a {@link gobblin.data.management.retention.version.finder.DatasetVersionFinder} to find all
 *  subdirectories that are versions of this dataset. After that, for each dataset, it uses a
 *  {@link gobblin.data.management.retention.policy.RetentionPolicy} to decide which versions of the dataset should be
 *  deleted. For each version deleted, if {@link #deleteEmptyDirectories} it will also look at all parent directories
 *  and delete directories that are now empty, up to but not including the dataset root.
 * </p>
 *
 * @param <T> type of {@link gobblin.data.management.retention.version.DatasetVersion} supported by this
 *           {@link gobblin.data.management.retention.dataset.Dataset}.
 */
public abstract class DatasetBase<T extends DatasetVersion> implements Dataset {

  public static final String CONFIGURATION_KEY_PREFIX = "gobblin.retention.";
  public static final String SIMULATE_KEY = CONFIGURATION_KEY_PREFIX + "simulate";
  public static final String SIMULATE_DEFAULT = Boolean.toString(false);
  public static final String SKIP_TRASH_KEY = CONFIGURATION_KEY_PREFIX + "skip.trash";
  public static final String SKIP_TRASH_DEFAULT = Boolean.toString(false);
  public static final String DELETE_EMPTY_DIRECTORIES_KEY = CONFIGURATION_KEY_PREFIX + "delete.empty.directories";
  public static final String DELETE_EMPTY_DIRECTORIES_DEFAULT = Boolean.toString(true);
  public static final String DELETE_AS_OWNER_KEY = CONFIGURATION_KEY_PREFIX + "delete.as.owner";
  public static final String DELETE_AS_OWNER_DEFAULT = Boolean.toString(true);

  protected final FileSystem fs;
  protected final ProxiedTrash trash;
  protected final boolean simulate;
  protected final boolean skipTrash;
  protected final boolean deleteEmptyDirectories;
  protected final boolean deleteAsOwner;

  protected final Logger log;

  /**
   * Get {@link gobblin.data.management.retention.version.finder.VersionFinder} to use.
   */
  public abstract VersionFinder<? extends T> getVersionFinder();

  /**
   * Get {@link gobblin.data.management.retention.policy.RetentionPolicy} to use.
   */
  public abstract RetentionPolicy<T> getRetentionPolicy();

  public DatasetBase(final FileSystem fs, final Properties props, Logger log) throws IOException {
    this(fs, props, Boolean.valueOf(props.getProperty(SIMULATE_KEY, SIMULATE_DEFAULT)), Boolean.valueOf(props
        .getProperty(SKIP_TRASH_KEY, SKIP_TRASH_DEFAULT)), Boolean.valueOf(props.getProperty(
        DELETE_EMPTY_DIRECTORIES_KEY, DELETE_EMPTY_DIRECTORIES_DEFAULT)), Boolean.valueOf(props.getProperty(
        DELETE_AS_OWNER_KEY, DELETE_AS_OWNER_DEFAULT)), log);
  }

  /**
   * Constructor for {@link DatasetBase}.
   * @param fs {@link org.apache.hadoop.fs.FileSystem} where files are located.
   * @param properties {@link java.util.Properties} for object.
   * @param simulate whether to simulate deletes.
   * @param skipTrash if true, delete files and directories immediately.
   * @param deleteEmptyDirectories if true, newly empty parent directories will be deleted.
   * @param deleteAsOwner if true, all deletions will be executed as the owner of the file / directory.
   * @param log logger to use.
   * @throws IOException
   */
  public DatasetBase(FileSystem fs, Properties properties, boolean simulate, boolean skipTrash,
      boolean deleteEmptyDirectories, boolean deleteAsOwner, Logger log) throws IOException {
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
  }

  /**
   * Perform the cleanup of old / deprecated dataset versions. See {@link gobblin.data.management.retention.DatasetCleaner}
   * javadoc for more information.
   * @throws java.io.IOException
   */
  @Override
  public void clean() throws IOException {

    RetentionPolicy<T> retentionPolicy = getRetentionPolicy();
    VersionFinder<? extends T> versionFinder = getVersionFinder();

    if (!retentionPolicy.versionClass().isAssignableFrom(versionFinder.versionClass())) {
      throw new IOException("Incompatible dataset version classes.");
    }

    this.log.info("Cleaning dataset " + this);

    List<T> versions = Lists.newArrayList(getVersionFinder().findDatasetVersions(this));

    if (versions.isEmpty()) {
      this.log.warn("No dataset version can be found. Ignoring.");
      return;
    }

    Collections.sort(versions, Collections.reverseOrder());

    Collection<T> deletableVersions = getRetentionPolicy().listDeletableVersions(versions);

    if (deletableVersions.isEmpty()) {
      this.log.warn("No deletable dataset version can be found. Ignoring.");
      return;
    }

    Set<Path> possiblyEmptyDirectories = new HashSet<Path>();

    for (DatasetVersion versionToDelete : deletableVersions) {
      this.log.info("Deleting dataset version " + versionToDelete);

      Set<Path> pathsToDelete = versionToDelete.getPathsToDelete();
      this.log.info("Deleting paths: " + Arrays.toString(pathsToDelete.toArray()));

      boolean deletedAllPaths = true;

      for (Path path : pathsToDelete) {
        if (path.equals(datasetRoot())) {
          this.log.info("Not deleting dataset root path: " + path);
          continue;
        }

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
    if (!parent.equals(datasetRoot) && this.fs.listStatus(parent).length == 0) {
      this.fs.delete(parent, false);
      deleteEmptyParentDirectories(datasetRoot, parent.getParent());
    }
  }

  @Override
  public String toString() {
    return datasetRoot().toString();
  }

}
