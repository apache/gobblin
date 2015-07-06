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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import azkaban.utils.Props;

import gobblin.data.management.retention.version.DatasetVersion;
import gobblin.data.management.retention.policy.RetentionPolicy;
import gobblin.data.management.retention.version.finder.VersionFinder;
import gobblin.data.management.trash.Trash;


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

  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetBase.class);

  public static final String CONFIGURATION_KEY_PREFIX = "gobblin.retention.";
  public static final String SIMULATE_KEY = CONFIGURATION_KEY_PREFIX + "simulate";
  public static final boolean SIMULATE_DEFAULT = false;
  public static final String SKIP_TRASH_KEY = CONFIGURATION_KEY_PREFIX + "skip.trash";
  public static final boolean SKIP_TRASH_DEFAULT = false;
  public static final String DELETE_EMPTY_DIRECTORIES_KEY = CONFIGURATION_KEY_PREFIX + "delete.empty.directories";
  public static final boolean DELETE_EMPTY_DIRECTORIES_DEFAULT = true;

  protected final FileSystem fs;
  protected final Trash trash;
  protected final boolean simulate;
  protected final boolean skipTrash;
  protected final boolean deleteEmptyDirectories;

  /**
   * Get {@link gobblin.data.management.retention.version.finder.VersionFinder} to use.
   */
  public abstract VersionFinder<? extends T> getVersionFinder();

  /**
   * Get {@link gobblin.data.management.retention.policy.RetentionPolicy} to use.
   * @return
   */
  public abstract RetentionPolicy<T> getRetentionPolicy();

  public DatasetBase(FileSystem fs, Props props) throws IOException {
    this.fs = fs;

    this.simulate = props.getBoolean(SIMULATE_KEY, SIMULATE_DEFAULT);
    this.skipTrash = props.getBoolean(SKIP_TRASH_KEY, SKIP_TRASH_DEFAULT);
    this.deleteEmptyDirectories = props.getBoolean(DELETE_EMPTY_DIRECTORIES_KEY, DELETE_EMPTY_DIRECTORIES_DEFAULT);

    if(this.simulate || this.skipTrash) {
      this.trash = null;
    } else {
      this.trash = new Trash(fs, props);
    }
  }

  public DatasetBase(FileSystem fs, Trash trash, boolean simulate, boolean skipTrash, boolean deleteEmptyDirectories) {
    this.fs = fs;
    this.trash = trash;
    this.simulate = simulate;
    this.skipTrash = skipTrash;
    this.deleteEmptyDirectories = deleteEmptyDirectories;
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

    if(!retentionPolicy.versionClass().isAssignableFrom(versionFinder.versionClass())) {
      throw new IOException("Incompatible dataset version classes.");
    }

    LOGGER.info("Cleaning dataset " + this);

    List<T> versions = Lists.newArrayList(getVersionFinder().findDatasetVersions(this));
    Collections.sort(versions, Collections.reverseOrder());

    List<T> deletableVersions =
        getRetentionPolicy().preserveDeletableVersions(versions);

    Set<Path> possiblyEmptyDirectories = new HashSet<Path>();

    for(DatasetVersion versionToDelete : deletableVersions) {
      LOGGER.info("Deleting dataset version " + versionToDelete);

      Set<Path> pathsToDelete = versionToDelete.getPathsToDelete();
      LOGGER.info("Deleting paths: " + Arrays.toString(pathsToDelete.toArray()));

      boolean deletedAllPaths = true;

      for(Path path: pathsToDelete) {
        if (!this.simulate) {
          boolean successfullyDeleted;
          if (this.skipTrash) {
            successfullyDeleted = this.fs.delete(path, true);
          } else {
            successfullyDeleted = this.trash.moveToTrash(path);
          }
          if (successfullyDeleted) {
            possiblyEmptyDirectories.add(path.getParent());
          } else {
            LOGGER.error("Failed to delete path " + path + " in dataset version " + versionToDelete);
            deletedAllPaths = false;
          }
        }
      }

      if(!deletedAllPaths) {
        LOGGER.error("Failed to delete some paths in dataset version " + versionToDelete);
      }

    }

    if(this.deleteEmptyDirectories) {
      for (Path parentDirectory : possiblyEmptyDirectories) {
        deleteEmptyParentDirectories(datasetRoot(), parentDirectory);
      }
    }
  }

  private void deleteEmptyParentDirectories(Path datasetRoot, Path parent) throws IOException {
    if(!parent.equals(datasetRoot) && this.fs.listStatus(parent).length == 0) {
      this.fs.delete(parent, false);
      deleteEmptyParentDirectories(datasetRoot, parent.getParent());
    }
  }

  @Override
  public String toString() {
    return datasetRoot().toString();
  }

}
