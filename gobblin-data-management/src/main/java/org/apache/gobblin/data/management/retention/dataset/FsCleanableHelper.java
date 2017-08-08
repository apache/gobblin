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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import lombok.Getter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;

import com.typesafe.config.Config;

import org.apache.gobblin.data.management.trash.ProxiedTrash;
import org.apache.gobblin.data.management.trash.TrashFactory;
import org.apache.gobblin.data.management.version.FileSystemDatasetVersion;
import org.apache.gobblin.dataset.FileSystemDataset;
import org.apache.gobblin.util.PathUtils;

/**
 * A helper class to delete {@link Path}s of a FileSystemDatasetVersion.
 * <p>
 * Supports the following job level settings:
 * <ul>
 * <li> Simulate Mode - Only log paths to be deleted without deleting data by setting {@value #SIMULATE_KEY} to true.
 * <li> Skip Trash - Delete permanent by setting {@value #SKIP_TRASH_KEY} to true.
 * <li> Auto delete empty parent directories - By setting {@value #DELETE_EMPTY_DIRECTORIES_KEY} to true.
 * <li> Proxy as owner and delete - By setting {@value #DELETE_AS_OWNER_KEY} to true.
 * </ul>
 * </p>
 */
public class FsCleanableHelper {

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
  @Getter
  protected final ProxiedTrash trash;
  protected final boolean simulate;
  protected final boolean skipTrash;
  protected final boolean deleteEmptyDirectories;
  protected final boolean deleteAsOwner;
  protected final Logger log;

  public FsCleanableHelper(FileSystem fs, Properties properties, boolean simulate, boolean skipTrash, boolean deleteEmptyDirectories, boolean deleteAsOwner,
      Logger log) throws IOException {
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

  public FsCleanableHelper(final FileSystem fs, final Properties props, Config config, Logger log) throws IOException {
    this(fs, props, Boolean.valueOf(props.getProperty(SIMULATE_KEY, SIMULATE_DEFAULT)),
        Boolean.valueOf(props.getProperty(SKIP_TRASH_KEY, SKIP_TRASH_DEFAULT)),
        Boolean.valueOf(props.getProperty(DELETE_EMPTY_DIRECTORIES_KEY, DELETE_EMPTY_DIRECTORIES_DEFAULT)),
        Boolean.valueOf(props.getProperty(DELETE_AS_OWNER_KEY, DELETE_AS_OWNER_DEFAULT)), log);
  }

  /**
   * Delete a single {@link FileSystemDatasetVersion}. All the parent {@link Path}s are after deletion, are
   * added to <code>possiblyEmptyDirectories</code>. Caller need to call {@link #cleanEmptyDirectories(Set, FileSystemDataset)}
   * to delete empty parent directories if any.
   */
  public void clean(final FileSystemDatasetVersion versionToDelete, final Set<Path> possiblyEmptyDirectories) throws IOException {
    log.info("Deleting dataset version " + versionToDelete);

    Set<Path> pathsToDelete = versionToDelete.getPaths();
    log.info("Deleting paths: " + Arrays.toString(pathsToDelete.toArray()));

    boolean deletedAllPaths = true;

    for (Path path : pathsToDelete) {

      if (!this.fs.exists(path)) {
        log.info(String.format("Path %s in dataset version %s does not exist", path, versionToDelete));
        continue;
      }

      boolean successfullyDeleted = deleteAsOwner ? trash.moveToTrashAsOwner(path) : trash.moveToTrash(path);

      if (successfullyDeleted) {
        possiblyEmptyDirectories.add(path.getParent());
      } else {
        log.error("Failed to delete path " + path + " in dataset version " + versionToDelete);
        deletedAllPaths = false;
      }
    }

    if (!deletedAllPaths) {
      log.error("Failed to delete some paths in dataset version " + versionToDelete);
    }
  }

  /**
   * Delete all {@link FileSystemDatasetVersion}s <code>deletableVersions</code> and also delete any empty parent directories.
   *
   * @param fsDataset to which the version belongs.
   */
  public void clean(final Collection<? extends FileSystemDatasetVersion> deletableVersions, final FileSystemDataset fsDataset) throws IOException {
    if (deletableVersions.isEmpty()) {
      log.warn("No deletable dataset version can be found. Ignoring.");
      return;
    }
    Set<Path> possiblyEmptyDirectories = new HashSet<>();
    for (FileSystemDatasetVersion fsdv : deletableVersions) {
      clean(fsdv, possiblyEmptyDirectories);
    }
    cleanEmptyDirectories(possiblyEmptyDirectories, fsDataset);
  }

  /**
   * Deletes any empty paths in <code>possiblyEmptyDirectories</code> all the way upto the {@link FileSystemDataset#datasetRoot()}.
   */
  public void cleanEmptyDirectories(final Set<Path> possiblyEmptyDirectories, final FileSystemDataset fsDataset) throws IOException {
    if (this.deleteEmptyDirectories && !this.simulate) {
      for (Path parentDirectory : possiblyEmptyDirectories) {
        PathUtils.deleteEmptyParentDirectories(fs, fsDataset.datasetRoot(), parentDirectory);
      }
    }
  }
}
