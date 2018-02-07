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

package org.apache.gobblin.data.management.copy;

import org.apache.gobblin.commit.CommitStep;
import org.apache.gobblin.data.management.copy.entities.PrePublishStep;
import org.apache.gobblin.data.management.dataset.DatasetUtils;
import org.apache.gobblin.dataset.FileSystemDataset;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.FileListUtils;
import org.apache.gobblin.util.commit.DeleteFileCommitStep;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


/**
 * Implementation of {@link CopyableDataset} that creates a {@link CopyableFile} for every file that is a descendant if
 * the root directory.
 */
public class RecursiveCopyableDataset implements CopyableDataset, FileSystemDataset {

  private static final String CONFIG_PREFIX = CopyConfiguration.COPY_PREFIX + ".recursive";
  /** Like -update in distcp. Will update files that are different between source and target, and skip files already in target. */
  public static final String UPDATE_KEY = CONFIG_PREFIX + ".update";
  /** Like -delete in distcp. Will delete files in target that don't exist in source. */
  public static final String DELETE_KEY = CONFIG_PREFIX + ".delete";
  /** If true, will delete newly empty directories up to the dataset root. */
  public static final String DELETE_EMPTY_DIRECTORIES_KEY = CONFIG_PREFIX + ".deleteEmptyDirectories";

  private final Path rootPath;
  private final FileSystem fs;
  private final PathFilter pathFilter;
  // Glob used to find this dataset
  private final Path glob;
  private final CopyableFileFilter copyableFileFilter;
  private final boolean update;
  private final boolean delete;

  // Include empty directories in the source for copy
  private final boolean includeEmptyDirectories;
  // Delete empty directories in the destination
  private final boolean deleteEmptyDirectories;
  //Apply filter to directories
  private final boolean applyFilterToDirectories;

  private final Properties properties;

  public RecursiveCopyableDataset(final FileSystem fs, Path rootPath, Properties properties, Path glob) {

    this.rootPath = PathUtils.getPathWithoutSchemeAndAuthority(rootPath);
    this.fs = fs;

    this.pathFilter = DatasetUtils.instantiatePathFilter(properties);
    this.copyableFileFilter = DatasetUtils.instantiateCopyableFileFilter(properties);
    this.glob = glob;

    this.update = Boolean.parseBoolean(properties.getProperty(UPDATE_KEY));
    this.delete = Boolean.parseBoolean(properties.getProperty(DELETE_KEY));
    this.deleteEmptyDirectories = Boolean.parseBoolean(properties.getProperty(DELETE_EMPTY_DIRECTORIES_KEY));
    this.includeEmptyDirectories =
        Boolean.parseBoolean(properties.getProperty(CopyConfiguration.INCLUDE_EMPTY_DIRECTORIES));
    this.applyFilterToDirectories =
        Boolean.parseBoolean(properties.getProperty(CopyConfiguration.APPLY_FILTER_TO_DIRECTORIES, "false"));
    this.properties = properties;
  }

  @Override
  public Collection<? extends CopyEntity> getCopyableFiles(FileSystem targetFs, CopyConfiguration configuration)
      throws IOException {

    Path nonGlobSearchPath = PathUtils.deepestNonGlobPath(this.glob);
    Path targetPath =
        new Path(configuration.getPublishDir(), PathUtils.relativizePath(this.rootPath, nonGlobSearchPath));

    Map<Path, FileStatus> filesInSource =
        createPathMap(getFilesAtPath(this.fs, this.rootPath, this.pathFilter), this.rootPath);
    Map<Path, FileStatus> filesInTarget =
        createPathMap(getFilesAtPath(targetFs, targetPath, this.pathFilter), targetPath);

    List<Path> toCopy = Lists.newArrayList();
    Map<Path, FileStatus> toDelete = Maps.newHashMap();
    boolean requiresUpdate = false;

    for (Map.Entry<Path, FileStatus> entry : filesInSource.entrySet()) {
      FileStatus statusInTarget = filesInTarget.remove(entry.getKey());
      if (statusInTarget != null) {
        // in both
        if (!sameFile(filesInSource.get(entry.getKey()), statusInTarget)) {
          toCopy.add(entry.getKey());
          toDelete.put(entry.getKey(), statusInTarget);
          requiresUpdate = true;
        }
      } else {
        toCopy.add(entry.getKey());
      }
    }

    if (!this.update && requiresUpdate) {
      throw new IOException("Some files need to be copied but they already exist in the destination. "
          + "Aborting because not running in update mode.");
    }

    if (this.delete) {
      toDelete.putAll(filesInTarget);
    }

    List<CopyEntity> copyEntities = Lists.newArrayList();
    List<CopyableFile> copyableFiles = Lists.newArrayList();

    for (Path path : toCopy) {
      FileStatus file = filesInSource.get(path);
      Path filePathRelativeToSearchPath = PathUtils.relativizePath(file.getPath(), nonGlobSearchPath);
      Path thisTargetPath = new Path(configuration.getPublishDir(), filePathRelativeToSearchPath);
      CopyableFile copyableFile =
          CopyableFile.fromOriginAndDestination(this.fs, file, thisTargetPath, configuration).fileSet(datasetURN())
              .datasetOutputPath(thisTargetPath.toString()).ancestorsOwnerAndPermission(CopyableFile
              .resolveReplicatedOwnerAndPermissionsRecursively(this.fs, file.getPath().getParent(), nonGlobSearchPath,
                  configuration)).build();
      copyableFile.setFsDatasets(this.fs, targetFs);
      copyableFiles.add(copyableFile);
    }
    copyEntities.addAll(this.copyableFileFilter.filter(this.fs, targetFs, copyableFiles));

    if (!toDelete.isEmpty()) {
      CommitStep step = new DeleteFileCommitStep(targetFs, toDelete.values(), this.properties,
          this.deleteEmptyDirectories ? Optional.of(targetPath) : Optional.<Path>absent());

      copyEntities.add(new PrePublishStep(datasetURN(), Maps.<String, String>newHashMap(), step, 1));
    }

    return copyEntities;
  }

  @VisibleForTesting
  protected List<FileStatus> getFilesAtPath(FileSystem fs, Path path, PathFilter fileFilter)
      throws IOException {
    try {
      return FileListUtils
          .listFilesToCopyAtPath(fs, path, fileFilter, applyFilterToDirectories, includeEmptyDirectories);
    } catch (FileNotFoundException fnfe) {
      return Lists.newArrayList();
    }
  }

  @Override
  public Path datasetRoot() {
    return this.rootPath;
  }

  @Override
  public String datasetURN() {
    return datasetRoot().toString();
  }

  private Map<Path, FileStatus> createPathMap(List<FileStatus> files, Path prefix) {
    Map<Path, FileStatus> map = Maps.newHashMap();
    for (FileStatus status : files) {
      map.put(PathUtils.relativizePath(status.getPath(), prefix), status);
    }
    return map;
  }

  private static boolean sameFile(FileStatus fileInSource, FileStatus fileInTarget) {
    return fileInTarget.getLen() == fileInSource.getLen() && fileInSource.getModificationTime() <= fileInTarget
        .getModificationTime();
  }
}
