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

package gobblin.data.management.copy;

import gobblin.data.management.dataset.DatasetUtils;
import gobblin.util.PathUtils;
import gobblin.util.FileListUtils;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;


/**
 * Implementation of {@link CopyableDataset} that creates a {@link CopyableFile} for every file that is a descendant if
 * the root directory.
 */
public class RecursiveCopyableDataset extends SinglePartitionCopyableDataset {

  private final Path rootPath;
  private final FileSystem fs;
  private final Properties properties;
  private LoadingCache<Path, OwnerAndPermission> ownerAndPermissionCache;
  private final PathFilter pathFilter;

  public RecursiveCopyableDataset(final FileSystem fs, Path rootPath, Properties properties) {

    this.rootPath = PathUtils.getPathWithoutSchemeAndAuthority(rootPath);
    this.fs = fs;
    this.properties = properties;
    this.ownerAndPermissionCache = CacheBuilder.newBuilder().build(new CacheLoader<Path, OwnerAndPermission>() {
      @Override
      public OwnerAndPermission load(Path path) throws Exception {
        FileStatus fileStatus = fs.getFileStatus(path);
        return new OwnerAndPermission(fileStatus.getOwner(), fileStatus.getGroup(), fileStatus.getPermission());
      }
    });

    this.pathFilter = DatasetUtils.instantiatePathFilter(properties);
  }

  @Override public List<CopyableFile> getCopyableFiles(FileSystem targetFs, Path targetRoot) throws IOException {

    List<FileStatus> files = FileListUtils.listFilesRecursively(this.fs, this.rootPath, this.pathFilter);

    List<CopyableFile> copyableFiles = Lists.newArrayList();

    for (FileStatus file : files) {

      Path relativeOutputPath = getRelativeOuptutPath(file);

      Path outputPath = new Path(targetRoot, relativeOutputPath);

      OwnerAndPermission ownerAndPermission =
          new OwnerAndPermission(file.getOwner(), file.getGroup(), file.getPermission());
      List<OwnerAndPermission> ancestorOwnerAndPermissions = Lists.newArrayList();
      try {
        Path currentPath = PathUtils.getPathWithoutSchemeAndAuthority(file.getPath());
        while (currentPath != null && currentPath.getParent() != null && !currentPath.getParent().equals(this.rootPath)) {
          currentPath = currentPath.getParent();
          ancestorOwnerAndPermissions.add(this.ownerAndPermissionCache.get(currentPath));
        }
      } catch (ExecutionException ee) {
        // When cache loader failed.
      }

      FileChecksum checksum = this.fs.getFileChecksum(file.getPath());

      copyableFiles.add(new CopyableFile(file, outputPath, relativeOutputPath, ownerAndPermission,
          ancestorOwnerAndPermissions, checksum == null ? new byte[0] : checksum.getBytes()));
    }
    return copyableFiles;
  }

  /**
   * Get the expected output path of the file under {@link #datasetTargetRoot()}. Subclasses can override this method if
   * the file name needs to be different at destination.
   *
   * @param file whose relative outputPath will be returned
   * @return the relativeOutputPath
   */
  protected Path getRelativeOuptutPath(FileStatus file) {
    return PathUtils.relativizePath(PathUtils.getPathWithoutSchemeAndAuthority(file.getPath()),
        PathUtils.getPathWithoutSchemeAndAuthority(datasetRoot()));
  }

  @Override
  public Path datasetRoot() {
    return this.rootPath;
  }

}
