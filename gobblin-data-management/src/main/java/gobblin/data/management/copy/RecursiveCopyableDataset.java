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

import gobblin.data.management.util.PathUtils;
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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;


/**
 * Implementation of {@link CopyableDataset} that creates a {@link CopyableFile} for every file that is a descendant
 * if the root directory.
 */
public class RecursiveCopyableDataset extends  SinglePartitionCopyableDataset {

  public static final String TARGET_DIRECTORY = "gobblin.copyable.dataset.target.directory";

  private final Path rootPath;
  private final FileSystem _fs;
  private final Path targetDirectory;
  private final Properties properties;
  private LoadingCache<Path, OwnerAndPermission> ownerAndPermissionCache;
  private final PathFilter pathFilter;

  public RecursiveCopyableDataset(FileSystem fs, Path rootPath, Properties properties) {

    Preconditions.checkArgument(!Strings.isNullOrEmpty(properties.getProperty(TARGET_DIRECTORY)),
        "Missing property " + TARGET_DIRECTORY);

    this.rootPath = PathUtils.getPathWithoutSchemeAndAuthority(rootPath);
    this._fs = fs;
    this.targetDirectory = new Path(properties.getProperty(TARGET_DIRECTORY));
    this.properties = properties;
    this.ownerAndPermissionCache = CacheBuilder.newBuilder().build(new CacheLoader<Path, OwnerAndPermission>() {
      @Override public OwnerAndPermission load(Path path) throws Exception {
        FileStatus fileStatus = _fs.getFileStatus(path);
        return new OwnerAndPermission(fileStatus.getOwner(), fileStatus.getGroup(), fileStatus.getPermission());
      }
    });

    this.pathFilter = new PathFilter() {

      @Override
      public boolean accept(Path path) {
        return path.getName().endsWith(".gpg") && path.getName().startsWith("Log");
      }
    };
  }

  @Override public List<CopyableFile> getCopyableFiles(FileSystem targetFileSystem) throws IOException {

    List<FileStatus> files = FileListUtils.listFilesRecursively(this._fs, this.rootPath, this.pathFilter);
    List<CopyableFile> copyableFiles = Lists.newArrayList();

    for(FileStatus file : files) {

      OwnerAndPermission ownerAndPermission =
          new OwnerAndPermission(file.getOwner(), file.getGroup(), file.getPermission());
      List<OwnerAndPermission> ancestorOwnerAndPermissions = Lists.newArrayList();
      try {
        Path currentPath = PathUtils.getPathWithoutSchemeAndAuthority(file.getPath());
        while(currentPath != null && !currentPath.getParent().equals(this.rootPath)) {
          currentPath = currentPath.getParent();
          ancestorOwnerAndPermissions.add(this.ownerAndPermissionCache.get(currentPath));
        }
      } catch(ExecutionException ee) {
        // When cache loader failed.
      }

      FileChecksum checksum = this._fs.getFileChecksum(file.getPath());

      copyableFiles.add(new CopyableFile(file, this.targetDirectory, ownerAndPermission, ancestorOwnerAndPermissions,
          checksum == null ? new byte[0] : checksum.getBytes()));
    }
    return copyableFiles;
  }

  @Override public Path datasetRoot() {
    return this.rootPath;
  }
}
