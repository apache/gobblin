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

package gobblin.data.management.copy;

import gobblin.data.management.dataset.DatasetUtils;
import gobblin.dataset.FileSystemDataset;
import gobblin.util.PathUtils;
import gobblin.util.FileListUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.google.common.collect.Lists;


/**
 * Implementation of {@link CopyableDataset} that creates a {@link CopyableFile} for every file that is a descendant if
 * the root directory.
 */
public class RecursiveCopyableDataset implements CopyableDataset, FileSystemDataset {

  private final Path rootPath;
  private final FileSystem fs;
  private final PathFilter pathFilter;
  // Glob used to find this dataset
  private final Path glob;
  private final CopyableFileFilter copyableFileFilter;

  public RecursiveCopyableDataset(final FileSystem fs, Path rootPath, Properties properties, Path glob) {

    this.rootPath = PathUtils.getPathWithoutSchemeAndAuthority(rootPath);
    this.fs = fs;

    this.pathFilter = DatasetUtils.instantiatePathFilter(properties);
    this.copyableFileFilter = DatasetUtils.instantiateCopyableFileFilter(properties);
    this.glob = glob;
  }

  @Override
  public Collection<? extends CopyEntity> getCopyableFiles(FileSystem targetFs, CopyConfiguration configuration)
      throws IOException {

    Path nonGlobSearchPath = PathUtils.deepestNonGlobPath(this.glob);

    List<FileStatus> files = FileListUtils.listFilesRecursively(this.fs, this.rootPath, this.pathFilter);

    List<CopyableFile> copyableFiles = Lists.newArrayList();

    for (FileStatus file : files) {
      Path filePathRelativeToSearchPath = PathUtils.relativizePath(file.getPath(), nonGlobSearchPath);
      Path targetPath = new Path(configuration.getPublishDir(), filePathRelativeToSearchPath);

      copyableFiles.add(CopyableFile.fromOriginAndDestination(this.fs, file, targetPath, configuration)
          .fileSet(file.getPath().getParent().toString())
          .ancestorsOwnerAndPermission(CopyableFile.resolveReplicatedOwnerAndPermissionsRecursively(this.fs,
              file.getPath().getParent(), nonGlobSearchPath, configuration))
          .build());
    }
    return this.copyableFileFilter.filter(this.fs, targetFs, copyableFiles);
  }

  @Override
  public Path datasetRoot() {
    return this.rootPath;
  }

  @Override
  public String datasetURN() {
    return datasetRoot().toString();
  }
}
