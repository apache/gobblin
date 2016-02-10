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
public class RecursiveCopyableDataset implements CopyableDataset {

  private final Path rootPath;
  private final FileSystem fs;
  private final PathFilter pathFilter;
  private final CopyableFileFilter copyableFileFilter;

  public RecursiveCopyableDataset(final FileSystem fs, Path rootPath, Properties properties) {

    this.rootPath = PathUtils.getPathWithoutSchemeAndAuthority(rootPath);
    this.fs = fs;

    this.pathFilter = DatasetUtils.instantiatePathFilter(properties);
    this.copyableFileFilter = DatasetUtils.instantiateCopyableFileFilter(properties);
  }

  @Override public Collection<CopyableFile> getCopyableFiles(FileSystem targetFs, CopyConfiguration configuration)
      throws IOException {

    List<FileStatus> files = FileListUtils.listFilesRecursively(this.fs, this.rootPath, this.pathFilter);

    List<CopyableFile> copyableFiles = Lists.newArrayList();

    for (FileStatus file : files) {
      copyableFiles.add(CopyableFile.builder(this.fs, file, this.rootPath, configuration).fileSet(file.getPath().getParent().toString()).build());
    }
    return copyableFileFilter.filter(this.fs, targetFs, copyableFiles);
  }

  @Override
  public Path datasetRoot() {
    return this.rootPath;
  }

}
