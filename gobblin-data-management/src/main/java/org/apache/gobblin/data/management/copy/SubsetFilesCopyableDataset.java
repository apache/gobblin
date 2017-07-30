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

package gobblin.data.management.copy;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

import gobblin.configuration.SourceState;
import gobblin.data.management.dataset.DatasetUtils;
import gobblin.dataset.FileSystemDataset;
import gobblin.metrics.event.EventSubmitter;
import gobblin.util.PathUtils;

import lombok.extern.slf4j.Slf4j;


/**
 * Implementation of {@link CopyableDataset} and {@link FileSystemDataset} that has an identifier in a root directory,
 * and only contains a subset of files in the root directory.
 */
@Slf4j
public class SubsetFilesCopyableDataset implements CopyableDataset, FileSystemDataset {
  private final Path rootPath;
  private final FileSystem fs;
  private final List<FileStatus> files;
  private final String identifier;
  private final Properties props;

  private EventSubmitter eventSubmitter;
  private SourceState state;

  public SubsetFilesCopyableDataset(final FileSystem fs, Path rootPath, Properties properties, String idenifier,
      List<FileStatus> subFiles) {
    this.rootPath = PathUtils.getPathWithoutSchemeAndAuthority(rootPath);
    this.fs = fs;
    this.files = subFiles;
    this.identifier = idenifier;
    this.props = properties;
  }

  public SubsetFilesCopyableDataset(final FileSystem fs, Path rootPath, Properties properties, String idenifier,
      List<FileStatus> subFiles, EventSubmitter eventSubmitter) {
    this(fs, rootPath, properties, idenifier, subFiles);
    this.eventSubmitter = eventSubmitter;
  }

  public SubsetFilesCopyableDataset(final FileSystem fs, Path rootPath, Properties properties, String idenifier,
      List<FileStatus> subFiles, EventSubmitter eventSubmitter, SourceState state) {
    this(fs, rootPath, properties, idenifier, subFiles, eventSubmitter);
    this.state = state;
  }

  @Override
  public String datasetURN() {
    return (rootPath + this.identifier).replace('/', '_');
  }

  @Override
  public Collection<? extends CopyEntity> getCopyableFiles(FileSystem targetFs, CopyConfiguration configuration)
      throws IOException {
    List<CopyableFile> copyableFiles = Lists.newArrayList();
    for (FileStatus fileStatus : this.files) {
      if (this.shouldAddToCopyableFiles(fileStatus)) {
        log.debug("Adding copyable file " + fileStatus.getPath() + "for " + identifier + " in " + this.rootPath);
        Path targetPath = this.getTargetPath(configuration.getPublishDir(), fileStatus.getPath(), this.identifier);
        copyableFiles.add(CopyableFile.fromOriginAndDestination(this.fs, fileStatus, targetPath, configuration)
            .destinationOwnerAndPermission(this.getDestinationOwnerAndPermission()).fileSet(this.fileSet(fileStatus))
            .build());
      }
    }
    return DatasetUtils.instantiateCopyableFileFilter(this.props, this.state, this)
        .filter(this.fs, targetFs, copyableFiles);
  }

  public Path getTargetPath(Path publishDir, Path originPath, String identifier) {
    Path filePathRelativeToSearchPath = PathUtils.relativizePath(originPath, this.rootPath);
    return new Path(publishDir, filePathRelativeToSearchPath);
  }

  public OwnerAndPermission getDestinationOwnerAndPermission() {
    return null;
  }

  public boolean shouldAddToCopyableFiles(FileStatus fileStatus) {
    return true;
  }

  public String fileSet(FileStatus fileStatus) {
    return this.identifier;
  }

  @Override
  public Path datasetRoot() {
    return rootPath;
  }
}
