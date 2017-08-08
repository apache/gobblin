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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.joda.time.DateTime;

import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopyableDataset;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.data.management.policy.SelectAfterTimeBasedPolicy;
import org.apache.gobblin.data.management.policy.VersionSelectionPolicy;
import org.apache.gobblin.data.management.version.TimestampedDatasetVersion;
import org.apache.gobblin.data.management.version.finder.DateTimeDatasetVersionFinder;
import org.apache.gobblin.data.management.version.finder.VersionFinder;
import org.apache.gobblin.dataset.FileSystemDataset;
import org.apache.gobblin.util.filters.HiddenFilter;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.executors.ScalingThreadPoolExecutor;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * Implementation of {@link CopyableDataset}. It depends on {@link #datasetVersionFinder} to find dataset versions and
 * {@link #versionSelectionPolicy} to select the dataset versions for copying. {@link #datasetVersionFinder} is pluggable
 * and must implement the interface {@link VersionSelectionPolicy<TimestampedDatasetVersion>}.
 *
 * The default logic for determining if a file is {@link CopyableFile} is based on the file existence and modified_timestamp at source and target {@link
 * FileSystem}s.
 */
@Slf4j
@Getter
@SuppressWarnings("unchecked")
public class TimestampBasedCopyableDataset implements CopyableDataset, FileSystemDataset {

  private final Path datasetRoot;
  private final VersionFinder<TimestampedDatasetVersion> datasetVersionFinder;
  private final VersionSelectionPolicy<TimestampedDatasetVersion> versionSelectionPolicy;
  private final ExecutorService executor;
  private final FileSystem srcFs;

  public static final String DATASET_VERSION_FINDER = "timestamp.based.copyable.dataset.version.finder";
  public static final String DEFAULT_DATASET_VERSION_FINDER = DateTimeDatasetVersionFinder.class.getName();

  public static final String COPY_POLICY = "timestamp.based.copyable.dataset.copy.policy";
  public static final String DEFAULT_COPY_POLICY = SelectAfterTimeBasedPolicy.class.getName();

  public static final String THREADPOOL_SIZE_TO_GET_COPYABLE_FILES = "threadpool.size.to.get.copyable.files";
  public static final String DEFAULT_THREADPOOL_SIZE_TO_GET_COPYABLE_FILES = "20";

  public TimestampBasedCopyableDataset(FileSystem fs, Properties props, Path datasetRoot) {
    this.srcFs = fs;
    this.datasetRoot = datasetRoot;
    try {
      Class<?> copyPolicyClass = Class.forName(props.getProperty(COPY_POLICY, DEFAULT_COPY_POLICY));
      this.versionSelectionPolicy =
          (VersionSelectionPolicy<TimestampedDatasetVersion>) copyPolicyClass.getConstructor(Properties.class)
              .newInstance(props);
      Class<?> timestampedDatasetVersionFinderClass =
          Class.forName(props.getProperty(DATASET_VERSION_FINDER, DEFAULT_DATASET_VERSION_FINDER));

      this.datasetVersionFinder =
          (VersionFinder<TimestampedDatasetVersion>) timestampedDatasetVersionFinderClass.getConstructor(
              FileSystem.class, Properties.class).newInstance(this.srcFs, props);
    } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException
        | InvocationTargetException exception) {
      throw new RuntimeException(exception);
    }
    this.executor =
        ScalingThreadPoolExecutor.newScalingThreadPool(0, Integer.parseInt(props.getProperty(
            THREADPOOL_SIZE_TO_GET_COPYABLE_FILES, DEFAULT_THREADPOOL_SIZE_TO_GET_COPYABLE_FILES)), 100, ExecutorsUtils
            .newThreadFactory(Optional.of(log), Optional.of(getClass().getSimpleName())));
  }

  @Override
  public Collection<CopyableFile> getCopyableFiles(FileSystem targetFs, CopyConfiguration configuration)
      throws IOException {
    log.info(String.format("Getting copyable files at root path: %s", this.datasetRoot));
    List<TimestampedDatasetVersion> versions = Lists.newArrayList(this.datasetVersionFinder.findDatasetVersions(this));
    if (versions.isEmpty()) {
      log.warn("No dataset version can be found. Ignoring.");
      return Lists.newArrayList();
    }

    Collection<TimestampedDatasetVersion> copyableVersions = this.versionSelectionPolicy.listSelectedVersions(versions);
    ConcurrentLinkedQueue<CopyableFile> copyableFileList = new ConcurrentLinkedQueue<>();
    List<Future<?>> futures = Lists.newArrayList();
    for (TimestampedDatasetVersion copyableVersion : copyableVersions) {
      futures.add(this.executor.submit(this.getCopyableFileGenetator(targetFs, configuration, copyableVersion,
          copyableFileList)));
    }

    try {
      for (Future<?> future : futures) {
        future.get();
      }
    } catch (ExecutionException | InterruptedException e) {
      throw new IOException("Failed to generate copyable files.", e);
    } finally {
      ExecutorsUtils.shutdownExecutorService(executor, Optional.of(log));
    }
    return copyableFileList;
  }

  @VisibleForTesting
  protected CopyableFileGenerator getCopyableFileGenetator(FileSystem targetFs, CopyConfiguration configuration,
      TimestampedDatasetVersion copyableVersion, ConcurrentLinkedQueue<CopyableFile> copyableFileList) {
    return new CopyableFileGenerator(this.srcFs, targetFs, configuration, this.datasetRoot,
        this.getTargetRoot(configuration.getPublishDir()), copyableVersion.getDateTime(), copyableVersion.getPaths(),
        copyableFileList, this.copyableFileFilter());
  }

  /**
   * @return {@link PathFilter} to find {@link CopyableFile}.
   * Can be overridden.
   */
  protected PathFilter copyableFileFilter() {
    return new HiddenFilter();
  }

  /**
   * @return the default targetRoot {@link Path}.
   */
  protected Path getTargetRoot(Path publishDir) {
    return new Path(publishDir, datasetRoot.getName());
  }

  @AllArgsConstructor
  protected static class CopyableFileGenerator implements Runnable {
    private final FileSystem srcFs;
    private final FileSystem targetFs;
    private final CopyConfiguration configuration;
    private final Path datasetRoot;
    private final Path targetRoot;
    private final DateTime versionDatetime;
    private final Collection<Path> locationsToCopy;
    private final ConcurrentLinkedQueue<CopyableFile> copyableFileList;
    private final PathFilter filter;

    @Override
    public void run() {
      for (Path locationToCopy : locationsToCopy) {
        long timestampFromPath = this.versionDatetime.getMillis();
        try {
          for (FileStatus singleFile : this.srcFs.listStatus(locationToCopy, this.filter)) {
            Path singleFilePath = singleFile.getPath();
            log.debug("Checking if it is a copyable file: " + singleFilePath);
            Path relativePath = PathUtils.relativizePath(singleFilePath, datasetRoot);
            Path targetPath = new Path(targetRoot, relativePath);
            if (this.isCopyableFile(singleFile, targetPath)) {
              log.debug("Will create workunit for: " + singleFilePath);
              copyableFileList
                  .add(this.generateCopyableFile(singleFile, targetPath, timestampFromPath, locationToCopy));
            }
          }
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException("Failed to get copyable files for " + locationToCopy, e);
        }
      }
    }

    @VisibleForTesting
    protected CopyableFile generateCopyableFile(FileStatus singleFile, Path targetPath, long timestampFromPath,
        Path locationToCopy) throws IOException {
      return CopyableFile.fromOriginAndDestination(srcFs, singleFile, targetPath, configuration)
          .originTimestamp(timestampFromPath).upstreamTimestamp(timestampFromPath)
          .fileSet(PathUtils.getPathWithoutSchemeAndAuthority(locationToCopy).toString()).build();
    }

    /***
     * Given a {@link FileStatus} at src FileSystem, decide if it is a {@link CopyableFile}.
     *
     *  Return true if the {@link Path} of the given {@link FileStatus} does not exist on target {@link FileSystem}, or it
     *  has a newer modification time stamp on source {@link FileSystem} than target {@link FileSystem}.
     */
    private boolean isCopyableFile(FileStatus srcFileStatus, Path targetPath) throws IOException {
      if (!this.targetFs.exists(targetPath)) {
        return true;
      } else if (srcFileStatus.getModificationTime() > this.targetFs.getFileStatus(targetPath).getModificationTime()) {
        return true;
      } else {
        return false;
      }
    }
  }

  @Override
  public String datasetURN() {
    return this.datasetRoot().toString();
  }

  @Override
  public Path datasetRoot() {
    return this.datasetRoot;
  }
}
