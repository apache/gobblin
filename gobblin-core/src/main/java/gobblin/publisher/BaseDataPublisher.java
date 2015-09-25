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

package gobblin.publisher;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.configuration.ConfigurationKeys;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.ParallelRunner;
import gobblin.util.WriterUtils;


/**
 * A basic implementation of {@link DataPublisher} that publishes the data from the writer output directory to
 * the final output directory.
 *
 * <p>
 *
 * The final output directory is specified by {@link ConfigurationKeys#DATA_PUBLISHER_FINAL_DIR}. The output of each
 * writer is written to this directory. Each individual writer can also specify a path in the config key
 * {@link ConfigurationKeys#WRITER_FILE_PATH}. Then the final output data for a writer will be
 * {@link ConfigurationKeys#DATA_PUBLISHER_FINAL_DIR}/{@link ConfigurationKeys#WRITER_FILE_PATH}. If the
 * {@link ConfigurationKeys#WRITER_FILE_PATH} is not specified, a default one is assigned. The default path is
 * constructed in the {@link gobblin.source.workunit.Extract#getOutputFilePath()} method.
 */
public class BaseDataPublisher extends DataPublisher {

  private static final Logger LOG = LoggerFactory.getLogger(BaseDataPublisher.class);

  protected final int numBranches;
  protected final List<FileSystem> fileSystemByBranches;
  protected final List<Optional<String>> publisherFinalDirOwnerGroupsByBranches;
  protected final List<FsPermission> permissions;
  protected final Closer closer;
  protected final int parallelRunnerThreads;
  protected final Map<String, ParallelRunner> parallelRunners = Maps.newHashMap();

  public BaseDataPublisher(State state) throws IOException {
    super(state);
    this.closer = Closer.create();
    Configuration conf = new Configuration();

    // Add all job configuration properties so they are picked up by Hadoop
    for (String key : this.getState().getPropertyNames()) {
      conf.set(key, this.getState().getProp(key));
    }

    this.numBranches = this.getState().getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1);

    this.fileSystemByBranches = Lists.newArrayListWithCapacity(this.numBranches);
    this.publisherFinalDirOwnerGroupsByBranches = Lists.newArrayListWithCapacity(this.numBranches);
    this.permissions = Lists.newArrayListWithCapacity(this.numBranches);

    // Get a FileSystem instance for each branch
    for (int i = 0; i < this.numBranches; i++) {
      URI uri = URI.create(this.getState().getProp(
          ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, this.numBranches, i),
          ConfigurationKeys.LOCAL_FS_URI));
      this.fileSystemByBranches.add(FileSystem.get(uri, conf));

      // The group(s) will be applied to the final publisher output directory(ies)
      this.publisherFinalDirOwnerGroupsByBranches.add(Optional.fromNullable(this.getState().getProp(ForkOperatorUtils
          .getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR_GROUP, this.numBranches, i))));

      // The permission(s) will be applied to all directories created by the publisher,
      // which do NOT include directories created by the writer and moved by the publisher.
      // The permissions of those directories are controlled by writer.file.permissions and writer.dir.permissions.
      this.permissions.add(new FsPermission(state.getPropAsShortWithRadix(
          ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISHER_PERMISSIONS, numBranches, i),
          FsPermission.getDefault().toShort(), ConfigurationKeys.PERMISSION_PARSING_RADIX)));
    }

    this.parallelRunnerThreads =
        state.getPropAsInt(ParallelRunner.PARALLEL_RUNNER_THREADS_KEY, ParallelRunner.DEFAULT_PARALLEL_RUNNER_THREADS);
  }

  @Override
  public void initialize() throws IOException {
    // Nothing needs to be done since the constructor already initializes the publisher.
  }

  @Override
  public void close() throws IOException {
    this.closer.close();
  }

  @Override
  public void publishData(WorkUnitState state) throws IOException {
    for (int branchId = 0; branchId < this.numBranches; branchId++) {
      publishSingleTaskData(state, branchId);
    }
  }

  /**
   * This method publishes output data for a single task based on the given {@link WorkUnitState}.
   * Output data from other tasks won't be published even if they are in the same folder.
   */
  private void publishSingleTaskData(WorkUnitState state, int branchId) throws IOException {
    publishData(state, branchId, true, new HashSet<Path>());
  }

  @Override
  public void publishData(Collection<? extends WorkUnitState> states) throws IOException {

    // We need a Set to collect unique writer output paths as multiple tasks may belong to the same extract. Tasks that
    // belong to the same Extract will by default have the same output directory
    Set<Path> writerOutputPathsMoved = Sets.newHashSet();

    for (WorkUnitState workUnitState : states) {
      for (int branchId = 0; branchId < this.numBranches; branchId++) {
        publishMultiTaskData(workUnitState, branchId, writerOutputPathsMoved);
      }

      // Upon successfully committing the data to the final output directory, set states
      // of successful tasks to COMMITTED. leaving states of unsuccessful ones unchanged.
      // This makes sense to the COMMIT_ON_PARTIAL_SUCCESS policy.
      workUnitState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
    }
  }

  /**
   * This method publishes task output data for the given {@link WorkUnitState}, but if there are output data of
   * other tasks in the same folder, it may also publish those data.
   */
  private void publishMultiTaskData(WorkUnitState state, int branchId, Set<Path> writerOutputPathsMoved)
      throws IOException {
    publishData(state, branchId, false, writerOutputPathsMoved);
  }

  protected void publishData(WorkUnitState state, int branchId, boolean publishSingleTaskData,
      Set<Path> writerOutputPathsMoved) throws IOException {
    // Get a ParallelRunner instance for moving files in parallel
    ParallelRunner parallelRunner = this.getParallelRunner(this.fileSystemByBranches.get(branchId));

    // The directory where the workUnitState wrote its output data.
    Path writerOutputDir = WriterUtils.getWriterOutputDir(state, this.numBranches, branchId);

    if (!this.fileSystemByBranches.get(branchId).exists(writerOutputDir)) {
      LOG.warn(String.format("Branch %d of WorkUnit %s produced no data", branchId, state.getId()));
      return;
    }

    // The directory where the final output directory for this job will be placed.
    // It is a combination of DATA_PUBLISHER_FINAL_DIR and WRITER_FILE_PATH.
    Path publisherOutputDir = getPublisherOutputDir(state, branchId);

    if (publishSingleTaskData) {

      // Create final output directory
      WriterUtils.mkdirsWithRecursivePermission(this.fileSystemByBranches.get(branchId), publisherOutputDir,
          this.permissions.get(branchId));
      addSingleTaskWriterOutputToExistingDir(writerOutputDir, publisherOutputDir, state, branchId, parallelRunner);
    } else {
      if (writerOutputPathsMoved.contains(writerOutputDir)) {
        // This writer output path has already been moved for another task of the same extract
        // If publishSingleTaskData=true, writerOutputPathMoved is ignored.
        return;
      }

      if (this.fileSystemByBranches.get(branchId).exists(publisherOutputDir)) {
        // The final output directory already exists, check if the job is configured to replace it.
        // If publishSingleTaskData=true, final output directory is never replaced.
        boolean replaceFinalOutputDir = this.getState().getPropAsBoolean(ForkOperatorUtils
            .getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISHER_REPLACE_FINAL_DIR, this.numBranches, branchId));

        // If the final output directory is not configured to be replaced, put new data to the existing directory.
        if (!replaceFinalOutputDir) {
          addWriterOutputToExistingDir(writerOutputDir, publisherOutputDir, state, branchId, parallelRunner);
          writerOutputPathsMoved.add(writerOutputDir);
          return;
        }
        // Delete the final output directory if it is configured to be replaced
        LOG.info("Deleting publisher output dir " + publisherOutputDir);
        this.fileSystemByBranches.get(branchId).delete(publisherOutputDir, true);
      } else {
        // Create the parent directory of the final output directory if it does not exist
        WriterUtils.mkdirsWithRecursivePermission(this.fileSystemByBranches.get(branchId),
            publisherOutputDir.getParent(), this.permissions.get(branchId));
      }

      LOG.info(String.format("Moving %s to %s", writerOutputDir, publisherOutputDir));
      parallelRunner.renamePath(writerOutputDir, publisherOutputDir,
          this.publisherFinalDirOwnerGroupsByBranches.get(branchId));
      writerOutputPathsMoved.add(writerOutputDir);
    }
  }

  /**
   * Get the output directory path this {@link BaseDataPublisher} will write to.
   *
   * <p>
   *   This is the default implementation. Subclasses of {@link BaseDataPublisher} may override this
   *   to write to a custom directory or write using a custom directory structure or naming pattern.
   * </p>
   *
   * @param workUnitState a {@link WorkUnitState} object
   * @param branchId the fork branch ID
   * @return the output directory path this {@link BaseDataPublisher} will write to
   */
  protected Path getPublisherOutputDir(WorkUnitState workUnitState, int branchId) {
    return WriterUtils.getDataPublisherFinalDir(workUnitState, this.numBranches, branchId);
  }

  protected void addSingleTaskWriterOutputToExistingDir(Path writerOutputDir, Path publisherOutputDir,
      WorkUnitState workUnitState, int branchId, ParallelRunner parallelRunner) throws IOException {
    if (!workUnitState.contains(ConfigurationKeys.WRITER_FINAL_OUTPUT_FILE_PATHS)) {
      LOG.warn("Missing property " + ConfigurationKeys.WRITER_FINAL_OUTPUT_FILE_PATHS
          + ". This task may have pulled no data.");
      return;
    }

    Iterable<String> taskOutputFiles = workUnitState.getPropAsList(ConfigurationKeys.WRITER_FINAL_OUTPUT_FILE_PATHS);

    for (String taskOutputFile : taskOutputFiles) {
      Path taskOutputPath = new Path(taskOutputFile);
      if (!this.fileSystemByBranches.get(branchId).exists(taskOutputPath)) {
        LOG.warn("Task output file " + taskOutputFile + " doesn't exist.");
        continue;
      }
      String pathSuffix = taskOutputFile
          .substring(taskOutputFile.indexOf(writerOutputDir.toString()) + writerOutputDir.toString().length() + 1);
      Path publisherOutputPath = new Path(publisherOutputDir, pathSuffix);
      WriterUtils.mkdirsWithRecursivePermission(this.fileSystemByBranches.get(branchId),
          publisherOutputPath.getParent(), this.permissions.get(branchId));

      LOG.info(String.format("Moving %s to %s", taskOutputFile, publisherOutputPath));
      parallelRunner.renamePath(taskOutputPath, publisherOutputPath, Optional.<String>absent());
    }
  }

  protected void addWriterOutputToExistingDir(Path writerOutputDir, Path publisherOutputDir,
      WorkUnitState workUnitState, int branchId, ParallelRunner parallelRunner) throws IOException {
    boolean preserveFileName = workUnitState.getPropAsBoolean(ForkOperatorUtils.getPropertyNameForBranch(
        ConfigurationKeys.SOURCE_FILEBASED_PRESERVE_FILE_NAME, this.numBranches, branchId), false);

    // Go through each file in writerOutputDir and move it into publisherOutputDir
    for (FileStatus status : this.fileSystemByBranches.get(branchId).listStatus(writerOutputDir)) {

      // Preserve the file name if configured, use specified name otherwise
      Path finalOutputPath =
          preserveFileName
              ? new Path(publisherOutputDir,
                  workUnitState.getProp(ForkOperatorUtils.getPropertyNameForBranch(
                      ConfigurationKeys.DATA_PUBLISHER_FINAL_NAME, this.numBranches, branchId)))
          : new Path(publisherOutputDir, status.getPath().getName());

      LOG.info(String.format("Moving %s to %s", status.getPath(), finalOutputPath));
      parallelRunner.renamePath(status.getPath(), finalOutputPath, Optional.<String>absent());
    }
  }

  private ParallelRunner getParallelRunner(FileSystem fs) {
    String uri = fs.getUri().toString();
    if (!this.parallelRunners.containsKey(uri)) {
      this.parallelRunners.put(uri, this.closer.register(new ParallelRunner(this.parallelRunnerThreads, fs)));
    }
    return this.parallelRunners.get(uri);
  }

  @Override
  public void publishMetadata(Collection<? extends WorkUnitState> states) throws IOException {
    // Nothing to do
  }

  @Override
  public void publishMetadata(WorkUnitState state) throws IOException {
    // Nothing to do
  }

}
