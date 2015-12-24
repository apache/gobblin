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
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import gobblin.util.Action;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.configuration.ConfigurationKeys;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.ParallelRunner;
import gobblin.util.WriterUtils;


/**
 * A basic implementation of {@link SingleTaskDataPublisher} that publishes the data from the writer output directory
 * to the final output directory.
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
public class BaseDataPublisher extends SingleTaskDataPublisher {

  private static final Logger LOG = LoggerFactory.getLogger(BaseDataPublisher.class);

  protected final int numBranches;
  protected final List<FileSystem> writerFileSystemByBranches;
  protected final List<FileSystem> publisherFileSystemByBranches;
  protected final List<Optional<String>> publisherFinalDirOwnerGroupsByBranches;
  protected final List<FsPermission> permissions;
  protected final Closer closer;
  protected final ParallelRunner parallelRunner;

  public BaseDataPublisher(State state) throws IOException {
    super(state);
    this.closer = Closer.create();
    Configuration conf = new Configuration();

    // Add all job configuration properties so they are picked up by Hadoop
    for (String key : this.getState().getPropertyNames()) {
      conf.set(key, this.getState().getProp(key));
    }

    this.numBranches = this.getState().getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1);

    this.writerFileSystemByBranches = Lists.newArrayListWithCapacity(this.numBranches);
    this.publisherFileSystemByBranches = Lists.newArrayListWithCapacity(this.numBranches);
    this.publisherFinalDirOwnerGroupsByBranches = Lists.newArrayListWithCapacity(this.numBranches);
    this.permissions = Lists.newArrayListWithCapacity(this.numBranches);

    // Get a FileSystem instance for each branch
    for (int i = 0; i < this.numBranches; i++) {
      URI writerUri = URI.create(this.getState().getProp(
          ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, this.numBranches, i),
          ConfigurationKeys.LOCAL_FS_URI));
      this.writerFileSystemByBranches.add(FileSystem.get(writerUri, conf));

      URI publisherUri = URI.create(this.getState().getProp(
          ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISHER_FILE_SYSTEM_URI, this.numBranches, i),
          writerUri.toString()));
      this.publisherFileSystemByBranches.add(FileSystem.get(publisherUri, conf));

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

    int parallelRunnerThreads =
        state.getPropAsInt(ParallelRunner.PARALLEL_RUNNER_THREADS_KEY, ParallelRunner.DEFAULT_PARALLEL_RUNNER_THREADS);
    this.parallelRunner = this.closer.register(new ParallelRunner(parallelRunnerThreads * this.numBranches));
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
    publishData(ImmutableList.of(state));
  }

  @Override
  public void publishData(Collection<? extends WorkUnitState> states) throws IOException {
    Set<WorkUnitPublishGroup> preparedGroups = Sets.newHashSet();
    for (WorkUnitState state : states) {
      ImmutableList.Builder<ParallelRunner.MoveCommand> movesBuilder = new ImmutableList.Builder<>();
      List<WorkUnitPublishGroup> groups = getWorkUnitPublishGroup(state);
      for (WorkUnitPublishGroup group : groups) {
        if (preparedGroups.add(group)) {
          prepareWorkUnitPublishGroup(group);
        }
        boolean preserveFileName = state.getPropAsBoolean(ForkOperatorUtils.getPropertyNameForBranch(
                ConfigurationKeys.SOURCE_FILEBASED_PRESERVE_FILE_NAME, this.numBranches, group.getBranchId()), false);

        String outputFilePropName = ForkOperatorUtils.getPropertyNameForBranch(
                ConfigurationKeys.WRITER_FINAL_OUTPUT_FILE_PATHS, this.numBranches, group.getBranchId());

        if (!state.contains(outputFilePropName)) {
          LOG.warn("Missing property " + outputFilePropName + ". This task may have pulled no data.");
          continue;
        }

        Iterable<String> taskOutputFiles = state.getPropAsList(outputFilePropName);
        for (String taskOutputFile : taskOutputFiles) {
          Path taskOutputPath = new Path(taskOutputFile);
          FileSystem writerFileSystem = this.writerFileSystemByBranches.get(group.getBranchId());
          if (!writerFileSystem.exists(taskOutputPath)) {
            LOG.warn("Task output file " + taskOutputFile + " doesn't exist.");
            continue;
          }
          String pathSuffix;
          if (preserveFileName) {
            pathSuffix = state.getProp(ForkOperatorUtils.getPropertyNameForBranch(
                    ConfigurationKeys.DATA_PUBLISHER_FINAL_NAME, this.numBranches, group.getBranchId()));
          } else {
              pathSuffix = taskOutputFile.substring(
                      taskOutputFile.indexOf(group.getWriterOutputDir().toString()) +
                              group.getWriterOutputDir().toString().length() + 1);
          }
          Path publisherOutputPath = new Path(group.getPublisherOutputDir(), pathSuffix);
          FileSystem publisherFileSystem = this.publisherFileSystemByBranches.get(group.getBranchId());
          WriterUtils.mkdirsWithRecursivePermission(publisherFileSystem, publisherOutputPath.getParent(),
                  this.permissions.get(group.getBranchId()));

          movesBuilder.add(new ParallelRunner.MoveCommand(writerFileSystem, taskOutputPath,
                  publisherFileSystem, publisherOutputPath));
        }
      }
      this.parallelRunner.movePaths(movesBuilder.build(), Optional.<String>absent(), new CommitAction(state));
    }
  }

  private void prepareWorkUnitPublishGroup(WorkUnitPublishGroup group) throws IOException {
    if (this.publisherFileSystemByBranches.get(group.getBranchId()).exists(group.getPublisherOutputDir())) {
      // The final output directory already exists, check if the job is configured to replace it.
      // If publishSingleTaskData=true, final output directory is never replaced.
      boolean replaceFinalOutputDir = this.getState().getPropAsBoolean(
              ForkOperatorUtils.getPropertyNameForBranch(
                      ConfigurationKeys.DATA_PUBLISHER_REPLACE_FINAL_DIR, this.numBranches, group.getBranchId()));

      // If the final output directory is configured to be replaced, delete the existing publisher output directory
      if (!replaceFinalOutputDir) {
        LOG.info("Deleting publisher output dir " + group.getPublisherOutputDir());
        this.publisherFileSystemByBranches.get(group.getBranchId()).delete(group.getPublisherOutputDir(), true);
      }
    } else {
      // Create the parent directory of the final output directory if it does not exist
      WriterUtils.mkdirsWithRecursivePermission(this.publisherFileSystemByBranches.get(group.getBranchId()),
              group.getPublisherOutputDir().getParent(), this.permissions.get(group.getBranchId()));
    }
  }

  @Override
  public void publishMetadata(Collection<? extends WorkUnitState> states) throws IOException {
    // Nothing to do
  }

  @Override
  public void publishMetadata(WorkUnitState state) throws IOException {
    // Nothing to do
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

  private List<WorkUnitPublishGroup> getWorkUnitPublishGroup(WorkUnitState state) throws IOException {
    ImmutableList.Builder<WorkUnitPublishGroup> builder = new ImmutableList.Builder<>();
    for (int branchId = 0; branchId < this.numBranches; branchId++) {
      // The directory where the workUnitState wrote its output data.
      Path writerOutputDir = WriterUtils.getWriterOutputDir(state, this.numBranches, branchId);

      if (!this.writerFileSystemByBranches.get(branchId).exists(writerOutputDir)) {
        LOG.warn(String.format("Branch %d of WorkUnit %s produced no data", branchId, state.getId()));
        continue;
      }

      // The directory where the final output directory for this job will be placed.
      // It is a combination of DATA_PUBLISHER_FINAL_DIR and WRITER_FILE_PATH.
      Path publisherOutputDir = getPublisherOutputDir(state, branchId);

      builder.add(new WorkUnitPublishGroup(branchId, writerOutputDir, publisherOutputDir));
    }
    return builder.build();
  }

  private static class CommitAction implements Action {
    private final WorkUnitState state;

    public CommitAction(WorkUnitState state) {
      this.state = state;
    }

    @Override
    public void apply() {
       state.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
    }
  }

  private class WorkUnitPublishGroup {
    private final int branchId;
    private final Path writerOutputDir;
    private final Path publisherOutputDir;

    public WorkUnitPublishGroup(int branchId, Path writerOutputDir, Path publisherOutputDir) {
      this.branchId = branchId;
      this.writerOutputDir = writerOutputDir;
      this.publisherOutputDir = publisherOutputDir;
    }

    public int getBranchId() {
      return branchId;
    }

    public Path getWriterOutputDir() {
      return writerOutputDir;
    }

    public Path getPublisherOutputDir() {
      return publisherOutputDir;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      WorkUnitPublishGroup that = (WorkUnitPublishGroup) o;
      return Objects.equals(branchId, that.branchId) &&
              Objects.equals(writerOutputDir, that.writerOutputDir) &&
              Objects.equals(publisherOutputDir, that.publisherOutputDir);
    }

    @Override
    public int hashCode() {
      return Objects.hash(branchId, writerOutputDir, publisherOutputDir);
    }
  }
}
