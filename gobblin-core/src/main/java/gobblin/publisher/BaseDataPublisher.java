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

package gobblin.publisher;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.configuration.ConfigurationKeys;
import gobblin.util.Action;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.HadoopUtils;
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
    Multimap<PublishGroup, PublishCommand> publishCommands = getGroupedPublishCommands(states);
    for (Map.Entry<PublishGroup, Collection<PublishCommand>> entry : publishCommands.asMap().entrySet()) {
      PublishGroup group = entry.getKey();
      if (canPublishByRenamingFolder(group.getSourceFileSystem(), group.getDestinationFileSystem())) {
        this.parallelRunner.renamePath(group.getSourceFileSystem(), group.getSourcePath(),
                  group.getDestinationPath(), Optional.<String>absent(),
                  Optional.<Action>of(getCompositeCommitAction(entry.getValue())));
      } else {
        for (PublishCommand command : entry.getValue()) {
          this.parallelRunner.movePath(group.getSourceFileSystem(), command.getSrc(),
                  group.getDestinationFileSystem(), command.getDst(), Optional.<String>absent(),
                  Optional.<Action>of(command.getCommitAction()));
        }
      }
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

  private boolean canPublishByRenamingFolder(FileSystem srcFs, FileSystem dstFs) {
    return HadoopUtils.areFileSystemsEquivalent(srcFs, dstFs) && HadoopUtils.isFolderRenameAtomic(srcFs);
  }

    private Multimap<PublishGroup, PublishCommand> getGroupedPublishCommands(Collection<? extends WorkUnitState> states) throws IOException {
      Set<PublishGroup> preparedGroups = Sets.newHashSet();
      Multimap<PublishGroup, PublishCommand> publishCommands = ArrayListMultimap.create();
      for (WorkUnitState state : states) {
        CommitAction commitAction = null;
        List<PublishGroup> groups = getPublishGroups(state);
        for (PublishGroup group : groups) {
          if (preparedGroups.add(group)) {
            preparePublishGroup(group);
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
            if (!group.getSourceFileSystem().exists(taskOutputPath)) {
              LOG.warn("Task output file " + taskOutputFile + " doesn't exist.");
              continue;
            }
            String pathSuffix;
            if (preserveFileName) {
              pathSuffix = state.getProp(ForkOperatorUtils.getPropertyNameForBranch(
                      ConfigurationKeys.DATA_PUBLISHER_FINAL_NAME, this.numBranches, group.getBranchId()));
            } else {
              pathSuffix = taskOutputFile.substring(
                      taskOutputFile.indexOf(group.getSourcePath().toString()) +
                              group.getSourcePath().toString().length() + 1);
            }
            Path publisherOutputPath = new Path(group.getDestinationPath(), pathSuffix);
            WriterUtils.mkdirsWithRecursivePermission(group.getDestinationFileSystem(),
                    publisherOutputPath.getParent(), this.permissions.get(group.getBranchId()));

            if (commitAction == null) {
              commitAction = new CommitAction(state);
            } else {
              commitAction.increment();
            }

            PublishCommand publishCommand = new PublishCommand(taskOutputPath, publisherOutputPath, commitAction);
            publishCommands.put(group, publishCommand);
          }
        }
      }
      return publishCommands;
  }

  private void preparePublishGroup(PublishGroup group) throws IOException {
    if (group.getDestinationFileSystem().exists(group.getDestinationPath())) {
      // The final output directory already exists, check if the job is configured to replace it.
      // If publishSingleTaskData=true, final output directory is never replaced.
      boolean replaceFinalOutputDir = this.getState().getPropAsBoolean(
              ForkOperatorUtils.getPropertyNameForBranch(
                      ConfigurationKeys.DATA_PUBLISHER_REPLACE_FINAL_DIR, this.numBranches, group.getBranchId()));

      // If the final output directory is configured to be replaced, delete the existing publisher output directory
      if (!replaceFinalOutputDir) {
        LOG.info("Deleting publisher output dir " + group.getDestinationPath());
        group.getDestinationFileSystem().delete(group.getDestinationPath(), true);
      }
    } else {
      // Create the parent directory of the final output directory if it does not exist
      WriterUtils.mkdirsWithRecursivePermission(this.publisherFileSystemByBranches.get(group.getBranchId()),
                group.getDestinationPath().getParent(), this.permissions.get(group.getBranchId()));
    }
  }

  private List<PublishGroup> getPublishGroups(WorkUnitState state) throws IOException {
    ImmutableList.Builder<PublishGroup> builder = new ImmutableList.Builder<>();
    for (int branchId = 0; branchId < this.numBranches; branchId++) {
      // The directory where the workUnitState wrote its output data.
      Path writerOutputDir = WriterUtils.getWriterOutputDir(state, this.numBranches, branchId);

      FileSystem writerFileSystem = this.writerFileSystemByBranches.get(branchId);
      if (!writerFileSystem.exists(writerOutputDir)) {
        LOG.warn(String.format("Branch %d of WorkUnit %s produced no data", branchId, state.getId()));
        continue;
      }

      FileSystem publishFileSystem = this.publisherFileSystemByBranches.get(branchId);
      // The directory where the final output directory for this job will be placed.
      // It is a combination of DATA_PUBLISHER_FINAL_DIR and WRITER_FILE_PATH.
      Path publisherOutputDir = getPublisherOutputDir(state, branchId);

      builder.add(new PublishGroup(branchId, writerFileSystem, writerOutputDir, publishFileSystem, publisherOutputDir));
    }
    return builder.build();
  }

  private CompositeCommitAction getCompositeCommitAction(Iterable<PublishCommand> publishCommands) {
    return new CompositeCommitAction(Iterables.transform(publishCommands,
            new Function<PublishCommand, CommitAction>() {
              @Override
              public CommitAction apply(PublishCommand publishCommand) {
                return publishCommand.getCommitAction();
              }
            }));
  }

  private static class CompositeCommitAction implements Action {
    private final Iterable<CommitAction> commitActions;

    public CompositeCommitAction(Iterable<CommitAction> commitActions) {
      this.commitActions = commitActions;
    }

    @Override
    public void apply() throws Exception {
      for (CommitAction commitAction : commitActions) {
        commitAction.apply();
      }
    }
  }

  private static class CommitAction implements Action {
    private final WorkUnitState state;
    private AtomicInteger requiredSuccesses = new AtomicInteger();

    public CommitAction(WorkUnitState state) {
      this.state = state;
      this.requiredSuccesses.incrementAndGet();
    }

    @Override
    public void apply() {
      if (requiredSuccesses.decrementAndGet() == 0) {
        state.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
      }
    }

    public void increment() {
      this.requiredSuccesses.incrementAndGet();
    }
  }

  private static class PublishGroup {
    private final int branchId;
    private final FileSystem srcFs;
    private final Path srcPath;
    private final FileSystem dstFs;
    private final Path dstPath;

    public PublishGroup(int branchId, FileSystem srcFs, Path srcPath, FileSystem dstFs, Path dstPath) {
      this.branchId = branchId;
      this.srcFs = srcFs;
      this.srcPath = srcPath;
      this.dstFs = dstFs;
      this.dstPath = dstPath;
    }

    public int getBranchId() {
      return branchId;
    }

    public FileSystem getSourceFileSystem() {
      return srcFs;
    }

    public Path getSourcePath() {
      return srcPath;
    }

    public FileSystem getDestinationFileSystem() {
      return dstFs;
    }

    public Path getDestinationPath() {
      return dstPath;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof PublishGroup)) return false;

      PublishGroup that = (PublishGroup) o;
      return Objects.equals(srcFs.getUri(), that.srcFs.getUri()) &&
              Objects.equals(srcPath, that.srcPath) &&
              Objects.equals(srcFs.getUri(), that.srcFs.getUri()) &&
              Objects.equals(dstPath, that.dstPath);
    }

    @Override
    public int hashCode() {
      return Objects.hash(srcFs.getUri(), srcPath, dstFs.getUri(), dstPath);
    }
  }

  public static class PublishCommand {
    private final Path src;
    private final Path dst;
    private final CommitAction commitAction;

    public PublishCommand(Path src, Path dst, CommitAction commitAction) {
      this.src = src;
      this.dst = dst;
      this.commitAction = commitAction;
    }

    public Path getSrc() {
      return src;
    }

    public Path getDst() {
      return dst;
    }

    public CommitAction getCommitAction() {
      return commitAction;
    }
  }
}
