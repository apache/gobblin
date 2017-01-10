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

package gobblin.publisher;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import gobblin.util.HadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
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
 * A basic implementation of {@link SingleTaskDataPublisher} that publishes the data from the writer output directory
 * to the final output directory.
 *
 * <p>
 * The final output directory is specified by {@link ConfigurationKeys#DATA_PUBLISHER_FINAL_DIR}. The output of each
 * writer is written to this directory. Each individual writer can also specify a path in the config key
 * {@link ConfigurationKeys#WRITER_FILE_PATH}. Then the final output data for a writer will be
 * {@link ConfigurationKeys#DATA_PUBLISHER_FINAL_DIR}/{@link ConfigurationKeys#WRITER_FILE_PATH}. If the
 * {@link ConfigurationKeys#WRITER_FILE_PATH} is not specified, a default one is assigned. The default path is
 * constructed in the {@link gobblin.source.workunit.Extract#getOutputFilePath()} method.
 * </p>
 *
 * <p>
 * This publisher records all dirs it publishes to in property {@link ConfigurationKeys#PUBLISHER_DIRS}. Each time it
 * publishes a {@link Path}, if the path is a directory, it records this path. If the path is a file, it records the
 * parent directory of the path. To change this behavior one may override
 * {@link #recordPublisherOutputDirs(Path, Path, int)}.
 * </p>
 */
public class BaseDataPublisher extends SingleTaskDataPublisher {

  private static final Logger LOG = LoggerFactory.getLogger(BaseDataPublisher.class);

  protected final int numBranches;
  protected final List<FileSystem> writerFileSystemByBranches;
  protected final List<FileSystem> publisherFileSystemByBranches;
  protected final List<FileSystem> metaDataWriterFileSystemByBranches;
  protected final List<Optional<String>> publisherFinalDirOwnerGroupsByBranches;
  protected final List<FsPermission> permissions;
  protected final Closer closer;
  protected final Closer parallelRunnerCloser;
  protected final int parallelRunnerThreads;
  protected final Map<String, ParallelRunner> parallelRunners = Maps.newHashMap();
  protected final Set<Path> publisherOutputDirs = Sets.newHashSet();

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
    this.metaDataWriterFileSystemByBranches = Lists.newArrayListWithCapacity(this.numBranches);
    this.publisherFinalDirOwnerGroupsByBranches = Lists.newArrayListWithCapacity(this.numBranches);
    this.permissions = Lists.newArrayListWithCapacity(this.numBranches);

    // Get a FileSystem instance for each branch
    for (int i = 0; i < this.numBranches; i++) {
      URI writerUri = URI.create(this.getState().getProp(
          ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, this.numBranches, i),
          ConfigurationKeys.LOCAL_FS_URI));
      this.writerFileSystemByBranches.add(FileSystem.get(writerUri, conf));

      URI publisherUri = URI.create(this.getState().getProp(ForkOperatorUtils.getPropertyNameForBranch(
          ConfigurationKeys.DATA_PUBLISHER_FILE_SYSTEM_URI, this.numBranches, i), writerUri.toString()));
      this.publisherFileSystemByBranches.add(FileSystem.get(publisherUri, conf));
      this.metaDataWriterFileSystemByBranches.add(FileSystem.get(publisherUri, conf));

      // The group(s) will be applied to the final publisher output directory(ies)
      this.publisherFinalDirOwnerGroupsByBranches.add(Optional.fromNullable(this.getState().getProp(ForkOperatorUtils
          .getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR_GROUP, this.numBranches, i))));

      // The permission(s) will be applied to all directories created by the publisher,
      // which do NOT include directories created by the writer and moved by the publisher.
      // The permissions of those directories are controlled by writer.file.permissions and writer.dir.permissions.
      this.permissions.add(new FsPermission(state.getPropAsShortWithRadix(
          ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISHER_PERMISSIONS, this.numBranches, i),
          FsPermission.getDefault().toShort(), ConfigurationKeys.PERMISSION_PARSING_RADIX)));
    }

    this.parallelRunnerThreads =
        state.getPropAsInt(ParallelRunner.PARALLEL_RUNNER_THREADS_KEY, ParallelRunner.DEFAULT_PARALLEL_RUNNER_THREADS);
    this.parallelRunnerCloser = Closer.create();
  }

  @Override
  public void initialize() throws IOException {
    // Nothing needs to be done since the constructor already initializes the publisher.
  }

  @Override
  public void close() throws IOException {
    try {
      for (Path path : this.publisherOutputDirs) {
        this.state.appendToSetProp(ConfigurationKeys.PUBLISHER_DIRS, path.toString());
      }
    } finally {
      this.closer.close();
    }
  }

  @Override
  public void publishData(WorkUnitState state) throws IOException {
    for (int branchId = 0; branchId < this.numBranches; branchId++) {
      publishSingleTaskData(state, branchId);
    }
    this.parallelRunnerCloser.close();
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
    }

    this.parallelRunnerCloser.close();

    for (WorkUnitState workUnitState : states) {
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
    ParallelRunner parallelRunner = this.getParallelRunner(this.writerFileSystemByBranches.get(branchId));

    // The directory where the workUnitState wrote its output data.
    Path writerOutputDir = WriterUtils.getWriterOutputDir(state, this.numBranches, branchId);

    if (!this.writerFileSystemByBranches.get(branchId).exists(writerOutputDir)) {
      LOG.warn(String.format("Branch %d of WorkUnit %s produced no data", branchId, state.getId()));
      return;
    }

    // The directory where the final output directory for this job will be placed.
    // It is a combination of DATA_PUBLISHER_FINAL_DIR and WRITER_FILE_PATH.
    Path publisherOutputDir = getPublisherOutputDir(state, branchId);

    if (publishSingleTaskData) {

      // Create final output directory
      WriterUtils.mkdirsWithRecursivePermission(this.publisherFileSystemByBranches.get(branchId), publisherOutputDir,
          this.permissions.get(branchId));
      addSingleTaskWriterOutputToExistingDir(writerOutputDir, publisherOutputDir, state, branchId, parallelRunner);
    } else {
      if (writerOutputPathsMoved.contains(writerOutputDir)) {
        // This writer output path has already been moved for another task of the same extract
        // If publishSingleTaskData=true, writerOutputPathMoved is ignored.
        return;
      }

      if (this.publisherFileSystemByBranches.get(branchId).exists(publisherOutputDir)) {
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
        this.publisherFileSystemByBranches.get(branchId).delete(publisherOutputDir, true);
      } else {
        // Create the parent directory of the final output directory if it does not exist
        WriterUtils.mkdirsWithRecursivePermission(this.publisherFileSystemByBranches.get(branchId),
            publisherOutputDir.getParent(), this.permissions.get(branchId));
      }

      movePath(parallelRunner, state, writerOutputDir, publisherOutputDir, branchId);
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
    String outputFilePropName = ForkOperatorUtils
        .getPropertyNameForBranch(ConfigurationKeys.WRITER_FINAL_OUTPUT_FILE_PATHS, this.numBranches, branchId);

    if (!workUnitState.contains(outputFilePropName)) {
      LOG.warn("Missing property " + outputFilePropName + ". This task may have pulled no data.");
      return;
    }

    Iterable<String> taskOutputFiles = workUnitState.getPropAsSet(outputFilePropName);
    for (String taskOutputFile : taskOutputFiles) {
      Path taskOutputPath = new Path(taskOutputFile);
      if (!this.writerFileSystemByBranches.get(branchId).exists(taskOutputPath)) {
        LOG.warn("Task output file " + taskOutputFile + " doesn't exist.");
        continue;
      }
      String pathSuffix = taskOutputFile
          .substring(taskOutputFile.indexOf(writerOutputDir.toString()) + writerOutputDir.toString().length() + 1);
      Path publisherOutputPath = new Path(publisherOutputDir, pathSuffix);
      WriterUtils.mkdirsWithRecursivePermission(this.publisherFileSystemByBranches.get(branchId),
          publisherOutputPath.getParent(), this.permissions.get(branchId));

      movePath(parallelRunner, workUnitState, taskOutputPath, publisherOutputPath, branchId);
    }
  }

  protected void addWriterOutputToExistingDir(Path writerOutputDir, Path publisherOutputDir,
      WorkUnitState workUnitState, int branchId, ParallelRunner parallelRunner) throws IOException {
    boolean preserveFileName = workUnitState.getPropAsBoolean(ForkOperatorUtils.getPropertyNameForBranch(
        ConfigurationKeys.SOURCE_FILEBASED_PRESERVE_FILE_NAME, this.numBranches, branchId), false);

    // Go through each file in writerOutputDir and move it into publisherOutputDir
    for (FileStatus status : this.writerFileSystemByBranches.get(branchId).listStatus(writerOutputDir)) {

      // Preserve the file name if configured, use specified name otherwise
      Path finalOutputPath =
          preserveFileName
              ? new Path(publisherOutputDir,
                  workUnitState.getProp(ForkOperatorUtils.getPropertyNameForBranch(
                      ConfigurationKeys.DATA_PUBLISHER_FINAL_NAME, this.numBranches, branchId)))
              : new Path(publisherOutputDir, status.getPath().getName());

      movePath(parallelRunner, workUnitState, status.getPath(), finalOutputPath, branchId);
    }
  }

  protected void movePath(ParallelRunner parallelRunner, State state, Path src, Path dst, int branchId)
      throws IOException {
    LOG.info(String.format("Moving %s to %s", src, dst));
    boolean overwrite = state.getPropAsBoolean(ConfigurationKeys.DATA_PUBLISHER_OVERWRITE_ENABLED, false);
    this.publisherOutputDirs.addAll(recordPublisherOutputDirs(src, dst, branchId));
    parallelRunner.movePath(src, this.publisherFileSystemByBranches.get(branchId), dst, overwrite,
        this.publisherFinalDirOwnerGroupsByBranches.get(branchId));
  }

  protected Collection<Path> recordPublisherOutputDirs(Path src, Path dst, int branchId) throws IOException {

    // Getting file status from src rather than dst, because at this time dst doesn't yet exist.
    // If src is a dir, add dst to the set of paths. Otherwise, add dst's parent.
    if (this.writerFileSystemByBranches.get(branchId).getFileStatus(src).isDirectory()) {
      return ImmutableList.<Path> of(dst);
    }
    return ImmutableList.<Path> of(dst.getParent());
  }

  private ParallelRunner getParallelRunner(FileSystem fs) {
    String uri = fs.getUri().toString();
    if (!this.parallelRunners.containsKey(uri)) {
      this.parallelRunners.put(uri,
          this.parallelRunnerCloser.register(new ParallelRunner(this.parallelRunnerThreads, fs)));
    }
    return this.parallelRunners.get(uri);
  }

  @Override
  public void publishMetadata(Collection<? extends WorkUnitState> states) throws IOException {
        for (WorkUnitState workUnitState : states) {
        publishMetadata(workUnitState);
    }
  }

  @Override
  public void publishMetadata(WorkUnitState state) throws IOException {
    String metadataValue = state.getProp(ConfigurationKeys.DATA_PUBLISHER_METADATA_STR);
    if (metadataValue == null) {
      //Nothing to write
      return;
    }

    if (state.getProp(ConfigurationKeys.DATA_PUBLISHER_METADATA_OUTPUT_DIR) == null) {
      LOG.error("Missing metadata output directory path : "  + ConfigurationKeys.DATA_PUBLISHER_METADATA_OUTPUT_DIR
                + " in the config");
      return;
    }

    //Write for each branch
    for (int branchId = 0; branchId < this.numBranches; branchId++) {
      FileSystem fs = this.metaDataWriterFileSystemByBranches.get(branchId);

      String filePrefix = state.getProp(ConfigurationKeys.DATA_PUBLISHER_METADATA_OUTPUT_FILE);
      String fileName = ForkOperatorUtils.getPropertyNameForBranch(filePrefix, this.numBranches, branchId);
      String metaDataOutputDirStr = state.getProp(ConfigurationKeys.DATA_PUBLISHER_METADATA_OUTPUT_DIR);

      Path metaDataOutputPath = new Path(metaDataOutputDirStr);
      try {
        if (!fs.exists(metaDataOutputPath)) {
          WriterUtils.mkdirsWithRecursivePermission(fs, metaDataOutputPath, this.permissions.get(branchId));
        }

        Path metaFilepath = new Path(metaDataOutputDirStr, fileName);

        //Delete the file if metadata already exists
        if (fs.exists(metaFilepath)) {
          HadoopUtils.deletePath(fs, metaFilepath, false);
        }

        try (FSDataOutputStream outputStream = this.closer.register(fs.create(metaFilepath))) {
          outputStream.write(metadataValue.getBytes("UTF-8"));
        }
      } catch (IOException e) {
        LOG.error("metadata file is not generated: " + e, e);
      }
    }
  }
}
