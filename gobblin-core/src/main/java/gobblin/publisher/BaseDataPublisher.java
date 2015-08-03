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
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  protected final List<FileSystem> fss = Lists.newArrayList();
  protected final Closer closer;
  protected final int numBranches;
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

    // Get a FileSystem instance for each branch
    for (int i = 0; i < this.numBranches; i++) {
      URI uri = URI.create(this.getState().getProp(
          ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, this.numBranches, i),
          ConfigurationKeys.LOCAL_FS_URI));
      this.fss.add(FileSystem.get(uri, conf));
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
  public void publishData(Collection<? extends WorkUnitState> states) throws IOException {

    // We need a Set to collect unique writer output paths as multiple tasks may belong to the same extract. Tasks that
    // belong to the same Extract will by default have the same output directory
    Set<Path> writerOutputPathsMoved = Sets.newHashSet();

    for (WorkUnitState workUnitState : states) {
      for (int branchId = 0; branchId < this.numBranches; branchId++) {
        // Get a ParallelRunner instance for moving files in parallel
        ParallelRunner parallelRunner = this.getParallelRunner(this.fss.get(branchId));

        // The directory where the workUnitState wrote its output data.
        // It is a combination of WRITER_OUTPUT_DIR and WRITER_FILE_PATH.
        Path writerOutputDir = WriterUtils.getWriterOutputDir(workUnitState, this.numBranches, branchId);

        if (writerOutputPathsMoved.contains(writerOutputDir)) {
          // This writer output path has already been moved for another task of the same extract
          continue;
        }

        if (!this.fss.get(branchId).exists(writerOutputDir)) {
          LOG.warn(String.format("Branch %d of WorkUnit %s produced no data", branchId, workUnitState.getId()));
          continue;
        }

        // The directory where the final output directory for this job will be placed.
        // It is a combination of DATA_PUBLISHER_FINAL_DIR and WRITER_FILE_PATH.
        Path publisherOutputDir = WriterUtils.getDataPublisherFinalDir(workUnitState, this.numBranches, branchId);

        if (this.fss.get(branchId).exists(publisherOutputDir)) {
          // The final output directory already exists, check if the job is configured to replace it.
          boolean replaceFinalOutputDir = this.getState().getPropAsBoolean(ForkOperatorUtils.getPropertyNameForBranch(
              ConfigurationKeys.DATA_PUBLISHER_REPLACE_FINAL_DIR, this.numBranches, branchId));

          // If the final output directory is not configured to be replaced, put new data to the existing directory.
          if (!replaceFinalOutputDir) {
            addWriterOutputToExistingDir(writerOutputDir, publisherOutputDir, workUnitState, branchId, parallelRunner);
            writerOutputPathsMoved.add(writerOutputDir);
            continue;
          }
          // Delete the final output directory if it is configured to be replaced
          this.fss.get(branchId).delete(publisherOutputDir, true);
        } else {
          // Create the parent directory of the final output directory if it does not exist
          this.fss.get(branchId).mkdirs(publisherOutputDir.getParent());
        }

        LOG.info(String.format("Moving %s to %s", writerOutputDir, publisherOutputDir));
        parallelRunner.renamePath(writerOutputDir, publisherOutputDir);
        writerOutputPathsMoved.add(writerOutputDir);
      }

      // Upon successfully committing the data to the final output directory, set states
      // of successful tasks to COMMITTED. leaving states of unsuccessful ones unchanged.
      // This makes sense to the COMMIT_ON_PARTIAL_SUCCESS policy.
      workUnitState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
    }
  }

  protected void addWriterOutputToExistingDir(Path writerOutputDir, Path publisherOutputDir,
      WorkUnitState workUnitState, int branchId, ParallelRunner parallelRunner) throws IOException {
    boolean preserveFileName = workUnitState.getPropAsBoolean(ForkOperatorUtils.getPropertyNameForBranch(
        ConfigurationKeys.SOURCE_FILEBASED_PRESERVE_FILE_NAME, this.numBranches, branchId), false);

    // Go through each file in writerOutputDir and move it into publisherOutputDir
    for (FileStatus status : this.fss.get(branchId).listStatus(writerOutputDir)) {

      // Preserve the file name if configured, use specified name otherwise
      Path finalOutputPath =
          preserveFileName
              ? new Path(publisherOutputDir,
                  workUnitState.getProp(ForkOperatorUtils.getPropertyNameForBranch(
                      ConfigurationKeys.DATA_PUBLISHER_FINAL_NAME, this.numBranches, branchId)))
          : new Path(publisherOutputDir, status.getPath().getName());

      LOG.info(String.format("Moving %s to %s", status.getPath(), finalOutputPath));
      parallelRunner.renamePath(status.getPath(), finalOutputPath);
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
}
