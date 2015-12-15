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

package gobblin.runtime;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Queues;

import gobblin.configuration.ConfigurationKeys;
import gobblin.util.ExecutorsUtils;
import gobblin.util.ParallelRunner;


/**
 * An extension to {@link AbstractJobLauncher} for launching a Gobblin job on a distributed
 * execution framework such as Hadoop MapReduce or Yarn.
 *
 * <p>
 *   This class implements common methods needed to launch and run a Gobblin job on the
 *   distributed execution framework of choice. For example, one common task is to collect
 *   output {@link TaskState}s stored in files output by the mappers or containers.
 * </p>
 *
 * @author ynli
 */
public abstract class DistributedJobLauncher extends AbstractJobLauncher {

  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedJobLauncher.class);


  protected final FileSystem fs;

  // Number of ParallelRunner threads to be used for state serialization/deserialization
  protected final int stateSerDeRunnerThreads;

  // Interval in seconds between two runs of the collector of output TaskStates
  private final int outputTaskStatesCollectorIntervalSeconds;

  // A scheduled executor for the collector of output TaskStates
  private final ScheduledExecutorService outputTaskStatesCollectorExecutor;

  public DistributedJobLauncher(Properties jobProps, FileSystem fs, Map<String, String> eventMetadata)
      throws Exception {
    super(jobProps, eventMetadata);

    this.fs = fs;

    this.stateSerDeRunnerThreads = Integer.parseInt(jobProps.getProperty(ParallelRunner.PARALLEL_RUNNER_THREADS_KEY,
        Integer.toString(ParallelRunner.DEFAULT_PARALLEL_RUNNER_THREADS)));

    this.outputTaskStatesCollectorIntervalSeconds = Integer.parseInt(jobProps
        .getProperty(ConfigurationKeys.TASK_STATE_COLLECTOR_INTERVAL_SECONDS,
            Integer.toString(ConfigurationKeys.DEFAULT_TASK_STATE_COLLECTOR_INTERVAL_SECONDS)));

    this.outputTaskStatesCollectorExecutor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("OutputTaskStatesCollector")));
  }

  /**
   * Schedule the collector of output {@link TaskState}s.
   *
   * <p>
   *   The collector runs every minute and collects all the newly available output {@link TaskState}
   *   files at the time the collector starts running. Each output {@link TaskState} file is deleted
   *   after the {@link TaskState} it stores is successfully collected.
   * </p>
   *
   * @param outputTaskStateDir the directory where files storing output {@link TaskState}s are stored
   * @throws IOException if if fails to close the {@link ParallelRunner}
   */
  protected void scheduleOutputTaskStatesCollector(final Path outputTaskStateDir) throws IOException {
    this.outputTaskStatesCollectorExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          collectOutputTaskStates(outputTaskStateDir);
        } catch (IOException ioe) {
          LOGGER.error("Failed to collect output task states", ioe);
          jobContext.getJobState().setState(JobState.RunningState.FAILED);
        }
      }
    }, 0, this.outputTaskStatesCollectorIntervalSeconds, TimeUnit.SECONDS);
  }

  /**
   * Shutdown the collector of output {@link TaskState}s.
   */
  protected void shutdownOutputTaskStatesCollector() {
    ExecutorsUtils.shutdownExecutorService(this.outputTaskStatesCollectorExecutor, Optional.of(LOGGER));
  }

  /**
   * Collect output {@link TaskState}s of tasks of the job launched.
   *
   * <p>
   *   This method collects all available output {@link TaskState} files at the time it is called. It
   *   uses a {@link ParallelRunner} to deserialize the {@link TaskState}s. Each {@link TaskState}
   *   file gets deleted after the {@link TaskState} it stores is successfully collected.
   * </p>
   *
   * @param outputTaskStateDir the directory where files storing output {@link TaskState}s are stored
   * @throws IOException if it fails to collect the output {@link TaskState}s
   */
  protected void collectOutputTaskStates(Path outputTaskStateDir)
      throws IOException {
    if (!this.fs.exists(outputTaskStateDir)) {
      LOGGER.warn(String.format("Output task state path %s does not exist", outputTaskStateDir));
      return;
    }

    FileStatus[] fileStatuses = this.fs.listStatus(outputTaskStateDir, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().endsWith(TASK_STATE_STORE_TABLE_SUFFIX);
      }
    });
    if (fileStatuses == null || fileStatuses.length == 0) {
      LOGGER.warn("No output task state files found in " + outputTaskStateDir);
      return;
    }

    Queue<TaskState> taskStateQueue = Queues.newConcurrentLinkedQueue();
    try (ParallelRunner stateSerDeRunner = new ParallelRunner(stateSerDeRunnerThreads, this.fs)) {
      for (FileStatus status : fileStatuses) {
        LOGGER.info("Found output task state file " + status.getPath());
        // Deserialize the TaskState and delete the file
        stateSerDeRunner.deserializeFromSequenceFile(Text.class, TaskState.class, status.getPath(),
            taskStateQueue, true);
      }
    }

    LOGGER.info(String.format("Collected task state of %d completed tasks", taskStateQueue.size()));

    for (TaskState taskState : taskStateQueue) {
      // Notify the listeners for each collected output TaskState
      this.eventBus.post(new NewOutputTaskStateEvent(taskState));
    }
  }

  @Override
  public void close() throws IOException {
    try {
      // Shutdown the collector of output TaskStates if it has not been shutdown yet
      shutdownOutputTaskStatesCollector();
    } finally {
      super.close();
    }
  }
}
