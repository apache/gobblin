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

package gobblin.runtime;

import java.io.IOException;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Queues;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.AbstractScheduledService;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metastore.FsStateStore;
import gobblin.util.ParallelRunner;


/**
 * An {@link AbstractScheduledService} for collecting output {@link TaskState}s of completed {@link Task}s
 * stored in files, which get deleted once the {@link TaskState}s they store are successfully collected.
 * For each batch of {@link TaskState}s collected, it posts a {@link NewTaskCompletionEvent} to notify
 * parties that are interested in such events.
 *
 * @author Yinan Li
 */
public class TaskStateCollectorService extends AbstractScheduledService {

  private static final Logger LOGGER = LoggerFactory.getLogger(TaskStateCollectorService.class);

  private final JobState jobState;

  private final FileSystem fs;

  private final EventBus eventBus;

  // Number of ParallelRunner threads to be used for state serialization/deserialization
  private final int stateSerDeRunnerThreads;

  // Interval in seconds between two runs of the collector of output TaskStates
  private final int outputTaskStatesCollectorIntervalSeconds;

  private final Path outputTaskStateDir;

  public TaskStateCollectorService(Properties jobProps, JobState jobState, EventBus eventBus, FileSystem fs,
      Path outputTaskStateDir) {
    this.jobState = jobState;
    this.eventBus = eventBus;
    this.fs = fs;
    this.outputTaskStateDir = outputTaskStateDir;

    this.stateSerDeRunnerThreads = Integer.parseInt(jobProps.getProperty(ParallelRunner.PARALLEL_RUNNER_THREADS_KEY,
        Integer.toString(ParallelRunner.DEFAULT_PARALLEL_RUNNER_THREADS)));

    this.outputTaskStatesCollectorIntervalSeconds =
        Integer.parseInt(jobProps.getProperty(ConfigurationKeys.TASK_STATE_COLLECTOR_INTERVAL_SECONDS,
            Integer.toString(ConfigurationKeys.DEFAULT_TASK_STATE_COLLECTOR_INTERVAL_SECONDS)));
  }

  @Override
  protected void runOneIteration() throws Exception {
    collectOutputTaskStates();
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(this.outputTaskStatesCollectorIntervalSeconds,
        this.outputTaskStatesCollectorIntervalSeconds, TimeUnit.SECONDS);
  }

  @Override
  protected void startUp() throws Exception {
    LOGGER.info("Starting the " + TaskStateCollectorService.class.getSimpleName());
    super.startUp();
  }

  @Override
  protected void shutDown() throws Exception {
    LOGGER.info("Stopping the " + TaskStateCollectorService.class.getSimpleName());
    try {
      runOneIteration();
    } finally {
      super.shutDown();
    }
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
   * @throws IOException if it fails to collect the output {@link TaskState}s
   */
  private void collectOutputTaskStates() throws IOException {
    if (!this.fs.exists(this.outputTaskStateDir)) {
      LOGGER.warn(String.format("Output task state path %s does not exist", this.outputTaskStateDir));
      return;
    }

    FileStatus[] fileStatuses = this.fs.listStatus(this.outputTaskStateDir, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().endsWith(AbstractJobLauncher.TASK_STATE_STORE_TABLE_SUFFIX)
            && !path.getName().startsWith(FsStateStore.TMP_FILE_PREFIX);
      }
    });
    if (fileStatuses == null || fileStatuses.length == 0) {
      LOGGER.warn("No output task state files found in " + this.outputTaskStateDir);
      return;
    }

    Queue<TaskState> taskStateQueue = Queues.newConcurrentLinkedQueue();
    try (ParallelRunner stateSerDeRunner = new ParallelRunner(this.stateSerDeRunnerThreads, this.fs)) {
      for (FileStatus status : fileStatuses) {
        LOGGER.debug("Found output task state file " + status.getPath());
        // Deserialize the TaskState and delete the file
        stateSerDeRunner.deserializeFromSequenceFile(Text.class, TaskState.class, status.getPath(), taskStateQueue,
            true);
      }
    } catch (IOException ioe) {
      LOGGER.warn("Could not read all task state files.");
    }

    LOGGER.info(String.format("Collected task state of %d completed tasks", taskStateQueue.size()));

    // Add the TaskStates of completed tasks to the JobState so when the control
    // returns to the launcher, it sees the TaskStates of all completed tasks.
    for (TaskState taskState : taskStateQueue) {
      taskState.setJobState(this.jobState);
      this.jobState.addTaskState(taskState);
    }

    // Notify the listeners for the completion of the tasks
    this.eventBus.post(new NewTaskCompletionEvent(ImmutableList.copyOf(taskStateQueue)));
  }
}
