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

package org.apache.gobblin.runtime;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Queues;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.AbstractScheduledService;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.FsStateStore;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.runtime.troubleshooter.Issue;
import org.apache.gobblin.runtime.troubleshooter.IssueRepository;
import org.apache.gobblin.runtime.troubleshooter.TroubleshooterException;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.service.ServiceConfigKeys;

import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ParallelRunner;


/**
 * An {@link AbstractScheduledService} for collecting output {@link TaskState}s of completed {@link Task}s
 * stored in files, which get deleted once the {@link TaskState}s they store are successfully collected.
 * For each batch of {@link TaskState}s collected, it posts a {@link NewTaskCompletionEvent} to notify
 * parties that are interested in such events.
 *
 * @author Yinan Li
 */
@Slf4j
public class TaskStateCollectorService extends AbstractScheduledService {

  private static final Logger LOGGER = LoggerFactory.getLogger(TaskStateCollectorService.class);

  private final JobState jobState;

  private final EventBus eventBus;

  private final EventSubmitter eventSubmitter;

  // Number of ParallelRunner threads to be used for state serialization/deserialization
  private final int stateSerDeRunnerThreads;

  // Interval in seconds between two runs of the collector of output TaskStates
  private final int outputTaskStatesCollectorIntervalSeconds;

  private final StateStore<TaskState> taskStateStore;

  private final Path outputTaskStateDir;

  private double totalSizeToCopy;

  private double bytesCopiedSoFar;

  private double totalNumWorkUnits;

  private double workUnitsCompletedSoFar;

  private double lastPercentageReported;

  /**
   * Add a closeable action to run after each existence-checking of task state file.
   * A typical example to plug here is hive registration:
   * We do hive registration everytime there are available taskStates deserialized from storage, on the driver level.
   */
  @Getter
  private final Optional<TaskStateCollectorServiceHandler> optionalTaskCollectorHandler;
  private final Closer handlerCloser = Closer.create();

  private final boolean isJobProceedOnCollectorServiceFailure;

  /**
   * By default, whether {@link TaskStateCollectorService} finishes successfully or not won't influence
   * job's proceed.
   */
  private static final boolean defaultPolicyOnCollectorServiceFailure = true;

  private final IssueRepository issueRepository;
  private final AtomicBoolean reportedIssueConsumptionWarning = new AtomicBoolean(false);

  public TaskStateCollectorService(Properties jobProps, JobState jobState, EventBus eventBus,
      EventSubmitter eventSubmitter, StateStore<TaskState> taskStateStore, Path outputTaskStateDir,
      IssueRepository issueRepository) {
    this.jobState = jobState;
    this.eventBus = eventBus;
    this.eventSubmitter = eventSubmitter;
    this.taskStateStore = taskStateStore;
    this.outputTaskStateDir = outputTaskStateDir;
    this.issueRepository = issueRepository;

    this.stateSerDeRunnerThreads = Integer.parseInt(jobProps.getProperty(ParallelRunner.PARALLEL_RUNNER_THREADS_KEY,
        Integer.toString(ParallelRunner.DEFAULT_PARALLEL_RUNNER_THREADS)));

    this.outputTaskStatesCollectorIntervalSeconds =
        Integer.parseInt(jobProps.getProperty(ConfigurationKeys.TASK_STATE_COLLECTOR_INTERVAL_SECONDS,
            Integer.toString(ConfigurationKeys.DEFAULT_TASK_STATE_COLLECTOR_INTERVAL_SECONDS)));

    if (!StringUtils.isBlank(jobProps.getProperty(ConfigurationKeys.TASK_STATE_COLLECTOR_HANDLER_CLASS))) {
      String handlerTypeName = jobProps.getProperty(ConfigurationKeys.TASK_STATE_COLLECTOR_HANDLER_CLASS);
      try {
        ClassAliasResolver<TaskStateCollectorServiceHandler.TaskStateCollectorServiceHandlerFactory> aliasResolver =
            new ClassAliasResolver<>(TaskStateCollectorServiceHandler.TaskStateCollectorServiceHandlerFactory.class);
        TaskStateCollectorServiceHandler.TaskStateCollectorServiceHandlerFactory handlerFactory =
            aliasResolver.resolveClass(handlerTypeName).newInstance();
        optionalTaskCollectorHandler = Optional.of(handlerCloser.register(handlerFactory.createHandler(this.jobState)));
      } catch (ReflectiveOperationException rfe) {
        throw new RuntimeException("Could not construct TaskCollectorHandler " + handlerTypeName, rfe);
      }
    } else {
      optionalTaskCollectorHandler = Optional.absent();
    }

    isJobProceedOnCollectorServiceFailure =
        jobState.getPropAsBoolean(ConfigurationKeys.JOB_PROCEED_ON_TASK_STATE_COLLECOTR_SERVICE_FAILURE,
            defaultPolicyOnCollectorServiceFailure);
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
      this.handlerCloser.close();
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
    List<String> taskStateNames = taskStateStore.getTableNames(outputTaskStateDir.getName(), new Predicate<String>() {
      @Override
      public boolean apply(String input) {
        return input.endsWith(AbstractJobLauncher.TASK_STATE_STORE_TABLE_SUFFIX)
        && !input.startsWith(FsStateStore.TMP_FILE_PREFIX);
      }});

    if (taskStateNames == null || taskStateNames.size() == 0) {
      LOGGER.debug("No output task state files found in " + this.outputTaskStateDir);
      return;
    }

    final Queue<TaskState> taskStateQueue = Queues.newConcurrentLinkedQueue();
    try (ParallelRunner stateSerDeRunner = new ParallelRunner(this.stateSerDeRunnerThreads, null)) {
      for (final String taskStateName : taskStateNames) {
        LOGGER.debug("Found output task state file " + taskStateName);
        // Deserialize the TaskState and delete the file
        stateSerDeRunner.submitCallable(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            TaskState taskState = taskStateStore.getAll(outputTaskStateDir.getName(), taskStateName).get(0);
            taskStateQueue.add(taskState);
            taskStateStore.delete(outputTaskStateDir.getName(), taskStateName);
            return null;
          }
        }, "Deserialize state for " + taskStateName);
      }
    } catch (IOException ioe) {
      LOGGER.warn("Could not read all task state files.");
    }

    LOGGER.info(String.format("Collected task state of %d completed tasks", taskStateQueue.size()));

    // Add the TaskStates of completed tasks to the JobState so when the control
    // returns to the launcher, it sees the TaskStates of all completed tasks.
    for (TaskState taskState : taskStateQueue) {
      consumeTaskIssues(taskState);
      taskState.setJobState(this.jobState);
      this.jobState.addTaskState(taskState);

      if (this.jobState.getPropAsBoolean(ConfigurationKeys.REPORT_JOB_PROGRESS, ConfigurationKeys.DEFAULT_REPORT_JOB_PROGRESS)) {
        reportJobProgress(taskState);
      }
    }

    // Finish any additional steps defined in handler on driver level.
    // Currently implemented handler for Hive registration only.
    if (optionalTaskCollectorHandler.isPresent()) {
      LOGGER.info("Execute Pipelined TaskStateCollectorService Handler for " + taskStateQueue.size() + " tasks");

      try {
        optionalTaskCollectorHandler.get().handle(taskStateQueue);
      } catch (Throwable t) {
        if (isJobProceedOnCollectorServiceFailure) {
          log.error("Failed to commit dataset while job proceeds", t);
          SafeDatasetCommit.setTaskFailureException(taskStateQueue, t);
        } else {
          throw new RuntimeException("Hive Registration as the TaskStateCollectorServiceHandler failed.", t);
        }
      }
    }

    // Notify the listeners for the completion of the tasks
    this.eventBus.post(new NewTaskCompletionEvent(ImmutableList.copyOf(taskStateQueue)));
  }

  /**
   * Uses the size of work units to determine a job's progress and reports the progress as a percentage via
   * GobblinTrackingEvents
   * @param taskState of job launched
   */
  private void reportJobProgress(TaskState taskState) {
    String stringSize = taskState.getProp(ServiceConfigKeys.WORK_UNIT_SIZE);
    if (stringSize == null) {
      LOGGER.warn("Expected to report job progress but work unit byte size property null");
      return;
    }

    Long taskByteSize = Long.parseLong(stringSize);

    // If progress reporting is enabled, value should be present
    if (!this.jobState.contains(ServiceConfigKeys.TOTAL_WORK_UNIT_SIZE)) {
      LOGGER.warn("Expected to report job progress but total bytes to copy property null");
      return;
    }
    this.totalSizeToCopy = this.jobState.getPropAsLong(ServiceConfigKeys.TOTAL_WORK_UNIT_SIZE);

    // If total size in bytes cannot be calculated, then default to progress reporting in terms of workunits
    Double newPercentageCopied;
    if (this.totalSizeToCopy == 0) {
      this.totalNumWorkUnits = this.jobState.getPropAsLong(AbstractJobLauncher.NUM_WORKUNITS);
      this.workUnitsCompletedSoFar += 1;

      if (this.totalNumWorkUnits == 0) {
        LOGGER.warn("Expected to report job progress but work units are not countable");
        return;
      }
      newPercentageCopied = this.workUnitsCompletedSoFar / this.totalNumWorkUnits;
    } else {
      this.bytesCopiedSoFar += taskByteSize;
      newPercentageCopied = this.bytesCopiedSoFar / this.totalSizeToCopy;
    }


    // Avoid flooding Kafka message queue by sending GobblinTrackingEvents only when threshold is passed
    // Report progress when it reaches 100% regardless of difference from lastPercentageReported
    if (newPercentageCopied - this.lastPercentageReported >= ConfigurationKeys.DEFAULT_PROGRESS_REPORTING_THRESHOLD ||
        (Math.abs(newPercentageCopied - 1.0) < 0.001)) {
      this.lastPercentageReported = newPercentageCopied;
      int percentageToReport = (int) Math.round(this.lastPercentageReported * 100);

      Map<String, String> progress = new HashMap<>();
      progress.put(TimingEvent.JOB_COMPLETION_PERCENTAGE, String.valueOf(percentageToReport));

      LOGGER.info("Sending copy progress event with percentage " + percentageToReport + "%");
      new TimingEvent(this.eventSubmitter, TimingEvent.JOB_COMPLETION_PERCENTAGE).stop(progress);
    }
  }

  private void consumeTaskIssues(TaskState taskState) {
    List<Issue> taskIssues = taskState.getTaskIssues();

    /* A single job can spawn tens of thousands of tasks, and in case of wide-spread errors they will all produce
     * similar Issues with large exception stack traces. If the process that collects task states keeps all of them in
     * job state, it can run out of memory. To avoid that, we're forwarding issues to central repository that has
     * size limits, and then remove them from the task state.
     * */

    if (taskIssues != null) {
      try {
        issueRepository.put(taskIssues);
      } catch (TroubleshooterException e) {
        if (reportedIssueConsumptionWarning.compareAndSet(false, true)) {
          log.warn("Failed to consume task issues", e);
        }
      }
      taskState.setTaskIssues(null);
    }
  }
}
