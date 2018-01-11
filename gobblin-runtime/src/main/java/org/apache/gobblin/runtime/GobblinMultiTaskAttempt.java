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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.gobblin_scopes.TaskScopeInstance;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.broker.iface.SubscopedBrokerBuilder;
import org.apache.gobblin.commit.CommitStep;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.JobEvent;
import org.apache.gobblin.runtime.task.TaskFactory;
import org.apache.gobblin.runtime.task.TaskIFaceWrapper;
import org.apache.gobblin.runtime.task.TaskUtils;
import org.apache.gobblin.runtime.util.JobMetrics;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.Either;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.executors.IteratorExecutor;

import javax.annotation.Nullable;


/**
 * Attempt of running multiple {@link Task}s generated from a list of{@link WorkUnit}s.
 * A {@link GobblinMultiTaskAttempt} is usually a unit of workunits that are assigned to one container.
 */
@Alpha
public class GobblinMultiTaskAttempt {

  /**
   * An enumeration of policies on when a {@link GobblinMultiTaskAttempt} will be committed.
   */
  public enum CommitPolicy {
    /**
     * Commit {@link GobblinMultiTaskAttempt} immediately after running is done.
     */
    IMMEDIATE,
    /**
     * Not committing {@link GobblinMultiTaskAttempt} but leaving it to user customized launcher.
     */
    CUSTOMIZED
  }

  private static final String TASK_STATE_STORE_SUCCESS_MARKER_SUFFIX = ".suc";
  private final Logger log;
  private final Iterator<WorkUnit> workUnits;
  private final String jobId;
  private final JobState jobState;
  private final TaskStateTracker taskStateTracker;
  private final TaskExecutor taskExecutor;
  private final Optional<String> containerIdOptional;
  private final Optional<StateStore<TaskState>> taskStateStoreOptional;
  private final SharedResourcesBroker<GobblinScopeTypes> jobBroker;
  private List<Task> tasks;

  /**
   * Additional commit steps that may be added by different launcher, and can be environment specific.
   * Usually it should be clean-up steps, which are always executed at the end of {@link #commit()}.
   */
  private List<CommitStep> cleanupCommitSteps;

  public GobblinMultiTaskAttempt(Iterator<WorkUnit> workUnits,
                                 String jobId,
                                 JobState jobState,
                                 TaskStateTracker taskStateTracker,
                                 TaskExecutor taskExecutor,
                                 Optional<String> containerIdOptional,
                                 Optional<StateStore<TaskState>> taskStateStoreOptional,
                                 SharedResourcesBroker<GobblinScopeTypes> jobBroker) {
    super();
    this.workUnits = workUnits;
    this.jobId = jobId;
    this.jobState = jobState;
    this.taskStateTracker = taskStateTracker;
    this.taskExecutor = taskExecutor;
    this.containerIdOptional = containerIdOptional;
    this.taskStateStoreOptional = taskStateStoreOptional;
    this.log = LoggerFactory.getLogger(GobblinMultiTaskAttempt.class.getName() + "-" +
               containerIdOptional.or("noattempt"));
    this.jobBroker = jobBroker;
  }

  /**
   * Run {@link #workUnits} assigned in this attempt.
   * @throws IOException
   * @throws InterruptedException
   */
  public void run()
      throws IOException, InterruptedException {
    if (!this.workUnits.hasNext()) {
      log.warn("No work units to run in container " + containerIdOptional.or(""));
      this.tasks = new ArrayList<>();
      return;
    }

    CountUpAndDownLatch countDownLatch = new CountUpAndDownLatch(0);
    this.tasks = runWorkUnits(countDownLatch);
    log.info("Waiting for submitted tasks of job {} to complete in container {}...", jobId,
        containerIdOptional.or(""));
    while (countDownLatch.getCount() > 0) {
      log.info(String.format("%d out of %d tasks of job %s are running in container %s", countDownLatch.getCount(),
          countDownLatch.getRegisteredParties(), jobId, containerIdOptional.or("")));
      if (countDownLatch.await(10, TimeUnit.SECONDS)) {
        break;
      }
    }
    log.info("All assigned tasks of job {} have completed in container {}", jobId, containerIdOptional.or(""));
  }

  /**
   * Commit {@link #tasks} by 1. calling {@link Task#commit()} in parallel; 2. executing any additional {@link CommitStep};
   * 3. persist task statestore.
   * @throws IOException
   */
  public void commit()
      throws IOException {
    if (this.tasks == null || this.tasks.isEmpty()) {
      log.warn("No tasks to be committed in container " + containerIdOptional.or(""));
      return;
    }
    Iterator<Callable<Void>> callableIterator =
        Iterators.transform(this.tasks.iterator(), new Function<Task, Callable<Void>>() {
          @Override
          public Callable<Void> apply(final Task task) {
            return new Callable<Void>() {
              @Nullable
              @Override
              public Void call()
                  throws Exception {
                task.commit();
                return null;
              }
            };
          }
        });

    try {
      List<Either<Void, ExecutionException>> executionResults =
          new IteratorExecutor<>(callableIterator, this.getTaskCommitThreadPoolSize(),
              ExecutorsUtils.newDaemonThreadFactory(Optional.of(log), Optional.of("Task-committing-pool-%d")))
              .executeAndGetResults();
      IteratorExecutor.logFailures(executionResults, log, 10);
    } catch (InterruptedException ie) {
      log.error("Committing of tasks interrupted. Aborting.");
      throw new RuntimeException(ie);
    } finally {
      persistTaskStateStore();
      if (this.cleanupCommitSteps != null) {
        for (CommitStep cleanupCommitStep : this.cleanupCommitSteps) {
          log.info("Executing additional commit step.");
          cleanupCommitStep.execute();
        }
      }
    }
  }

  /**
   * A method that shuts down all running tasks managed by this instance.
   * TODO: Call this from the right place.
   */
  public void shutdownTasks()
      throws InterruptedException {
    log.info("Shutting down tasks");
    for (Task task: this.tasks) {
      task.shutdown();
    }

    for (Task task: this.tasks) {
      task.awaitShutdown(1000);
    }
  }

  private void persistTaskStateStore()
      throws IOException {
    if (!this.taskStateStoreOptional.isPresent()) {
      log.info("Task state store does not exist.");
      return;
    }

    StateStore<TaskState> taskStateStore = this.taskStateStoreOptional.get();
    for (Task task : this.tasks) {
      String taskId = task.getTaskId();
      // Delete the task state file for the task if it already exists.
      // This usually happens if the task is retried upon failure.
      if (taskStateStore.exists(jobId, taskId + AbstractJobLauncher.TASK_STATE_STORE_TABLE_SUFFIX)) {
        taskStateStore.delete(jobId, taskId + AbstractJobLauncher.TASK_STATE_STORE_TABLE_SUFFIX);
      }
    }

    boolean hasTaskFailure = false;
    for (Task task : tasks) {
      log.info("Writing task state for task " + task.getTaskId());
      taskStateStore.put(task.getJobId(), task.getTaskId() + AbstractJobLauncher.TASK_STATE_STORE_TABLE_SUFFIX,
          task.getTaskState());

      if (task.getTaskState().getWorkingState() == WorkUnitState.WorkingState.FAILED) {
        hasTaskFailure = true;
      }
    }

    if (hasTaskFailure) {
      for (Task task : tasks) {
        if (task.getTaskState().contains(ConfigurationKeys.TASK_FAILURE_EXCEPTION_KEY)) {
          log.error(String.format("Task %s failed due to exception: %s", task.getTaskId(),
              task.getTaskState().getProp(ConfigurationKeys.TASK_FAILURE_EXCEPTION_KEY)));
        }

        // If there are task failures then the tasks may be reattempted. Save a copy of the task state that is used
        // to filter out successful tasks on subsequent attempts.
        if (task.getTaskState().getWorkingState() == WorkUnitState.WorkingState.SUCCESSFUL ||
            task.getTaskState().getWorkingState() == WorkUnitState.WorkingState.COMMITTED) {
          taskStateStore.put(task.getJobId(), task.getTaskId() + TASK_STATE_STORE_SUCCESS_MARKER_SUFFIX,
              task.getTaskState());
        }
      }

      throw new IOException(
          String.format("Not all tasks running in container %s completed successfully", containerIdOptional.or("")));
    }
  }

  public boolean isSpeculativeExecutionSafe() {
    for (Task task : tasks) {
      if (!task.isSpeculativeExecutionSafe()) {
        log.info("One task is not safe for speculative execution.");
        return false;
      }
    }
    log.info("All tasks are safe for speculative execution.");
    return true;
  }

  private final int getTaskCommitThreadPoolSize() {
    return Integer.parseInt(this.jobState.getProp(ConfigurationKeys.TASK_EXECUTOR_THREADPOOL_SIZE_KEY,
        Integer.toString(ConfigurationKeys.DEFAULT_TASK_EXECUTOR_THREADPOOL_SIZE)));
  }

  public void addCleanupCommitStep(CommitStep commitStep) {
    if (this.cleanupCommitSteps == null) {
      this.cleanupCommitSteps = Lists.newArrayList(commitStep);
    } else {
      this.cleanupCommitSteps.add(commitStep);
    }
  }

  /**
   * Determine if the task executed successfully in a prior attempt by checkitn the task state store for the success
   * marker.
   * @param taskId task id to check
   * @return whether the task was processed successfully in a prior attempt
   */
  private boolean taskSuccessfulInPriorAttempt(String taskId) {
    if (this.taskStateStoreOptional.isPresent()) {
      StateStore<TaskState> taskStateStore = this.taskStateStoreOptional.get();
      // Delete the task state file for the task if it already exists.
      // This usually happens if the task is retried upon failure.
      try {
        if (taskStateStore.exists(jobId, taskId + TASK_STATE_STORE_SUCCESS_MARKER_SUFFIX)) {
          log.info("Skipping task {} that successfully executed in a prior attempt.", taskId);

          // skip tasks that executed successfully in a previous attempt
          return true;
        }
      } catch (IOException e) {
        // if an error while looking up the task state store then treat like it was not processed
        return false;
      }
    }

    return false;
  }

  /**
   * Run a given list of {@link WorkUnit}s of a job.
   *
   * <p>
   *   This method assumes that the given list of {@link WorkUnit}s have already been flattened and
   *   each {@link WorkUnit} contains the task ID in the property {@link ConfigurationKeys#TASK_ID_KEY}.
   * </p>
   *
   * @param countDownLatch a {@link java.util.concurrent.CountDownLatch} waited on for job completion
   * @return a list of {@link Task}s from the {@link WorkUnit}s
   */
  private List<Task> runWorkUnits(CountUpAndDownLatch countDownLatch) {

    List<Task> tasks = Lists.newArrayList();
    while (this.workUnits.hasNext()) {
      WorkUnit workUnit = this.workUnits.next();
      String taskId = workUnit.getProp(ConfigurationKeys.TASK_ID_KEY);

      // skip tasks that executed successsfully in a prior attempt
      if (taskSuccessfulInPriorAttempt(taskId)) {
        continue;
      }

      countDownLatch.countUp();
      SubscopedBrokerBuilder<GobblinScopeTypes, ?> taskBrokerBuilder =
          this.jobBroker.newSubscopedBuilder(new TaskScopeInstance(taskId));
      WorkUnitState workUnitState = new WorkUnitState(workUnit, this.jobState, taskBrokerBuilder);
      workUnitState.setId(taskId);
      workUnitState.setProp(ConfigurationKeys.JOB_ID_KEY, this.jobId);
      workUnitState.setProp(ConfigurationKeys.TASK_ID_KEY, taskId);
      if (this.containerIdOptional.isPresent()) {
        workUnitState.setProp(ConfigurationKeys.TASK_ATTEMPT_ID_KEY, this.containerIdOptional.get());
      }
      // Create a new task from the work unit and submit the task to run
      Task task = createTaskRunnable(workUnitState, countDownLatch);
      this.taskStateTracker.registerNewTask(task);
      tasks.add(task);
      this.taskExecutor.execute(task);
    }

    new EventSubmitter.Builder(JobMetrics.get(this.jobId).getMetricContext(), "gobblin.runtime").build()
        .submit(JobEvent.TASKS_SUBMITTED, "tasksCount", Long.toString(countDownLatch.getRegisteredParties()));

    return tasks;
  }

  private Task createTaskRunnable(WorkUnitState workUnitState, CountDownLatch countDownLatch) {
    Optional<TaskFactory> taskFactoryOpt = TaskUtils.getTaskFactory(workUnitState);
    if (taskFactoryOpt.isPresent()) {
      return new TaskIFaceWrapper(taskFactoryOpt.get().createTask(new TaskContext(workUnitState)), new TaskContext(workUnitState),
          countDownLatch, this.taskStateTracker);
    } else {
      return new Task(new TaskContext(workUnitState), this.taskStateTracker, this.taskExecutor,
          Optional.of(countDownLatch));
    }
  }

  public void runAndOptionallyCommitTaskAttempt(CommitPolicy multiTaskAttemptCommitPolicy)
      throws IOException, InterruptedException {
    run();
    if (multiTaskAttemptCommitPolicy.equals(GobblinMultiTaskAttempt.CommitPolicy.IMMEDIATE)) {
      this.log.info("Will commit tasks directly.");
      commit();
    } else if (!isSpeculativeExecutionSafe()) {
      throw new RuntimeException(
          "Specualtive execution is enabled. However, the task context is not safe for speculative execution.");
    }
  }

  /**
   * FIXME this method is provided for backwards compatibility in the LocalJobLauncher since it does
   * not access the task state store. This should be addressed as all task executions should be
   * updating the task state.
   */
  public static GobblinMultiTaskAttempt runWorkUnits(JobContext jobContext, Iterator<WorkUnit> workUnits,
      TaskStateTracker taskStateTracker, TaskExecutor taskExecutor,
      CommitPolicy multiTaskAttemptCommitPolicy)
      throws IOException, InterruptedException {
    GobblinMultiTaskAttempt multiTaskAttempt =
        new GobblinMultiTaskAttempt(workUnits, jobContext.getJobId(), jobContext.getJobState(), taskStateTracker, taskExecutor,
            Optional.<String>absent(), Optional.<StateStore<TaskState>>absent(), jobContext.getJobBroker());
    multiTaskAttempt.runAndOptionallyCommitTaskAttempt(multiTaskAttemptCommitPolicy);
    return multiTaskAttempt;
  }

  /**
   * Run a given list of {@link WorkUnit}s of a job.
   *
   * <p>
   *   This method creates {@link GobblinMultiTaskAttempt} to actually run the {@link Task}s of the {@link WorkUnit}s, and optionally commit.
   * </p>
   *
   * @param jobId the job ID
   * @param workUnits the given list of {@link WorkUnit}s to submit to run
   * @param taskStateTracker a {@link TaskStateTracker} for task state tracking
   * @param taskExecutor a {@link TaskExecutor} for task execution
   * @param taskStateStore a {@link StateStore} for storing {@link TaskState}s
   * @param multiTaskAttemptCommitPolicy {@link GobblinMultiTaskAttempt.CommitPolicy} for committing {@link GobblinMultiTaskAttempt}
   * @throws IOException if there's something wrong with any IO operations
   * @throws InterruptedException if the task execution gets cancelled
   */
  public static GobblinMultiTaskAttempt runWorkUnits(String jobId, String containerId, JobState jobState,
      List<WorkUnit> workUnits, TaskStateTracker taskStateTracker, TaskExecutor taskExecutor,
      StateStore<TaskState> taskStateStore,
      CommitPolicy multiTaskAttemptCommitPolicy, SharedResourcesBroker<GobblinScopeTypes> jobBroker)
      throws IOException, InterruptedException {
    GobblinMultiTaskAttempt multiTaskAttempt =
        new GobblinMultiTaskAttempt(workUnits.iterator(), jobId, jobState, taskStateTracker, taskExecutor,
            Optional.of(containerId), Optional.of(taskStateStore), jobBroker);

    multiTaskAttempt.runAndOptionallyCommitTaskAttempt(multiTaskAttemptCommitPolicy);
    return multiTaskAttempt;
  }
}
