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
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.gobblin_scopes.TaskScopeInstance;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.broker.iface.SubscopedBrokerBuilder;
import org.apache.gobblin.commit.CommitStep;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.JobEvent;
import org.apache.gobblin.runtime.api.TaskEventMetadataGenerator;
import org.apache.gobblin.runtime.task.TaskFactory;
import org.apache.gobblin.runtime.task.TaskIFaceWrapper;
import org.apache.gobblin.runtime.task.TaskUtils;
import org.apache.gobblin.runtime.troubleshooter.InMemoryIssueRepository;
import org.apache.gobblin.runtime.troubleshooter.IssueRepository;
import org.apache.gobblin.runtime.troubleshooter.TroubleshooterException;
import org.apache.gobblin.runtime.util.JobMetrics;
import org.apache.gobblin.runtime.util.TaskMetrics;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.Either;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.TaskEventMetadataUtils;
import org.apache.gobblin.util.executors.IteratorExecutor;
import org.apache.gobblin.util.retry.RetryerFactory;

import static org.apache.gobblin.util.retry.RetryerFactory.RETRY_INTERVAL_MS;
import static org.apache.gobblin.util.retry.RetryerFactory.RETRY_TIME_OUT_MS;


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
  private final String attemptId;
  private final JobState jobState;
  private final TaskStateTracker taskStateTracker;
  private final TaskExecutor taskExecutor;
  private final Optional<String> containerIdOptional;
  private final Optional<StateStore<TaskState>> taskStateStoreOptional;
  private final SharedResourcesBroker<GobblinScopeTypes> jobBroker;
  private final TaskEventMetadataGenerator taskEventMetadataGenerator;
  @Setter
  private Predicate<GobblinMultiTaskAttempt> interruptionPredicate = (gmta) -> false;
  private List<Task> tasks;
  @Getter
  private volatile AtomicBoolean stopped = new AtomicBoolean(false);

  private final IssueRepository issueRepository;

  /**
   * Additional commit steps that may be added by different launcher, and can be environment specific.
   * Usually it should be clean-up steps, which are always executed at the end of {@link #commit()}.
   */
  private List<CommitStep> cleanupCommitSteps;

  public GobblinMultiTaskAttempt(Iterator<WorkUnit> workUnits, String jobId, JobState jobState,
      TaskStateTracker taskStateTracker, TaskExecutor taskExecutor, Optional<String> containerIdOptional,
      Optional<StateStore<TaskState>> taskStateStoreOptional, SharedResourcesBroker<GobblinScopeTypes> jobBroker) {

    this(workUnits, jobId, jobState, taskStateTracker, taskExecutor, containerIdOptional, taskStateStoreOptional,
         jobBroker, new InMemoryIssueRepository());
  }

  public GobblinMultiTaskAttempt(Iterator<WorkUnit> workUnits, String jobId, JobState jobState,
      TaskStateTracker taskStateTracker, TaskExecutor taskExecutor, Optional<String> containerIdOptional,
      Optional<StateStore<TaskState>> taskStateStoreOptional, SharedResourcesBroker<GobblinScopeTypes> jobBroker,
      IssueRepository issueRepository) {
    super();
    this.workUnits = workUnits;
    this.jobId = jobId;
    this.issueRepository = issueRepository;
    this.attemptId = this.getClass().getName() + "." + this.jobId;
    this.jobState = jobState;
    this.taskStateTracker = taskStateTracker;
    this.taskExecutor = taskExecutor;
    this.containerIdOptional = containerIdOptional;
    this.taskStateStoreOptional = taskStateStoreOptional;
    this.log =
        LoggerFactory.getLogger(GobblinMultiTaskAttempt.class.getName() + "-" + containerIdOptional.or("noattempt"));
    this.jobBroker = jobBroker;
    this.tasks = new ArrayList<>();
    this.taskEventMetadataGenerator = TaskEventMetadataUtils.getTaskEventMetadataGenerator(jobState);
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
      return;
    }

    CountUpAndDownLatch countDownLatch = new CountUpAndDownLatch(0);
    Pair<List<Task>, Boolean> executionResult = runWorkUnits(countDownLatch);
    this.tasks = executionResult.getFirst();

    // The task attempt has already been stopped and the task list is empty. This indicates that a cancel has been
    // invoked prior to creation of underlying Gobblin tasks. In a normal scenario, where a cancel is invoked after
    // successful task creation, the task list is guaranteed to be non-empty and we shouldn't enter the following block.
    if (this.tasks.isEmpty() && this.stopped.get()) {
      return;
    }

    // Indicating task submission failure, propagating exception as it should be noticeable to job launcher.
    // Submission failure could be task-creation failure, or state-tracker failed to be scheduled so that the actual
    // task isn't submitted into the executor.
    if (!executionResult.getSecond()) {
      throw new TaskCreationException("Failing in submitting at least one task before execution.");
    }

    log.info("Waiting for submitted tasks of job {} to complete in container {}...", jobId, containerIdOptional.or(""));
    try {
      while (countDownLatch.getCount() > 0) {
        if (this.interruptionPredicate.test(this)) {
          log.info("Interrupting task execution due to satisfied predicate.");
          interruptTaskExecution(countDownLatch);
          break;
        }
        long totalTasks = countDownLatch.totalParties.get();
        long runningTasks = countDownLatch.getCount();
        log.info(String.format("%d out of %d tasks of job %s are running in container %s. %d tasks finished.",
            runningTasks, totalTasks, jobId, containerIdOptional.or(""), totalTasks - runningTasks));
        if (countDownLatch.await(10, TimeUnit.SECONDS)) {
          break;
        }
      }
    } catch (InterruptedException interrupt) {
      log.info("Job interrupted by InterruptedException.");
      interruptTaskExecution(countDownLatch);
    }
    log.info("All assigned tasks of job {} have completed in container {}", jobId, containerIdOptional.or(""));
  }

  private void interruptTaskExecution(CountDownLatch countDownLatch)
      throws InterruptedException {
    log.info("Job interrupted. Attempting a graceful shutdown of the job.");
    this.tasks.forEach(Task::shutdown);
    if (!countDownLatch.await(5, TimeUnit.SECONDS)) {
      log.warn("Graceful shutdown of job timed out. Killing all outstanding tasks.");
      try {
        this.taskExecutor.shutDown();
      } catch (Throwable t) {
        throw new RuntimeException("Failed to shutdown task executor.", t);
      }
    }
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

      reportTaskIssues();
      persistTaskStateStore();

      if (this.cleanupCommitSteps != null) {
        for (CommitStep cleanupCommitStep : this.cleanupCommitSteps) {
          log.info("Executing additional commit step.");
          cleanupCommitStep.execute();
        }
      }
    }
  }

  private void reportTaskIssues() {
    if (issueRepository == null) {
      log.info("Automatic troubleshooting is not configured for this task. "
                   + "Make sure to pass issue repository to turn it on.");
      return;
    }

    try {
      for (Task task : this.tasks) {
        task.getTaskState().setTaskIssues(issueRepository.getAll());
      }
    } catch (TroubleshooterException e) {
      log.warn("Failed to save task issues", e);
    }
  }

  /**
   * A method that shuts down all running tasks managed by this instance.
   * TODO: Call this from the right place.
   */
  public synchronized void shutdownTasks()
      throws InterruptedException {
    log.info("Shutting down tasks");
    for (Task task : this.tasks) {
      task.shutdown();
    }

    for (Task task : this.tasks) {
      task.awaitShutdown(1000);
    }

    for (Task task : this.tasks) {
      if (task.cancel()) {
        log.info("Task {} cancelled.", task.getTaskId());
      } else {
        log.info("Task {} could not be cancelled.", task.getTaskId());
      }
    }
    this.stopped.set(true);
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
      String errorMsg = String.format("Tasks in container %s failed", containerIdOptional.or(""));
      for (Task task : tasks) {
        if (task.getTaskState().contains(ConfigurationKeys.TASK_FAILURE_EXCEPTION_KEY)) {
          errorMsg = String.format("Task failed: %s (Gobblin task id %s, container id %s)",
                                   task.getTaskState().getProp(ConfigurationKeys.TASK_FAILURE_EXCEPTION_KEY),
                                   task.getTaskId(), containerIdOptional.or(""));
        }

        // If there are task failures then the tasks may be reattempted. Save a copy of the task state that is used
        // to filter out successful tasks on subsequent attempts.
        if (task.getTaskState().getWorkingState() == WorkUnitState.WorkingState.SUCCESSFUL
            || task.getTaskState().getWorkingState() == WorkUnitState.WorkingState.COMMITTED) {
          taskStateStore
              .put(task.getJobId(), task.getTaskId() + TASK_STATE_STORE_SUCCESS_MARKER_SUFFIX, task.getTaskState());
        }
      }

      throw new IOException(errorMsg);
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
   * Determine if the task executed successfully in a prior attempt by checking the task state store for the success
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
   * @return a list of {@link Task}s from the {@link WorkUnit}s, as well as if there's a failure in task creation
   * which should be handled separately to avoid silently starving on certain workunit.
   */
  private synchronized Pair<List<Task>, Boolean> runWorkUnits(CountUpAndDownLatch countDownLatch) {
    List<Task> tasks = Lists.newArrayList();
    //Has the task-attempt already been cancelled? This can happen for instance when a cancellation has been invoked on
    // the GobblinMultiTaskAttempt instance (e.g. in the case of Helix task cancellation) before the Gobblin tasks
    // have been submitted to the underlying task executor.
    if (this.stopped.get()) {
      return new Pair<>(tasks, false);
    }
    // A flag indicating if there are any tasks not submitted successfully.
    // Caller of this method should handle tasks with submission failures accordingly.
    boolean areAllTasksSubmitted = true;
    while (this.workUnits.hasNext()) {
      WorkUnit workUnit = this.workUnits.next();
      String taskId = workUnit.getProp(ConfigurationKeys.TASK_ID_KEY);

      // skip tasks that executed successfully in a prior attempt
      if (taskSuccessfulInPriorAttempt(taskId)) {
        continue;
      }

      SubscopedBrokerBuilder<GobblinScopeTypes, ?> taskBrokerBuilder =
          this.jobBroker.newSubscopedBuilder(new TaskScopeInstance(taskId));
      WorkUnitState workUnitState = new WorkUnitState(workUnit, this.jobState, taskBrokerBuilder);
      workUnitState.setId(taskId);
      workUnitState.setProp(ConfigurationKeys.JOB_ID_KEY, this.jobId);
      workUnitState.setProp(ConfigurationKeys.TASK_ID_KEY, taskId);
      workUnitState.setProp(ConfigurationKeys.TASK_START_TIME_MILLIS_KEY, Long.toString(System.currentTimeMillis()));

      if (this.containerIdOptional.isPresent()) {
        workUnitState.setProp(ConfigurationKeys.TASK_ATTEMPT_ID_KEY, this.containerIdOptional.get());
      }

      // Create a new task from the work unit and submit the task to run.
      // If an exception occurs here then the count down latch is decremented
      // to avoid being stuck waiting for a task that was not created and submitted successfully.
      Task task = null;
      try {
        countDownLatch.countUp();
        task = createTaskWithRetry(workUnitState, countDownLatch);
        this.taskStateTracker.registerNewTask(task);
        task.setTaskFuture(this.taskExecutor.submit(task));
        tasks.add(task);
      } catch (Throwable e) {
        if (e instanceof OutOfMemoryError) {
          log.error("Encountering memory error in task creation/execution stage, please investigate memory usage:", e);
          printMemoryUsage();
        }

        if (task == null) {
          if (e instanceof RetryException) {
            // Indicating task being null due to failure in creation even after retrying.
            areAllTasksSubmitted = false;
          }
          // task could not be created, so directly count down
          countDownLatch.countDown();
          log.error("Could not create task for workunit {}", workUnit, e);
        } else if (!task.hasTaskFuture()) {
          // Task was created and may have been registered, but not submitted, so call the
          // task state tracker task run completion directly since the task cancel does nothing if not submitted
          this.taskStateTracker.onTaskRunCompletion(task);
          areAllTasksSubmitted = false;
          log.error("Could not submit task for workunit {}", workUnit, e);
        } else {
          // task was created and submitted, but failed later, so cancel the task to decrement the CountDownLatch
          task.cancel();
          log.error("Failure after task submitted for workunit {}", workUnit, e);
        }
      }
    }

    EventSubmitter.Builder eventSubmitterBuilder = new EventSubmitter.Builder(JobMetrics.get(this.jobId, new JobMetrics.CreatorTag(this.attemptId)).getMetricContext(),
        "gobblin.runtime");
    eventSubmitterBuilder.addMetadata(this.taskEventMetadataGenerator.getMetadata(jobState, JobEvent.TASKS_SUBMITTED));
    eventSubmitterBuilder.build().submit(JobEvent.TASKS_SUBMITTED, "tasksCount", Integer.toString(tasks.size()));

    return new Pair<>(tasks, areAllTasksSubmitted);
  }

  private void printMemoryUsage() {
    MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage heapMemory = memoryBean.getHeapMemoryUsage();
    MemoryUsage nonHeapMemory = memoryBean.getNonHeapMemoryUsage();
    String format = "%-15s%-15s%-15s%-15s";

    this.log.info("Heap Memory");
    this.log.info(String.format(format, "init", "used", "Committed", "max"));
    this.log.info(String
        .format(format, heapMemory.getInit(), heapMemory.getUsed(), heapMemory.getCommitted(), heapMemory.getMax()));

    this.log.info("Non-heap Memory");
    this.log.info(String.format(format, "init", "used", "Committed", "max"));
    this.log.info(String.format(format, nonHeapMemory.getInit(), nonHeapMemory.getUsed(), nonHeapMemory.getCommitted(),
        nonHeapMemory.getMax()));
  }

  private Task createTaskRunnable(WorkUnitState workUnitState, CountDownLatch countDownLatch) {
    Optional<TaskFactory> taskFactoryOpt = TaskUtils.getTaskFactory(workUnitState);
    final TaskContext taskContext = new TaskContext(workUnitState);
    if (taskFactoryOpt.isPresent()) {
      return new TaskIFaceWrapper(taskFactoryOpt.get().createTask(taskContext), taskContext, countDownLatch,
          this.taskStateTracker);
    } else {
      return new Task(taskContext, this.taskStateTracker, this.taskExecutor, Optional.of(countDownLatch));
    }
  }

  /**
   * As the initialization of {@link Task} could have unstable external connection which could be healed through
   * retry, adding retry-wrapper here for the sake of fault-tolerance.
   */
  @VisibleForTesting
  Task createTaskWithRetry(WorkUnitState workUnitState, CountDownLatch countDownLatch) throws RetryException {
    Properties defaultRetryConfig = new Properties();
    defaultRetryConfig.setProperty(RETRY_TIME_OUT_MS, TimeUnit.MINUTES.toMillis(1L) + "");
    defaultRetryConfig.setProperty(RETRY_INTERVAL_MS, TimeUnit.SECONDS.toMillis(2L) + "");
    Config config = ConfigUtils.propertiesToConfig(this.jobState.getProperties())
        .withFallback(ConfigUtils.propertiesToConfig(defaultRetryConfig));
    Retryer<Task> retryer = RetryerFactory.newInstance(config);
    // An "effectively final" variable for counting how many retried has been done, mostly for logging purpose.
    final AtomicInteger counter = new AtomicInteger(0);

    try {
      return retryer.call(new Callable<Task>() {
        @Override
        public Task call()
            throws Exception {
          counter.incrementAndGet();
          log.info(String.format("Task creation attempt %s", counter.get()));
          return createTaskRunnable(workUnitState, countDownLatch);
        }
      });
    } catch (ExecutionException ee) {
      throw new RuntimeException("Failure in executing retryer due to, ", ee);
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
          "Speculative execution is enabled. However, the task context is not safe for speculative execution.");
    }
  }

  /**
   * <p> During the task execution, the fork/task instances will create metric contexts (fork, task, job, container)
   * along the hierarchy up to the root metric context. Although root metric context has a weak reference to
   * those metric contexts, they are meanwhile cached by GobblinMetricsRegistry. Here we will remove all those
   * strong reference from the cache to make sure it can be reclaimed by Java GC when JVM has run out of memory.
   *
   * <p> Task metrics are cleaned by iterating all tasks. Job level metrics cleaning needs some caveat. The
   * cleaning will only succeed if the creator of this job level metrics initiates the removal. This means if a task
   * tries to remove the {@link JobMetrics} which is created by others, the removal won't take effect. This is handled by
   * {@link JobMetrics#attemptRemove(String, Tag)}.
   */
  public void cleanMetrics() {
    tasks.forEach(task -> {
      TaskMetrics.remove(task);
      JobMetrics.attemptRemove(this.jobId, new JobMetrics.CreatorTag(task.getTaskId()));
    });

    JobMetrics.attemptRemove(this.jobId, new JobMetrics.CreatorTag(this.attemptId));
  }

  /**
   * FIXME this method is provided for backwards compatibility in the LocalJobLauncher since it does
   * not access the task state store. This should be addressed as all task executions should be
   * updating the task state.
   */
  public static GobblinMultiTaskAttempt runWorkUnits(JobContext jobContext, Iterator<WorkUnit> workUnits,
      TaskStateTracker taskStateTracker, TaskExecutor taskExecutor, CommitPolicy multiTaskAttemptCommitPolicy)
      throws IOException, InterruptedException {
    GobblinMultiTaskAttempt multiTaskAttempt =
        new GobblinMultiTaskAttempt(workUnits, jobContext.getJobId(), jobContext.getJobState(), taskStateTracker,
            taskExecutor, Optional.<String>absent(), Optional.<StateStore<TaskState>>absent(),
            jobContext.getJobBroker(), jobContext.getIssueRepository());
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
      StateStore<TaskState> taskStateStore, CommitPolicy multiTaskAttemptCommitPolicy,
      SharedResourcesBroker<GobblinScopeTypes> jobBroker, IssueRepository issueRepository,
      Predicate<GobblinMultiTaskAttempt> interruptionPredicate)
      throws IOException, InterruptedException {

    // dump the work unit if tracking logs are enabled
    if (jobState.getPropAsBoolean(ConfigurationKeys.WORK_UNIT_ENABLE_TRACKING_LOGS)) {
      Logger log = LoggerFactory.getLogger(GobblinMultiTaskAttempt.class.getName());
      log.info("Work unit tracking log: {}", workUnits);
    }

    GobblinMultiTaskAttempt multiTaskAttempt =
        new GobblinMultiTaskAttempt(workUnits.iterator(), jobId, jobState, taskStateTracker, taskExecutor,
            Optional.of(containerId), Optional.of(taskStateStore), jobBroker, issueRepository);
    multiTaskAttempt.setInterruptionPredicate(interruptionPredicate);

    multiTaskAttempt.runAndOptionallyCommitTaskAttempt(multiTaskAttemptCommitPolicy);
    return multiTaskAttempt;
  }

  public int getNumTasksCreated() {
    return this.tasks.size();
  }
}
