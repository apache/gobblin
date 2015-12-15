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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.CaseFormat;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.metastore.StateStore;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.GobblinMetricsRegistry;
import gobblin.metrics.MetricContext;
import gobblin.metrics.event.EventNames;
import gobblin.metrics.event.EventSubmitter;
import gobblin.metrics.event.TimingEvent;
import gobblin.runtime.util.JobMetrics;
import gobblin.runtime.util.TimingEventNames;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.ExecutorsUtils;
import gobblin.util.JobLauncherUtils;
import gobblin.util.ParallelRunner;


/**
 * An abstract implementation of {@link JobLauncher} that handles common tasks for launching and running a job.
 *
 * @author ynli
 */
public abstract class AbstractJobLauncher implements JobLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractJobLauncher.class);

  public static final String TASK_STATE_STORE_TABLE_SUFFIX = ".tst";

  public static final String JOB_STATE_FILE_NAME = "job.state";

  public static final String WORK_UNIT_FILE_EXTENSION = ".wu";
  public static final String MULTI_WORK_UNIT_FILE_EXTENSION = ".mwu";

  // Job configuration properties
  protected final Properties jobProps;

  // This contains all job context information
  protected final JobContext jobContext;

  // This (optional) JobLock is used to prevent the next scheduled run
  // of the job from starting if the current run has not finished yet
  protected Optional<JobLock> jobLockOptional = Optional.absent();

  // A conditional variable for which the condition is satisfied if a cancellation is requested
  protected final Object cancellationRequest = new Object();

  // A flag indicating whether a cancellation has been requested or not
  protected volatile boolean cancellationRequested = false;

  // A conditional variable for which the condition is satisfied if the cancellation is executed
  protected final Object cancellationExecution = new Object();

  // A flag indicating whether a cancellation has been executed or not
  protected volatile boolean cancellationExecuted = false;

  // A single-thread executor for executing job cancellation
  protected final ExecutorService cancellationExecutor;

  // An MetricContext to track runtime metrics only if metrics are enabled.
  protected final Optional<MetricContext> runtimeMetricContext;

  // An EventBuilder with basic metadata.
  protected final EventSubmitter eventSubmitter;

  // This is for dispatching events related to job launching and execution to registered subscribers
  protected final EventBus eventBus = new EventBus(AbstractJobLauncher.class.getSimpleName());

  // A list of JobListeners that will be injected into the user provided JobListener
  private final List<JobListener> mandatoryJobListeners = Lists.newArrayList();

  public AbstractJobLauncher(Properties jobProps, Map<String, String> eventMetadata) throws Exception {
    Preconditions.checkArgument(jobProps.containsKey(ConfigurationKeys.JOB_NAME_KEY),
        "A job must have a job name specified by job.name");

    // Make a copy for both the system and job configuration properties
    this.jobProps = new Properties();
    this.jobProps.putAll(jobProps);

    this.jobContext = new JobContext(this.jobProps, LOG);
    this.eventBus.register(this.jobContext);

    this.cancellationExecutor = Executors.newSingleThreadExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(LOG), Optional.of("CancellationExecutor")));

    this.runtimeMetricContext =
        this.jobContext.getJobMetricsOptional().transform(new Function<JobMetrics, MetricContext>() {
          @Override
          public MetricContext apply(JobMetrics input) {
            return input.getMetricContext();
          }
        });

    this.eventSubmitter =
        new EventSubmitter.Builder(this.runtimeMetricContext, "gobblin.runtime").addMetadata(eventMetadata).build();

    JobExecutionEventSubmitter jobExecutionEventSubmitter = new JobExecutionEventSubmitter(this.eventSubmitter);
    this.mandatoryJobListeners.add(new JobExecutionEventSubmitterListener(jobExecutionEventSubmitter));
  }

  /**
   * A default implementation of {@link JobLauncher#cancelJob(JobListener)}.
   *
   * <p>
   *   This implementation relies on two conditional variables: one for the condition that a cancellation
   *   is requested, and the other for the condition that the cancellation is executed. Upon entrance, the
   *   method notifies the cancellation executor started by {@link #startCancellationExecutor()} on the
   *   first conditional variable to indicate that a cancellation has been requested so the executor is
   *   unblocked. Then it waits on the second conditional variable for the cancellation to be executed.
   * </p>
   *
   * <p>
   *   The actual execution of the cancellation is handled by the cancellation executor started by the
   *   method {@link #startCancellationExecutor()} that uses the {@link #executeCancellation()} method
   *   to execute the cancellation.
   * </p>
   *
   * {@inheritDoc JobLauncher#cancelJob(JobListener)}
   */
  @Override
  public void cancelJob(JobListener jobListener) throws JobException {
    synchronized (this.cancellationRequest) {
      if (this.cancellationRequested) {
        // Return immediately if a cancellation has already been requested
        return;
      }

      this.cancellationRequested = true;
      // Notify the cancellation executor that a cancellation has been requested
      this.cancellationRequest.notify();
    }

    synchronized (this.cancellationExecution) {
      try (CloseableJobListener parallelJobListener = getParallelCombinedJobListener(jobListener)) {
        while (!this.cancellationExecuted) {
          // Wait for the cancellation to be executed
          this.cancellationExecution.wait();
        }

        parallelJobListener.onJobCancellation(this.jobContext.getJobState());
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      } catch (IOException e) {
        throw new JobException("Failed to execute all JobListeners", e);
      }
    }
  }

  @Override
  public void launchJob(JobListener jobListener) throws JobException {
    if (this.jobContext.getJobMetricsOptional().isPresent()) {
      this.jobContext.getJobMetricsOptional().get().startMetricReporting(this.jobProps);
    }

    TimingEvent launchJobTimer =
        this.eventSubmitter.getTimingEvent(TimingEventNames.LauncherTimings.FULL_JOB_EXECUTION);
    String jobId = this.jobContext.getJobId();
    JobState jobState = this.jobContext.getJobState();

    try {
      if (!tryLockJob()) {
        this.eventSubmitter.submit(gobblin.metrics.event.EventNames.LOCK_IN_USE);
        throw new JobException(String.format(
            "Previous instance of job %s is still running, skipping this scheduled run", this.jobContext.getJobName()));
      }

      TimingEvent workUnitsCreationTimer =
          this.eventSubmitter.getTimingEvent(TimingEventNames.LauncherTimings.WORK_UNITS_CREATION);
      // Generate work units of the job from the source
      Optional<List<WorkUnit>> workUnits = Optional.fromNullable(this.jobContext.getSource().getWorkunits(jobState));
      workUnitsCreationTimer.stop();

      // The absence means there is something wrong getting the work units
      if (!workUnits.isPresent()) {
        this.eventSubmitter.submit(gobblin.metrics.event.EventNames.WORK_UNITS_MISSING);
        jobState.setState(JobState.RunningState.FAILED);
        throw new JobException("Failed to get work units for job " + jobId);
      }

      // No work unit to run
      if (workUnits.get().isEmpty()) {
        this.eventSubmitter.submit(gobblin.metrics.event.EventNames.WORK_UNITS_EMPTY);
        LOG.warn("No work units have been created for job " + jobId);
        return;
      }

      TimingEvent stagingDataCleanTimer =
          this.eventSubmitter.getTimingEvent(TimingEventNames.RunJobTimings.MR_STAGING_DATA_CLEAN);
      // Cleanup left-over staging data possibly from the previous run. This is particularly
      // important if the current batch of WorkUnits include failed WorkUnits from the previous
      // run which may still have left-over staging data not cleaned up yet.
      cleanLeftoverStagingData(workUnits.get(), jobState);
      stagingDataCleanTimer.stop();

      long startTime = System.currentTimeMillis();
      jobState.setStartTime(startTime);
      jobState.setState(JobState.RunningState.RUNNING);

      LOG.info("Starting job " + jobId);

      TimingEvent workUnitsPreparationTimer =
          this.eventSubmitter.getTimingEvent(TimingEventNames.LauncherTimings.WORK_UNITS_PREPARATION);
      prepareWorkUnits(JobLauncherUtils.flattenWorkUnits(workUnits.get()), jobState);
      workUnitsPreparationTimer.stop();

      // Write job execution info to the job history store before the job starts to run
      this.jobContext.storeJobExecutionInfo();

      TimingEvent jobRunTimer = this.eventSubmitter.getTimingEvent(TimingEventNames.LauncherTimings.JOB_RUN);
      // Start the job and wait for it to finish
      runWorkUnits(workUnits.get());
      jobRunTimer.stop();

      this.eventSubmitter.submit(CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, "JOB_" + jobState.getState()));

      // Check and set final job jobPropsState upon job completion
      if (jobState.getState() == JobState.RunningState.CANCELLED) {
        LOG.info(String.format("Job %s has been cancelled, aborting now", jobId));
        return;
      }

      TimingEvent jobCommitTimer = this.eventSubmitter.getTimingEvent(TimingEventNames.LauncherTimings.JOB_COMMIT);
      this.jobContext.finalizeJobStateBeforeCommit();
      this.jobContext.commit();
      postProcessJobState(jobState);
      jobCommitTimer.stop();
    } catch (Throwable t) {
      jobState.setState(JobState.RunningState.FAILED);
      String errMsg = "Failed to launch and run job " + jobId;
      LOG.error(errMsg + ": " + t, t);
    } finally {
      try {
        long endTime = System.currentTimeMillis();
        jobState.setEndTime(endTime);
        jobState.setDuration(endTime - jobState.getStartTime());

        TimingEvent jobCleanupTimer = this.eventSubmitter.getTimingEvent(TimingEventNames.LauncherTimings.JOB_CLEANUP);
        cleanupStagingData(jobState);
        jobCleanupTimer.stop();

        // Write job execution info to the job history store upon job termination
        this.jobContext.storeJobExecutionInfo();
      } finally {
        launchJobTimer.stop();
        unlockJob();
      }
    }

    for (JobState.DatasetState datasetState : this.jobContext.getDatasetStatesByUrns().values()) {
      // Set the overall job state to FAILED if the job failed to process any dataset
      if (datasetState.getState() == JobState.RunningState.FAILED) {
        jobState.setState(JobState.RunningState.FAILED);
        break;
      }
    }

    try (CloseableJobListener parallelJobListener = getParallelCombinedJobListener(jobListener)) {
      parallelJobListener.onJobCompletion(jobState);
    } catch (IOException ioe) {
      throw new JobException("Failed to execute all JobListeners", ioe);
    }

    // Stop metrics reporting
    if (this.jobContext.getJobMetricsOptional().isPresent()) {
      this.jobContext.getJobMetricsOptional().get().triggerMetricReporting();
      this.jobContext.getJobMetricsOptional().get().stopMetricReporting();
      JobMetrics.remove(jobState);
    }

    if (jobState.getState() == JobState.RunningState.FAILED) {
      throw new JobException(String.format("Job %s failed", jobId));
    }
  }

  /**
   * Subclasses can override this method to do whatever processing on the {@link TaskState}s,
   * e.g., aggregate task-level metrics into job-level metrics.
   *
   * @deprecated Use {@link #postProcessJobState(JobState)
   */
  @Deprecated
  protected void postProcessTaskStates(@SuppressWarnings("unused") List<TaskState> taskStates) {
    // Do nothing
  }

  /**
   * Subclasses can override this method to do whatever processing on the {@link JobState} and its
   * associated {@link TaskState}s, e.g., aggregate task-level metrics into job-level metrics.
   */
  @SuppressWarnings("deprecation")
  protected void postProcessJobState(JobState jobState) {
    postProcessTaskStates(jobState.getTaskStates());
  }

  @Override
  public void close() throws IOException {
    this.cancellationExecutor.shutdownNow();
    try {
      this.jobContext.getSource().shutdown(this.jobContext.getJobState());
    } finally {
      if (GobblinMetrics.isEnabled(this.jobProps)) {
        GobblinMetricsRegistry.getInstance().remove(this.jobContext.getJobId());
      }
    }
  }

  /**
   * Run the given job.
   *
   * <p>
   *   The contract between {@link AbstractJobLauncher#launchJob(JobListener)} and this method is this method
   *   is responsible for for setting {@link JobState.RunningState} properly and upon returning from this method
   *   (either normally or due to exceptions) whatever {@link JobState.RunningState} is set in this method is
   *   used to determine if the job has finished.
   * </p>
   *
   * @param workUnits List of {@link WorkUnit}s of the job
   */
  protected abstract void runWorkUnits(List<WorkUnit> workUnits) throws Exception;

  /**
   * Get a {@link JobLock} to be used for the job.
   *
   * @return {@link JobLock} to be used for the job
   */
  protected abstract JobLock getJobLock() throws IOException;

  /**
   * Execute the job cancellation.
   */
  protected abstract void executeCancellation();

  /**
   * Start the scheduled executor for executing job cancellation.
   *
   * <p>
   *   The executor, upon started, waits on the condition variable indicating a cancellation is requested,
   *   i.e., it waits for a cancellation request to arrive. If a cancellation is requested, the executor
   *   is unblocked and calls {@link #executeCancellation()} to execute the cancellation. Upon completion
   *   of the cancellation execution, the executor notifies the caller that requested the cancellation on
   *   the conditional variable indicating the cancellation has been executed so the caller is unblocked.
   *   Upon successful execution of the cancellation, it sets the job state to
   *   {@link JobState.RunningState#CANCELLED}.
   * </p>
   */
  protected void startCancellationExecutor() {
    this.cancellationExecutor.execute(new Runnable() {
      @Override
      public void run() {
        synchronized (cancellationRequest) {
          try {
            while (!cancellationRequested) {
              // Wait for a cancellation request to arrive
              cancellationRequest.wait();
            }
            LOG.info("Cancellation has been requested for job " + jobContext.getJobId());
            executeCancellation();
            LOG.info("Cancellation has been executed for job " + jobContext.getJobId());
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
        }

        synchronized (cancellationExecution) {
          cancellationExecuted = true;
          jobContext.getJobState().setState(JobState.RunningState.CANCELLED);
          // Notify that the cancellation has been executed
          cancellationExecution.notifyAll();
        }
      }
    });
  }

  /**
   * Prepare the flattened {@link WorkUnit}s for execution by populating the job and task IDs.
   */
  private void prepareWorkUnits(List<WorkUnit> workUnits, JobState jobState) {
    int taskIdSequence = 0;
    for (WorkUnit workUnit : workUnits) {
      workUnit.setProp(ConfigurationKeys.JOB_ID_KEY, this.jobContext.getJobId());
      String taskId = JobLauncherUtils.newTaskId(this.jobContext.getJobId(), taskIdSequence++);
      workUnit.setId(taskId);
      workUnit.setProp(ConfigurationKeys.TASK_ID_KEY, taskId);
      jobState.incrementTaskCount();
      // Pre-add a task state so if the task fails and no task state is written out,
      // there is still task state for the task when job/task states are persisted.
      jobState.addTaskState(new TaskState(new WorkUnitState(workUnit)));
    }
  }

  /**
   * Try acquiring the job lock and return whether the lock is successfully locked.
   */
  private boolean tryLockJob() {
    try {
      if (this.jobContext.isJobLockEnabled()) {
        this.jobLockOptional = Optional.of(getJobLock());
      }
      return !this.jobLockOptional.isPresent() || this.jobLockOptional.get().tryLock();
    } catch (IOException ioe) {
      LOG.error(String.format("Failed to acquire job lock for job %s: %s", this.jobContext.getJobId(), ioe), ioe);
      return false;
    }
  }

  /**
   * Unlock a completed or failed job.
   */
  private void unlockJob() {
    if (this.jobLockOptional.isPresent()) {
      try {
        // Unlock so the next run of the same job can proceed
        this.jobLockOptional.get().unlock();
      } catch (IOException ioe) {
        LOG.error(String.format("Failed to unlock for job %s: %s", this.jobContext.getJobId(), ioe), ioe);
      }
    }
  }

  /**
   * Cleanup the left-over staging data possibly from the previous run of the job that may have failed
   * and not cleaned up its staging data.
   */
  private void cleanLeftoverStagingData(List<WorkUnit> workUnits, JobState jobState) {
    try {
      if (this.jobContext.shouldCleanupStagingDataPerTask()) {
        Closer closer = Closer.create();
        Map<String, ParallelRunner> parallelRunners = Maps.newHashMap();
        try {
          for (WorkUnit workUnit : JobLauncherUtils.flattenWorkUnits(workUnits)) {
            WorkUnit fatWorkUnit = WorkUnit.copyOf(workUnit);
            fatWorkUnit.addAllIfNotExist(jobState);
            JobLauncherUtils.cleanTaskStagingData(fatWorkUnit, LOG, closer, parallelRunners);
          }
        } catch (Throwable t) {
          throw closer.rethrow(t);
        } finally {
          closer.close();
        }
      } else {
        JobLauncherUtils.cleanJobStagingData(jobState, LOG);
      }
    } catch (Throwable t) {
      // Catch Throwable instead of just IOException to make sure failure of this won't affect the current run
      LOG.error("Failed to clean leftover staging data", t);
    }
  }

  /**
   * Cleanup the job's task staging data. This is not doing anything in case job succeeds
   * and data is successfully committed because the staging data has already been moved
   * to the job output directory. But in case the job fails and data is not committed,
   * we want the staging data to be cleaned up.
   *
   * Property {@link ConfigurationKeys#CLEANUP_STAGING_DATA_PER_TASK} controls whether to cleanup
   * staging data per task, or to cleanup entire job's staging data at once.
   */
  private void cleanupStagingData(JobState jobState) {
    if (this.jobContext.shouldCleanupStagingDataPerTask()) {
      cleanupStagingDataPerTask(jobState);
    } else {
      cleanupStagingDataForEntireJob(jobState);
    }
  }

  /**
   * Combines the specified {@link JobListener} with the {@link #mandatoryJobListeners} for this job. Uses
   * {@link JobListeners#parallelJobListener(List)} to create a {@link CloseableJobListener} that will execute all
   * the {@link JobListener}s in parallel.
   */
  private CloseableJobListener getParallelCombinedJobListener(JobListener jobListener) {
    List<JobListener> jobListeners = Lists.newArrayList(this.mandatoryJobListeners);
    jobListeners.add(jobListener);
    return JobListeners.parallelJobListener(jobListeners);
  }

  /**
   * Run a given list of {@link WorkUnit}s of a job.
   *
   * <p>
   *   This method calls {@link #runWorkUnits(String, List, TaskStateTracker, TaskExecutor, CountDownLatch)}
   *   to actually run the {@link Task}s of the {@link WorkUnit}s.
   * </p>
   *
   * @param jobId the job ID
   * @param workUnits the given list of {@link WorkUnit}s to submit to run
   * @param taskStateTracker a {@link TaskStateTracker} for task state tracking
   * @param taskExecutor a {@link TaskExecutor} for task execution
   * @param taskStateStore a {@link StateStore} for storing {@link TaskState}s
   * @param logger a {@link Logger} for logging
   * @throws IOException if there's something wrong with any IO operations
   * @throws InterruptedException if the task execution gets cancelled
   */
  public static void runWorkUnits(String jobId, String containerId, List<WorkUnit> workUnits,
      TaskStateTracker taskStateTracker,  TaskExecutor taskExecutor, StateStore<TaskState> taskStateStore,
      Logger logger) throws IOException, InterruptedException {

    if (workUnits.isEmpty()) {
      logger.warn("No work units to run in container " + containerId);
      return;
    }

    for (WorkUnit workUnit : workUnits) {
      String taskId = workUnit.getProp(ConfigurationKeys.TASK_ID_KEY);
      // Delete the task state file for the task if it already exists.
      // This usually happens if the task is retried upon failure.
      if (taskStateStore.exists(jobId, taskId + AbstractJobLauncher.TASK_STATE_STORE_TABLE_SUFFIX)) {
        taskStateStore.delete(jobId, taskId + AbstractJobLauncher.TASK_STATE_STORE_TABLE_SUFFIX);
      }
    }

    CountDownLatch countDownLatch = new CountDownLatch(workUnits.size());
    List<Task> tasks = runWorkUnits(jobId, workUnits, taskStateTracker, taskExecutor, countDownLatch);

    logger.info(String.format("Waiting for submitted tasks of job %s to complete in container %s...",
        jobId, containerId));
    while (countDownLatch.getCount() > 0) {
      logger.info(String.format("%d out of %d tasks of job %s are running in container %s",
          countDownLatch.getCount(), workUnits.size(), jobId, containerId));
      if (countDownLatch.await(10, TimeUnit.SECONDS)) {
        break;
      }
    }
    logger.info(String.format("All assigned tasks of job %s have completed in container %s", jobId, containerId));

    boolean hasTaskFailure = false;
    for (Task task : tasks) {
      logger.info("Writing task state for task " + task.getTaskId());
      taskStateStore.put(task.getJobId(), task.getTaskId() + AbstractJobLauncher.TASK_STATE_STORE_TABLE_SUFFIX,
          task.getTaskState());

      if (task.getTaskState().getWorkingState() == WorkUnitState.WorkingState.FAILED) {
        hasTaskFailure = true;
      }
    }

    if (hasTaskFailure) {
      for (Task task : tasks) {
        if (task.getTaskState().contains(ConfigurationKeys.TASK_FAILURE_EXCEPTION_KEY)) {
          logger.error(String.format("Task %s failed due to exception: %s", task.getTaskId(),
              task.getTaskState().getProp(ConfigurationKeys.TASK_FAILURE_EXCEPTION_KEY)));
        }
      }

      throw new IOException(String.format("Not all tasks running in container %s completed successfully", containerId));
    }
  }

  /**
   * Run a given list of {@link WorkUnit}s of a job.
   *
   * <p>
   *   This method assumes that the given list of {@link WorkUnit}s have already been flattened and
   *   each {@link WorkUnit} contains the task ID in the property {@link ConfigurationKeys#TASK_ID_KEY}.
   * </p>
   *
   * @param jobId the job ID
   * @param workUnits the given list of {@link WorkUnit}s to submit to run
   * @param stateTracker a {@link TaskStateTracker} for task state tracking
   * @param taskExecutor a {@link TaskExecutor} for task execution
   * @param countDownLatch a {@link java.util.concurrent.CountDownLatch} waited on for job completion
   * @return a list of {@link Task}s from the {@link WorkUnit}s
   */
  public static List<Task> runWorkUnits(String jobId, List<WorkUnit> workUnits, TaskStateTracker stateTracker,
      TaskExecutor taskExecutor, CountDownLatch countDownLatch) {

    List<Task> tasks = Lists.newArrayList();
    for (WorkUnit workUnit : workUnits) {
      String taskId = workUnit.getProp(ConfigurationKeys.TASK_ID_KEY);
      WorkUnitState workUnitState = new WorkUnitState(workUnit);
      workUnitState.setId(taskId);
      workUnitState.setProp(ConfigurationKeys.JOB_ID_KEY, jobId);
      workUnitState.setProp(ConfigurationKeys.TASK_ID_KEY, taskId);

      // Create a new task from the work unit and submit the task to run
      Task task = new Task(new TaskContext(workUnitState), stateTracker, taskExecutor, Optional.of(countDownLatch));
      stateTracker.registerNewTask(task);
      tasks.add(task);
      taskExecutor.execute(task);
    }

    new EventSubmitter.Builder(JobMetrics.get(jobId).getMetricContext(), "gobblin.runtime").build()
        .submit(EventNames.TASKS_SUBMITTED, "tasksCount", Integer.toString(workUnits.size()));

    return tasks;
  }

  private static void cleanupStagingDataPerTask(JobState jobState) {
    Closer closer = Closer.create();
    Map<String, ParallelRunner> parallelRunners = Maps.newHashMap();
    try {
      for (TaskState taskState : jobState.getTaskStates()) {
        TaskState fatTaskState = new TaskState(taskState);
        fatTaskState.addAllIfNotExist(jobState);
        try {
          JobLauncherUtils.cleanTaskStagingData(fatTaskState, LOG, closer, parallelRunners);
        } catch (IOException e) {
          LOG.error(String.format("Failed to clean staging data for task %s: %s", taskState.getTaskId(), e), e);
        }
      }
    } finally {
      try {
        closer.close();
      } catch (IOException e) {
        LOG.error("Failed to clean staging data", e);
      }
    }
  }

  private static void cleanupStagingDataForEntireJob(JobState jobState) {
    try {
      JobLauncherUtils.cleanJobStagingData(jobState, LOG);
    } catch (IOException e) {
      LOG.error("Failed to clean staging data for job " + jobState.getJobId(), e);
    }
  }
}
