/* (c) 2014 LinkedIn Corp. All rights reserved.
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
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.GobblinMetricsRegistry;
import gobblin.publisher.DataPublisher;
import gobblin.source.extractor.JobCommitPolicy;
import gobblin.source.workunit.MultiWorkUnit;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.ExecutorsUtils;
import gobblin.util.JobLauncherUtils;


/**
 * An abstract implementation of {@link JobLauncher} that handles common tasks for launching and running a job.
 *
 * @author ynli
 */
public abstract class AbstractJobLauncher implements JobLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractJobLauncher.class);

  protected static final String TASK_STATE_STORE_TABLE_SUFFIX = ".tst";
  protected static final String JOB_STATE_STORE_TABLE_SUFFIX = ".jst";

  // System configuration properties
  protected final Properties sysProps;

  // Job configuration properties
  protected final Properties jobProps;

  // This contains all job context information
  protected final JobContext jobContext;

  // This (optional) JobLock is used to prevent the next scheduled run
  // of the job from starting if the current run has not finished yet
  protected Optional<JobLock> jobLockOptional = Optional.absent();

  // A flag that tells if cancellation is requested for the job
  protected volatile boolean cancellationRequested = false;

  // A flag that tells if the cancellation has been executed for the job
  protected volatile boolean cancellationExecuted = false;

  // A lock to be waited on for cancellation execution
  protected final Object cancellationLock = new Object();

  // A single-thread executor for executing job cancellation
  protected final ScheduledThreadPoolExecutor cancellationExecutor;

  public AbstractJobLauncher(Properties sysProps, Properties jobProps)
      throws Exception {
    Preconditions.checkArgument(
        jobProps.containsKey(ConfigurationKeys.JOB_NAME_KEY), "A job must have a job name specified by job.name");

    // Make a copy for both the system and job configuration properties
    this.sysProps = new Properties();
    this.sysProps.putAll(sysProps);
    this.jobProps = new Properties();
    this.jobProps.putAll(jobProps);

    this.jobContext = new JobContext(this.sysProps, this.jobProps, LOG);

    this.cancellationExecutor = new ScheduledThreadPoolExecutor(1, ExecutorsUtils.newThreadFactory(
        Optional.of(LOG), Optional.of("CancellationExecutor")));
    this.cancellationExecutor.setMaximumPoolSize(1);
  }

  /**
   * Submit a given list of {@link WorkUnit}s of a job to run.
   *
   * <p>
   *   This method assumes that the given list of {@link WorkUnit}s have already been flattened and
   *   each {@link WorkUnit} contains the task ID in the property {@link ConfigurationKeys#TASK_ID_KEY}.
   * </p>
   *
   * @param jobId job ID
   * @param workUnits given list of {@link WorkUnit}s to submit to run
   * @param stateTracker a {@link TaskStateTracker} for task state tracking
   * @param taskExecutor a {@link TaskExecutor} for task execution
   * @param countDownLatch a {@link java.util.concurrent.CountDownLatch} waited on for job completion
   * @return a list of {@link Task}s from the {@link WorkUnit}s
   * @throws InterruptedException
   */
  public static List<Task> submitWorkUnits(String jobId, List<WorkUnit> workUnits, TaskStateTracker stateTracker,
      TaskExecutor taskExecutor, CountDownLatch countDownLatch)
      throws InterruptedException {

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
      LOG.info(String.format("Submitting task %s to run", taskId));
      taskExecutor.submit(task);
    }

    return tasks;
  }

  /**
   * This implementation simply sets a flag that indicate cancellation is requested and waits
   * for the cancellation to be executed. It does not guarantee successful job cancellation.
   *
   * {@inheritDoc JobLauncher#cancelJob(JobListener)}
   */
  @Override
  public void cancelJob(JobListener jobListener)
      throws JobException {
    synchronized (this.cancellationExecutor) {
      if (this.cancellationRequested) {
        LOG.info(String.format("Job %s has already been cancelled or cancellation has already been requested",
            this.jobContext.getJobId()));
        return;
      }

      // Request cancellation
      this.cancellationRequested = true;

      // Wait for the cancellation to be executed
      synchronized (this.cancellationLock) {
        try {
          this.cancellationLock.wait(TimeUnit.SECONDS.toMillis(60));
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }

    if (jobListener != null) {
      jobListener.onJobCancellation(this.jobContext.getJobState());
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void launchJob(JobListener jobListener)
      throws JobException {
    if (!tryLockJob()) {
      throw new JobException(
          String.format("Previous instance of job %s is still running, skipping this scheduled run",
              this.jobContext.getJobName()));
    }

    String jobId = this.jobContext.getJobId();
    JobState jobState = this.jobContext.getJobState();

    // Generate work units of the job from the source
    Optional<List<WorkUnit>> workUnits = Optional.fromNullable(this.jobContext.getSource().getWorkunits(jobState));
    // The absence means there is something wrong getting the work units
    if (!workUnits.isPresent()) {
      unlockJob();
      throw new JobException("Failed to get work units for job " + jobId);
    }

    // No work unit to run
    if (workUnits.get().isEmpty()) {
      LOG.warn("No work units have been created for job " + jobId);
      unlockJob();
      return;
    }

    long startTime = System.currentTimeMillis();
    jobState.setStartTime(startTime);
    jobState.setState(JobState.RunningState.RUNNING);

    LOG.info("Starting job " + jobId);

    // Add work units and assign task IDs to them
    int taskIdSequence = 0;
    int multiTaskIdSequence = 0;
    for (WorkUnit workUnit : workUnits.get()) {
      if (workUnit instanceof MultiWorkUnit) {
        String multiTaskId = JobLauncherUtils.newMultiTaskId(jobId, multiTaskIdSequence++);
        workUnit.setProp(ConfigurationKeys.TASK_ID_KEY, multiTaskId);
        workUnit.setId(multiTaskId);
        for (WorkUnit innerWorkUnit : ((MultiWorkUnit) workUnit).getWorkUnits()) {
          addWorkUnit(innerWorkUnit, taskIdSequence++, jobState);
        }
      } else {
        addWorkUnit(workUnit, taskIdSequence++, jobState);
      }
    }

    try {
      if (this.jobContext.getJobMetricsOptional().isPresent()) {
        this.jobContext.getJobMetricsOptional().get().startMetricReporting(this.jobProps);
      }

      // Write job execution info to the job history store before the job starts to run
      storeJobExecutionInfo();

      // Start the job and wait for it to finish
      runWorkUnits(workUnits.get());

      // Check and set final job jobPropsState upon job completion
      if (jobState.getState() == JobState.RunningState.CANCELLED) {
        LOG.info(String.format("Job %s has been cancelled", jobId));
        return;
      }

      setFinalJobState(jobState);
      // Commit and publish job data
      commitJob(jobState);
    } catch (Throwable t) {
      jobState.setState(JobState.RunningState.FAILED);
      String errMsg = "Failed to launch and run job " + jobId;
      LOG.error(errMsg + ": " + t, t);
      throw new JobException(errMsg, t);
    } finally {
      long endTime = System.currentTimeMillis();
      jobState.setEndTime(endTime);
      jobState.setDuration(endTime - startTime);

      // Persist job state regardless if the job succeeded or failed
      try {
        persistJobState(jobState);
      } catch (Throwable t) {
        LOG.error(String.format("Failed to persist job state of job %s: %s", jobId, t), t);
        jobState.setState(JobState.RunningState.FAILED);
      }

      cleanupStagingData(jobState);

      // Write job execution info to the job history store upon job termination
      storeJobExecutionInfo();

      if (this.jobContext.getJobMetricsOptional().isPresent()) {
        this.jobContext.getJobMetricsOptional().get().stopMetricReporting();
      }

      unlockJob();
    }

    if (jobListener != null) {
      jobListener.onJobCompletion(jobState);
    }

    // Throw an exception at the end if the job failed so the caller knows the job failure
    if (jobState.getState() == JobState.RunningState.FAILED) {
      throw new JobException(String.format("Job %s failed", jobId));
    }
  }

  @Override
  public void close()
      throws IOException {
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
  protected abstract void runWorkUnits(List<WorkUnit> workUnits)
      throws Exception;

  /**
   * Get a {@link JobLock} to be used for the job.
   *
   * @return {@link JobLock} to be used for the job
   */
  protected abstract JobLock getJobLock()
      throws IOException;

  /**
   * Execute the job cancellation.
   */
  protected abstract void executeCancellation();

  /**
   * Start the scheduled executor for executing job cancellation.
   *
   * <p>
   *   The executor will call {@link #executeCancellation()} periodically to execute the
   *   cancellation if and only if a cancellation has been requested but not executed yet.
   * </p>
   */
  protected void startCancellationExecutor() {
    this.cancellationExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        synchronized (cancellationLock) {
          if (!cancellationExecuted && cancellationRequested) {
            executeCancellation();
            cancellationExecuted = true;
            // Notify the caller that requested the cancellation
            cancellationLock.notify();
          }
        }
      }
    }, 0, 1, TimeUnit.SECONDS);
  }

  /**
   * Add the given {@link WorkUnit} for execution.
   */
  private void addWorkUnit(WorkUnit workUnit, int sequence, JobState jobState) {
    workUnit.setProp(ConfigurationKeys.JOB_ID_KEY, this.jobContext.getJobId());
    String taskId = JobLauncherUtils.newTaskId(this.jobContext.getJobId(), sequence);
    workUnit.setId(taskId);
    workUnit.setProp(ConfigurationKeys.TASK_ID_KEY, taskId);
    jobState.addTask();
    // Pre-add a task state so if the task fails and no task state is written out,
    // there is still task state for the task when job/task states are persisted.
    jobState.addTaskState(new TaskState(new WorkUnitState(workUnit)));
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
   * Store job execution information into the job history store.
   */
  private void storeJobExecutionInfo() {
    if (this.jobContext.getJobHistoryStoreOptional().isPresent()) {
      try {
        this.jobContext.getJobHistoryStoreOptional().get().put(this.jobContext.getJobState().toJobExecutionInfo());
      } catch (IOException ioe) {
        LOG.error("Failed to write job execution information to the job history store: " + ioe, ioe);
      }
    }
  }

  /**
   * Set final {@link JobState} of the given job.
   */
  private void setFinalJobState(JobState jobState) {
    jobState.setEndTime(System.currentTimeMillis());
    jobState.setDuration(jobState.getEndTime() - jobState.getStartTime());

    for (TaskState taskState : jobState.getTaskStates()) {
      // Set fork.branches explicitly here so the rest job flow can pick it up
      jobState.setProp(ConfigurationKeys.FORK_BRANCHES_KEY,
          taskState.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1));

      // Determine the final job state based on the task states and the job commit policy.
      // If COMMIT_ON_FULL_SUCCESS is used, the job is considered failed if any task failed.
      // On the other hand, if COMMIT_ON_PARTIAL_SUCCESS is used, the job is considered
      // successful even if some tasks failed.
      if (taskState.getWorkingState() != WorkUnitState.WorkingState.SUCCESSFUL
          && this.jobContext.getJobCommitPolicy() == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS) {
        jobState.setState(JobState.RunningState.FAILED);
        break;
      }
    }

    if (jobState.getState() == JobState.RunningState.SUCCESSFUL) {
      // Reset the failure count if the job successfully completed
      jobState.setProp(ConfigurationKeys.JOB_FAILURES_KEY, 0);
    }

    if (jobState.getState() == JobState.RunningState.FAILED) {
      // Increment the failure count by 1 if the job failed
      int failures = jobState.getPropAsInt(ConfigurationKeys.JOB_FAILURES_KEY, 0) + 1;
      jobState.setProp(ConfigurationKeys.JOB_FAILURES_KEY, failures);
    }
  }

  /**
   * Check if it is OK to commit the output data of the job.
   */
  private boolean canCommit(JobCommitPolicy commitPolicy, JobState jobState) {
    // Only commit job data if 1) COMMIT_ON_PARTIAL_SUCCESS is used,
    // or 2) COMMIT_ON_FULL_SUCCESS is used and the job has succeeded.
    return commitPolicy == JobCommitPolicy.COMMIT_ON_PARTIAL_SUCCESS ||
        (commitPolicy == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS &&
            jobState.getState() == JobState.RunningState.SUCCESSFUL);
  }

  /**
   * Commit the job's output data.
   */
  @SuppressWarnings("unchecked")
  private void commitJob(JobState jobState) throws Exception {
    JobCommitPolicy jobCommitPolicy = this.jobContext.getJobCommitPolicy();
    if (!canCommit(jobCommitPolicy, jobState)) {
      LOG.info("Job data will not be committed due to commit policy: " + jobCommitPolicy);
      return;
    }

    LOG.info(String.format("Publishing job data of job %s with commit policy %s", this.jobContext.getJobId(),
        jobCommitPolicy.name()));

    Closer closer = Closer.create();
    try {
      Class<? extends DataPublisher> dataPublisherClass = (Class<? extends DataPublisher>) Class.forName(
          jobState.getProp(ConfigurationKeys.DATA_PUBLISHER_TYPE, ConfigurationKeys.DEFAULT_DATA_PUBLISHER_TYPE));
      Constructor<? extends DataPublisher> dataPublisherConstructor = dataPublisherClass.getConstructor(State.class);
      DataPublisher publisher = closer.register(dataPublisherConstructor.newInstance(jobState));
      publisher.initialize();
      publisher.publish(jobState.getTaskStates());
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }

    // Set the job state to COMMITTED upon successful commit
    jobState.setState(JobState.RunningState.COMMITTED);
  }

  /**
   * Persist job state of a completed job.
   */
  private void persistJobState(JobState jobState) throws IOException {
    JobState.RunningState runningState = jobState.getState();
    if (runningState == JobState.RunningState.PENDING ||
        runningState == JobState.RunningState.RUNNING ||
        runningState == JobState.RunningState.CANCELLED) {
      // Do not persist job state if the job has not completed
      return;
    }

    String jobName = jobState.getJobName();
    String jobId = jobState.getJobId();

    LOG.info("Persisting job state of job " + jobId);
    this.jobContext.getJobStateStore().put(jobName, jobId + JOB_STATE_STORE_TABLE_SUFFIX, jobState);
    this.jobContext.getJobStateStore().createAlias(jobName, jobId + JOB_STATE_STORE_TABLE_SUFFIX,
        "current" + JOB_STATE_STORE_TABLE_SUFFIX);
  }

  /**
   * Cleanup the job's task staging data. This is not doing anything in case job succeeds
   * and data is successfully committed because the staging data has already been moved
   * to the job output directory. But in case the job fails and data is not committed,
   * we want the staging data to be cleaned up.
   */
  private void cleanupStagingData(JobState jobState) {
    for (TaskState taskState : jobState.getTaskStates()) {
      try {
        JobLauncherUtils.cleanStagingData(taskState, LOG);
      } catch (IOException ioe) {
        LOG.error(String.format("Failed to clean staging data for task %s: %s", taskState.getTaskId(), ioe), ioe);
      }
    }
  }
}
