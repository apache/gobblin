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
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.inject.Guice;
import com.google.inject.Injector;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.metastore.FsStateStore;
import gobblin.metastore.JobHistoryStore;
import gobblin.metastore.MetaStoreModule;
import gobblin.metastore.StateStore;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.GobblinMetricsRegistry;
import gobblin.publisher.DataPublisher;
import gobblin.runtime.util.JobMetrics;
import gobblin.source.extractor.JobCommitPolicy;
import gobblin.source.Source;
import gobblin.source.workunit.MultiWorkUnit;
import gobblin.source.workunit.WorkUnit;
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
  protected final Properties properties;

  // State store for persisting job states
  private final StateStore<JobState> jobStateStore;

  // Store for runtime job execution information
  private final Optional<JobHistoryStore> jobHistoryStore;

  // Job configuration properties
  protected final Properties jobProps;
  protected final String jobName;
  protected final String jobId;
  protected final JobState jobState;
  private final JobCommitPolicy jobCommitPolicy;
  private final boolean jobLockEnabled;
  private final Optional<JobMetrics> jobMetricsOptional;
  private final Source<?, ?> source;

  // A JobLock is optional depending on if it is enabled
  protected Optional<JobLock> jobLockOptional = Optional.absent();

  // A flag set to true if the job is successfully cancelled
  protected volatile boolean isCancelled = false;

  @SuppressWarnings("unchecked")
  public AbstractJobLauncher(Properties properties, Properties jobProps)
      throws Exception {
    Preconditions.checkArgument(
        jobProps.containsKey(ConfigurationKeys.JOB_NAME_KEY), "A job must have a job name specified by job.name");

    // Make a copy for both the system and job configuration properties
    this.properties = new Properties();
    this.properties.putAll(properties);
    this.jobProps = new Properties();
    this.jobProps.putAll(jobProps);

    this.jobStateStore = new FsStateStore<JobState>(
        properties.getProperty(ConfigurationKeys.STATE_STORE_FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI),
        properties.getProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY),
        JobState.class);

    boolean jobHistoryStoreEnabled = Boolean.valueOf(
        properties.getProperty(ConfigurationKeys.JOB_HISTORY_STORE_ENABLED_KEY, Boolean.FALSE.toString()));
    if (jobHistoryStoreEnabled) {
      Injector injector = Guice.createInjector(new MetaStoreModule(properties));
      this.jobHistoryStore = Optional.of(injector.getInstance(JobHistoryStore.class));
    } else {
      this.jobHistoryStore = Optional.absent();
    }

    this.jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    this.jobId = this.jobProps.containsKey(ConfigurationKeys.JOB_ID_KEY) ?
        this.jobProps.getProperty(ConfigurationKeys.JOB_ID_KEY) : JobLauncherUtils.newJobId(this.jobName);
    this.jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, this.jobId);

    this.jobCommitPolicy = JobCommitPolicy.getCommitPolicy(this.jobProps);

    this.jobLockEnabled = Boolean.valueOf(
        this.jobProps.getProperty(ConfigurationKeys.JOB_LOCK_ENABLED_KEY, Boolean.TRUE.toString()));

    State jobPropsState = new State();
    jobPropsState.addAll(this.jobProps);
    JobState previousJobState = getPreviousJobState(this.jobName);
    this.jobState = new JobState(jobPropsState,
        previousJobState.getTaskStatesAsWorkUnitStates(), this.jobName, this.jobId);
    // Remember the number of consecutive failures of this job in the past
    this.jobState.setProp(ConfigurationKeys.JOB_FAILURES_KEY,
        previousJobState.getPropAsInt(ConfigurationKeys.JOB_FAILURES_KEY, 0));

    if (GobblinMetrics.isEnabled(this.jobProps)) {
      this.jobMetricsOptional = Optional.of(JobMetrics.get(this.jobState));
      this.jobState.setProp(ConfigurationKeys.METRIC_CONTEXT_NAME_KEY, this.jobMetricsOptional.get().getName());
    } else {
      this.jobMetricsOptional = Optional.absent();
    }

    this.source = new SourceDecorator(initSource(), this.jobId, LOG);
  }

  /**
   * Run the given list of {@link WorkUnit}s of the given job.
   *
   * <p>
   *   This method assumes that the given list of {@link WorkUnit}s have already been flattened and
   *   each {@link WorkUnit} contains the task ID in the property {@link ConfigurationKeys#TASK_ID_KEY}.
   * </p>
   *
   * @param jobId job ID
   * @param workUnits given list of {@link WorkUnit}s to run
   * @param stateTracker a {@link TaskStateTracker} for task state tracking
   * @param taskExecutor a {@link TaskExecutor} for task execution
   * @param countDownLatch a {@link java.util.concurrent.CountDownLatch} waited on for job completion
   * @return a list of {@link Task}s from the {@link WorkUnit}s
   * @throws InterruptedException
   */
  public static List<Task> runWorkUnits(String jobId, List<WorkUnit> workUnits, TaskStateTracker stateTracker,
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

    LOG.info(String.format("Waiting for submitted tasks of job %s to complete...", jobId));
    while (countDownLatch.getCount() > 0) {
      LOG.info(String
          .format("%d out of %d tasks of job %s are running", countDownLatch.getCount(), workUnits.size(), jobId));
      countDownLatch.await(1, TimeUnit.MINUTES);
    }
    LOG.info(String.format("All tasks of job %s have completed", jobId));

    return tasks;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void launchJob(JobListener jobListener)
      throws JobException {
    if (!tryLockJob()) {
      throw new JobException(
          String.format("Previous instance of job %s is still running, skipping this scheduled run", this.jobName));
    }

    // Generate work units of the job from the source
    Optional<List<WorkUnit>> workUnits = Optional.fromNullable(this.source.getWorkunits(this.jobState));
    // The absence means there is something wrong getting the work units
    if (!workUnits.isPresent()) {
      unlockJob();
      throw new JobException("Failed to get work units for job " + this.jobId);
    }

    // No work unit to run
    if (workUnits.get().isEmpty()) {
      LOG.warn("No work units have been created for job " + this.jobId);
      unlockJob();
      return;
    }

    long startTime = System.currentTimeMillis();
    this.jobState.setStartTime(startTime);
    this.jobState.setState(JobState.RunningState.RUNNING);

    LOG.info("Starting job " + this.jobId);

    // Add work units and assign task IDs to them
    int taskIdSequence = 0;
    int multiTaskIdSequence = 0;
    for (WorkUnit workUnit : workUnits.get()) {
      if (workUnit instanceof MultiWorkUnit) {
        String multiTaskId = JobLauncherUtils.newMultiTaskId(this.jobId, multiTaskIdSequence++);
        workUnit.setProp(ConfigurationKeys.TASK_ID_KEY, multiTaskId);
        workUnit.setId(multiTaskId);
        for (WorkUnit innerWorkUnit : ((MultiWorkUnit) workUnit).getWorkUnits()) {
          addWorkUnit(innerWorkUnit, taskIdSequence++);
        }
      } else {
        addWorkUnit(workUnit, taskIdSequence++);
      }
    }

    try {
      if (this.jobMetricsOptional.isPresent()) {
        this.jobMetricsOptional.get().startMetricReporting(this.jobProps);
      }

      // Write job execution info to the job history store before the job starts to run
      storeJobExecutionInfo();

      // Start the job and wait for it to finish
      runWorkUnits(workUnits.get());

      // Check and set final job jobPropsState upon job completion
      if (this.jobState.getState() == JobState.RunningState.CANCELLED) {
        LOG.info(String.format("Job %s has been cancelled", this.jobId));
        return;
      }

      setFinalJobState();
      // Commit and publish job data
      commitJob();
    } catch (Throwable t) {
      this.jobState.setState(JobState.RunningState.FAILED);
      String errMsg = "Failed to launch and run job " + this.jobId;
      LOG.error(errMsg + ": " + t, t);
      throw new JobException(errMsg, t);
    } finally {
      long endTime = System.currentTimeMillis();
      this.jobState.setEndTime(endTime);
      this.jobState.setDuration(endTime - startTime);

      // Persist job state regardless if the job succeeded or failed
      try {
        persistJobState();
      } catch (Throwable t) {
        LOG.error(String.format("Failed to persist job state of job %s: %s", this.jobId, t), t);
        this.jobState.setState(JobState.RunningState.FAILED);
      }

      cleanupStagingData();

      // Write job execution info to the job history store upon job termination
      storeJobExecutionInfo();

      if (this.jobMetricsOptional.isPresent()) {
        this.jobMetricsOptional.get().stopMetricReporting();
      }

      unlockJob();
    }

    if (jobListener != null) {
      jobListener.onJobCompletion(this.jobState);
    }

    // Throw an exception at the end if the job failed so the caller knows the job failure
    if (this.jobState.getState() == JobState.RunningState.FAILED) {
      throw new JobException(String.format("Job %s failed", this.jobId));
    }
  }

  @Override
  public void close()
      throws IOException {
    try {
      this.source.shutdown(this.jobState);
    } finally {
      if (GobblinMetrics.isEnabled(this.jobProps)) {
        GobblinMetricsRegistry.getInstance().remove(this.jobId);
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
   * Initialize the source for the given job.
   */
  private Source<?, ?> initSource()
      throws Exception {
    return (Source<?, ?>) Class.forName(this.jobProps.getProperty(ConfigurationKeys.SOURCE_CLASS_KEY)).newInstance();
  }

  /**
   * Add the given {@link WorkUnit} for execution.
   */
  private void addWorkUnit(WorkUnit workUnit, int sequence) {
    workUnit.setProp(ConfigurationKeys.JOB_ID_KEY, this.jobState.getJobId());
    String taskId = JobLauncherUtils.newTaskId(this.jobState.getJobId(), sequence);
    workUnit.setId(taskId);
    workUnit.setProp(ConfigurationKeys.TASK_ID_KEY, taskId);
    this.jobState.addTask();
    // Pre-add a task state so if the task fails and no task state is written out,
    // there is still task state for the task when job/task states are persisted.
    this.jobState.addTaskState(new TaskState(new WorkUnitState(workUnit)));
  }

  /**
   * Get the job state of the most recent run of the job.
   */
  private JobState getPreviousJobState(String jobName)
      throws IOException {
    if (this.jobStateStore.exists(jobName, "current" + JOB_STATE_STORE_TABLE_SUFFIX)) {
      // Read the job state of the most recent run of the job
      List<JobState> previousJobStateList =
          this.jobStateStore.getAll(jobName, "current" + JOB_STATE_STORE_TABLE_SUFFIX);
      if (!previousJobStateList.isEmpty()) {
        // There should be a single job state on the list if the list is not empty
        return previousJobStateList.get(0);
      }
    }

    LOG.warn("No previous job state found for job " + jobName);
    return new JobState();
  }

  /**
   * Try acquiring the job lock and return whether the lock is successfully locked.
   */
  private boolean tryLockJob() {
    try {
      if (this.jobLockEnabled) {
        this.jobLockOptional = Optional.of(getJobLock());
      }
      return !this.jobLockOptional.isPresent() || this.jobLockOptional.get().tryLock();
    } catch (IOException ioe) {
      LOG.error(String.format("Failed to acquire job lock for job %s: %s", this.jobName, ioe), ioe);
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
        LOG.error(String.format("Failed to unlock for job %s: %s", this.jobName, ioe), ioe);
      }
    }
  }

  /**
   * Store job execution information into the job history store.
   */
  private void storeJobExecutionInfo() {
    if (this.jobHistoryStore.isPresent()) {
      try {
        this.jobHistoryStore.get().put(this.jobState.toJobExecutionInfo());
      } catch (IOException ioe) {
        LOG.error("Failed to write job execution information to the job history store: " + ioe, ioe);
      }
    }
  }

  /**
   * Set final {@link JobState} of the given job.
   */
  private void setFinalJobState() {
    this.jobState.setEndTime(System.currentTimeMillis());
    this.jobState.setDuration(this.jobState.getEndTime() - this.jobState.getStartTime());

    for (TaskState taskState : this.jobState.getTaskStates()) {
      // Set fork.branches explicitly here so the rest job flow can pick it up
      this.jobState.setProp(ConfigurationKeys.FORK_BRANCHES_KEY,
          taskState.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1));

      // Determine the final job state based on the task states and the job commit policy.
      // If COMMIT_ON_FULL_SUCCESS is used, the job is considered failed if any task failed.
      // On the other hand, if COMMIT_ON_PARTIAL_SUCCESS is used, the job is considered
      // successful even if some tasks failed.
      if (taskState.getWorkingState() != WorkUnitState.WorkingState.SUCCESSFUL
          && this.jobCommitPolicy == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS) {
        this.jobState.setState(JobState.RunningState.FAILED);
        break;
      }
    }

    if (this.jobState.getState() == JobState.RunningState.SUCCESSFUL) {
      // Reset the failure count if the job successfully completed
      this.jobState.setProp(ConfigurationKeys.JOB_FAILURES_KEY, 0);
    }

    if (this.jobState.getState() == JobState.RunningState.FAILED) {
      // Increment the failure count by 1 if the job failed
      int failures = this.jobState.getPropAsInt(ConfigurationKeys.JOB_FAILURES_KEY, 0) + 1;
      this.jobState.setProp(ConfigurationKeys.JOB_FAILURES_KEY, failures);
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
  private void commitJob() throws Exception {
    if (!canCommit(this.jobCommitPolicy, this.jobState)) {
      LOG.info("Job data will not be committed due to commit policy: " + this.jobCommitPolicy);
      return;
    }

    LOG.info(String.format("Publishing job data of job %s with commit policy %s", this.jobId,
        this.jobCommitPolicy.name()));

    Closer closer = Closer.create();
    try {
      Class<? extends DataPublisher> dataPublisherClass = (Class<? extends DataPublisher>) Class.forName(
          this.jobState.getProp(ConfigurationKeys.DATA_PUBLISHER_TYPE, ConfigurationKeys.DEFAULT_DATA_PUBLISHER_TYPE));
      Constructor<? extends DataPublisher> dataPublisherConstructor = dataPublisherClass.getConstructor(State.class);
      DataPublisher publisher = closer.register(dataPublisherConstructor.newInstance(this.jobState));
      publisher.initialize();
      publisher.publish(this.jobState.getTaskStates());
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }

    // Set the job state to COMMITTED upon successful commit
    this.jobState.setState(JobState.RunningState.COMMITTED);
  }

  /**
   * Persist job state of a completed job.
   */
  private void persistJobState() throws IOException {
    JobState.RunningState runningState = this.jobState.getState();
    if (runningState == JobState.RunningState.PENDING ||
        runningState == JobState.RunningState.RUNNING ||
        runningState == JobState.RunningState.CANCELLED) {
      // Do not persist job state if the job has not completed
      return;
    }

    String jobName = this.jobState.getJobName();
    String jobId = this.jobState.getJobId();

    LOG.info("Persisting job state of job " + jobId);
    this.jobStateStore.put(jobName, jobId + JOB_STATE_STORE_TABLE_SUFFIX, this.jobState);
    this.jobStateStore.createAlias(jobName, jobId + JOB_STATE_STORE_TABLE_SUFFIX,
        "current" + JOB_STATE_STORE_TABLE_SUFFIX);
  }

  /**
   * Cleanup the job's task staging data. This is not doing anything in case job succeeds
   * and data is successfully committed because the staging data has already been moved
   * to the job output directory. But in case the job fails and data is not committed,
   * we want the staging data to be cleaned up.
   */
  private void cleanupStagingData() {
    for (TaskState taskState : this.jobState.getTaskStates()) {
      try {
        JobLauncherUtils.cleanStagingData(taskState, LOG);
      } catch (IOException ioe) {
        LOG.error(String.format("Failed to clean staging data for task %s: %s", taskState.getTaskId(), ioe), ioe);
      }
    }
  }
}
