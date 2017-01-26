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

package gobblin.runtime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.CaseFormat;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Closer;
import com.typesafe.config.ConfigFactory;

import gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import gobblin.broker.SharedResourcesBrokerFactory;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.commit.CommitSequence;
import gobblin.commit.CommitSequenceStore;
import gobblin.commit.DeliverySemantics;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.initializer.ConverterInitializerFactory;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.GobblinMetricsRegistry;
import gobblin.metrics.MetricContext;
import gobblin.metrics.Tag;
import gobblin.metrics.event.EventSubmitter;
import gobblin.metrics.event.JobEvent;
import gobblin.metrics.event.TimingEvent;
import gobblin.runtime.listeners.CloseableJobListener;
import gobblin.runtime.listeners.JobExecutionEventSubmitterListener;
import gobblin.runtime.listeners.JobListener;
import gobblin.runtime.listeners.JobListeners;
import gobblin.runtime.locks.JobLock;
import gobblin.runtime.locks.JobLockEventListener;
import gobblin.runtime.locks.JobLockException;
import gobblin.runtime.locks.LegacyJobLockFactoryManager;
import gobblin.runtime.util.JobMetrics;
import gobblin.source.workunit.MultiWorkUnit;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.ClusterNameTags;
import gobblin.util.ExecutorsUtils;
import gobblin.util.JobLauncherUtils;
import gobblin.util.ParallelRunner;
import gobblin.writer.initializer.WriterInitializerFactory;

import javax.annotation.Nullable;


/**
 * An abstract implementation of {@link JobLauncher} that handles common tasks for launching and running a job.
 *
 * @author Yinan Li
 */
public abstract class AbstractJobLauncher implements JobLauncher {

  static final Logger LOG = LoggerFactory.getLogger(AbstractJobLauncher.class);

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

  public AbstractJobLauncher(Properties jobProps, List<? extends Tag<?>> metadataTags) throws Exception {
    this(jobProps, metadataTags, null);
  }

  public AbstractJobLauncher(Properties jobProps, List<? extends Tag<?>> metadataTags,
      @Nullable SharedResourcesBroker<GobblinScopeTypes> instanceBroker)
      throws Exception {
    Preconditions.checkArgument(jobProps.containsKey(ConfigurationKeys.JOB_NAME_KEY),
        "A job must have a job name specified by job.name");

    // Add clusterIdentifier tag so that it is added to any new TaskState created
    List<Tag<?>> clusterNameTags = Lists.newArrayList();
    clusterNameTags.addAll(Tag.fromMap(ClusterNameTags.getClusterNameTags()));
    GobblinMetrics.addCustomTagsToProperties(jobProps, clusterNameTags);

    // Make a copy for both the system and job configuration properties
    this.jobProps = new Properties();
    this.jobProps.putAll(jobProps);

    if (!tryLockJob(this.jobProps)) {
      throw new JobException(String.format(
          "Previous instance of job %s is still running, skipping this scheduled run",
          this.jobProps .getProperty(ConfigurationKeys.JOB_NAME_KEY)));
    }

    try {
      if (instanceBroker == null) {
        instanceBroker = createDefaultInstanceBroker(jobProps);
      }

      this.jobContext = new JobContext(this.jobProps, LOG, instanceBroker);
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

      this.eventSubmitter = buildEventSubmitter(metadataTags);

      // Add all custom tags to the JobState so that tags are added to any new TaskState created
      GobblinMetrics.addCustomTagToState(this.jobContext.getJobState(), metadataTags);

      JobExecutionEventSubmitter jobExecutionEventSubmitter = new JobExecutionEventSubmitter(this.eventSubmitter);
      this.mandatoryJobListeners.add(new JobExecutionEventSubmitterListener(jobExecutionEventSubmitter));
    } catch (Exception e) {
      unlockJob();
      throw e;
    }
  }

  private static SharedResourcesBroker<GobblinScopeTypes> createDefaultInstanceBroker(Properties jobProps) {
    LOG.warn("Creating a job specific {}. Objects will only be shared at the job level.", SharedResourcesBroker.class.getSimpleName());
    return SharedResourcesBrokerFactory.createDefaultTopLevelBroker(ConfigFactory.parseProperties(jobProps),
        GobblinScopeTypes.GLOBAL.defaultScopeInstance());
  }

  /**
   * The JobContext of the particular job.
   *
   * @return {@link JobContext} of the job
   */
  JobContext getJobContext() {
    return this.jobContext;
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
  public void cancelJob(JobListener jobListener)
      throws JobException {
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
      try {
        while (!this.cancellationExecuted) {
          // Wait for the cancellation to be executed
          this.cancellationExecution.wait();
        }
        try {
          this.jobContext.close();
        } catch (IOException ioe) {
          LOG.error("Could not close job context.", ioe);
        }
        notifyListeners(this.jobContext, jobListener, TimingEvent.LauncherTimings.JOB_CANCEL, new JobListenerAction() {
          @Override
          public void apply(JobListener jobListener, JobContext jobContext)
              throws Exception {
            jobListener.onJobCancellation(jobContext);
          }
        });
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * This method checks if a work unit should be skipped. If yes, then it will removed
   * from the list of workUnits and it's state will be saved.
   * @param workUnits passed from {@link #launchJob(JobListener)}
   * @param jobState passed from {@link #launchJob(JobListener)}
   */
  private Optional<List<WorkUnit>> removeSkippedWorkUnits(Optional<List<WorkUnit>> workUnits, JobState jobState) {
    if (!workUnits.isPresent()) {
      return workUnits;
    }
    final List<WorkUnit> skippedWorkUnits = new ArrayList<>();
    for (WorkUnit workUnit : workUnits.get()) {
      if (workUnit instanceof MultiWorkUnit) {
        Preconditions.checkArgument(!workUnit.contains(ConfigurationKeys.WORK_UNIT_SKIP_KEY),
            "Error: MultiWorkUnit cannot be skipped");
        for (WorkUnit wu : ((MultiWorkUnit) workUnit).getWorkUnits()) {
          Preconditions.checkArgument(!wu.contains(ConfigurationKeys.WORK_UNIT_SKIP_KEY),
              "Error: MultiWorkUnit cannot contain skipped WorkUnit");
        }
      }
      if (workUnit.getPropAsBoolean(ConfigurationKeys.WORK_UNIT_SKIP_KEY, false)) {
        skippedWorkUnits.add(workUnit);
        WorkUnitState workUnitState = new WorkUnitState(workUnit, jobState);
        workUnitState.setWorkingState(WorkUnitState.WorkingState.SKIPPED);
        jobState.addTaskState(new TaskState(workUnitState));
      }
    }
    jobState.filterSkippedTaskStates();
    Predicate<WorkUnit> executableWorkUnit = new Predicate<WorkUnit>() {
      @Override
      public boolean apply(@Nullable WorkUnit workUnit) {
        return !skippedWorkUnits.contains(workUnit);
      }
    };
    return Optional.fromNullable((List<WorkUnit>) (ImmutableList.<WorkUnit>builder().addAll(
        Collections2.filter(workUnits.get(), executableWorkUnit)).build()));
  }

  @Override
  public void launchJob(JobListener jobListener)
      throws JobException {
    String jobId = this.jobContext.getJobId();
    JobState jobState = this.jobContext.getJobState();

    try {
      TimingEvent launchJobTimer = this.eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.FULL_JOB_EXECUTION);

      try (Closer closer = Closer.create()) {
        notifyListeners(this.jobContext, jobListener, TimingEvent.LauncherTimings.JOB_PREPARE, new JobListenerAction() {
          @Override
          public void apply(JobListener jobListener, JobContext jobContext)
              throws Exception {
            jobListener.onJobPrepare(jobContext);
          }
        });

        if (this.jobContext.getSemantics() == DeliverySemantics.EXACTLY_ONCE) {

          // If exactly-once is used, commit sequences of the previous run must be successfully compelted
          // before this run can make progress.
          executeUnfinishedCommitSequences(jobState.getJobName());
        }

        TimingEvent workUnitsCreationTimer =
            this.eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.WORK_UNITS_CREATION);
        // Generate work units of the job from the source
        Optional<List<WorkUnit>> workUnits = Optional.fromNullable(this.jobContext.getSource().getWorkunits(jobState));
        workUnitsCreationTimer.stop();

        // The absence means there is something wrong getting the work units
        if (!workUnits.isPresent()) {
          this.eventSubmitter.submit(JobEvent.WORK_UNITS_MISSING);
          jobState.setState(JobState.RunningState.FAILED);
          throw new JobException("Failed to get work units for job " + jobId);
        }

        // No work unit to run
        if (workUnits.get().isEmpty()) {
          this.eventSubmitter.submit(JobEvent.WORK_UNITS_EMPTY);
          LOG.warn("No work units have been created for job " + jobId);
          jobState.setState(JobState.RunningState.COMMITTED);
          notifyListeners(this.jobContext, jobListener, TimingEvent.LauncherTimings.JOB_COMPLETE,
              new JobListenerAction() {
                @Override
                public void apply(JobListener jobListener, JobContext jobContext)
                    throws Exception {
                  jobListener.onJobCompletion(jobContext);
                }
              });
          return;
        }

        //Initialize writer and converter(s)
        closer.register(WriterInitializerFactory.newInstace(jobState, workUnits.get())).initialize();
        closer.register(ConverterInitializerFactory.newInstance(jobState, workUnits.get())).initialize();

        TimingEvent stagingDataCleanTimer =
            this.eventSubmitter.getTimingEvent(TimingEvent.RunJobTimings.MR_STAGING_DATA_CLEAN);
        // Cleanup left-over staging data possibly from the previous run. This is particularly
        // important if the current batch of WorkUnits include failed WorkUnits from the previous
        // run which may still have left-over staging data not cleaned up yet.
        cleanLeftoverStagingData(workUnits.get(), jobState);
        stagingDataCleanTimer.stop();

        long startTime = System.currentTimeMillis();
        jobState.setStartTime(startTime);
        jobState.setState(JobState.RunningState.RUNNING);

        try {
          LOG.info("Starting job " + jobId);

          notifyListeners(this.jobContext, jobListener, TimingEvent.LauncherTimings.JOB_START, new JobListenerAction() {
            @Override
            public void apply(JobListener jobListener, JobContext jobContext)
                throws Exception {
              jobListener.onJobStart(jobContext);
            }
          });

          TimingEvent workUnitsPreparationTimer =
              this.eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.WORK_UNITS_PREPARATION);
          prepareWorkUnits(JobLauncherUtils.flattenWorkUnits(workUnits.get()), jobState);
          workUnitsPreparationTimer.stop();

          // Write job execution info to the job history store before the job starts to run
          this.jobContext.storeJobExecutionInfo();

          TimingEvent jobRunTimer = this.eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.JOB_RUN);
          // Remove skipped workUnits from the list of work units to execute.
          workUnits = removeSkippedWorkUnits(workUnits, jobState);
          // Start the job and wait for it to finish
          runWorkUnits(workUnits.get());
          jobRunTimer.stop();

          this.eventSubmitter
              .submit(CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, "JOB_" + jobState.getState()));

          // Check and set final job jobPropsState upon job completion
          if (jobState.getState() == JobState.RunningState.CANCELLED) {
            LOG.info(String.format("Job %s has been cancelled, aborting now", jobId));
            return;
          }

          TimingEvent jobCommitTimer = this.eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.JOB_COMMIT);
          this.jobContext.finalizeJobStateBeforeCommit();
          this.jobContext.commit();
          postProcessJobState(jobState);
          jobCommitTimer.stop();
        } finally {
          long endTime = System.currentTimeMillis();
          jobState.setEndTime(endTime);
          jobState.setDuration(endTime - jobState.getStartTime());
        }
      } catch (Throwable t) {
        jobState.setState(JobState.RunningState.FAILED);
        String errMsg = "Failed to launch and run job " + jobId;
        LOG.error(errMsg + ": " + t, t);
      } finally {
        try {
          TimingEvent jobCleanupTimer = this.eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.JOB_CLEANUP);
          cleanupStagingData(jobState);
          jobCleanupTimer.stop();

          // Write job execution info to the job history store upon job termination
          this.jobContext.storeJobExecutionInfo();
        }
        finally {
          launchJobTimer.stop();
        }
      }

      for (JobState.DatasetState datasetState : this.jobContext.getDatasetStatesByUrns().values()) {
        // Set the overall job state to FAILED if the job failed to process any dataset
        if (datasetState.getState() == JobState.RunningState.FAILED) {
          jobState.setState(JobState.RunningState.FAILED);
          break;
        }
      }

      notifyListeners(this.jobContext, jobListener, TimingEvent.LauncherTimings.JOB_COMPLETE, new JobListenerAction() {
        @Override
        public void apply(JobListener jobListener, JobContext jobContext)
            throws Exception {
          jobListener.onJobCompletion(jobContext);
        }
      });

      if (jobState.getState() == JobState.RunningState.FAILED) {
        notifyListeners(this.jobContext, jobListener, TimingEvent.LauncherTimings.JOB_FAILED, new JobListenerAction() {
          @Override
          public void apply(JobListener jobListener, JobContext jobContext)
              throws Exception {
            jobListener.onJobFailure(jobContext);
          }
        });
        throw new JobException(String.format("Job %s failed", jobId));
      }
    } finally {
      // Stop metrics reporting
      if (this.jobContext.getJobMetricsOptional().isPresent()) {
        JobMetrics.remove(jobState);
      }
    }
  }

  private void executeUnfinishedCommitSequences(String jobName)
      throws IOException {
    Preconditions.checkState(this.jobContext.getCommitSequenceStore().isPresent());
    CommitSequenceStore commitSequenceStore = this.jobContext.getCommitSequenceStore().get();

    for (String datasetUrn : commitSequenceStore.get(jobName)) {
      Optional<CommitSequence> commitSequence = commitSequenceStore.get(jobName, datasetUrn);
      if (commitSequence.isPresent()) {
        commitSequence.get().execute();
      }
      commitSequenceStore.delete(jobName, datasetUrn);
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
  protected void postProcessJobState(JobState jobState) {
    postProcessTaskStates(jobState.getTaskStates());
  }

  @Override
  public void close()
      throws IOException {
    try {
      this.cancellationExecutor.shutdownNow();
      try {
        this.jobContext.getSource().shutdown(this.jobContext.getJobState());
      } finally {
        if (GobblinMetrics.isEnabled(this.jobProps)) {
          GobblinMetricsRegistry.getInstance().remove(this.jobContext.getJobId());
        }
      }
    } finally {
        unlockJob();
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
   * @param properties the job properties
   * @param jobLockEventListener the listener for lock events.
   * @return {@link JobLock} to be used for the job
   * @throws JobLockException throw when the {@link JobLock} fails to initialize
   */
  protected JobLock getJobLock(Properties properties, JobLockEventListener jobLockEventListener)
      throws JobLockException {
    return LegacyJobLockFactoryManager.getJobLock(properties, jobLockEventListener);
  }

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
        synchronized (AbstractJobLauncher.this.cancellationRequest) {
          try {
            while (!AbstractJobLauncher.this.cancellationRequested) {
              // Wait for a cancellation request to arrive
              AbstractJobLauncher.this.cancellationRequest.wait();
            }
            LOG.info("Cancellation has been requested for job " + AbstractJobLauncher.this.jobContext.getJobId());
            executeCancellation();
            LOG.info("Cancellation has been executed for job " + AbstractJobLauncher.this.jobContext.getJobId());
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
        }

        synchronized (AbstractJobLauncher.this.cancellationExecution) {
          AbstractJobLauncher.this.cancellationExecuted = true;
          AbstractJobLauncher.this.jobContext.getJobState().setState(JobState.RunningState.CANCELLED);
          // Notify that the cancellation has been executed
          AbstractJobLauncher.this.cancellationExecution.notifyAll();
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
      jobState.addTaskState(new TaskState(new WorkUnitState(workUnit, jobState)));
    }
  }

  /**
   * Try acquiring the job lock and return whether the lock is successfully locked.
   *
   * @param properties the job properties
   */
  private boolean tryLockJob(Properties properties) {
    try {
      if (Boolean.valueOf(properties.getProperty(ConfigurationKeys.JOB_LOCK_ENABLED_KEY, Boolean.TRUE.toString()))) {
        this.jobLockOptional = Optional.of(getJobLock(properties, new JobLockEventListener() {
          @Override
          public void onLost() {
            executeCancellation();
          }
        }));
      }
      return !this.jobLockOptional.isPresent() || this.jobLockOptional.get().tryLock();
    } catch (JobLockException ioe) {
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
      } catch (JobLockException ioe) {
        LOG.error(String.format("Failed to unlock for job %s: %s", this.jobContext.getJobId(), ioe), ioe);
      } finally {
        try {
          this.jobLockOptional.get().close();
        } catch (IOException e) {
          LOG.error(String.format("Failed to close job lock for job %s: %s", this.jobContext.getJobId(), e), e);

        } finally {
          this.jobLockOptional = Optional.absent();
        }
      }
    }
  }

  /**
   * Combines the specified {@link JobListener} with the {@link #mandatoryJobListeners} for this job. Uses
   * {@link JobListeners#parallelJobListener(List)} to create a {@link CloseableJobListener} that will execute all
   * the {@link JobListener}s in parallel.
   */
  private CloseableJobListener getParallelCombinedJobListener(JobState jobState, JobListener jobListener) {
    List<JobListener> jobListeners = Lists.newArrayList(this.mandatoryJobListeners);
    jobListeners.add(jobListener);

    Set<String> jobListenerClassNames = jobState.getPropAsSet(ConfigurationKeys.JOB_LISTENERS_KEY, StringUtils.EMPTY);
    for (String jobListenerClassName : jobListenerClassNames) {
      try {
        @SuppressWarnings("unchecked")
        Class<? extends JobListener> jobListenerClass =
            (Class<? extends JobListener>) Class.forName(jobListenerClassName);
        jobListeners.add(jobListenerClass.newInstance());
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        LOG.warn(String.format("JobListener could not be created due to %s", jobListenerClassName), e);
      }
    }

    return JobListeners.parallelJobListener(jobListeners);
  }

  /**
   * Takes a {@link List} of {@link Tag}s and returns a new {@link List} with the original {@link Tag}s as well as any
   * additional {@link Tag}s returned by {@link ClusterNameTags#getClusterNameTags()}.
   *
   * @see ClusterNameTags
   */
  private static List<Tag<?>> addClusterNameTags(List<? extends Tag<?>> tags) {
    return ImmutableList.<Tag<?>>builder().addAll(tags).addAll(Tag.fromMap(ClusterNameTags.getClusterNameTags()))
        .build();
  }

  /**
   * Build the {@link EventSubmitter} for this class.
   */
  private EventSubmitter buildEventSubmitter(List<? extends Tag<?>> tags) {
    return new EventSubmitter.Builder(this.runtimeMetricContext, "gobblin.runtime")
        .addMetadata(Tag.toMap(Tag.tagValuesToString(tags))).build();
  }

  /**
   * Cleanup the left-over staging data possibly from the previous run of the job that may have failed
   * and not cleaned up its staging data.
   *
   * Property {@link ConfigurationKeys#CLEANUP_STAGING_DATA_PER_TASK} controls whether to cleanup
   * staging data per task, or to cleanup entire job's staging data at once.
   *
   * Staging data will not be cleaned if the job has unfinished {@link CommitSequence}s.
   */
  private void cleanLeftoverStagingData(List<WorkUnit> workUnits, JobState jobState)
      throws JobException {
    if (jobState.getPropAsBoolean(ConfigurationKeys.CLEANUP_STAGING_DATA_BY_INITIALIZER, false)) {
      //Clean up will be done by initializer.
      return;
    }

    try {
      if (!canCleanStagingData(jobState)) {
        LOG.error("Job " + jobState.getJobName() + " has unfinished commit sequences. Will not clean up staging data.");
        return;
      }
    } catch (IOException e) {
      throw new JobException("Failed to check unfinished commit sequences", e);
    }

    try {
      if (this.jobContext.shouldCleanupStagingDataPerTask()) {
        Closer closer = Closer.create();
        Map<String, ParallelRunner> parallelRunners = Maps.newHashMap();
        try {
          for (WorkUnit workUnit : JobLauncherUtils.flattenWorkUnits(workUnits)) {
            JobLauncherUtils.cleanTaskStagingData(new WorkUnitState(workUnit, jobState), LOG, closer, parallelRunners);
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
   *
   * Staging data will not be cleaned if the job has unfinished {@link CommitSequence}s.
   */
  private void cleanupStagingData(JobState jobState)
      throws JobException {
    if (jobState.getPropAsBoolean(ConfigurationKeys.CLEANUP_STAGING_DATA_BY_INITIALIZER, false)) {
      //Clean up will be done by initializer.
      return;
    }

    try {
      if (!canCleanStagingData(jobState)) {
        LOG.error("Job " + jobState.getJobName() + " has unfinished commit sequences. Will not clean up staging data.");
        return;
      }
    } catch (IOException e) {
      throw new JobException("Failed to check unfinished commit sequences", e);
    }

    if (this.jobContext.shouldCleanupStagingDataPerTask()) {
      cleanupStagingDataPerTask(jobState);
    } else {
      cleanupStagingDataForEntireJob(jobState);
    }
  }

  /**
   * Staging data cannot be cleaned if exactly once semantics is used, and the job has unfinished
   * commit sequences.
   */
  private boolean canCleanStagingData(JobState jobState)
      throws IOException {
    return this.jobContext.getSemantics() != DeliverySemantics.EXACTLY_ONCE || !this.jobContext.getCommitSequenceStore()
        .get().exists(jobState.getJobName());
  }

  private static void cleanupStagingDataPerTask(JobState jobState) {
    Closer closer = Closer.create();
    Map<String, ParallelRunner> parallelRunners = Maps.newHashMap();
    try {
      for (TaskState taskState : jobState.getTaskStates()) {
        try {
          JobLauncherUtils.cleanTaskStagingData(taskState, LOG, closer, parallelRunners);
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

  private void notifyListeners(JobContext jobContext, JobListener jobListener, String timerEventName,
      JobListenerAction action)
      throws JobException {
    TimingEvent timer = this.eventSubmitter.getTimingEvent(timerEventName);
    try (CloseableJobListener parallelJobListener = getParallelCombinedJobListener(this.jobContext.getJobState(),
        jobListener)) {
      action.apply(parallelJobListener, jobContext);
    } catch (Exception e) {
      throw new JobException("Failed to execute all JobListeners", e);
    } finally {
      timer.stop();
    }
  }

  private interface JobListenerAction {
    void apply(JobListener jobListener, JobContext jobContext)
        throws Exception;
  }
}
