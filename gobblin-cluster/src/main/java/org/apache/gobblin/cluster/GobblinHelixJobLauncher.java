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

package org.apache.gobblin.cluster;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.helix.HelixManager;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.CountEventBuilder;
import org.apache.gobblin.metrics.event.JobEvent;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.rest.LauncherTypeEnum;
import org.apache.gobblin.runtime.AbstractJobLauncher;
import org.apache.gobblin.runtime.ExecutionModel;
import org.apache.gobblin.runtime.JobException;
import org.apache.gobblin.runtime.JobLauncher;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.Task;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.runtime.TaskStateCollectorService;
import org.apache.gobblin.runtime.listeners.JobListener;
import org.apache.gobblin.runtime.util.StateStores;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.JobLauncherUtils;
import org.apache.gobblin.util.ParallelRunner;
import org.apache.gobblin.util.PropertiesUtils;
import org.apache.gobblin.util.SerializationUtils;


/**
 * An implementation of {@link JobLauncher} that launches a Gobblin job using the Helix task framework.
 *
 * <p>
 *   This class uses the Helix task execution framework to run tasks of Gobblin jobs. It creates one Helix
 *   {@link JobQueue} per job and submits every scheduled runs of a job to its {@link JobQueue}, where Helix
 *   picks up them and submit them for execution. After submitting the job run to its {@link JobQueue}, it
 *   waits for the job to complete and collects the output {@link TaskState}(s) upon completion.
 * </p>
 *
 * <p>
 *   Each {@link WorkUnit} of the job is persisted to the {@link FileSystem} of choice and the path to the file
 *   storing the serialized {@link WorkUnit} is passed to the Helix task running the {@link WorkUnit} as a
 *   user-defined property {@link GobblinClusterConfigurationKeys#WORK_UNIT_FILE_PATH}. Upon startup, the Helix
 *   task reads the property for the file path and de-serializes the {@link WorkUnit} from the file.
 * </p>
 *
 * <p>
 *   This class is instantiated by the {@link GobblinHelixJobScheduler} on every job submission to launch the Gobblin job.
 *   The actual task execution happens in the {@link GobblinTaskRunner}, usually in a different process.
 * </p>
 *
 * @author Yinan Li
 */
@Alpha
@Slf4j
public class GobblinHelixJobLauncher extends AbstractJobLauncher {

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinHelixJobLauncher.class);

  private final HelixManager helixManager;
  private final TaskDriver helixTaskDriver;
  private final String helixWorkFlowName;
  private JobListener jobListener;

  private final FileSystem fs;
  private final Path appWorkDir;
  private final Path inputWorkUnitDir;
  private final Path outputTaskStateDir;

  // Number of ParallelRunner threads to be used for state serialization/deserialization
  private final int stateSerDeRunnerThreads;

  private final TaskStateCollectorService taskStateCollectorService;
  private final Optional<GobblinHelixMetrics> helixMetrics;
  private volatile boolean jobSubmitted = false;
  private final ConcurrentHashMap<String, Boolean> runningMap;
  private final StateStores stateStores;
  private final Config jobConfig;
  private final long workFlowExpiryTimeSeconds;
  private final long helixJobStopTimeoutSeconds;
  private final long helixWorkflowSubmissionTimeoutSeconds;
  private Map<String, TaskConfig> workUnitToHelixConfig;
  private Retryer<Boolean> taskRetryer;

  public GobblinHelixJobLauncher(Properties jobProps, final HelixManager helixManager, Path appWorkDir,
      List<? extends Tag<?>> metadataTags, ConcurrentHashMap<String, Boolean> runningMap,
      Optional<GobblinHelixMetrics> helixMetrics) throws Exception {

    super(jobProps, HelixUtils.initBaseEventTags(jobProps, metadataTags));
    LOGGER.debug("GobblinHelixJobLauncher: jobProps {}, appWorkDir {}", jobProps, appWorkDir);
    this.helixManager = helixManager;
    this.helixTaskDriver = new TaskDriver(this.helixManager);
    this.runningMap = runningMap;
    this.appWorkDir = appWorkDir;
    this.inputWorkUnitDir = new Path(appWorkDir, GobblinClusterConfigurationKeys.INPUT_WORK_UNIT_DIR_NAME);
    this.outputTaskStateDir = new Path(this.appWorkDir,
        GobblinClusterConfigurationKeys.OUTPUT_TASK_STATE_DIR_NAME + Path.SEPARATOR + this.jobContext.getJobId());

    this.helixWorkFlowName = this.jobContext.getJobId();
    this.jobContext.getJobState().setJobLauncherType(LauncherTypeEnum.CLUSTER);

    this.stateSerDeRunnerThreads = Integer.parseInt(jobProps.getProperty(ParallelRunner.PARALLEL_RUNNER_THREADS_KEY,
        Integer.toString(ParallelRunner.DEFAULT_PARALLEL_RUNNER_THREADS)));
    jobConfig = ConfigUtils.propertiesToConfig(jobProps);

    this.workFlowExpiryTimeSeconds =
        ConfigUtils.getLong(jobConfig, GobblinClusterConfigurationKeys.HELIX_WORKFLOW_EXPIRY_TIME_SECONDS,
            GobblinClusterConfigurationKeys.DEFAULT_HELIX_WORKFLOW_EXPIRY_TIME_SECONDS);

    this.helixJobStopTimeoutSeconds =
        ConfigUtils.getLong(jobConfig, GobblinClusterConfigurationKeys.HELIX_JOB_STOP_TIMEOUT_SECONDS,
            GobblinClusterConfigurationKeys.DEFAULT_HELIX_JOB_STOP_TIMEOUT_SECONDS);

    this.helixWorkflowSubmissionTimeoutSeconds = ConfigUtils.getLong(jobConfig,
        GobblinClusterConfigurationKeys.HELIX_WORKFLOW_SUBMISSION_TIMEOUT_SECONDS,
        GobblinClusterConfigurationKeys.DEFAULT_HELIX_WORKFLOW_SUBMISSION_TIMEOUT_SECONDS);

    Config stateStoreJobConfig = ConfigUtils.propertiesToConfig(jobProps)
        .withValue(ConfigurationKeys.STATE_STORE_FS_URI_KEY, ConfigValueFactory.fromAnyRef(
            new URI(appWorkDir.toUri().getScheme(), null, appWorkDir.toUri().getHost(), appWorkDir.toUri().getPort(),
                "/", null, null).toString()));

    this.stateStores =
        new StateStores(stateStoreJobConfig, appWorkDir, GobblinClusterConfigurationKeys.OUTPUT_TASK_STATE_DIR_NAME,
            appWorkDir, GobblinClusterConfigurationKeys.INPUT_WORK_UNIT_DIR_NAME, appWorkDir,
            GobblinClusterConfigurationKeys.JOB_STATE_DIR_NAME);

    URI fsUri = URI.create(jobProps.getProperty(ConfigurationKeys.FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI));
    this.fs = FileSystem.get(fsUri, new Configuration());

    this.taskStateCollectorService =
        new TaskStateCollectorService(jobProps, this.jobContext.getJobState(), this.eventBus, this.eventSubmitter,
            this.stateStores.getTaskStateStore(), this.outputTaskStateDir, this.getIssueRepository());

    this.helixMetrics = helixMetrics;
    this.workUnitToHelixConfig = new HashMap<>();
    this.taskRetryer = RetryerBuilder.<Boolean>newBuilder()
        .retryIfException()
        .withStopStrategy(StopStrategies.stopAfterAttempt(3)).build();
    startCancellationExecutor();
  }

  @Override
  public void close() throws IOException {
    try {
      executeCancellation();
    } finally {
      super.close();
    }
  }

  public String getJobId() {
    return this.jobContext.getJobId();
  }

  @Override
  protected void runWorkUnits(List<WorkUnit> workUnits) throws Exception {
    try {
      CountEventBuilder countEventBuilder = new CountEventBuilder(JobEvent.WORK_UNITS_CREATED, workUnits.size());
      this.eventSubmitter.submit(countEventBuilder);
      LOGGER.info("Emitting WorkUnitsCreated Count: " + countEventBuilder.getCount());

      long workUnitStartTime = System.currentTimeMillis();
      workUnits.forEach((k) -> k.setProp(ConfigurationKeys.WORK_UNIT_CREATION_TIME_IN_MILLIS, workUnitStartTime));

      // Start the output TaskState collector service
      this.taskStateCollectorService.startAsync().awaitRunning();

      TimingEvent jobSubmissionTimer =
          this.eventSubmitter.getTimingEvent(TimingEvent.RunJobTimings.HELIX_JOB_SUBMISSION);

      synchronized (this.cancellationRequest) {
        if (!this.cancellationRequested) {
          long submitStart = System.currentTimeMillis();
          if (helixMetrics.isPresent()) {
            helixMetrics.get().submitMeter.mark();
          }
          submitJobToHelix(createHelixJob(workUnits));
          if (helixMetrics.isPresent()) {
            this.helixMetrics.get().updateTimeForHelixSubmit(submitStart);
          }
          jobSubmissionTimer.stop();
          LOGGER.info(String.format("Submitted job %s to Helix", this.jobContext.getJobId()));
          this.jobSubmitted = true;
        } else {
          LOGGER.warn("Job {} not submitted to Helix as it was requested to be cancelled.", this.jobContext.getJobId());
        }
      }

      TimingEvent jobRunTimer = this.eventSubmitter.getTimingEvent(TimingEvent.RunJobTimings.HELIX_JOB_RUN);
      long waitStart = System.currentTimeMillis();
      waitForJobCompletion();
      if (helixMetrics.isPresent()) {
        this.helixMetrics.get().updateTimeForHelixWait(waitStart);
      }
      jobRunTimer.stop();
      LOGGER.info(String.format("Job %s completed", this.jobContext.getJobId()));
    } finally {
      // The last iteration of output TaskState collecting will run when the collector service gets stopped
      this.taskStateCollectorService.stopAsync().awaitTerminated();
      cleanupWorkingDirectory();
    }
  }

  @Override
  protected void executeCancellation() {
    if (this.jobSubmitted) {
      try {
        if (this.cancellationRequested && !this.cancellationExecuted) {
          boolean cancelByDelete = ConfigUtils.getBoolean(jobConfig, GobblinClusterConfigurationKeys.CANCEL_HELIX_JOB_BY_DELETE,
              GobblinClusterConfigurationKeys.DEFAULT_CANCEL_HELIX_JOB_BY_DELETE);
          HelixUtils.cancelWorkflow(this.helixWorkFlowName, this.helixManager, helixJobStopTimeoutSeconds * 1000, cancelByDelete);
          log.info("Canceled the workflow {}", this.helixWorkFlowName);
        }
      } catch (RuntimeException e) {
        // Cancellation may throw an exception, but Helix set the job state to STOP/DELETE and it should eventually be cleaned up
        // We will keep this.cancellationExecuted and this.cancellationRequested to true and not propagate the exception
        log.error("Failed to cancel workflow {} in Helix", helixWorkFlowName, e);
      } catch (InterruptedException e) {
        log.error("Thread interrupted while trying to cancel the workflow {} in Helix", helixWorkFlowName);
        Thread.currentThread().interrupt();
      }
    }
  }

  protected void removeTasksFromCurrentJob(List<String> workUnitIdsToRemove) throws IOException, ExecutionException,
                                                                                    RetryException {
    String jobName = this.jobContext.getJobId();
    try (ParallelRunner stateSerDeRunner = new ParallelRunner(this.stateSerDeRunnerThreads, this.fs)) {
      for (String workUnitId : workUnitIdsToRemove) {
        taskRetryer.call(new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            String taskId = workUnitToHelixConfig.get(workUnitId).getId();
            boolean remove =
                HelixUtils.deleteTaskFromHelixJob(helixWorkFlowName, jobName, taskId, helixTaskDriver);
            if (remove) {
              log.info(String.format("Removed helix task %s with gobblin task id  %s from helix job %s:%s ", taskId,
                  workUnitId, helixWorkFlowName, jobName));
            } else {
              throw new IOException(
                  String.format("Cannot remove task %s from helix job %s:%s", workUnitId,
                      helixWorkFlowName, jobName));
            }
            return true;
          }
        });
        deleteWorkUnitFromStateStore(workUnitId, stateSerDeRunner);
        log.info(String.format("remove task state for %s in state store", workUnitId));
        this.workUnitToHelixConfig.remove(workUnitId);
      }
    }
  }

  protected void addTasksToCurrentJob(List<WorkUnit> workUnitsToAdd) throws IOException, ExecutionException,
                                                                            RetryException {
    String jobName = this.jobContext.getJobId();
    try (ParallelRunner stateSerDeRunner = new ParallelRunner(this.stateSerDeRunnerThreads, this.fs)) {
      for (WorkUnit workunit : workUnitsToAdd) {
        TaskConfig taskConfig = getTaskConfig(workunit, stateSerDeRunner);
        this.taskRetryer.call(new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            boolean added =
                HelixUtils.addTaskToHelixJob(helixWorkFlowName, jobName, taskConfig, helixTaskDriver);
            if (added) {
              log.info(
                  String.format("Added task %s to helix job %s:%s ", workunit.getId(), helixWorkFlowName, jobName));
            } else {
              log.error(
                  String.format("Failed to add task %s to helix job %s:%s ", workunit.getId(), helixWorkFlowName,
                      jobName));
              throw new IOException(
                  String.format("Cannot add task %s to helix job %s:%s", workunit.getId(),
                      helixWorkFlowName, jobName));
            }
            return true;
          }
        });
      }
    }
  }

  /**
   * Create a job from a given batch of {@link WorkUnit}s.
   */
  JobConfig.Builder createHelixJob(List<WorkUnit> workUnits) throws IOException {
    Map<String, TaskConfig> taskConfigMap = Maps.newHashMap();

    try (ParallelRunner stateSerDeRunner = new ParallelRunner(this.stateSerDeRunnerThreads, this.fs)) {
      int multiTaskIdSequence = 0;
      for (WorkUnit workUnit : workUnits) {
        if (workUnit instanceof MultiWorkUnit) {
          workUnit.setId(JobLauncherUtils.newMultiTaskId(this.jobContext.getJobId(), multiTaskIdSequence++));
        }
        addWorkUnit(workUnit, stateSerDeRunner, taskConfigMap);
      }

      Path jobStateFilePath;

      // write the job.state using the state store if present, otherwise serialize directly to the file
      if (this.stateStores.haveJobStateStore()) {
        jobStateFilePath = GobblinClusterUtils.getJobStateFilePath(true, this.appWorkDir, this.jobContext.getJobId());
        this.stateStores.getJobStateStore()
            .put(jobStateFilePath.getParent().getName(), jobStateFilePath.getName(), this.jobContext.getJobState());
      } else {
        jobStateFilePath = GobblinClusterUtils.getJobStateFilePath(false, this.appWorkDir, this.jobContext.getJobId());
        SerializationUtils.serializeState(this.fs, jobStateFilePath, this.jobContext.getJobState());
      }

      // Block on persistence of all workunits to be finished.
      // It is necessary when underlying storage being slow and Helix activate task-execution before the workunit being persisted.
      stateSerDeRunner.waitForTasks(Long.MAX_VALUE);

      LOGGER.debug("GobblinHelixJobLauncher.createHelixJob: jobStateFilePath {}, jobState {} jobProperties {}",
          jobStateFilePath, this.jobContext.getJobState().toString(), this.jobContext.getJobState().getProperties());

      return translateGobblinJobConfigToHelixJobConfig(this.jobContext.getJobState(), workUnits, taskConfigMap);
    }
  }

  /**
   * Populate {@link JobConfig.Builder} with relevant gobblin job-configurations.
   */
  JobConfig.Builder translateGobblinJobConfigToHelixJobConfig(JobState gobblinJobState, List<WorkUnit> workUnits,
      Map<String, TaskConfig> taskConfigMap) {
    JobConfig.Builder jobConfigBuilder = new JobConfig.Builder();

    // Helix task attempts = retries + 1 (fallback to general task retry for backward compatibility)
    jobConfigBuilder.setMaxAttemptsPerTask(
        gobblinJobState.getPropAsInt(GobblinClusterConfigurationKeys.HELIX_TASK_MAX_ATTEMPTS_KEY,
            gobblinJobState.getPropAsInt(ConfigurationKeys.MAX_TASK_RETRIES_KEY,
                ConfigurationKeys.DEFAULT_MAX_TASK_RETRIES)) + 1);

    // Helix task timeout (fallback to general task timeout for backward compatibility)
    jobConfigBuilder.setTimeoutPerTask(
        gobblinJobState.getPropAsLong(GobblinClusterConfigurationKeys.HELIX_TASK_TIMEOUT_SECONDS,
            gobblinJobState.getPropAsLong(ConfigurationKeys.TASK_TIMEOUT_SECONDS,
                ConfigurationKeys.DEFAULT_TASK_TIMEOUT_SECONDS)) * 1000);

    jobConfigBuilder.setFailureThreshold(workUnits.size());
    jobConfigBuilder.addTaskConfigMap(taskConfigMap).setCommand(GobblinTaskRunner.GOBBLIN_TASK_FACTORY_NAME);
    jobConfigBuilder.setNumConcurrentTasksPerInstance(
        ConfigUtils.getInt(jobConfig, GobblinClusterConfigurationKeys.HELIX_CLUSTER_TASK_CONCURRENCY,
            GobblinClusterConfigurationKeys.HELIX_CLUSTER_TASK_CONCURRENCY_DEFAULT));

    if (this.jobConfig.hasPath(GobblinClusterConfigurationKeys.HELIX_JOB_TAG_KEY)) {
      String jobTag = this.jobConfig.getString(GobblinClusterConfigurationKeys.HELIX_JOB_TAG_KEY);
      log.info("Job {} has tags associated : {}", this.jobContext.getJobId(), jobTag);
      jobConfigBuilder.setInstanceGroupTag(jobTag);
    }

    if (this.jobConfig.hasPath(GobblinClusterConfigurationKeys.HELIX_JOB_TYPE_KEY)) {
      String jobType = this.jobConfig.getString(GobblinClusterConfigurationKeys.HELIX_JOB_TYPE_KEY);
      log.info("Job {} has types associated : {}", this.jobContext.getJobId(), jobType);
      jobConfigBuilder.setJobType(jobType);
    }

    if (Task.getExecutionModel(ConfigUtils.configToState(jobConfig)).equals(ExecutionModel.STREAMING)) {
      jobConfigBuilder.setRebalanceRunningTask(true);
    }

    jobConfigBuilder.setExpiry(
        gobblinJobState.getPropAsLong(GobblinClusterConfigurationKeys.HELIX_WORKFLOW_EXPIRY_TIME_SECONDS,
            GobblinClusterConfigurationKeys.DEFAULT_HELIX_WORKFLOW_EXPIRY_TIME_SECONDS));

    Map<String, String> jobConfigMap = new HashMap<>();
    if (this.jobConfig.hasPath(GobblinClusterConfigurationKeys.HELIX_JOB_CONTAINER_MEMORY_MBS)) {
      jobConfigMap.put(GobblinClusterConfigurationKeys.HELIX_JOB_CONTAINER_MEMORY_MBS,
          jobConfig.getString(GobblinClusterConfigurationKeys.HELIX_JOB_CONTAINER_MEMORY_MBS));
      log.info("Job {} has specific memory requirement:{}, add this config to command config map",
          this.jobContext.getJobId(), jobConfig.getString(GobblinClusterConfigurationKeys.HELIX_JOB_CONTAINER_MEMORY_MBS));
    }
    if (this.jobConfig.hasPath(GobblinClusterConfigurationKeys.HELIX_JOB_CONTAINER_CORES)) {
      jobConfigMap.put(GobblinClusterConfigurationKeys.HELIX_JOB_CONTAINER_CORES,
          jobConfig.getString(GobblinClusterConfigurationKeys.HELIX_JOB_CONTAINER_CORES));
      log.info("Job {} has specific Vcore requirement:{}, add this config to command config map",
          this.jobContext.getJobId(), jobConfig.getString(GobblinClusterConfigurationKeys.HELIX_JOB_CONTAINER_CORES));
    }
    jobConfigBuilder.setJobCommandConfigMap(jobConfigMap);
    return jobConfigBuilder;
  }

  /**
   * Submit a job to run.
   */
  private void submitJobToHelix(JobConfig.Builder jobConfigBuilder) throws Exception {
    HelixUtils.submitJobToWorkFlow(jobConfigBuilder, this.helixWorkFlowName, this.jobContext.getJobId(),
        this.helixTaskDriver, this.helixManager,
        Duration.ofSeconds(this.workFlowExpiryTimeSeconds),
        Duration.ofSeconds(this.helixWorkflowSubmissionTimeoutSeconds));
  }

  public void launchJob(@Nullable JobListener jobListener) throws JobException {
    this.jobListener = jobListener;
    boolean isLaunched = false;
    this.runningMap.putIfAbsent(this.jobContext.getJobName(), false);

    Throwable errorInJobLaunching = null;
    try {
      if (this.runningMap.replace(this.jobContext.getJobName(), false, true)) {
        LOGGER.info("Job {} will be executed, add into running map.", this.jobContext.getJobId());
        isLaunched = true;
        launchJobImpl(jobListener);
      } else {
        LOGGER.warn("Job {} will not be executed because other jobs are still running.", this.jobContext.getJobId());
      }

      // TODO: Better error handling. The current impl swallows exceptions for jobs that were started by this method call.
      // One potential way to improve the error handling is to make this error swallowing configurable
    } catch (Throwable t) {
      errorInJobLaunching = t;
      if (isLaunched) {
        // Attempts to cancel the helix workflow if an error occurs during launch
        cancelJob(jobListener);
      }
    } finally {
      if (isLaunched) {
        if (this.runningMap.replace(this.jobContext.getJobName(), true, false)) {
          LOGGER.info("Job {} is done, remove from running map.", this.jobContext.getJobId());
        } else {
          throw errorInJobLaunching == null ? new IllegalStateException(
              "A launched job should have running state equal to true in the running map.")
              : new RuntimeException("Failure in launching job:", errorInJobLaunching);
        }
      }
    }
  }


  /**
   * This method looks silly at first glance but exists for a reason.
   *
   * The method {@link GobblinHelixJobLauncher#launchJob(JobListener)} contains boiler plate for handling exceptions and
   * mutating the runningMap to communicate state back to the {@link GobblinHelixJobScheduler}. The boiler plate swallows
   * exceptions when launching the job because many use cases require that 1 job failure should not affect other jobs by causing the
   * entire process to fail through an uncaught exception.
   *
   * This method is useful for unit testing edge cases where we expect {@link JobException}s during the underlying launch operation.
   * It would be nice to not swallow exceptions, but the implications of doing that will require careful refactoring since
   * the class {@link GobblinHelixJobLauncher} and {@link GobblinHelixJobScheduler} are shared for 2 quite different cases
   * between GaaS and streaming. GaaS typically requiring many short lifetime workflows (where a failure is tolerated) and
   * streaming requiring a small number of long running workflows (where failure to submit is unexpected and is not
   * tolerated)
   *
   * @throws JobException
   */
  @VisibleForTesting
  void launchJobImpl(@Nullable JobListener jobListener) throws JobException {
    super.launchJob(jobListener);
  }

  private TaskConfig getTaskConfig(WorkUnit workUnit, ParallelRunner stateSerDeRunner) throws IOException {
    String workUnitFilePath =
        persistWorkUnit(new Path(this.inputWorkUnitDir, this.jobContext.getJobId()), workUnit, stateSerDeRunner);

    Map<String, String> rawConfigMap = Maps.newHashMap();
    rawConfigMap.put(GobblinClusterConfigurationKeys.WORK_UNIT_FILE_PATH, workUnitFilePath);
    rawConfigMap.put(ConfigurationKeys.JOB_NAME_KEY, this.jobContext.getJobName());
    rawConfigMap.put(ConfigurationKeys.JOB_ID_KEY, this.jobContext.getJobId());
    rawConfigMap.put(ConfigurationKeys.TASK_ID_KEY, workUnit.getId());
    rawConfigMap.put(GobblinClusterConfigurationKeys.TASK_SUCCESS_OPTIONAL_KEY, "true");
    TaskConfig taskConfig = TaskConfig.Builder.from(rawConfigMap);
    workUnitToHelixConfig.put(workUnit.getId(), taskConfig);
    return taskConfig;
  }

  /**
   * Add a single {@link WorkUnit} (flattened) to persistent storage so that worker can fetch that based on information
   * fetched in Helix task.
   */
  private void addWorkUnit(WorkUnit workUnit, ParallelRunner stateSerDeRunner, Map<String, TaskConfig> taskConfigMap)
      throws IOException {
    taskConfigMap.put(workUnit.getId(), getTaskConfig(workUnit, stateSerDeRunner));
  }

  /**
   * Delete a single {@link WorkUnit} (flattened) from state store.
   */
  private void deleteWorkUnitFromStateStore(String workUnitId, ParallelRunner stateSerDeRunner) {
    String workUnitFilePath =
        workUnitToHelixConfig.get(workUnitId).getConfigMap().get(GobblinClusterConfigurationKeys.WORK_UNIT_FILE_PATH);
    final StateStore stateStore;
    Path workUnitFile = new Path(workUnitFilePath);
    final String fileName = workUnitFile.getName();
    final String storeName = workUnitFile.getParent().getName();
    if (fileName.endsWith(JobLauncherUtils.MULTI_WORK_UNIT_FILE_EXTENSION)) {
      stateStore = stateStores.getMwuStateStore();
    } else {
      stateStore = stateStores.getWuStateStore();
    }
    stateSerDeRunner.submitCallable(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        stateStore.delete(storeName, fileName);
        return null;
      }
    }, "Delete state " + fileName + " from store " + storeName);
  }

  /**
   * Persist a single {@link WorkUnit} (flattened) to a file.
   */
  private String persistWorkUnit(final Path workUnitFileDir, final WorkUnit workUnit, ParallelRunner stateSerDeRunner)
      throws IOException {
    final StateStore stateStore;
    String workUnitFileName = workUnit.getId();

    if (workUnit instanceof MultiWorkUnit) {
      workUnitFileName += JobLauncherUtils.MULTI_WORK_UNIT_FILE_EXTENSION;
      stateStore = stateStores.getMwuStateStore();
    } else {
      workUnitFileName += JobLauncherUtils.WORK_UNIT_FILE_EXTENSION;
      stateStore = stateStores.getWuStateStore();
    }

    Path workUnitFile = new Path(workUnitFileDir, workUnitFileName);
    final String fileName = workUnitFile.getName();
    final String storeName = workUnitFile.getParent().getName();
    stateSerDeRunner.submitCallable(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        stateStore.put(storeName, fileName, workUnit);
        return null;
      }
    }, "Serialize state to store " + storeName + " file " + fileName);

    return workUnitFile.toString();
  }

  private void waitForJobCompletion() throws InterruptedException {
    boolean timeoutEnabled = Boolean.parseBoolean(
        this.jobProps.getProperty(GobblinClusterConfigurationKeys.HELIX_JOB_TIMEOUT_ENABLED_KEY,
            GobblinClusterConfigurationKeys.DEFAULT_HELIX_JOB_TIMEOUT_ENABLED));
    long timeoutInSeconds = Long.parseLong(
        this.jobProps.getProperty(GobblinClusterConfigurationKeys.HELIX_JOB_TIMEOUT_SECONDS,
            GobblinClusterConfigurationKeys.DEFAULT_HELIX_JOB_TIMEOUT_SECONDS));

    long stoppingStateTimeoutInSeconds = PropertiesUtils.getPropAsLong(this.jobProps,
        GobblinClusterConfigurationKeys.HELIX_JOB_STOPPING_STATE_TIMEOUT_SECONDS,
        GobblinClusterConfigurationKeys.DEFAULT_HELIX_JOB_STOPPING_STATE_TIMEOUT_SECONDS);

    try {
      HelixUtils.waitJobCompletion(this.helixManager, this.helixWorkFlowName, this.jobContext.getJobId(),
          timeoutEnabled ? Optional.of(timeoutInSeconds) : Optional.empty(), stoppingStateTimeoutInSeconds);
    } catch (TimeoutException te) {
      HelixUtils.handleJobTimeout(helixWorkFlowName, jobContext.getJobId(), helixManager, this, this.jobListener);
    }
  }

  /**
   * Delete persisted {@link WorkUnit}s and {@link JobState} upon job completion.
   */
  private void cleanupWorkingDirectory() throws IOException {
    LOGGER.info("Deleting persisted work units for job " + this.jobContext.getJobId());
    stateStores.getWuStateStore().delete(this.jobContext.getJobId());

    // delete the directory that stores the task state files
    stateStores.getTaskStateStore().delete(outputTaskStateDir.getName());

    LOGGER.info("Deleting job state file for job " + this.jobContext.getJobId());

    if (this.stateStores.haveJobStateStore()) {
      this.stateStores.getJobStateStore().delete(this.jobContext.getJobId());
    } else {
      Path jobStateFilePath =
          GobblinClusterUtils.getJobStateFilePath(false, this.appWorkDir, this.jobContext.getJobId());
      this.fs.delete(jobStateFilePath, false);
    }
  }
}
