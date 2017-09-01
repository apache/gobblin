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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.gobblin.runtime.JobException;
import org.apache.gobblin.runtime.listeners.JobListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.IdealStateChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.IdealState;
import org.apache.helix.task.GobblinJobRebalancer;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.WorkflowContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.rest.LauncherTypeEnum;
import org.apache.gobblin.runtime.AbstractJobLauncher;
import org.apache.gobblin.runtime.ExecutionModel;
import org.apache.gobblin.runtime.JobLauncher;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.Task;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.runtime.TaskStateCollectorService;
import org.apache.gobblin.runtime.util.StateStores;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.Id;
import org.apache.gobblin.util.JobLauncherUtils;
import org.apache.gobblin.util.ParallelRunner;
import org.apache.gobblin.util.SerializationUtils;

import javax.annotation.Nullable;


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
 *   This class runs in the {@link GobblinClusterManager}. The actual task execution happens in the in the
 *   {@link GobblinTaskRunner}.
 * </p>
 *
 * @author Yinan Li
 */
@Alpha
public class GobblinHelixJobLauncher extends AbstractJobLauncher {

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinHelixJobLauncher.class);

  private static final String WORK_UNIT_FILE_EXTENSION = ".wu";

  private final HelixManager helixManager;
  private final TaskDriver helixTaskDriver;
  private final String helixQueueName;
  private final String jobResourceName;

  private final FileSystem fs;
  private final Path appWorkDir;
  private final Path inputWorkUnitDir;
  private final Path outputTaskStateDir;

  // Number of ParallelRunner threads to be used for state serialization/deserialization
  private final int stateSerDeRunnerThreads;

  private final TaskStateCollectorService taskStateCollectorService;

  private volatile boolean jobSubmitted = false;
  private volatile boolean jobComplete = false;
  private final ConcurrentHashMap<String, Boolean> runningMap;
  private final StateStores stateStores;
  private final Config jobConfig;

  public GobblinHelixJobLauncher(Properties jobProps, final HelixManager helixManager, Path appWorkDir,
      List<? extends Tag<?>> metadataTags, ConcurrentHashMap<String, Boolean> runningMap)
      throws Exception {
    super(jobProps, addAdditionalMetadataTags(jobProps, metadataTags));

    this.helixManager = helixManager;
    this.helixTaskDriver = new TaskDriver(this.helixManager);
    this.runningMap = runningMap;
    this.appWorkDir = appWorkDir;
    this.inputWorkUnitDir = new Path(appWorkDir, GobblinClusterConfigurationKeys.INPUT_WORK_UNIT_DIR_NAME);
    this.outputTaskStateDir = new Path(this.appWorkDir, GobblinClusterConfigurationKeys.OUTPUT_TASK_STATE_DIR_NAME +
        Path.SEPARATOR + this.jobContext.getJobId());

    this.helixQueueName = this.jobContext.getJobName();
    this.jobResourceName = TaskUtil.getNamespacedJobName(this.helixQueueName, this.jobContext.getJobId());

    this.jobContext.getJobState().setJobLauncherType(LauncherTypeEnum.CLUSTER);

    this.stateSerDeRunnerThreads = Integer.parseInt(jobProps.getProperty(ParallelRunner.PARALLEL_RUNNER_THREADS_KEY,
        Integer.toString(ParallelRunner.DEFAULT_PARALLEL_RUNNER_THREADS)));

    jobConfig = ConfigUtils.propertiesToConfig(jobProps);

    Config stateStoreJobConfig = ConfigUtils.propertiesToConfig(jobProps)
        .withValue(ConfigurationKeys.STATE_STORE_FS_URI_KEY, ConfigValueFactory.fromAnyRef(
            new URI(appWorkDir.toUri().getScheme(), null, appWorkDir.toUri().getHost(),
                appWorkDir.toUri().getPort(), null, null, null).toString()));

    this.stateStores = new StateStores(stateStoreJobConfig, appWorkDir,
        GobblinClusterConfigurationKeys.OUTPUT_TASK_STATE_DIR_NAME, appWorkDir,
        GobblinClusterConfigurationKeys.INPUT_WORK_UNIT_DIR_NAME);

    URI fsUri = URI.create(jobProps.getProperty(ConfigurationKeys.FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI));
    this.fs = FileSystem.get(fsUri, new Configuration());

    this.taskStateCollectorService = new TaskStateCollectorService(jobProps, this.jobContext.getJobState(),
        this.eventBus, this.stateStores.taskStateStore, outputTaskStateDir);

    if (Task.getExecutionModel(ConfigUtils.configToState(jobConfig)).equals(ExecutionModel.STREAMING)) {
      // Fix-up Ideal State with a custom rebalancer that will re-balance long-running jobs
      final String clusterName =
          ConfigUtils.getString(jobConfig, GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY, "");
      final String rebalancerToReplace = "org.apache.helix.task.JobRebalancer";
      final String rebalancerClassDesired = GobblinJobRebalancer.class.getName();
      final String jobResourceName = this.jobResourceName;

      if (!clusterName.isEmpty()) {
        this.helixManager.addIdealStateChangeListener(new IdealStateChangeListener() {
          @Override
          public void onIdealStateChange(List<IdealState> list, NotificationContext notificationContext) {
            HelixAdmin helixAdmin = helixManager.getClusterManagmentTool();
            for (String resource : helixAdmin.getResourcesInCluster(clusterName)) {
              if (resource.equals(jobResourceName)) {
                IdealState idealState = helixAdmin.getResourceIdealState(clusterName, resource);
                if (idealState != null) {
                  String rebalancerClassFound = idealState.getRebalancerClassName();
                  if (rebalancerToReplace.equals(rebalancerClassFound)) {
                    idealState.setRebalancerClassName(rebalancerClassDesired);
                    helixAdmin.setResourceIdealState(clusterName, resource, idealState);
                  }
                }
              }
            }
          }
        });
      }
    }
  }

  @Override
  public void close() throws IOException {
    try {
      executeCancellation();
    } finally {
      super.close();
    }
  }

  @Override
  protected void runWorkUnits(List<WorkUnit> workUnits) throws Exception {
    try {
      // Start the output TaskState collector service
      this.taskStateCollectorService.startAsync().awaitRunning();

      TimingEvent jobSubmissionTimer =
          this.eventSubmitter.getTimingEvent(TimingEvent.RunJobTimings.HELIX_JOB_SUBMISSION);
      submitJobToHelix(createJob(workUnits));
      jobSubmissionTimer.stop();
      LOGGER.info(String.format("Submitted job %s to Helix", this.jobContext.getJobId()));
      this.jobSubmitted = true;

      TimingEvent jobRunTimer = this.eventSubmitter.getTimingEvent(TimingEvent.RunJobTimings.HELIX_JOB_RUN);
      waitForJobCompletion();
      jobRunTimer.stop();
      LOGGER.info(String.format("Job %s completed", this.jobContext.getJobId()));
      this.jobComplete = true;
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
        // #HELIX-0.6.7-WORKAROUND
        // working around helix 0.6.7 job delete issue with custom taskDriver
        GobblinHelixTaskDriver taskDriver = new GobblinHelixTaskDriver(this.helixManager);
        taskDriver.deleteJob(this.helixQueueName, this.jobContext.getJobId());
      } catch (IllegalArgumentException e) {
        LOGGER.warn(String.format("Failed to cleanup job %s in Helix", this.jobContext.getJobId()), e);
      }
    }
  }

  /**
   * Create a job from a given batch of {@link WorkUnit}s.
   */
  private JobConfig.Builder createJob(List<WorkUnit> workUnits) throws IOException {
    Map<String, TaskConfig> taskConfigMap = Maps.newHashMap();

    try (ParallelRunner stateSerDeRunner = new ParallelRunner(this.stateSerDeRunnerThreads, this.fs)) {
      int multiTaskIdSequence = 0;
      for (WorkUnit workUnit : workUnits) {
        if (workUnit instanceof MultiWorkUnit) {
          workUnit.setId(JobLauncherUtils.newMultiTaskId(this.jobContext.getJobId(), multiTaskIdSequence++));
        }
        addWorkUnit(workUnit, stateSerDeRunner, taskConfigMap);
      }

      Path jobStateFilePath = new Path(this.appWorkDir, this.jobContext.getJobId() + "." + JOB_STATE_FILE_NAME);
      SerializationUtils.serializeState(this.fs, jobStateFilePath, this.jobContext.getJobState());
    }

    JobConfig.Builder jobConfigBuilder = new JobConfig.Builder();
    jobConfigBuilder.setMaxAttemptsPerTask(this.jobContext.getJobState().getPropAsInt(
        ConfigurationKeys.MAX_TASK_RETRIES_KEY, ConfigurationKeys.DEFAULT_MAX_TASK_RETRIES));
    jobConfigBuilder.setFailureThreshold(workUnits.size());
    jobConfigBuilder.addTaskConfigMap(taskConfigMap).setCommand(GobblinTaskRunner.GOBBLIN_TASK_FACTORY_NAME);
    jobConfigBuilder.setNumConcurrentTasksPerInstance(ConfigUtils.getInt(jobConfig,
        GobblinClusterConfigurationKeys.HELIX_CLUSTER_TASK_CONCURRENCY,
        GobblinClusterConfigurationKeys.HELIX_CLUSTER_TASK_CONCURRENCY_DEFAULT));
    return jobConfigBuilder;
  }

  /**
   * Submit a job to run.
   */
  private void submitJobToHelix(JobConfig.Builder jobConfigBuilder) throws Exception {
    // Create one queue for each job with the job name being the queue name
    if (null == this.helixTaskDriver.getWorkflowConfig(this.helixManager, this.helixQueueName)) {
      JobQueue jobQueue = new JobQueue.Builder(this.helixQueueName).build();
      this.helixTaskDriver.createQueue(jobQueue);
      LOGGER.info("Created job queue {}", this.helixQueueName);
    } else {
      LOGGER.info("Job queue {} already exists", this.helixQueueName);
    }

    // Put the job into the queue
    this.helixTaskDriver.enqueueJob(this.jobContext.getJobName(), this.jobContext.getJobId(), jobConfigBuilder);

  }

  public void launchJob(@Nullable JobListener jobListener)
      throws JobException {
    boolean isLaunched = false;
    this.runningMap.putIfAbsent(this.jobContext.getJobName(), false);
    try {
      if (this.runningMap.replace(this.jobContext.getJobName(), false, true)) {
        LOGGER.info ("Job {} will be executed, add into running map.", this.jobContext.getJobId());
        isLaunched = true;
        super.launchJob(jobListener);
      } else {
        LOGGER.warn ("Job {} will not be executed because other jobs are still running.", this.jobContext.getJobId());
      }
    } finally {
      if (isLaunched) {
        if (this.runningMap.replace(this.jobContext.getJobName(), true, false)) {
          LOGGER.info ("Job {} is done, remove from running map.", this.jobContext.getJobId());
        } else {
          throw new IllegalStateException("A launched job should have running state equal to true in the running map.");
        }
      }
    }
  }

  /**
   * Add a single {@link WorkUnit} (flattened).
   */
  private void addWorkUnit(WorkUnit workUnit, ParallelRunner stateSerDeRunner,
      Map<String, TaskConfig> taskConfigMap) throws IOException {
    String workUnitFilePath = persistWorkUnit(
        new Path(this.inputWorkUnitDir, this.jobContext.getJobId()), workUnit, stateSerDeRunner);

    Map<String, String> rawConfigMap = Maps.newHashMap();
    rawConfigMap.put(GobblinClusterConfigurationKeys.WORK_UNIT_FILE_PATH, workUnitFilePath);
    rawConfigMap.put(ConfigurationKeys.JOB_NAME_KEY, this.jobContext.getJobName());
    rawConfigMap.put(ConfigurationKeys.JOB_ID_KEY, this.jobContext.getJobId());
    rawConfigMap.put(ConfigurationKeys.TASK_ID_KEY, workUnit.getId());
    rawConfigMap.put(GobblinClusterConfigurationKeys.TASK_SUCCESS_OPTIONAL_KEY, "true");

    taskConfigMap.put(workUnit.getId(), TaskConfig.Builder.from(rawConfigMap));
  }

  /**
   * Persist a single {@link WorkUnit} (flattened) to a file.
   */
  private String persistWorkUnit(final Path workUnitFileDir, final WorkUnit workUnit, ParallelRunner stateSerDeRunner)
      throws IOException {
    final StateStore stateStore;
    String workUnitFileName = workUnit.getId();

    if (workUnit instanceof MultiWorkUnit) {
      workUnitFileName += MULTI_WORK_UNIT_FILE_EXTENSION;
      stateStore = stateStores.mwuStateStore;
    } else {
      workUnitFileName += WORK_UNIT_FILE_EXTENSION;
      stateStore = stateStores.wuStateStore;
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
    while (true) {
      WorkflowContext workflowContext = TaskDriver.getWorkflowContext(this.helixManager, this.helixQueueName);
      if (workflowContext != null) {
        org.apache.helix.task.TaskState helixJobState = workflowContext.getJobState(this.jobResourceName);
        if (helixJobState == org.apache.helix.task.TaskState.COMPLETED ||
            helixJobState == org.apache.helix.task.TaskState.FAILED ||
            helixJobState == org.apache.helix.task.TaskState.STOPPED) {
          return;
        }
      }

      Thread.sleep(1000);
    }
  }

  /**
   * Delete persisted {@link WorkUnit}s and {@link JobState} upon job completion.
   */
  private void cleanupWorkingDirectory() throws IOException {
    LOGGER.info("Deleting persisted work units for job " + this.jobContext.getJobId());
    stateStores.wuStateStore.delete(this.jobContext.getJobId());
    LOGGER.info("Deleting job state file for job " + this.jobContext.getJobId());
    Path jobStateFilePath = new Path(this.appWorkDir, this.jobContext.getJobId() + "." + JOB_STATE_FILE_NAME);
    this.fs.delete(jobStateFilePath, false);
  }

  /**
   * Inject in some additional properties
   * @param jobProps job properties
   * @param inputTags list of metadata tags
   * @return
   */
  private static List<? extends Tag<?>> addAdditionalMetadataTags(Properties jobProps, List<? extends Tag<?>> inputTags) {
    List<Tag<?>> metadataTags = Lists.newArrayList(inputTags);
    String jobId;

    // generate job id if not already set
    if (jobProps.containsKey(ConfigurationKeys.JOB_ID_KEY)) {
      jobId = jobProps.getProperty(ConfigurationKeys.JOB_ID_KEY);
    } else {
      jobId = JobLauncherUtils.newJobId(JobState.getJobNameFromProps(jobProps));
      jobProps.put(ConfigurationKeys.JOB_ID_KEY, jobId);
    }

    String jobExecutionId = Long.toString(Id.Job.parse(jobId).getSequence());

    // only inject flow tags if a flow name is defined
    if (jobProps.containsKey(ConfigurationKeys.FLOW_NAME_KEY)) {
      metadataTags.add(new Tag<>(GobblinClusterMetricTagNames.FLOW_GROUP,
          jobProps.getProperty(ConfigurationKeys.FLOW_GROUP_KEY, "")));
      metadataTags.add(
          new Tag<>(GobblinClusterMetricTagNames.FLOW_NAME, jobProps.getProperty(ConfigurationKeys.FLOW_NAME_KEY)));

      // use job execution id if flow execution id is not present
      metadataTags.add(new Tag<>(GobblinClusterMetricTagNames.FLOW_EXECUTION_ID,
          jobProps.getProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, jobExecutionId)));
    }

    metadataTags.add(new Tag<>(GobblinClusterMetricTagNames.JOB_GROUP,
        jobProps.getProperty(ConfigurationKeys.JOB_GROUP_KEY, "")));
    metadataTags.add(new Tag<>(GobblinClusterMetricTagNames.JOB_NAME,
        jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY, "")));
    metadataTags.add(new Tag<>(GobblinClusterMetricTagNames.JOB_EXECUTION_ID, jobExecutionId));

    return metadataTags;
  }
}
