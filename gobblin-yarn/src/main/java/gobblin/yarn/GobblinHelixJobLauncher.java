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

package gobblin.yarn;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.helix.HelixManager;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.WorkflowContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.Tag;
import gobblin.metrics.event.TimingEvent;
import gobblin.rest.LauncherTypeEnum;
import gobblin.runtime.AbstractJobLauncher;
import gobblin.runtime.FileBasedJobLock;
import gobblin.runtime.JobLauncher;
import gobblin.runtime.JobLock;
import gobblin.runtime.TaskState;
import gobblin.runtime.TaskStateCollectorService;
import gobblin.runtime.util.TimingEventNames;
import gobblin.source.workunit.MultiWorkUnit;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.JobLauncherUtils;
import gobblin.util.ParallelRunner;
import gobblin.util.SerializationUtils;


/**
 * An implementation of {@link JobLauncher} that launches a Gobblin job on Yarn using the Helix task framework.
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
 *   user-defined property {@link GobblinYarnConfigurationKeys#WORK_UNIT_FILE_PATH}. Upon startup, the Helix
 *   task reads the property for the file path and de-serializes the {@link WorkUnit} from the file.
 * </p>
 *
 * <p>
 *   This class runs in the {@link GobblinApplicationMaster}. The actual task execution happens in the Yarn
 *   containers and is managed by the {@link GobblinWorkUnitRunner}.
 * </p>
 *
 * @author Yinan Li
 */
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

  public GobblinHelixJobLauncher(Properties jobProps, HelixManager helixManager, FileSystem fs, Path appWorkDir,
      List<? extends Tag<?>> metadataTags)
      throws Exception {
    super(jobProps, metadataTags);

    this.helixManager = helixManager;
    this.helixTaskDriver = new TaskDriver(this.helixManager);

    this.fs = fs;
    this.appWorkDir = appWorkDir;
    this.inputWorkUnitDir = new Path(appWorkDir, GobblinYarnConfigurationKeys.INPUT_WORK_UNIT_DIR_NAME);
    this.outputTaskStateDir = new Path(this.appWorkDir, GobblinYarnConfigurationKeys.OUTPUT_TASK_STATE_DIR_NAME +
        Path.SEPARATOR + this.jobContext.getJobId());

    this.helixQueueName = this.jobContext.getJobName();
    this.jobResourceName = TaskUtil.getNamespacedJobName(this.helixQueueName, this.jobContext.getJobId());

    this.jobContext.getJobState().setJobLauncherType(LauncherTypeEnum.YARN);

    this.stateSerDeRunnerThreads = Integer.parseInt(jobProps.getProperty(ParallelRunner.PARALLEL_RUNNER_THREADS_KEY,
        Integer.toString(ParallelRunner.DEFAULT_PARALLEL_RUNNER_THREADS)));

    this.taskStateCollectorService = new TaskStateCollectorService(jobProps, this.jobContext.getJobState(),
        this.eventBus, this.fs, outputTaskStateDir);
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
          this.eventSubmitter.getTimingEvent(TimingEventNames.RunJobTimings.HELIX_JOB_SUBMISSION);
      submitJobToHelix(createJob(workUnits));
      jobSubmissionTimer.stop();
      LOGGER.info(String.format("Submitted job %s to Helix", this.jobContext.getJobId()));
      this.jobSubmitted = true;

      TimingEvent jobRunTimer = this.eventSubmitter.getTimingEvent(TimingEventNames.RunJobTimings.HELIX_JOB_RUN);
      waitForJobCompletion();
      jobRunTimer.stop();
      LOGGER.info(String.format("Job %s completed", this.jobContext.getJobId()));
      this.jobComplete = true;
    } finally {
      // The last iteration of output TaskState collecting will run when the collector service gets stopped
      this.taskStateCollectorService.stopAsync().awaitTerminated();
      deletePersistedWorkUnitsForJob();
    }
  }

  @Override
  protected JobLock getJobLock() throws IOException {
    return new FileBasedJobLock(this.fs, this.jobProps.getProperty(ConfigurationKeys.JOB_LOCK_DIR_KEY),
        this.jobContext.getJobName());
  }

  @Override
  protected void executeCancellation() {
    if (this.jobSubmitted && !this.jobComplete) {
      this.helixTaskDriver.deleteJob(this.helixQueueName, this.jobContext.getJobId());
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
    jobConfigBuilder.addTaskConfigMap(taskConfigMap).setCommand(GobblinWorkUnitRunner.GOBBLIN_TASK_FACTORY_NAME);

    return jobConfigBuilder;
  }

  /**
   * Submit a job to run.
   */
  private void submitJobToHelix(JobConfig.Builder jobConfigBuilder) throws Exception {
    // Create one queue for each job with the job name being the queue name
    JobQueue jobQueue = new JobQueue.Builder(this.helixQueueName).build();
    try {
      this.helixTaskDriver.createQueue(jobQueue);
    } catch (IllegalArgumentException iae) {
      LOGGER.info(String.format("Job queue %s already exists", jobQueue.getName()));
    }

    // Put the job into the queue
    this.helixTaskDriver.enqueueJob(this.jobContext.getJobName(), this.jobContext.getJobId(), jobConfigBuilder);
  }

  /**
   * Add a single {@link WorkUnit} (flattened).
   */
  private void addWorkUnit(WorkUnit workUnit, ParallelRunner stateSerDeRunner,
      Map<String, TaskConfig> taskConfigMap) throws IOException {
    String workUnitFilePath = persistWorkUnit(
        new Path(this.inputWorkUnitDir, this.jobContext.getJobId()), workUnit, stateSerDeRunner);

    Map<String, String> rawConfigMap = Maps.newHashMap();
    rawConfigMap.put(GobblinYarnConfigurationKeys.WORK_UNIT_FILE_PATH, workUnitFilePath);
    rawConfigMap.put(ConfigurationKeys.JOB_NAME_KEY, this.jobContext.getJobName());
    rawConfigMap.put(ConfigurationKeys.JOB_ID_KEY, this.jobContext.getJobId());
    rawConfigMap.put(ConfigurationKeys.TASK_ID_KEY, workUnit.getId());
    rawConfigMap.put(GobblinYarnConfigurationKeys.TASK_SUCCESS_OPTIONAL_KEY, "true");

    taskConfigMap.put(workUnit.getId(), TaskConfig.from(rawConfigMap));
  }

  /**
   * Persist a single {@link WorkUnit} (flattened) to a file.
   */
  private String persistWorkUnit(Path workUnitFileDir, WorkUnit workUnit, ParallelRunner stateSerDeRunner)
      throws IOException {
    String workUnitFileName = workUnit.getId() + (workUnit instanceof MultiWorkUnit ?
        MULTI_WORK_UNIT_FILE_EXTENSION : WORK_UNIT_FILE_EXTENSION);
    Path workUnitFile = new Path(workUnitFileDir, workUnitFileName);
    stateSerDeRunner.serializeToFile(workUnit, workUnitFile);
    return workUnitFile.toString();
  }

  private void waitForJobCompletion() throws InterruptedException {
    while (true) {
      WorkflowContext workflowContext = TaskUtil.getWorkflowContext(this.helixManager, this.helixQueueName);
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
   * Delete persisted {@link WorkUnit}s upon job completion.
   */
  private void deletePersistedWorkUnitsForJob() throws IOException {
    Path workUnitDir = new Path(this.inputWorkUnitDir, this.jobContext.getJobId());
    if (this.fs.exists(workUnitDir)) {
      LOGGER.info("Deleting persisted work units under " + workUnitDir);
      this.fs.delete(workUnitDir, true);
    }
  }
}
