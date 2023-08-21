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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.cluster.temporal.GobblinTemporalWorkflow;
import org.apache.gobblin.cluster.temporal.Shared;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.CountEventBuilder;
import org.apache.gobblin.metrics.event.JobEvent;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.rest.LauncherTypeEnum;
import org.apache.gobblin.runtime.AbstractJobLauncher;
import org.apache.gobblin.runtime.JobException;
import org.apache.gobblin.runtime.JobLauncher;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskStateCollectorService;
import org.apache.gobblin.runtime.listeners.JobListener;
import org.apache.gobblin.runtime.util.StateStores;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaSource;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.Id;
import org.apache.gobblin.util.JobLauncherUtils;
import org.apache.gobblin.util.ParallelRunner;
import org.apache.gobblin.util.PropertiesUtils;
import org.apache.gobblin.util.SerializationUtils;

import static org.apache.gobblin.cluster.GobblinTemporalClusterManager.createServiceStubs;


/**
 * An implementation of {@link JobLauncher} that launches a Gobblin job using the Temporal task framework.
 *
 * <p>
 *   Each {@link WorkUnit} of the job is persisted to the {@link FileSystem} of choice and the path to the file
 *   storing the serialized {@link WorkUnit} is passed to the Temporal task running the {@link WorkUnit} as a
 *   user-defined property {@link GobblinClusterConfigurationKeys#WORK_UNIT_FILE_PATH}. Upon startup, the gobblin
 *   task reads the property for the file path and de-serializes the {@link WorkUnit} from the file.
 * </p>
 *
 * <p>
 *   This class is instantiated by the {@link GobblinTemporalJobScheduler} on every job submission to launch the Gobblin job.
 *   The actual task execution happens in the {@link GobblinTemporalTaskRunner}, usually in a different process.
 * </p>
 */
@Alpha
@Slf4j
public class GobblinTemporalJobLauncher extends AbstractJobLauncher {

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinTemporalJobLauncher.class);

  private static final String WORK_UNIT_FILE_EXTENSION = ".wu";

  private final FileSystem fs;
  private final Path appWorkDir;
  private final Path inputWorkUnitDir;
  private final Path outputTaskStateDir;

  // Number of ParallelRunner threads to be used for state serialization/deserialization
  private final int stateSerDeRunnerThreads;

  private final TaskStateCollectorService taskStateCollectorService;
  private final ConcurrentHashMap<String, Boolean> runningMap;
  @Getter
  private final StateStores stateStores;

  private WorkflowServiceStubs workflowServiceStubs;
  private WorkflowClient client;

  public GobblinTemporalJobLauncher(Properties jobProps, Path appWorkDir,
      List<? extends Tag<?>> metadataTags, ConcurrentHashMap<String, Boolean> runningMap)
      throws Exception {
    super(jobProps, initBaseEventTags(jobProps, metadataTags));
    LOGGER.debug("GobblinTemporalJobLauncher: jobProps {}, appWorkDir {}", jobProps, appWorkDir);
    this.runningMap = runningMap;
    this.appWorkDir = appWorkDir;
    this.inputWorkUnitDir = new Path(appWorkDir, GobblinClusterConfigurationKeys.INPUT_WORK_UNIT_DIR_NAME);
    this.outputTaskStateDir = new Path(this.appWorkDir,
        GobblinClusterConfigurationKeys.OUTPUT_TASK_STATE_DIR_NAME + Path.SEPARATOR + this.jobContext.getJobId());

    this.jobContext.getJobState().setJobLauncherType(LauncherTypeEnum.CLUSTER);

    this.stateSerDeRunnerThreads = Integer.parseInt(jobProps.getProperty(ParallelRunner.PARALLEL_RUNNER_THREADS_KEY,
        Integer.toString(ParallelRunner.DEFAULT_PARALLEL_RUNNER_THREADS)));

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

    this.workflowServiceStubs = createServiceStubs();
    this.client = WorkflowClient.newInstance(
            workflowServiceStubs, WorkflowClientOptions.newBuilder().setNamespace("gobblin-fastingest-internpoc").build());

    /*
     * Set Workflow options such as WorkflowId and Task Queue so the worker knows where to list and which workflows to execute.
     */
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

      if (!this.cancellationRequested) {
        submitJobToTemporal(workUnits);
        jobSubmissionTimer.stop();
        LOGGER.info(String.format("Submitted job %s to Temporal", this.jobContext.getJobId()));
      } else {
        LOGGER.warn("Job {} not submitted to Temporal as it was requested to be cancelled.", this.jobContext.getJobId());
      }

      TimingEvent jobRunTimer = this.eventSubmitter.getTimingEvent(TimingEvent.RunJobTimings.HELIX_JOB_RUN);
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
    LOGGER.info("Cancel temporal workflow");
  }

  protected void removeTasksFromCurrentJob(List<String> workUnitIdsToRemove) {
    LOGGER.info("Temporal removeTasksFromCurrentJob");
  }

  protected void addTasksToCurrentJob(List<WorkUnit> workUnitsToAdd) {
    LOGGER.info("Temporal addTasksToCurrentJob");
  }

  /**
   * Submit a job to run.
   */
  private void submitJobToTemporal(List<WorkUnit> workUnits) throws Exception{
    try (ParallelRunner stateSerDeRunner = new ParallelRunner(this.stateSerDeRunnerThreads, this.fs)) {
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
      stateSerDeRunner.waitForTasks(Long.MAX_VALUE);

      LOGGER.debug("GobblinTemporalJobLauncher.createTemporalJob: jobStateFilePath {}, jobState {} jobProperties {}",
          jobStateFilePath, this.jobContext.getJobState().toString(), this.jobContext.getJobState().getProperties());

      String jobStateFilePathStr = jobStateFilePath.toString();

      List<CompletableFuture<Void>> futures = new ArrayList<>();
      AtomicInteger multiTaskIdSequence = new AtomicInteger(0);
      AtomicInteger workflowCount = new AtomicInteger(0);
      int workflowSize = 100;
      ExecutorService executor = Executors.newFixedThreadPool(workflowSize);

      for (int i = 0; i < workflowSize; i++) {
        WorkUnit workUnit = workUnits.get(i);
        futures.add(CompletableFuture.runAsync(() -> {
          try {
            if (workUnit instanceof MultiWorkUnit) {
              workUnit.setId(JobLauncherUtils.newMultiTaskId(this.jobContext.getJobId(), multiTaskIdSequence.getAndIncrement()));
            }
            String workUnitFilePathStr = persistWorkUnit(new Path(this.inputWorkUnitDir, this.jobContext.getJobId()), workUnit, stateSerDeRunner);
            String workflowId = workUnit.getProp(KafkaSource.TOPIC_NAME) + "_" + workflowCount.getAndIncrement();
            WorkflowOptions options = WorkflowOptions.newBuilder()
                .setTaskQueue(Shared.GOBBLIN_TEMPORAL_TASK_QUEUE)
                .setWorkflowId(workflowId)
                .build();
            GobblinTemporalWorkflow workflow = this.client.newWorkflowStub(GobblinTemporalWorkflow.class, options);
            LOGGER.info("Setting up temporal workflow {}", workflowId);
            workflow.runTask(jobProps, appWorkDir.toString(), getJobId(), workUnitFilePathStr, jobStateFilePathStr);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }, executor));
      }
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }
  }

  public void launchJob(@Nullable JobListener jobListener) throws JobException {
    LOGGER.info("Launching Temporal Job");
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
      // One potential way to improve the error handling is to make this error swallowing conifgurable
    } catch (Throwable t) {
      errorInJobLaunching = t;
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
   * The method {@link GobblinTemporalJobLauncher#launchJob(JobListener)} contains boiler plate for handling exceptions and
   * mutating the runningMap to communicate state back to the {@link GobblinTemporalJobScheduler}. The boiler plate swallows
   * exceptions when launching the job because many use cases require that 1 job failure should not affect other jobs by causing the
   * entire process to fail through an uncaught exception.
   *
   * This method is useful for unit testing edge cases where we expect {@link JobException}s during the underlying launch operation.
   * It would be nice to not swallow exceptions, but the implications of doing that will require careful refactoring since
   * the class {@link GobblinTemporalJobLauncher} and {@link GobblinTemporalJobScheduler} are shared for 2 quite different cases
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

  /**
   * Persist a single {@link WorkUnit} (flattened) to a file.
   */
  private String persistWorkUnit(final Path workUnitFileDir, final WorkUnit workUnit, ParallelRunner stateSerDeRunner) {
    final StateStore stateStore;
    String workUnitFileName = workUnit.getId();

    if (workUnit instanceof MultiWorkUnit) {
      workUnitFileName += MULTI_WORK_UNIT_FILE_EXTENSION;
      stateStore = stateStores.getMwuStateStore();
    } else {
      workUnitFileName += WORK_UNIT_FILE_EXTENSION;
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

  public static List<? extends Tag<?>> initBaseEventTags(Properties jobProps,
      List<? extends Tag<?>> inputTags) {
    List<Tag<?>> metadataTags = Lists.newArrayList(inputTags);
    String jobId;

    // generate job id if not already set
    if (jobProps.containsKey(ConfigurationKeys.JOB_ID_KEY)) {
      jobId = jobProps.getProperty(ConfigurationKeys.JOB_ID_KEY);
    } else {
      jobId = JobLauncherUtils.newJobId(JobState.getJobNameFromProps(jobProps),
          PropertiesUtils.getPropAsLong(jobProps, ConfigurationKeys.FLOW_EXECUTION_ID_KEY, System.currentTimeMillis()));
      jobProps.put(ConfigurationKeys.JOB_ID_KEY, jobId);
    }

    String jobExecutionId = Long.toString(Id.Job.parse(jobId).getSequence());

    // only inject flow tags if a flow name is defined
    if (jobProps.containsKey(ConfigurationKeys.FLOW_NAME_KEY)) {
      metadataTags.add(new Tag<>(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD,
          jobProps.getProperty(ConfigurationKeys.FLOW_GROUP_KEY, "")));
      metadataTags.add(new Tag<>(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD,
          jobProps.getProperty(ConfigurationKeys.FLOW_NAME_KEY)));

      // use job execution id if flow execution id is not present
      metadataTags.add(new Tag<>(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD,
          jobProps.getProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, jobExecutionId)));
    }

    if (jobProps.containsKey(ConfigurationKeys.JOB_CURRENT_ATTEMPTS)) {
      metadataTags.add(new Tag<>(TimingEvent.FlowEventConstants.CURRENT_ATTEMPTS_FIELD,
          jobProps.getProperty(ConfigurationKeys.JOB_CURRENT_ATTEMPTS, "1")));
      metadataTags.add(new Tag<>(TimingEvent.FlowEventConstants.CURRENT_GENERATION_FIELD,
          jobProps.getProperty(ConfigurationKeys.JOB_CURRENT_GENERATION, "1")));
      metadataTags.add(new Tag<>(TimingEvent.FlowEventConstants.SHOULD_RETRY_FIELD,
          "false"));
    }

    metadataTags.add(new Tag<>(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD,
        jobProps.getProperty(ConfigurationKeys.JOB_GROUP_KEY, "")));
    metadataTags.add(new Tag<>(TimingEvent.FlowEventConstants.JOB_NAME_FIELD,
        jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY, "")));
    metadataTags.add(new Tag<>(TimingEvent.FlowEventConstants.JOB_EXECUTION_ID_FIELD, jobExecutionId));

    log.debug("AddAdditionalMetadataTags: metadataTags {}", metadataTags);

    return metadataTags;
  }
}
