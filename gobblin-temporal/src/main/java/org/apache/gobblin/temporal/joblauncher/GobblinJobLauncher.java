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

package org.apache.gobblin.temporal.joblauncher;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.eventbus.EventBus;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.GobblinClusterUtils;
import org.apache.gobblin.cluster.HelixUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.CountEventBuilder;
import org.apache.gobblin.metrics.event.JobEvent;
import org.apache.gobblin.rest.LauncherTypeEnum;
import org.apache.gobblin.runtime.AbstractJobLauncher;
import org.apache.gobblin.runtime.JobException;
import org.apache.gobblin.runtime.JobLauncher;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskStateCollectorService;
import org.apache.gobblin.runtime.listeners.JobListener;
import org.apache.gobblin.runtime.util.StateStores;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.ParallelRunner;

/**
 * An implementation of {@link JobLauncher} that launches a Gobblin job using the Temporal task framework.
 * Most of this code is lifted from {@link org.apache.gobblin.cluster.GobblinHelixJobLauncher} and maybe in the future
 * it may make sense to converge the code once Temporal on Gobblin is in a mature state.
 *
 * <p>
 *   Each {@link WorkUnit} of the job is persisted to the {@link FileSystem} of choice and the path to the file
 *   storing the serialized {@link WorkUnit} is passed to the Temporal task running the {@link WorkUnit} as a
 *   user-defined property {@link GobblinClusterConfigurationKeys#WORK_UNIT_FILE_PATH}. Upon startup, the gobblin
 *   task reads the property for the file path and de-serializes the {@link WorkUnit} from the file.
 * </p>
 */
@Alpha
@Slf4j
public abstract class GobblinJobLauncher extends AbstractJobLauncher {
  protected final EventBus eventBus;
  protected static final String WORK_UNIT_FILE_EXTENSION = ".wu";
  protected final FileSystem fs;
  protected final Path appWorkDir;
  protected final Path inputWorkUnitDir;
  protected final Path outputTaskStateDir;

  // Number of ParallelRunner threads to be used for state serialization/deserialization
  protected final int stateSerDeRunnerThreads;

  protected final TaskStateCollectorService taskStateCollectorService;
  protected final ConcurrentHashMap<String, Boolean> runningMap;
  @Getter
  protected final StateStores stateStores;
  protected JobListener jobListener;
  protected volatile boolean jobSubmitted = false;


  public GobblinJobLauncher(Properties jobProps, Path appWorkDir,
      List<? extends Tag<?>> metadataTags, ConcurrentHashMap<String, Boolean> runningMap, EventBus eventbus)
      throws Exception {
    super(jobProps, HelixUtils.initBaseEventTags(jobProps, metadataTags));
    log.debug("GobblinJobLauncher: jobProps {}, appWorkDir {}", jobProps, appWorkDir);
    this.runningMap = runningMap;
    this.appWorkDir = appWorkDir;
    this.inputWorkUnitDir = new Path(appWorkDir, GobblinClusterConfigurationKeys.INPUT_WORK_UNIT_DIR_NAME);
    this.outputTaskStateDir = new Path(this.appWorkDir,
        GobblinClusterConfigurationKeys.OUTPUT_TASK_STATE_DIR_NAME + Path.SEPARATOR + this.jobContext.getJobId());

    this.jobContext.getJobState().setJobLauncherType(LauncherTypeEnum.CLUSTER);

    this.stateSerDeRunnerThreads = Integer.parseInt(jobProps.getProperty(ParallelRunner.PARALLEL_RUNNER_THREADS_KEY,
        Integer.toString(ParallelRunner.DEFAULT_PARALLEL_RUNNER_THREADS)));
    this.eventBus = eventbus;

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
      log.info("Emitting WorkUnitsCreated Count: " + countEventBuilder.getCount());

      long workUnitStartTime = System.currentTimeMillis();
      workUnits.forEach((k) -> k.setProp(ConfigurationKeys.WORK_UNIT_CREATION_TIME_IN_MILLIS, workUnitStartTime));

      // Start the output TaskState collector service
      this.taskStateCollectorService.startAsync().awaitRunning();

      synchronized (this.cancellationRequest) {
        if (!this.cancellationRequested) {
          submitJob(workUnits);
          log.info(String.format("Submitted job %s", this.jobContext.getJobId()));
          this.jobSubmitted = true;
        } else {
          log.warn("Job {} not submitted as it was requested to be cancelled.", this.jobContext.getJobId());
        }
      }

      waitJob();
      log.info(String.format("Job %s completed", this.jobContext.getJobId()));
    } finally {
      // The last iteration of output TaskState collecting will run when the collector service gets stopped
      this.taskStateCollectorService.stopAsync().awaitTerminated();
      cleanupWorkingDirectory();
    }
  }

  protected void submitJob(List<WorkUnit> workUnits) throws Exception {
  }

  protected void waitJob() throws InterruptedException {
  }

  @Override
  protected void executeCancellation() {
  }

  public void launchJob(@Nullable JobListener jobListener) throws JobException {
    this.jobListener = jobListener;
    log.info("Launching Job");
    boolean isLaunched = false;
    this.runningMap.putIfAbsent(this.jobContext.getJobName(), false);

    Throwable errorInJobLaunching = null;
    try {
      if (this.runningMap.replace(this.jobContext.getJobName(), false, true)) {
        log.info("Job {} will be executed, add into running map.", this.jobContext.getJobId());
        isLaunched = true;
        launchJobImpl(jobListener);
      } else {
        log.warn("Job {} will not be executed because other jobs are still running.", this.jobContext.getJobId());
      }

      // TODO: Better error handling. The current impl swallows exceptions for jobs that were started by this method call.
      // One potential way to improve the error handling is to make this error swallowing conifgurable
    } catch (Throwable t) {
      errorInJobLaunching = t;
      if (isLaunched) {
        // Attempts to cancel workflow if an error occurs during launch
        cancelJob(jobListener);
      }
    } finally {
      handleLaunchFinalization();

      if (isLaunched) {
        if (this.runningMap.replace(this.jobContext.getJobName(), true, false)) {
          log.info("Job {} is done, remove from running map.", this.jobContext.getJobId());
        } else {
          throw errorInJobLaunching == null ? new IllegalStateException(
              "A launched job should have running state equal to true in the running map.")
              : new RuntimeException("Failure in launching job:", errorInJobLaunching);
        }
      }
    }
  }

  protected void handleLaunchFinalization() {
  }

  /**
   * This method looks silly at first glance but exists for a reason.
   *
   * The method {@link GobblinJobLauncher#launchJob(JobListener)} contains boiler plate for handling exceptions and
   * mutating the runningMap to communicate state back to the {@link GobblinJobScheduler}. The boiler plate swallows
   * exceptions when launching the job because many use cases require that 1 job failure should not affect other jobs by causing the
   * entire process to fail through an uncaught exception.
   *
   * This method is useful for unit testing edge cases where we expect {@link JobException}s during the underlying launch operation.
   * It would be nice to not swallow exceptions, but the implications of doing that will require careful refactoring since
   * the class {@link GobblinJobLauncher} and {@link GobblinJobScheduler} are shared for 2 quite different cases
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
   * Delete persisted {@link WorkUnit}s and {@link JobState} upon job completion.
   */
  protected void cleanupWorkingDirectory() throws IOException {
    log.info("Deleting persisted work units for job " + this.jobContext.getJobId());
    stateStores.getWuStateStore().delete(this.jobContext.getJobId());

    // delete the directory that stores the task state files
    stateStores.getTaskStateStore().delete(outputTaskStateDir.getName());

    log.info("Deleting job state file for job " + this.jobContext.getJobId());

    if (this.stateStores.haveJobStateStore()) {
      this.stateStores.getJobStateStore().delete(this.jobContext.getJobId());
    } else {
      Path jobStateFilePath =
          GobblinClusterUtils.getJobStateFilePath(false, this.appWorkDir, this.jobContext.getJobId());
      this.fs.delete(jobStateFilePath, false);
    }
  }
}

