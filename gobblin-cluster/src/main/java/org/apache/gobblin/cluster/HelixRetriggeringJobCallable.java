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

import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;

import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.hadoop.fs.Path;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Striped;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.JobException;
import org.apache.gobblin.runtime.api.JobExecutionMonitor;
import org.apache.gobblin.runtime.api.MutableJobCatalog;
import org.apache.gobblin.runtime.listeners.JobListener;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PropertiesUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 * A {@link Callable} that can run a given job multiple times iff:
 *  1) Re-triggering is enabled and
 *  2) Job stops early.
 *
 * Based on the job properties, a job can be processed immediately (non-distribution mode) or forwarded to a remote
 * node (distribution mode). Details are as follows:
 *
 * <p> Non-Distribution Mode:
 *    If {@link GobblinClusterConfigurationKeys#DISTRIBUTED_JOB_LAUNCHER_ENABLED} is false, the job will be handled
 *    by {@link HelixRetriggeringJobCallable#runJobLauncherLoop()}, which simply launches {@link GobblinHelixJobLauncher}
 *    and submit the work units to Helix. Helix will dispatch the work units to different worker nodes. The worker node will
 *    handle the work units via launching {@link GobblinHelixTask}.
 *
 *    See {@link GobblinHelixJobLauncher} for job launcher details.
 *    See {@link GobblinHelixTask} for work unit handling details.
 * </p>
 *
 * <p> Distribution Mode:
 *   If {@link GobblinClusterConfigurationKeys#DISTRIBUTED_JOB_LAUNCHER_ENABLED} is true, the job will be handled
 *   by {@link HelixRetriggeringJobCallable#runJobExecutionLauncher()}, which simply launches
 *   {@link GobblinHelixDistributeJobExecutionLauncher} and submit a planning job to Helix. Helix will dispatch this
 *   planning job to a task-driver node. The task-driver node will handle this planning job via launching
 *   {@link GobblinHelixJobTask}.
 *
 *   The {@link GobblinHelixJobTask} will again launch {@link GobblinHelixJobLauncher} to submit the actual job
 *   to Helix. Helix will dispatch the work units to other worker nodes. Similar to Non-Distribution Node,
 *   some worker nodes will handle those work units by launching {@link GobblinHelixTask}.
 *
 *    See {@link GobblinHelixDistributeJobExecutionLauncher} for planning job launcher details.
 *    See {@link GobblinHelixJobTask} for planning job handling details.
 *    See {@link GobblinHelixJobLauncher} for job launcher details.
 *    See {@link GobblinHelixTask} for work unit handling details.
 * </p>
 */
@Slf4j
@Alpha
class HelixRetriggeringJobCallable implements Callable {
  private final GobblinHelixJobScheduler jobScheduler;
  private final MutableJobCatalog jobCatalog;
  private final Properties sysProps;
  private final Properties jobProps;
  private final JobListener jobListener;
  private final GobblinHelixPlanningJobLauncherMetrics planningJobLauncherMetrics;
  private final GobblinHelixMetrics helixMetrics;
  private final Path appWorkDir;
  private final HelixManager jobHelixManager;
  private final Optional<HelixManager> taskDriverHelixManager;
  protected HelixJobsMapping jobsMapping;
  private GobblinHelixJobLauncher currentJobLauncher = null;
  private JobExecutionMonitor currentJobMonitor = null;
  private boolean isDistributeJobEnabled = false;
  private final String jobUri;
  private boolean jobDeleteAttempted = false;
  private final Striped<Lock> locks;
  private final MetricContext metricContext;
  private final EventSubmitter eventSubmitter;

  public HelixRetriggeringJobCallable(
      GobblinHelixJobScheduler jobScheduler,
      MutableJobCatalog jobCatalog,
      Properties sysProps,
      Properties jobProps,
      JobListener jobListener,
      GobblinHelixPlanningJobLauncherMetrics planningJobLauncherMetrics,
      GobblinHelixMetrics helixMetrics,
      Path appWorkDir,
      HelixManager jobHelixManager,
      Optional<HelixManager> taskDriverHelixManager,
      HelixJobsMapping jobsMapping,
      Striped<Lock> locks,
      MetricContext metricContext) {
    this.jobScheduler = jobScheduler;
    this.jobCatalog = jobCatalog;
    this.sysProps = sysProps;
    this.jobProps = jobProps;
    this.jobListener = jobListener;
    this.planningJobLauncherMetrics = planningJobLauncherMetrics;
    this.helixMetrics = helixMetrics;
    this.appWorkDir = appWorkDir;
    this.jobHelixManager = jobHelixManager;
    this.taskDriverHelixManager = taskDriverHelixManager;
    this.isDistributeJobEnabled = isDistributeJobEnabled();
    this.jobUri = jobProps.getProperty(GobblinClusterConfigurationKeys.JOB_SPEC_URI);
    this.jobsMapping = jobsMapping;
    this.locks = locks;
    this.metricContext = metricContext;
    eventSubmitter = new EventSubmitter.Builder(this.metricContext, "gobblin.runtime").build();
  }

  private boolean isRetriggeringEnabled() {
    return PropertiesUtils.getPropAsBoolean(jobProps, ConfigurationKeys.JOB_RETRIGGERING_ENABLED,
        ConfigurationKeys.DEFAULT_JOB_RETRIGGERING_ENABLED);
  }

  private boolean isDistributeJobEnabled() {
    Properties combinedProps = new Properties();
    combinedProps.putAll(sysProps);
    combinedProps.putAll(jobProps);
    return (PropertiesUtils.getPropAsBoolean(combinedProps,
        GobblinClusterConfigurationKeys.DISTRIBUTED_JOB_LAUNCHER_ENABLED,
        Boolean.toString(GobblinClusterConfigurationKeys.DEFAULT_DISTRIBUTED_JOB_LAUNCHER_ENABLED)));
  }

  @Override
  public Void call() throws JobException {
    boolean deleteJobWhenException = PropertiesUtils.getPropAsBoolean(this.jobProps,
        GobblinClusterConfigurationKeys.JOB_ALWAYS_DELETE,
        "false");

    try {
        this.jobsMapping.setDistributedJobMode(this.jobUri, this.isDistributeJobEnabled);
    } catch (IOException e) {
        throw new JobException("Could not update jobsMapping", e);
    }

    try {
      if (this.isDistributeJobEnabled) {
        runJobExecutionLauncher();
      } else {
        runJobLauncherLoop();
      }

      deleteJobSpec();
    } catch (Exception e) { // delete job spec when exception occurred
      if (deleteJobWhenException) {
        deleteJobSpec();
      }
      throw e;
    }

    return null;
  }

  private void deleteJobSpec() throws JobException {
    boolean runOnce = Boolean.valueOf(jobProps.getProperty(ConfigurationKeys.JOB_RUN_ONCE_KEY, "false"));
    boolean hasSchedule = jobProps.containsKey(ConfigurationKeys.JOB_SCHEDULE_KEY);
    if (runOnce || !hasSchedule) {
      if (this.jobCatalog != null) {
        try {
          if (!this.jobDeleteAttempted) {
            log.info("Deleting job spec on {}", this.jobUri);
            this.jobScheduler.unscheduleJob(this.jobUri);
            this.jobCatalog.remove(new URI(jobUri));
            this.jobDeleteAttempted = true;
          }
        } catch (URISyntaxException e) {
          log.error("Failed to remove job with bad uri " + jobUri, e);
        }
      }
    }
  }

  @VisibleForTesting
  static GobblinHelixJobLauncher buildJobLauncherForCentralizedMode(GobblinHelixJobScheduler jobScheduler, Properties jobProps) throws Exception {
    //In centralized job launcher mode, the JOB_ID_KEY should be null or should not contain the
    //"ActualJob" substring, which is intended for the distributed job launcher mode.
    //This ensures that workflows in centralized mode are cleaned up properly when cluster is restarted.
    String jobId = jobProps.getProperty(ConfigurationKeys.JOB_ID_KEY);
    if (jobId != null) {
      Preconditions.checkArgument(!jobId.contains(GobblinClusterConfigurationKeys.ACTUAL_JOB_NAME_PREFIX),
          "Job Id should not contain ActualJob in centralized mode.");
    }
    return jobScheduler.buildJobLauncher(jobProps);
  }

  /**
   * A method to run a Gobblin job with ability to re-trigger the job if neeeded. This method instantiates a
   * {@link GobblinHelixJobLauncher} and submits the underlying Gobblin job to a {link GobblinHelixJobScheduler}.
   * The method will re-submit the job if it has been terminated "early" e.g. before all data has been pulled.
   * This method should be called only when distributed job launcher mode is disabled.
   */
   private void runJobLauncherLoop() throws JobException {
    try {
      this.jobHelixManager.connect();
      while (true) {
        currentJobLauncher = buildJobLauncherForCentralizedMode(jobScheduler, jobProps);
        // in "run once" case, job scheduler will remove current job from the scheduler
        boolean isEarlyStopped = this.jobScheduler.runJob(jobProps, jobListener, currentJobLauncher);
        boolean isRetriggerEnabled = this.isRetriggeringEnabled();
        if (isEarlyStopped && isRetriggerEnabled) {
          log.info("Job {} will be re-triggered.", jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY));
        } else {
          break;
        }
      }
    } catch (Exception e) {
      log.error("Failed to run job {}", jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), e);
      throw new JobException("Failed to run job " + jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), e);
    } finally {
      this.jobHelixManager.disconnect();
      currentJobLauncher = null;
    }
  }

  /**
   * <p> Launch a planning job. The actual job will be launched
   * on task driver instance, which will handle the early-stop case
   * by a single while-loop.
   *
   * @see {@link GobblinHelixJobTask#run()} for the task driver logic.
   */
  private void runJobExecutionLauncher() throws JobException {
    long startTime = 0;
    String newPlanningId;
    Closer closer = Closer.create();
    try {
      HelixManager planningJobHelixManager = this.taskDriverHelixManager.orElse(this.jobHelixManager);
      planningJobHelixManager.connect();

      String builderStr = jobProps.getProperty(GobblinClusterConfigurationKeys.DISTRIBUTED_JOB_LAUNCHER_BUILDER,
          GobblinHelixDistributeJobExecutionLauncher.Builder.class.getName());

      // Check if any existing planning job is running
      Optional<String> planningJobIdFromStore = jobsMapping.getPlanningJobId(this.jobUri);
      boolean nonblocking = false;
      // start of critical section to check if a job with same job name is running
      Lock jobLock = locks.get(this.jobUri);
      jobLock.lock();

      try {
        if (planningJobIdFromStore.isPresent() && !canRun(planningJobIdFromStore.get(), planningJobHelixManager)) {
          TimingEvent timer = new TimingEvent(eventSubmitter, TimingEvent.JOB_SKIPPED_TIME);
          HashMap<String, String> metadata = new HashMap<>(Tag.toMap(Tag.tagValuesToString(
              HelixUtils.initBaseEventTags(jobProps, Lists.newArrayList()))));
          timer.stop(metadata);
          planningJobLauncherMetrics.skippedPlanningJobs.mark();
          return;
        }

        log.info("Planning job for {} does not exist. First time run.", this.jobUri);

        GobblinHelixDistributeJobExecutionLauncher.Builder builder = GobblinConstructorUtils.<GobblinHelixDistributeJobExecutionLauncher.Builder>invokeLongestConstructor(
            new ClassAliasResolver(GobblinHelixDistributeJobExecutionLauncher.Builder.class).resolveClass(builderStr));

        // Make a separate copy because we could update some of attributes in job properties (like adding planning id).
        Properties jobPlanningProps = new Properties();
        jobPlanningProps.putAll(this.jobProps);

        // Inject planning id and start time
        newPlanningId = HelixJobsMapping.createPlanningJobId(jobPlanningProps);
        jobPlanningProps.setProperty(GobblinClusterConfigurationKeys.PLANNING_ID_KEY, newPlanningId);
        jobPlanningProps.setProperty(GobblinClusterConfigurationKeys.PLANNING_JOB_CREATE_TIME, String.valueOf(System.currentTimeMillis()));

        builder.setSysProps(this.sysProps);
        builder.setJobPlanningProps(jobPlanningProps);
        builder.setPlanningJobHelixManager(planningJobHelixManager);
        builder.setAppWorkDir(this.appWorkDir);
        builder.setJobsMapping(this.jobsMapping);
        builder.setPlanningJobLauncherMetrics(this.planningJobLauncherMetrics);
        builder.setHelixMetrics(this.helixMetrics);

        // if the distributed job launcher should wait for planning job completion

        Config combined = ConfigUtils.propertiesToConfig(jobPlanningProps)
            .withFallback(ConfigUtils.propertiesToConfig(sysProps));

        nonblocking = ConfigUtils
            .getBoolean(combined, GobblinClusterConfigurationKeys.NON_BLOCKING_PLANNING_JOB_ENABLED,
                GobblinClusterConfigurationKeys.DEFAULT_NON_BLOCKING_PLANNING_JOB_ENABLED);

        log.info("Planning job {} started.", newPlanningId);
        GobblinHelixDistributeJobExecutionLauncher launcher = builder.build();
        closer.register(launcher);
        this.jobsMapping.setPlanningJobId(this.jobUri, newPlanningId);
        startTime = System.currentTimeMillis();
        this.currentJobMonitor = launcher.launchJob(null);

        // make sure the planning job is initialized (or visible) to other parallel running threads,
        // so that the same critical section check (querying Helix for job completeness)
        // can be applied.
        HelixUtils.waitJobInitialization(planningJobHelixManager, newPlanningId, newPlanningId);
      } finally {
        planningJobHelixManager.disconnect();
        // end of the critical section to check if a job with same job name is running
        jobLock.unlock();
      }

      // we can remove the job spec from the catalog because Helix will drive this job to the end.
      this.deleteJobSpec();

      // If we are using non-blocking mode, this get() only guarantees the planning job is submitted.
      // It doesn't guarantee the job will finish because internally we won't wait for Helix completion.
      this.currentJobMonitor.get();
      this.currentJobMonitor = null;
      if (nonblocking) {
        log.info("Planning job {} submitted successfully.", newPlanningId);
      } else {
        log.info("Planning job {} finished.", newPlanningId);
        this.planningJobLauncherMetrics.updateTimeForCompletedPlanningJobs(startTime);
      }
    } catch (Exception e) {
      if (startTime != 0) {
        this.planningJobLauncherMetrics.updateTimeForFailedPlanningJobs(startTime);
      }
      log.error("Failed to run planning job for {}", this.jobUri, e);
      throw new JobException("Failed to run planning job for " + this.jobUri, e);
    } finally {
      try {
        closer.close();
      } catch (IOException e) {
        throw new JobException("Cannot properly close planning job for " + this.jobUri, e);
      }
    }
  }

    /**
     * This method checks if a job can be submitted to helix for execution.
     * A job can run, 1) if the previous job with the same job id is finished,
     * 2) if the previous job is running but can be killed specified by property
     * {@link GobblinClusterConfigurationKeys#KILL_DUPLICATE_PLANNING_JOB}, default being true
     * @param previousJobId job id from the previous execution
     * @param helixManager helix manager
     * @return true if the job can run on Helix
     * @throws JobException if previous job deletion fails
     * @throws InterruptedException
     */
  private boolean canRun(String previousJobId, HelixManager helixManager) throws JobException, InterruptedException {
    if (HelixUtils.isJobFinished(previousJobId, previousJobId, helixManager)) {
      log.info("Previous planning job {} has reached to the final state. Start a new one.", previousJobId);
    } else {
      boolean killDuplicateJob = PropertiesUtils
          .getPropAsBoolean(this.jobProps, GobblinClusterConfigurationKeys.KILL_DUPLICATE_PLANNING_JOB, String.valueOf(GobblinClusterConfigurationKeys.DEFAULT_KILL_DUPLICATE_PLANNING_JOB));

      if (!killDuplicateJob) {
        log.info("Previous planning job {} has not finished yet. Skip this job.", previousJobId);
        return false;
      } else {
        log.info("Previous planning job {} has not finished yet. Kill it.", previousJobId);
        long timeOut = PropertiesUtils
            .getPropAsLong(sysProps, GobblinClusterConfigurationKeys.HELIX_WORKFLOW_DELETE_TIMEOUT_SECONDS,
                GobblinClusterConfigurationKeys.DEFAULT_HELIX_WORKFLOW_DELETE_TIMEOUT_SECONDS) * 1000;
        try {
          HelixUtils.deleteWorkflow(previousJobId, helixManager, timeOut);
        } catch (HelixException e) {
          log.info("Helix cannot delete previous planning job id {} within {} seconds.", previousJobId,
              timeOut / 1000);
          throw new JobException("Helix cannot delete previous planning job id " + previousJobId, e);
        }
      }
    }
    return true;
  }

  void cancel() throws JobException {
    this.jobScheduler.jobSchedulerMetrics.numCancellationStart.incrementAndGet();

    if (isDistributeJobEnabled) {
      if (currentJobMonitor != null) {
        currentJobMonitor.cancel(false);
      }
    } else {
      if (currentJobLauncher != null) {
        currentJobLauncher.cancelJob(this.jobListener);
      }
    }

    this.jobScheduler.jobSchedulerMetrics.numCancellationComplete.incrementAndGet();
  }
}