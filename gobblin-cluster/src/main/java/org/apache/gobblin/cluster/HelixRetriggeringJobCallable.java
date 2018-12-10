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

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
import org.apache.helix.HelixManager;

import com.google.common.io.Closer;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.runtime.JobException;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.api.JobExecutionMonitor;
import org.apache.gobblin.runtime.listeners.JobListener;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.JobLauncherUtils;
import org.apache.gobblin.util.PathUtils;
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
  private final Properties sysProps;
  private final Properties jobProps;
  private final JobListener jobListener;
  private final GobblinHelixPlanningJobLauncherMetrics planningJobLauncherMetrics;
  private final Path appWorkDir;
  private final HelixManager jobHelixManager;
  private final Optional<HelixManager> taskDriverHelixManager;
  protected HelixJobsMapping jobsMapping;
  private GobblinHelixJobLauncher currentJobLauncher = null;
  private JobExecutionMonitor currentJobMonitor = null;
  private boolean isDistributeJobEnabled = false;

  public HelixRetriggeringJobCallable(
      GobblinHelixJobScheduler jobScheduler,
      Properties sysProps,
      Properties jobProps,
      JobListener jobListener,
      GobblinHelixPlanningJobLauncherMetrics planningJobLauncherMetrics,
      Path appWorkDir,
      HelixManager jobHelixManager,
      Optional<HelixManager> taskDriverHelixManager) {
    this.jobScheduler = jobScheduler;
    this.sysProps = sysProps;
    this.jobProps = jobProps;
    this.jobListener = jobListener;
    this.planningJobLauncherMetrics = planningJobLauncherMetrics;
    this.appWorkDir = appWorkDir;
    this.jobHelixManager = jobHelixManager;
    this.taskDriverHelixManager = taskDriverHelixManager;
    this.isDistributeJobEnabled = isDistributeJobEnabled();
    this.jobsMapping = new HelixJobsMapping(ConfigUtils.propertiesToConfig(sysProps),
                                            PathUtils.getRootPath(appWorkDir).toUri(),
                                            appWorkDir.toString());
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
    if (this.isDistributeJobEnabled) {
      runJobExecutionLauncher();
    } else {
      runJobLauncherLoop();
    }

    return null;
  }

  /**
   * <p> In some cases, the job launcher will be early stopped.
   * It can be due to the large volume of input source data.
   * In such case, we need to re-launch the same job until
   * the job launcher determines it is safe to stop.
   */
  private void runJobLauncherLoop() throws JobException {
    try {
      while (true) {
        currentJobLauncher = this.jobScheduler.buildJobLauncher(jobProps);
        // in "run once" case, job scheduler will remove current job from the scheduler
        boolean isEarlyStopped = this.jobScheduler.runJob(jobProps, jobListener, currentJobLauncher);
        boolean isRetriggerEnabled = this.isRetriggeringEnabled();
        if (isEarlyStopped && isRetriggerEnabled) {
          log.info("Job {} will be re-triggered.", jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY));
        } else {
          break;
        }
        currentJobLauncher = null;
      }
    } catch (Exception e) {
      log.error("Failed to run job {}", jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), e);
      throw new JobException("Failed to run job " + jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), e);
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
    try {
      String builderStr = jobProps.getProperty(GobblinClusterConfigurationKeys.DISTRIBUTED_JOB_LAUNCHER_BUILDER,
          GobblinHelixDistributeJobExecutionLauncher.Builder.class.getName());

      // Check if any existing planning job is running
      String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
      Optional<String> planningJobIdFromStore = jobsMapping.getPlanningJobId(jobName);

      if (planningJobIdFromStore.isPresent()) {
        String previousPlanningJobId = planningJobIdFromStore.get();
        HelixManager planningJobManager = this.taskDriverHelixManager.isPresent()?
            this.taskDriverHelixManager.get() : this.jobHelixManager;

        if (HelixUtils.isJobFinished(previousPlanningJobId, previousPlanningJobId, planningJobManager)) {
          log.info("Previous planning job {} has reached to the final state. Start a new one.", previousPlanningJobId);
        } else {
          log.info("Previous planning job {} has not finished yet. Skip it.", previousPlanningJobId);
          return;
        }
      } else {
        log.info("Planning job for {} does not exist. First time run.", jobName);
      }

      GobblinHelixDistributeJobExecutionLauncher.Builder builder = GobblinConstructorUtils
          .<GobblinHelixDistributeJobExecutionLauncher.Builder>invokeLongestConstructor(new ClassAliasResolver(
              GobblinHelixDistributeJobExecutionLauncher.Builder.class).resolveClass(builderStr));

      // Make a separate copy because we could update some of attributes in job properties (like adding planning id).
      Properties jobPlanningProps = new Properties();
      jobPlanningProps.putAll(this.jobProps);

      // Inject planning id and start time
      String planningId = JobLauncherUtils.newJobId(GobblinClusterConfigurationKeys.PLANNING_JOB_NAME_PREFIX
          + JobState.getJobNameFromProps(jobPlanningProps));
      jobPlanningProps.setProperty(GobblinClusterConfigurationKeys.PLANNING_ID_KEY, planningId);
      jobPlanningProps.setProperty(GobblinClusterConfigurationKeys.PLANNING_JOB_CREATE_TIME, String.valueOf(System.currentTimeMillis()));

      builder.setSysProps(this.sysProps);
      builder.setJobPlanningProps(jobPlanningProps);
      builder.setJobHelixManager(this.jobHelixManager);
      builder.setTaskDriverHelixManager(this.taskDriverHelixManager);
      builder.setAppWorkDir(this.appWorkDir);

      try (Closer closer = Closer.create()) {
        log.info("Planning job {} started.", planningId);
        GobblinHelixDistributeJobExecutionLauncher launcher = builder.build();
        closer.register(launcher);
        this.jobsMapping.setPlanningJobId(jobName, planningId);
        startTime = System.currentTimeMillis();
        this.currentJobMonitor = launcher.launchJob(null);
        this.currentJobMonitor.get();
        this.currentJobMonitor = null;
        log.info("Planning job {} finished.", planningId);
        Instrumented.updateTimer(
            com.google.common.base.Optional.of(this.planningJobLauncherMetrics.timeForCompletedPlanningJobs),
            System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
      } catch (Throwable t) {
        if (startTime != 0) {
          Instrumented.updateTimer(
              com.google.common.base.Optional.of(this.planningJobLauncherMetrics.timeForFailedPlanningJobs),
              System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
        }
        throw new JobException("Failed to launch and run planning job " + jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), t);
      }
    } catch (Exception e) {
      if (startTime != 0) {
        Instrumented.updateTimer(
            com.google.common.base.Optional.of(this.planningJobLauncherMetrics.timeForFailedPlanningJobs),
            System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
      }
      log.error("Failed to run planning job {}", jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), e);
      throw new JobException("Failed to run planning job " + jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), e);
    }
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