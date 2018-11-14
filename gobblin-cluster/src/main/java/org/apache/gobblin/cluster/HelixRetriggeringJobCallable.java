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

import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.hadoop.fs.Path;
import org.apache.helix.HelixManager;

import com.google.common.io.Closer;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.JobException;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.api.ExecutionResult;
import org.apache.gobblin.runtime.api.JobExecutionMonitor;
import org.apache.gobblin.runtime.listeners.JobListener;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.JobLauncherUtils;
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
 *    by {@link HelixRetriggeringJobCallable#launchJobLauncherLoop()}, which simply launches {@link GobblinHelixJobLauncher}
 *    and submit the work units to Helix. Helix will dispatch the work units to different worker nodes. The worker node will
 *    handle the work units by {@link GobblinHelixTask}.
 *
 *    See {@link GobblinHelixJobLauncher} for job launcher details.
 *    See {@link GobblinHelixTask} for work unit handling details.
 * </p>
 *
 * <p> Distribution Mode:
 *   If {@link GobblinClusterConfigurationKeys#DISTRIBUTED_JOB_LAUNCHER_ENABLED} is true, the job will be handled
 *   by {@link HelixRetriggeringJobCallable#launchJobExecutionLauncherLoop()}}, which simply launches
 *   {@link GobblinHelixDistributeJobExecutionLauncher} and submit a planning job to Helix. Helix will dispatch this
 *   planning job to a worker node. The worker node will handle this planning job by {@link GobblinHelixJobTask}.
 *
 *   The {@link GobblinHelixJobTask} will launch {@link GobblinHelixJobLauncher} and it will again submit the actual
 *   work units to Helix. Helix will dispatch the work units to other worker nodes. Similar to Non-Distribution Node,
 *   some worker nodes will handle those work units by {@link GobblinHelixTask}.
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
  private final Path appWorkDir;
  private final HelixManager helixManager;

  private GobblinHelixJobLauncher currentJobLauncher = null;
  private JobExecutionMonitor currentJobMonitor = null;
  private boolean isDistributeJobEnabled = false;

  public HelixRetriggeringJobCallable(
      GobblinHelixJobScheduler jobScheduler,
      Properties sysProps,
      Properties jobProps,
      JobListener jobListener,
      Path appWorkDir,
      HelixManager helixManager) {
    this.jobScheduler = jobScheduler;
    this.sysProps = sysProps;
    this.jobProps = jobProps;
    this.jobListener = jobListener;
    this.appWorkDir = appWorkDir;
    this.helixManager = helixManager;
    this.isDistributeJobEnabled = isDistributeJobEnabled();
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
      launchJobExecutionLauncherLoop();
    } else {
      launchJobLauncherLoop();
    }

    return null;
  }

  private void launchJobLauncherLoop() throws JobException {
    try {
      while (true) {
        currentJobLauncher = this.jobScheduler.buildJobLauncher(jobProps);
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

  private void launchJobExecutionLauncherLoop() throws JobException {
    try {
      while (true) {
        String builderStr = jobProps.getProperty(GobblinClusterConfigurationKeys.DISTRIBUTED_JOB_LAUNCHER_BUILDER,
            GobblinHelixDistributeJobExecutionLauncher.Builder.class.getName());

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
        builder.setManager(this.helixManager);
        builder.setAppWorkDir(this.appWorkDir);

        try (Closer closer = Closer.create()) {
          GobblinHelixDistributeJobExecutionLauncher launcher = builder.build();
          closer.register(launcher);
          this.currentJobMonitor = launcher.launchJob(null);
          ExecutionResult result = this.currentJobMonitor.get();
          boolean isEarlyStopped = ((GobblinHelixDistributeJobExecutionLauncher.DistributeJobResult) result).isEarlyStopped();
          boolean isRetriggerEnabled = this.isRetriggeringEnabled();
          if (isEarlyStopped && isRetriggerEnabled) {
            log.info("DistributeJob {} will be re-triggered.", jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY));
          } else {
            break;
          }
          currentJobMonitor = null;
        } catch (Throwable t) {
          throw new JobException("Failed to launch and run job " + jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), t);
        }
      }
    } catch (Exception e) {
      log.error("Failed to run job {}", jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), e);
      throw new JobException("Failed to run job " + jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), e);
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