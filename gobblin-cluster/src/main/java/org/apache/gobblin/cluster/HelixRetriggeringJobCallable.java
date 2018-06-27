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
import org.apache.gobblin.runtime.JobLauncher;
import org.apache.gobblin.runtime.api.ExecutionResult;
import org.apache.gobblin.runtime.api.JobExecutionMonitor;
import org.apache.gobblin.runtime.listeners.JobListener;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.PropertiesUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 * A {@link Callable} that can run a given job multiple times iff:
 *  1) Re-triggering is enabled and
 *  2) Job stops early.
 *
 * Moreover based on the job properties, a job can be processed immediately (non-distributed) or forwarded to a remote
 * node (distributed) for handling. Details are illustrated as follows:
 *
 * <p>
 *   If {@link GobblinClusterConfigurationKeys#DISTRIBUTED_JOB_LAUNCHER_ENABLED} is false, the job will be handled
 *   by {@link HelixRetriggeringJobCallable#launchJobLauncherLoop()}, which simply submits the job to Helix for execution.
 *
 *   See {@link GobblinHelixJobLauncher} for job launcher details.
 * </p>
 *
 * <p>
 *   If {@link GobblinClusterConfigurationKeys#DISTRIBUTED_JOB_LAUNCHER_ENABLED} is true, the job will be handled
 *   by {@link HelixRetriggeringJobCallable#launchJobExecutionLauncherLoop()}}. It will first create a planning job with
 *   {@link GobblinTaskRunner#GOBBLIN_JOB_FACTORY_NAME} pre-configured, so that Helix can forward this planning job to
 *   any nodes that has implemented the Helix task factory model matching the same name. See {@link TaskRunnerSuiteThreadModel}
 *   implementation of how task factory model is setup.
 *
 *   Once the planning job reaches to the remote end, it will be handled by {@link GobblinHelixJobTask} which is
 *   created by {@link GobblinHelixJobTask}. The actual handling is similar to the non-distributed mode, where
 *   {@link GobblinHelixJobLauncher} is invoked.
 * </p>
 */
@Slf4j
@Alpha
class HelixRetriggeringJobCallable implements Callable {
  private GobblinHelixJobScheduler jobScheduler;
  private Properties sysProps;
  private Properties jobProps;
  private JobListener jobListener;
  private JobLauncher currentJobLauncher = null;
  private JobExecutionMonitor currentJobMonitor = null;
  private Path appWorkDir;
  private HelixManager helixManager;

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
    if (isDistributeJobEnabled()) {
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
    } finally {
      this.jobScheduler.getJobLaunchers().remove(jobProps.getProperty(GobblinHelixJobScheduler.JOB_URI));
    }
  }

  private void launchJobExecutionLauncherLoop() throws JobException {
    try {
      while (true) {
        String builderStr = jobProps.getProperty(GobblinClusterConfigurationKeys.DISTRIBUTED_JOB_LAUNCHER_BUILDER, GobblinHelixDistributeJobExecutionLauncher.Builder.class.getName());
        GobblinHelixDistributeJobExecutionLauncher.Builder builder = GobblinConstructorUtils.<GobblinHelixDistributeJobExecutionLauncher.Builder>invokeLongestConstructor(
            new ClassAliasResolver(GobblinHelixDistributeJobExecutionLauncher.Builder.class).resolveClass(builderStr));

        builder.setSysProperties(this.sysProps);
        builder.setJobProperties(this.jobProps);
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

  public void cancel() throws JobException {
    if (currentJobLauncher != null) {
      currentJobLauncher.cancelJob(this.jobListener);
    } else if (currentJobMonitor != null) {
      currentJobMonitor.cancel(false);
    }
  }
}