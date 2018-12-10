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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.fs.Path;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskResult;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.instrumented.StandardMetricsBridge;
import org.apache.gobblin.metrics.ContextAwareTimer;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.runtime.JobException;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PropertiesUtils;


/**
 * An implementation of Helix's {@link org.apache.helix.task.Task} that runs original {@link GobblinHelixJobLauncher}.
 */
@Slf4j
class GobblinHelixJobTask implements Task {

  private final TaskConfig taskConfig;
  private final Config sysConfig;
  private final Properties jobPlusSysConfig;
  private final HelixJobsMapping jobsMapping;
  private final String applicationName;
  private final String instanceName;
  private final String planningJobId;
  private final HelixManager jobHelixManager;
  private final Path appWorkDir;
  private final List<? extends Tag<?>> metadataTags;
  private GobblinHelixJobLauncher launcher;
  private GobblinHelixJobTaskMetrics jobTaskMetrics;
  private GobblinHelixJobLauncherListener jobLauncherListener;

  public GobblinHelixJobTask (TaskCallbackContext context,
                              HelixJobsMapping jobsMapping,
                              TaskRunnerSuiteBase.Builder builder,
                              GobblinHelixJobLauncherMetrics launcherMetrics,
                              GobblinHelixJobTaskMetrics jobTaskMetrics) {
    this.applicationName = builder.getApplicationName();
    this.instanceName = builder.getInstanceName();
    this.jobTaskMetrics = jobTaskMetrics;
    this.taskConfig = context.getTaskConfig();
    this.sysConfig = builder.getConfig();
    this.jobHelixManager = builder.getJobHelixManager();
    this.jobPlusSysConfig = ConfigUtils.configToProperties(sysConfig);
    this.jobLauncherListener = new GobblinHelixJobLauncherListener(launcherMetrics);

    Map<String, String> configMap = this.taskConfig.getConfigMap();
    for (Map.Entry<String, String> entry: configMap.entrySet()) {
      if (entry.getKey().startsWith(GobblinHelixDistributeJobExecutionLauncher.JOB_PROPS_PREFIX)) {
          String key = entry.getKey().replaceFirst(GobblinHelixDistributeJobExecutionLauncher.JOB_PROPS_PREFIX, "");
        jobPlusSysConfig.put(key, entry.getValue());
      }
    }

    if (!jobPlusSysConfig.containsKey(GobblinClusterConfigurationKeys.PLANNING_ID_KEY)) {
      throw new RuntimeException("Job doesn't have planning ID");
    }

    this.planningJobId = jobPlusSysConfig.getProperty(GobblinClusterConfigurationKeys.PLANNING_ID_KEY);
    this.jobsMapping = jobsMapping;
    this.appWorkDir = builder.getAppWorkPath();
    this.metadataTags = Tag.fromMap(new ImmutableMap.Builder<String, Object>()
        .put(GobblinClusterMetricTagNames.APPLICATION_NAME, builder.getApplicationName())
        .put(GobblinClusterMetricTagNames.APPLICATION_ID, builder.getApplicationId())
        .build());
  }

   static class GobblinHelixJobTaskMetrics extends StandardMetricsBridge.StandardMetrics {
    static final String TIME_BETWEEN_JOB_SUBMISSION_AND_EXECUTION = "timeBetweenJobSubmissionAndExecution";
    final ContextAwareTimer timeBetweenJobSubmissionAndExecution;

    public GobblinHelixJobTaskMetrics(MetricContext metricContext, int windowSizeInMin) {
      timeBetweenJobSubmissionAndExecution = metricContext.contextAwareTimer(TIME_BETWEEN_JOB_SUBMISSION_AND_EXECUTION,
          windowSizeInMin, TimeUnit.MINUTES);
      this.contextAwareMetrics.add(timeBetweenJobSubmissionAndExecution);
    }

    public void updateTimeBetweenJobSubmissionAndExecution(Properties jobProps) {
      long jobSubmitTime = Long.parseLong(jobProps.getProperty(GobblinClusterConfigurationKeys.PLANNING_JOB_CREATE_TIME, "0"));
      if (jobSubmitTime != 0) {
        Instrumented.updateTimer(com.google.common.base.Optional.of(this.timeBetweenJobSubmissionAndExecution),
            System.currentTimeMillis() - jobSubmitTime,
            TimeUnit.MILLISECONDS);
      }
    }
  }

  private GobblinHelixJobLauncher createJobLauncher()
      throws Exception {
    return new GobblinHelixJobLauncher(jobPlusSysConfig,
        this.jobHelixManager,
        this.appWorkDir,
        this.metadataTags,
        new ConcurrentHashMap<>());
  }

  /**
   * Launch the actual {@link GobblinHelixJobLauncher}.
   */
  @Override
  public TaskResult run() {
    log.info("Running planning job {} [{} {}]", this.planningJobId, this.applicationName, this.instanceName);
    this.jobTaskMetrics.updateTimeBetweenJobSubmissionAndExecution(this.jobPlusSysConfig);

    try (Closer closer = Closer.create()) {
      String jobName = jobPlusSysConfig.getProperty(ConfigurationKeys.JOB_NAME_KEY);

      Optional<String> planningIdFromStateStore = this.jobsMapping.getPlanningJobId(jobName);

      long timeOut = PropertiesUtils.getPropAsLong(jobPlusSysConfig,
                                         GobblinClusterConfigurationKeys.HELIX_WORKFLOW_DELETE_TIMEOUT_SECONDS,
                                         GobblinClusterConfigurationKeys.DEFAULT_HELIX_WORKFLOW_DELETE_TIMEOUT_SECONDS) * 1000;

      if (planningIdFromStateStore.isPresent() && !planningIdFromStateStore.get().equals(this.planningJobId)) {
        return new TaskResult(TaskResult.Status.FAILED, "Exception occurred for job " + planningJobId
            + ": because planning job in state store has different id (" + planningIdFromStateStore.get() + ")");
      }

      while (true) {
        Optional<String> actualJobIdFromStateStore = this.jobsMapping.getActualJobId(jobName);
        if (actualJobIdFromStateStore.isPresent()) {
          String previousActualJobId = actualJobIdFromStateStore.get();
          if (HelixUtils.isJobFinished(previousActualJobId, previousActualJobId, this.jobHelixManager)) {
            log.info("Previous actual job {} [plan: {}] finished, will launch a new job.", previousActualJobId, this.planningJobId);
          } else {
            log.info("Previous actual job {} [plan: {}] not finished, kill it now.", previousActualJobId, this.planningJobId);
            try {
              HelixUtils.deleteWorkflow(previousActualJobId, this.jobHelixManager, timeOut);
            } catch (HelixException e) {
              log.error("Helix cannot delete previous actual job id {} within 5 min.", previousActualJobId);
              return new TaskResult(TaskResult.Status.FAILED, ExceptionUtils.getFullStackTrace(e));
            }
          }
        } else {
          log.info("Actual job {} does not exist. First time run.", this.planningJobId);
        }

        this.launcher = createJobLauncher();

        this.jobsMapping.setActualJobId(jobName, this.planningJobId, this.launcher.getJobId());

        closer.register(launcher).launchJob(this.jobLauncherListener);

        if (!this.launcher.isEarlyStopped()) {
          break;
        } else {
          log.info("Planning job {} has more runs due to early stop.", this.planningJobId);
        }
      }
    } catch (Exception e) {
      log.info("Failing planning job {}", this.planningJobId);
      return new TaskResult(TaskResult.Status.FAILED, "Exception occurred for job " + planningJobId + ":" + ExceptionUtils
          .getFullStackTrace(e));
    }
    log.info("Completing planning job {}", this.planningJobId);
    return new TaskResult(TaskResult.Status.COMPLETED, "");
  }

  @Override
  public void cancel() {
    log.info("Cancelling planning job {}", this.planningJobId);
    if (launcher != null) {
      try {
        launcher.cancelJob(this.jobLauncherListener);
      } catch (JobException e) {
        throw new RuntimeException("Unable to cancel planning job " + this.planningJobId + ": ", e);
      }
    }
  }
}
