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

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.fs.Path;
import org.apache.helix.HelixManager;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.runtime.api.ExecutionResult;
import org.apache.gobblin.runtime.api.JobExecutionLauncher;
import org.apache.gobblin.runtime.api.JobExecutionMonitor;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.MonitoredObject;
import org.apache.gobblin.runtime.util.StateStores;
import org.apache.gobblin.source.extractor.partition.Partitioner;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.JobLauncherUtils;
import org.apache.gobblin.util.PropertiesUtils;


/**
 * To avoid all the task driver logic ({@link GobblinHelixJobLauncher}) runs on the same instance (node), this
 * {@link JobExecutionLauncher} can distribute the original job (called planning job) to Helix. Helix will
 * assign this job to one participant. The participant can parse the original job properties and run the task driver.
 *
 * <p>
 *   For job submission, the Helix workflow name will be the original job name with prefix
 *   {@link GobblinClusterConfigurationKeys#PLANNING_JOB_NAME_PREFIX}. The Helix job name will be the auto-generated planning
 *   job ID with prefix {@link GobblinClusterConfigurationKeys#PLANNING_ID_KEY}.
 * </p>
 *
 * <p>
 *   We will associate this job to Helix's {@link org.apache.helix.task.TaskFactory}
 *   by specifying {@link GobblinTaskRunner#GOBBLIN_JOB_FACTORY_NAME} in the {@link JobConfig.Builder}.
 *   This job will only contain a single task, which is the same as planningID.
 * </p>
 */
@Alpha
@Slf4j
class GobblinHelixDistributeJobExecutionLauncher implements JobExecutionLauncher, Closeable {
  protected HelixManager helixManager;
  protected TaskDriver helixTaskDriver;
  protected Properties sysProperties;
  protected Properties jobProperties;
  protected StateStores stateStores;

  protected static final String PLANNING_WORK_UNIT_DIR_NAME = "_plan_workunits";
  protected static final String PLANNING_TASK_STATE_DIR_NAME = "_plan_taskstates";
  protected static final String PLANNING_JOB_STATE_DIR_NAME = "_plan_jobstates";

  protected static final String JOB_PROPS_PREFIX = "gobblin.jobProps.";

  private final long jobQueueDeleteTimeoutSeconds;

  private boolean jobSubmitted;

  public GobblinHelixDistributeJobExecutionLauncher(Builder builder) throws Exception {
    this.helixManager = builder.manager;
    this.helixTaskDriver = new TaskDriver(this.helixManager);
    this.sysProperties = builder.sysProperties;
    this.jobProperties = builder.jobProperties;
    this.jobSubmitted = false;
    Config combined = ConfigUtils.propertiesToConfig(jobProperties)
        .withFallback(ConfigUtils.propertiesToConfig(sysProperties));

    Config stateStoreJobConfig = combined
        .withValue(ConfigurationKeys.STATE_STORE_FS_URI_KEY, ConfigValueFactory.fromAnyRef(
            new URI(builder.appWorkDir.toUri().getScheme(), null, builder.appWorkDir.toUri().getHost(),
                builder.appWorkDir.toUri().getPort(), null, null, null).toString()));

    this.stateStores = new StateStores(stateStoreJobConfig,
        builder.appWorkDir, PLANNING_TASK_STATE_DIR_NAME,
        builder.appWorkDir, PLANNING_WORK_UNIT_DIR_NAME,
        builder.appWorkDir, PLANNING_JOB_STATE_DIR_NAME);

    this.jobQueueDeleteTimeoutSeconds = ConfigUtils.getLong(combined,
        GobblinClusterConfigurationKeys.HELIX_JOB_QUEUE_DELETE_TIMEOUT_SECONDS,
        GobblinClusterConfigurationKeys.DEFAULT_HELIX_JOB_QUEUE_DELETE_TIMEOUT_SECONDS);
  }

  @Override
  public void close()
      throws IOException {
    // we should delete the planning job at the end.
    if (this.jobSubmitted) {
      String planningName = getPlanningJobName(this.jobProperties);
      log.info("[DELETE] workflow {} in the close.", planningName);
      this.helixTaskDriver.delete(planningName);
    }
  }

  @Setter
  public static class Builder {
    Properties sysProperties;
    Properties jobProperties;
    HelixManager manager;
    Path appWorkDir;
    public GobblinHelixDistributeJobExecutionLauncher build() throws Exception {
      return new GobblinHelixDistributeJobExecutionLauncher(this);
    }
  }

  private String getPlanningJobName (Properties jobProps) {
    String jobName = JobState.getJobNameFromProps(jobProps);
    return GobblinClusterConfigurationKeys.PLANNING_JOB_NAME_PREFIX + jobName;
  }

  protected String getPlanningJobId (Properties jobProps) {
    if (jobProps.containsKey(GobblinClusterConfigurationKeys.PLANNING_ID_KEY)) {
      return jobProps.getProperty(GobblinClusterConfigurationKeys.PLANNING_ID_KEY);
    }
    String planningId = JobLauncherUtils.newJobId(getPlanningJobName(jobProps));
    jobProps.setProperty(GobblinClusterConfigurationKeys.PLANNING_ID_KEY, planningId);
    return planningId;
  }

  /**
   * Create a job config builder which has a single task that wraps the original jobProps.
   */
  private JobConfig.Builder createPlanningJob (Properties jobProps) {
    // Create a single task for job planning
    String planningId = getPlanningJobId(jobProps);
    Map<String, TaskConfig> taskConfigMap = Maps.newHashMap();
    Map<String, String> rawConfigMap = Maps.newHashMap();
    for (String key : jobProps.stringPropertyNames()) {
      rawConfigMap.put(JOB_PROPS_PREFIX + key, (String)jobProps.get(key));
    }
    rawConfigMap.put(GobblinClusterConfigurationKeys.TASK_SUCCESS_OPTIONAL_KEY, "true");

    // Create a single Job which only contains a single task
    taskConfigMap.put(planningId, TaskConfig.Builder.from(rawConfigMap));
    JobConfig.Builder jobConfigBuilder = new JobConfig.Builder();

    jobConfigBuilder.setTimeoutPerTask(PropertiesUtils.getPropAsLong(
        jobProps,
        ConfigurationKeys.HELIX_TASK_TIMEOUT_SECONDS,
        ConfigurationKeys.DEFAULT_HELIX_TASK_TIMEOUT_SECONDS) * 1000);

    jobConfigBuilder.setFailureThreshold(1);
    jobConfigBuilder.addTaskConfigMap(taskConfigMap).setCommand(GobblinTaskRunner.GOBBLIN_JOB_FACTORY_NAME);

    return jobConfigBuilder;
  }

  /**
   * Submit job to helix so that it can be re-assigned to one of its participants.
   * @param jobName A planning job name which has prefix {@link GobblinClusterConfigurationKeys#PLANNING_JOB_NAME_PREFIX}.
   * @param jobId   A planning job id created by {@link GobblinHelixDistributeJobExecutionLauncher#getPlanningJobId}.
   * @param jobConfigBuilder A job config builder which contains a single task.
   */
  private void submitJobToHelix(String jobName, String jobId, JobConfig.Builder jobConfigBuilder) throws Exception {
    TaskDriver taskDriver = new TaskDriver(this.helixManager);
    HelixUtils.submitJobToQueue(jobConfigBuilder,
        jobName,
        jobId,
        taskDriver,
        this.helixManager,
        this.jobQueueDeleteTimeoutSeconds);
    this.jobSubmitted = true;
  }

  @Override
  public DistributeJobMonitor launchJob(JobSpec jobSpec) {
    return new DistributeJobMonitor(new DistributeJobCallable(this.jobProperties));
  }

  @AllArgsConstructor
  private class DistributeJobCallable implements Callable<ExecutionResult> {
    Properties jobProps;
    @Override
    public DistributeJobResult call()
        throws Exception {
      String planningName = getPlanningJobName(this.jobProps);
      String planningId = getPlanningJobId(this.jobProps);
      JobConfig.Builder builder = createPlanningJob(this.jobProps);
      try {
        submitJobToHelix(planningName, planningId, builder);
        return waitForJobCompletion(planningName, planningId);
      } catch (Exception e) {
        log.error(planningName + " is not able to submit.");
        return new DistributeJobResult(Optional.empty(), Optional.of(e));
      }
    }
  }

  private DistributeJobResult waitForJobCompletion(String planningName, String planningId) throws InterruptedException {
    boolean timeoutEnabled = Boolean.parseBoolean(this.jobProperties.getProperty(ConfigurationKeys.HELIX_JOB_TIMEOUT_ENABLED_KEY,
        ConfigurationKeys.DEFAULT_HELIX_JOB_TIMEOUT_ENABLED));
    long timeoutInSeconds = Long.parseLong(this.jobProperties.getProperty(ConfigurationKeys.HELIX_JOB_TIMEOUT_SECONDS,
        ConfigurationKeys.DEFAULT_HELIX_JOB_TIMEOUT_SECONDS));

    try {
      HelixUtils.waitJobCompletion(
          GobblinHelixDistributeJobExecutionLauncher.this.helixManager,
          planningName,
          planningId,
          timeoutEnabled? Optional.of(timeoutInSeconds) : Optional.empty());
      return getResultFromUserContent();
    } catch (TimeoutException te) {
      helixTaskDriver.waitToStop(planningName, 10L);
      log.info("[DELETE] workflow {} timeout.", planningName);
      this.helixTaskDriver.delete(planningName);
      this.helixTaskDriver.resume(planningName);
      log.info("stopped the queue, deleted the job");
      return new DistributeJobResult(Optional.empty(), Optional.of(te));
    }
  }

  //TODO: change below to Helix UserConentStore
  @VisibleForTesting
  protected DistributeJobResult getResultFromUserContent() {
    String planningId = getPlanningJobId(this.jobProperties);
    try {
      TaskState taskState = this.stateStores.getTaskStateStore().get(planningId, planningId, planningId);
      return new DistributeJobResult(Optional.of(taskState.getProperties()), Optional.empty());
    } catch (IOException e) {
      return new DistributeJobResult(Optional.empty(), Optional.of(e));
    }
  }

  @Getter
  @AllArgsConstructor
  static class DistributeJobResult implements ExecutionResult {
    boolean isEarlyStopped = false;
    Optional<Properties> properties;
    Optional<Throwable> throwable;
    public DistributeJobResult(Optional<Properties> properties, Optional<Throwable> throwable) {
      this.properties = properties;
      this.throwable = throwable;
      if (properties.isPresent()) {
        isEarlyStopped = PropertiesUtils.getPropAsBoolean(this.properties.get(), Partitioner.IS_EARLY_STOPPED, "false");
      }
    }
  }

  static class DistributeJobMonitor extends FutureTask<ExecutionResult> implements JobExecutionMonitor {
    private ExecutorService executor = Executors.newSingleThreadExecutor();
    public DistributeJobMonitor (Callable<ExecutionResult> c) {
      super(c);
      this.executor.execute(this);
    }

    @Override
    public MonitoredObject getRunningState() {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public StandardMetrics getMetrics() {
    throw new UnsupportedOperationException();
  }

  @Nonnull
  @Override
  public MetricContext getMetricContext() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isInstrumentationEnabled() {
    return false;
  }

  @Override
  public List<Tag<?>> generateTags(State state) {
    return Lists.newArrayList();
  }

  @Override
  public void switchMetricContext(List<Tag<?>> tags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void switchMetricContext(MetricContext context) {
    throw new UnsupportedOperationException();
  }
}
