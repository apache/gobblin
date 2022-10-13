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
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.runtime.api.ExecutionResult;
import org.apache.gobblin.runtime.api.JobExecutionLauncher;
import org.apache.gobblin.runtime.api.JobExecutionMonitor;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.MonitoredObject;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PropertiesUtils;


/**
 * To avoid all the task driver logic ({@link GobblinHelixJobLauncher}) runs on the same
 * instance (manager), this {@link JobExecutionLauncher} will distribute the original job
 * to one of the task driver instance. The original task driver logic will be launched there.
 *
 * <p>
 *   For job submission, the Helix workflow name will be the original job name with prefix
 *   {@link GobblinClusterConfigurationKeys#PLANNING_JOB_NAME_PREFIX}. The Helix job name
 *   will be the auto-generated planning job ID with prefix
 *   {@link GobblinClusterConfigurationKeys#PLANNING_ID_KEY}.
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

  protected HelixManager planningJobHelixManager;
  protected TaskDriver helixTaskDriver;
  protected Properties sysProps;
  protected Properties jobPlanningProps;
  protected HelixJobsMapping jobsMapping;
  protected GobblinHelixPlanningJobLauncherMetrics planningJobLauncherMetrics;
  protected GobblinHelixMetrics helixMetrics;

  protected static final String JOB_PROPS_PREFIX = "gobblin.jobProps.";

  private final long workFlowExpiryTimeSeconds;

  private final long helixJobStopTimeoutSeconds;

  private boolean jobSubmitted;

  // A conditional variable for which the condition is satisfied if a cancellation is requested
  private final Object cancellationRequest = new Object();
  // A flag indicating whether a cancellation has been requested or not
  private volatile boolean cancellationRequested = false;
  // A flag indicating whether a cancellation has been executed or not
  private volatile boolean cancellationExecuted = false;
  // A flag indicating wheter a planning job should wait for its completion
  private boolean nonBlockingMode = false;

  @Getter
  private DistributeJobMonitor jobMonitor;

  private final Config combinedConfigs;

  public GobblinHelixDistributeJobExecutionLauncher(Builder builder) {
    this.planningJobHelixManager = builder.planningJobHelixManager;

    this.helixTaskDriver = new TaskDriver(this.planningJobHelixManager);
    this.sysProps = builder.sysProps;
    this.jobPlanningProps = builder.jobPlanningProps;
    this.jobSubmitted = false;

    combinedConfigs = ConfigUtils.propertiesToConfig(jobPlanningProps)
        .withFallback(ConfigUtils.propertiesToConfig(sysProps));

    this.workFlowExpiryTimeSeconds = ConfigUtils.getLong(this.combinedConfigs,
        GobblinClusterConfigurationKeys.HELIX_WORKFLOW_EXPIRY_TIME_SECONDS,
        GobblinClusterConfigurationKeys.DEFAULT_HELIX_WORKFLOW_EXPIRY_TIME_SECONDS);
    this.planningJobLauncherMetrics = builder.planningJobLauncherMetrics;
    this.nonBlockingMode = ConfigUtils.getBoolean(this.combinedConfigs,
        GobblinClusterConfigurationKeys.NON_BLOCKING_PLANNING_JOB_ENABLED,
        GobblinClusterConfigurationKeys.DEFAULT_NON_BLOCKING_PLANNING_JOB_ENABLED);
    this.helixMetrics = builder.helixMetrics;
    this.jobsMapping = builder.jobsMapping;
    this.helixJobStopTimeoutSeconds = ConfigUtils.getLong(this.combinedConfigs,
        GobblinClusterConfigurationKeys.HELIX_JOB_STOP_TIMEOUT_SECONDS,
        GobblinClusterConfigurationKeys.DEFAULT_HELIX_JOB_STOP_TIMEOUT_SECONDS);
  }

  @Override
  public void close()  throws IOException {
  }

  private void executeCancellation() {
    if (this.jobSubmitted) {
      String planningJobId = getPlanningJobId(this.jobPlanningProps);
      try {
        if (this.cancellationRequested && !this.cancellationExecuted) {
          boolean cancelByDelete = ConfigUtils.getBoolean(this.combinedConfigs, GobblinClusterConfigurationKeys.CANCEL_HELIX_JOB_BY_DELETE,
              GobblinClusterConfigurationKeys.DEFAULT_CANCEL_HELIX_JOB_BY_DELETE);
          HelixUtils.cancelWorkflow(planningJobId, this.planningJobHelixManager, helixJobStopTimeoutSeconds * 1000, cancelByDelete);
          log.info("Canceled the workflow {}", planningJobId);
        }
      } catch (HelixException e) {
        // Cancellation may throw an exception, but Helix set the job state to STOP/DELETE and it should eventually be cleaned up
        // We will keep this.cancellationExecuted and this.cancellationRequested to true and not propagate the exception
        log.error("Failed to cancel workflow {} in Helix", planningJobId, e);
      } catch (InterruptedException e) {
        log.error("Thread interrupted while trying to cancel the workflow {} in Helix", planningJobId);
        Thread.currentThread().interrupt();
      }
    }
  }

  @Setter
  public static class Builder {
    Properties sysProps;
    Properties jobPlanningProps;
    HelixManager planningJobHelixManager;
    Path appWorkDir;
    GobblinHelixPlanningJobLauncherMetrics planningJobLauncherMetrics;
    GobblinHelixMetrics helixMetrics;
    HelixJobsMapping jobsMapping;
    public GobblinHelixDistributeJobExecutionLauncher build() throws Exception {
      return new GobblinHelixDistributeJobExecutionLauncher(this);
    }
  }

  protected String getPlanningJobId (Properties jobPlanningProps) {
    if (jobPlanningProps.containsKey(GobblinClusterConfigurationKeys.PLANNING_ID_KEY)) {
      return jobPlanningProps.getProperty(GobblinClusterConfigurationKeys.PLANNING_ID_KEY);
    } else {
      throw new RuntimeException("Cannot find planning id");
    }
  }

  /**
   * Create a job config builder which has a single task that wraps the original jobProps.
   *
   * The planning job (which runs the original {@link GobblinHelixJobLauncher}) will be
   * executed on one of the Helix participants.
   *
   * We rely on the underlying {@link GobblinHelixJobLauncher} to correctly handle the task
   * execution timeout so that the planning job itself is relieved of the timeout constrain.
   *
   * In short, the planning job will run once and requires no timeout.
   */
  private JobConfig.Builder createJobBuilder (Properties jobProps) {
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

    // We want GobblinHelixJobLauncher only run once.
    jobConfigBuilder.setMaxAttemptsPerTask(1);

    // Planning job never timeout (Helix defaults 1h timeout, set a large number '1 month')
    jobConfigBuilder.setTimeoutPerTask(JobConfig.DEFAULT_TIMEOUT_PER_TASK * 24 * 30);

    // Planning job should have its own tag support
    if (jobProps.containsKey(GobblinClusterConfigurationKeys.HELIX_PLANNING_JOB_TAG_KEY)) {
      String jobPlanningTag = jobProps.getProperty(GobblinClusterConfigurationKeys.HELIX_PLANNING_JOB_TAG_KEY);
      log.info("PlanningJob {} has tags associated : {}", planningId, jobPlanningTag);
      jobConfigBuilder.setInstanceGroupTag(jobPlanningTag);
    }

    // Planning job should have its own type support
    if (jobProps.containsKey(GobblinClusterConfigurationKeys.HELIX_PLANNING_JOB_TYPE_KEY)) {
      String jobType = jobProps.getProperty(GobblinClusterConfigurationKeys.HELIX_PLANNING_JOB_TYPE_KEY);
      log.info("PlanningJob {} has types associated : {}", planningId, jobType);
      jobConfigBuilder.setJobType(jobType);
    }

    jobConfigBuilder.setNumConcurrentTasksPerInstance(PropertiesUtils.getPropAsInt(jobProps,
        GobblinClusterConfigurationKeys.HELIX_CLUSTER_TASK_CONCURRENCY,
        GobblinClusterConfigurationKeys.HELIX_CLUSTER_TASK_CONCURRENCY_DEFAULT));

    jobConfigBuilder.setFailureThreshold(1);
    jobConfigBuilder.addTaskConfigMap(taskConfigMap).setCommand(GobblinTaskRunner.GOBBLIN_JOB_FACTORY_NAME);

    return jobConfigBuilder;
  }

  /**
   * Submit a planning job to helix so that it can launched from a remote node.
   * @param jobName A planning job name which has prefix {@link GobblinClusterConfigurationKeys#PLANNING_JOB_NAME_PREFIX}.
   * @param jobId   A planning job id created by {@link GobblinHelixDistributeJobExecutionLauncher#getPlanningJobId}.
   * @param jobConfigBuilder A job config builder which contains a single task.
   */
  private void submitJobToHelix(String jobName, String jobId, JobConfig.Builder jobConfigBuilder) throws Exception {
    TaskDriver taskDriver = new TaskDriver(this.planningJobHelixManager);
    HelixUtils.submitJobToWorkFlow(jobConfigBuilder,
        jobName,
        jobId,
        taskDriver,
        this.planningJobHelixManager,
        this.workFlowExpiryTimeSeconds);
    this.jobSubmitted = true;
  }

  @Override
  public DistributeJobMonitor launchJob(@Nullable  JobSpec jobSpec) {
    this.jobMonitor = new DistributeJobMonitor(new DistributeJobCallable(this.jobPlanningProps));
    return this.jobMonitor;
  }

  @AllArgsConstructor
  private class DistributeJobCallable implements Callable<ExecutionResult> {
    Properties jobPlanningProps;
    @Override
    public DistributeJobResult call()
        throws Exception {
      String planningId = getPlanningJobId(this.jobPlanningProps);
      JobConfig.Builder builder = createJobBuilder(this.jobPlanningProps);
      try {
        long submitStartTime = System.currentTimeMillis();
        GobblinHelixDistributeJobExecutionLauncher.this.helixMetrics.submitMeter.mark();
        submitJobToHelix(planningId, planningId, builder);
        GobblinHelixDistributeJobExecutionLauncher.this.helixMetrics.updateTimeForHelixSubmit(submitStartTime);
        long waitStartTime = System.currentTimeMillis();

        // we should not wait if in non-blocking mode.
        DistributeJobResult rst = new DistributeJobResult();
        if (!GobblinHelixDistributeJobExecutionLauncher.this.nonBlockingMode) {
          rst = waitForJobCompletion(planningId, planningId);
          GobblinHelixDistributeJobExecutionLauncher.this.helixMetrics.updateTimeForHelixWait(waitStartTime);
        }

        return rst;
      } catch (Exception e) {
        log.error(planningId + " is not able to submit.");
        return new DistributeJobResult();
      }
    }
  }

  private DistributeJobResult waitForJobCompletion(String workFlowName, String jobName) throws InterruptedException {
    boolean timeoutEnabled = Boolean.parseBoolean(this.jobPlanningProps.getProperty(
        GobblinClusterConfigurationKeys.HELIX_JOB_TIMEOUT_ENABLED_KEY,
        GobblinClusterConfigurationKeys.DEFAULT_HELIX_JOB_TIMEOUT_ENABLED));
    long timeoutInSeconds = Long.parseLong(this.jobPlanningProps.getProperty(
        GobblinClusterConfigurationKeys.HELIX_JOB_TIMEOUT_SECONDS,
        GobblinClusterConfigurationKeys.DEFAULT_HELIX_JOB_TIMEOUT_SECONDS));

    long stoppingStateTimeoutInSeconds = PropertiesUtils
        .getPropAsLong(this.jobPlanningProps, GobblinClusterConfigurationKeys.HELIX_JOB_STOPPING_STATE_TIMEOUT_SECONDS,
            GobblinClusterConfigurationKeys.DEFAULT_HELIX_JOB_STOPPING_STATE_TIMEOUT_SECONDS);

    try {
      HelixUtils.waitJobCompletion(
          GobblinHelixDistributeJobExecutionLauncher.this.planningJobHelixManager,
          workFlowName,
          jobName,
          timeoutEnabled ? Optional.of(timeoutInSeconds) : Optional.empty(),
          stoppingStateTimeoutInSeconds);
      return getResultFromUserContent();
    } catch (TimeoutException te) {
      HelixUtils.handleJobTimeout(workFlowName, jobName,
          planningJobHelixManager, this, null);
      return new DistributeJobResult();
    }
  }

  protected DistributeJobResult getResultFromUserContent() {
    return new DistributeJobResult();
  }

  /**
   * If {@link GobblinClusterConfigurationKeys#NON_BLOCKING_PLANNING_JOB_ENABLED} is enabled
   * this result object contains nothing; otherwise this result object can be used to contain
   * any values returned from other task-driver instances.
   */
  static class DistributeJobResult implements ExecutionResult {
  }

  private class DistributeJobMonitor extends FutureTask<ExecutionResult> implements JobExecutionMonitor {
    private ExecutorService executor = Executors.newSingleThreadExecutor();
    DistributeJobMonitor (Callable<ExecutionResult> c) {
      super(c);
      this.executor.execute(this);
    }

    @Override
    public MonitoredObject getRunningState() {
      throw new UnsupportedOperationException();
    }

    /**
     * We override Future's cancel method, which means we do not send the interrupt to the underlying thread.
     * Instead of that, we submit a STOP request to handle, and the underlying thread is supposed to gracefully accept
     * the STOPPED state and exit in {@link #waitForJobCompletion} method.
     * @param mayInterruptIfRunning this is ignored.
     * @return true always
     */
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      GobblinHelixDistributeJobExecutionLauncher.this.executeCancellation();
      return true;
    }
  }

  /**
   * This method calls the underlying {@link DistributeJobMonitor}'s cancel method.
   * It uses a conditional variable {@link GobblinHelixDistributeJobExecutionLauncher#cancellationRequest}
   * and a flag {@link GobblinHelixDistributeJobExecutionLauncher#cancellationRequested} to avoid double cancellation.
   */
  public void cancel() {
    DistributeJobMonitor jobMonitor = getJobMonitor();
    if (jobMonitor != null) {
      synchronized (GobblinHelixDistributeJobExecutionLauncher.this.cancellationRequest) {
        if (GobblinHelixDistributeJobExecutionLauncher.this.cancellationRequested) {
          // Return immediately if a cancellation has already been requested
          return;
        }
        GobblinHelixDistributeJobExecutionLauncher.this.cancellationRequested = true;
      }
      jobMonitor.cancel(true);
      GobblinHelixDistributeJobExecutionLauncher.this.cancellationExecuted = true;
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
