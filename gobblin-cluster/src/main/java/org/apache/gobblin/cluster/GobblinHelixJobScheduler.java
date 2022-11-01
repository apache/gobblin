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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;

import org.apache.hadoop.fs.Path;
import org.apache.helix.HelixManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rholder.retry.AttemptTimeLimiters;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.Striped;
import com.typesafe.config.Config;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.cluster.event.CancelJobConfigArrivalEvent;
import org.apache.gobblin.cluster.event.DeleteJobConfigArrivalEvent;
import org.apache.gobblin.cluster.event.NewJobConfigArrivalEvent;
import org.apache.gobblin.cluster.event.UpdateJobConfigArrivalEvent;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.instrumented.StandardMetricsBridge;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.runtime.JobException;
import org.apache.gobblin.runtime.api.MutableJobCatalog;
import org.apache.gobblin.runtime.listeners.JobListener;
import org.apache.gobblin.scheduler.JobScheduler;
import org.apache.gobblin.scheduler.SchedulerService;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.PropertiesUtils;


/**
 * An extension to {@link JobScheduler} that schedules and runs
 * Gobblin jobs on Helix.
 *
 * <p> The actual job running logic is handled by
 * {@link HelixRetriggeringJobCallable}. This callable will first
 * determine if this job should be launched from the same node
 * where the scheduler is running, or from a remote node.
 *
 * <p> If the job should be launched from the scheduler node,
 * {@link GobblinHelixJobLauncher} is invoked. Else the
 * {@link GobblinHelixDistributeJobExecutionLauncher} is invoked.
 *
 * <p> More details can be found at {@link HelixRetriggeringJobCallable}.
 */
@Alpha
public class GobblinHelixJobScheduler extends JobScheduler implements StandardMetricsBridge {

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinHelixJobScheduler.class);
  private static final String COMMON_JOB_PROPS = "gobblin.common.job.props";

  private final Properties commonJobProperties;
  private final HelixManager jobHelixManager;
  private final Optional<HelixManager> taskDriverHelixManager;
  private final EventBus eventBus;
  private final Path appWorkDir;
  private final List<? extends Tag<?>> metadataTags;
  private final ConcurrentHashMap<String, Boolean> jobRunningMap;
  private final MutableJobCatalog jobCatalog;
  private final MetricContext metricContext;

  final GobblinHelixMetrics helixMetrics;
  final GobblinHelixJobSchedulerMetrics jobSchedulerMetrics;
  final GobblinHelixJobLauncherMetrics launcherMetrics;
  final GobblinHelixPlanningJobLauncherMetrics planningJobLauncherMetrics;
  final HelixJobsMapping jobsMapping;
  final Striped<Lock> locks = Striped.lazyWeakLock(256);
  private final long helixWorkflowListingTimeoutMillis;

  private boolean startServicesCompleted;
  private final long helixJobStopTimeoutMillis;

  public GobblinHelixJobScheduler(Config sysConfig,
                                  HelixManager jobHelixManager,
                                  Optional<HelixManager> taskDriverHelixManager,
                                  EventBus eventBus,
                                  Path appWorkDir, List<? extends Tag<?>> metadataTags,
                                  SchedulerService schedulerService,
                                  MutableJobCatalog jobCatalog) throws Exception {

    super(ConfigUtils.configToProperties(sysConfig), schedulerService);
    this.commonJobProperties = ConfigUtils.configToProperties(ConfigUtils.getConfigOrEmpty(sysConfig, COMMON_JOB_PROPS));
    this.jobHelixManager = jobHelixManager;
    this.taskDriverHelixManager = taskDriverHelixManager;
    this.eventBus = eventBus;
    this.jobRunningMap = new ConcurrentHashMap<>();
    this.appWorkDir = appWorkDir;
    this.metadataTags = metadataTags;
    this.jobCatalog = jobCatalog;
    this.metricContext = Instrumented.getMetricContext(new org.apache.gobblin.configuration.State(properties), this.getClass());

    int metricsWindowSizeInMin = ConfigUtils.getInt(sysConfig,
                                                    ConfigurationKeys.METRIC_TIMER_WINDOW_SIZE_IN_MINUTES,
                                                    ConfigurationKeys.DEFAULT_METRIC_TIMER_WINDOW_SIZE_IN_MINUTES);

    this.launcherMetrics = new GobblinHelixJobLauncherMetrics("launcherInScheduler",
                                                              this.metricContext,
                                                              metricsWindowSizeInMin);

    this.jobSchedulerMetrics = new GobblinHelixJobSchedulerMetrics(this.jobExecutor,
                                                                   this.metricContext,
                                                                   metricsWindowSizeInMin);

    this.jobsMapping = new HelixJobsMapping(ConfigUtils.propertiesToConfig(properties),
        PathUtils.getRootPath(appWorkDir).toUri(),
        appWorkDir.toString());

    this.planningJobLauncherMetrics = new GobblinHelixPlanningJobLauncherMetrics("planningLauncherInScheduler",
                                                                          this.metricContext,
                                                                          metricsWindowSizeInMin, this.jobsMapping);

    this.helixMetrics = new GobblinHelixMetrics("helixMetricsInJobScheduler",
                                                  this.metricContext,
                                                  metricsWindowSizeInMin);

    this.startServicesCompleted = false;

    this.helixJobStopTimeoutMillis = ConfigUtils.getLong(sysConfig, GobblinClusterConfigurationKeys.HELIX_JOB_STOP_TIMEOUT_SECONDS,
        GobblinClusterConfigurationKeys.DEFAULT_HELIX_JOB_STOP_TIMEOUT_SECONDS) * 1000;

    this.helixWorkflowListingTimeoutMillis = ConfigUtils.getLong(sysConfig, GobblinClusterConfigurationKeys.HELIX_WORKFLOW_LISTING_TIMEOUT_SECONDS,
        GobblinClusterConfigurationKeys.DEFAULT_HELIX_WORKFLOW_LISTING_TIMEOUT_SECONDS) * 1000;

  }

  @Override
  public Collection<StandardMetrics> getStandardMetricsCollection() {
    return ImmutableList.of(this.launcherMetrics,
                            this.jobSchedulerMetrics,
                            this.planningJobLauncherMetrics,
                            this.helixMetrics);
  }

  @Override
  protected void startUp() throws Exception {
    this.eventBus.register(this);
    super.startUp();
    this.startServicesCompleted = true;
  }

  @Override
  public void scheduleJob(Properties jobProps, JobListener jobListener) throws JobException {
    try {
      while (!startServicesCompleted) {
        LOGGER.info("{} service is not fully up, waiting here...", this.getClass().getName());
        Thread.sleep(1000);
      }

      scheduleJob(jobProps,
                  jobListener,
                  Maps.newHashMap(),
                  GobblinHelixJob.class);

    } catch (Exception e) {
      throw new JobException("Failed to schedule job " + jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), e);
    }
  }

  @Override
  protected void startServices() throws Exception {

    boolean cleanAllDistJobs = PropertiesUtils.getPropAsBoolean(this.properties,
        GobblinClusterConfigurationKeys.CLEAN_ALL_DIST_JOBS,
        String.valueOf(GobblinClusterConfigurationKeys.DEFAULT_CLEAN_ALL_DIST_JOBS));

    if (cleanAllDistJobs) {
      for (org.apache.gobblin.configuration.State state : this.jobsMapping.getAllStates()) {
        String jobUri = state.getId();
        LOGGER.info("Delete mapping for job " + jobUri);
        this.jobsMapping.deleteMapping(jobUri);
      }
    }
  }

  @Override
  public void runJob(Properties jobProps, JobListener jobListener) throws JobException {
    new HelixRetriggeringJobCallable(this,
        this.jobCatalog,
        this.properties,
        jobProps,
        jobListener,
        this.planningJobLauncherMetrics,
        this.helixMetrics,
        this.appWorkDir,
        this.jobHelixManager,
        this.taskDriverHelixManager,
        this.jobsMapping,
        this.locks,
        this.metricContext).call();
  }

  @Override
  public GobblinHelixJobLauncher buildJobLauncher(Properties jobProps)
      throws Exception {
    Properties combinedProps = new Properties();
    combinedProps.putAll(properties);
    combinedProps.putAll(jobProps);

    return new GobblinHelixJobLauncher(combinedProps,
        this.jobHelixManager,
        this.appWorkDir,
        this.metadataTags,
        this.jobRunningMap,
        Optional.of(this.helixMetrics));
  }

  public Future<?> scheduleJobImmediately(Properties jobProps, JobListener jobListener) {
    HelixRetriggeringJobCallable retriggeringJob = new HelixRetriggeringJobCallable(this,
        this.jobCatalog,
        this.properties,
        jobProps,
        jobListener,
        this.planningJobLauncherMetrics,
        this.helixMetrics,
        this.appWorkDir,
        this.jobHelixManager,
        this.taskDriverHelixManager,
        this.jobsMapping,
        this.locks,
        this.metricContext);

    final Future<?> future = this.jobExecutor.submit(retriggeringJob);
    return new Future() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        if (!GobblinHelixJobScheduler.this.isCancelRequested()) {
          return false;
        }
        boolean result = true;
        try {
          retriggeringJob.cancel();
        } catch (JobException e) {
          LOGGER.error("Failed to cancel job " + jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), e);
          result = false;
        }
        if (mayInterruptIfRunning) {
          result &= future.cancel(true);
        }
        return result;
      }

      @Override
      public boolean isCancelled() {
        return future.isCancelled();
      }

      @Override
      public boolean isDone() {
        return future.isDone();
      }

      @Override
      public Object get() throws InterruptedException, ExecutionException {
        return future.get();
      }

      @Override
      public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return future.get(timeout, unit);
      }
    };
  }

  @Subscribe
  public void handleNewJobConfigArrival(NewJobConfigArrivalEvent newJobArrival) {
    String jobUri = newJobArrival.getJobName();
    LOGGER.info("Received new job configuration of job " + jobUri);
    try {
      Properties jobProps = new Properties();
      jobProps.putAll(this.commonJobProperties);
      jobProps.putAll(newJobArrival.getJobConfig());

      // set uri so that we can delete this job later
      jobProps.setProperty(GobblinClusterConfigurationKeys.JOB_SPEC_URI, jobUri);

      this.jobSchedulerMetrics.updateTimeBeforeJobScheduling(jobProps);

      if (jobProps.containsKey(ConfigurationKeys.JOB_SCHEDULE_KEY)) {
        LOGGER.info("Scheduling job " + jobUri);
        scheduleJob(jobProps,
                    new GobblinHelixJobLauncherListener(this.launcherMetrics));
      } else {
        LOGGER.info("No job schedule found, so running job " + jobUri);
        this.jobExecutor.execute(new NonScheduledJobRunner(jobProps,
                                 new GobblinHelixJobLauncherListener(this.launcherMetrics)));
      }
    } catch (JobException je) {
      LOGGER.error("Failed to schedule or run job " + jobUri, je);
    }
  }

  @Subscribe
  public void handleUpdateJobConfigArrival(UpdateJobConfigArrivalEvent updateJobArrival) {
    LOGGER.info("Received update for job configuration of job " + updateJobArrival.getJobName());
    try {
      handleDeleteJobConfigArrival(new DeleteJobConfigArrivalEvent(updateJobArrival.getJobName(),
          updateJobArrival.getJobConfig()));
    } catch (Exception je) {
      LOGGER.error("Failed to update job " + updateJobArrival.getJobName(), je);
    }

    try {
      handleNewJobConfigArrival(new NewJobConfigArrivalEvent(updateJobArrival.getJobName(),
          updateJobArrival.getJobConfig()));
    } catch (Exception je) {
      LOGGER.error("Failed to update job " + updateJobArrival.getJobName(), je);
    }
  }

  private void waitForJobCompletion(String jobName) {
    while (this.jobRunningMap.getOrDefault(jobName, false)) {
      LOGGER.info("Waiting for job {} to stop...", jobName);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOGGER.warn("Interrupted exception encountered: ", e);
      }
    }
  }

  @Subscribe
  public void handleDeleteJobConfigArrival(DeleteJobConfigArrivalEvent deleteJobArrival) throws InterruptedException {
    LOGGER.info("Received delete for job configuration of job " + deleteJobArrival.getJobName());
    try {
      unscheduleJob(deleteJobArrival.getJobName());
      cancelJobIfRequired(deleteJobArrival);
    } catch (JobException je) {
      LOGGER.error("Failed to unschedule job " + deleteJobArrival.getJobName());
    }
  }

  @Subscribe
  public void handleCancelJobConfigArrival(CancelJobConfigArrivalEvent cancelJobArrival)
      throws InterruptedException {
    String jobUri = cancelJobArrival.getJoburi();
    LOGGER.info("Received cancel for job configuration of job " + jobUri);
    Optional<String> distributedJobMode;
    Optional<String> planningJob = Optional.empty();
    Optional<String> actualJob = Optional.empty();
    boolean cancelByDelete = PropertiesUtils.getPropAsBoolean(this.commonJobProperties, GobblinClusterConfigurationKeys.CANCEL_HELIX_JOB_BY_DELETE,
        String.valueOf(GobblinClusterConfigurationKeys.DEFAULT_CANCEL_HELIX_JOB_BY_DELETE));

    this.jobSchedulerMetrics.numCancellationStart.incrementAndGet();

    try {
      distributedJobMode = this.jobsMapping.getDistributedJobMode(jobUri);
      if (distributedJobMode.isPresent() && Boolean.parseBoolean(distributedJobMode.get())) {
        planningJob = this.jobsMapping.getPlanningJobId(jobUri);
      } else {
        actualJob = this.jobsMapping.getActualJobId(jobUri);
      }
    } catch (IOException e) {
      LOGGER.warn("jobsMapping could not be retrieved for job {}", jobUri);
      return;
    }

    if (planningJob.isPresent()) {
      LOGGER.info("Cancelling planning job helix workflow: {}", planningJob.get());
      HelixUtils.cancelWorkflow(planningJob.get(), this.taskDriverHelixManager.get(), this.helixJobStopTimeoutMillis, cancelByDelete);
    }

    if (actualJob.isPresent()) {
      LOGGER.info("Cancelling actual job helix workflow: {}", actualJob.get());
      HelixUtils.cancelWorkflow(actualJob.get(), this.jobHelixManager, this.helixJobStopTimeoutMillis, cancelByDelete);
    }

    this.jobSchedulerMetrics.numCancellationStart.decrementAndGet();
  }

  private void cancelJobIfRequired(DeleteJobConfigArrivalEvent deleteJobArrival) throws InterruptedException {
    Properties jobConfig = deleteJobArrival.getJobConfig();
    if (PropertiesUtils.getPropAsBoolean(jobConfig, GobblinClusterConfigurationKeys.CANCEL_RUNNING_JOB_ON_DELETE,
        GobblinClusterConfigurationKeys.DEFAULT_CANCEL_RUNNING_JOB_ON_DELETE)) {
      LOGGER.info("Cancelling workflow: {}", deleteJobArrival.getJobName());

      //Workaround for preventing indefinite hangs observed in TaskDriver.getWorkflows() call.
      Callable<Map<String, String>> workflowsCallable = () -> HelixUtils.getWorkflowIdsFromJobNames(this.jobHelixManager,
          Collections.singletonList(deleteJobArrival.getJobName()));
      Retryer<Map<String, String>> retryer = RetryerBuilder.<Map<String, String>>newBuilder()
          .retryIfException()
          .withStopStrategy(StopStrategies.stopAfterAttempt(5))
          .withAttemptTimeLimiter(AttemptTimeLimiters.fixedTimeLimit(this.helixWorkflowListingTimeoutMillis, TimeUnit.MILLISECONDS)).build();
      Map<String, String> jobNameToWorkflowIdMap;
      try {
        jobNameToWorkflowIdMap = retryer.call(workflowsCallable);
      } catch (ExecutionException | RetryException e) {
        LOGGER.error("Exception encountered when getting workflows from Helix; likely a Helix/Zk issue.", e);
        return;
      }

      if (jobNameToWorkflowIdMap.containsKey(deleteJobArrival.getJobName())) {
        String workflowId = jobNameToWorkflowIdMap.get(deleteJobArrival.getJobName());
        boolean cancelByDelete = PropertiesUtils.getPropAsBoolean(jobConfig, GobblinClusterConfigurationKeys.CANCEL_HELIX_JOB_BY_DELETE,
            String.valueOf(GobblinClusterConfigurationKeys.DEFAULT_CANCEL_HELIX_JOB_BY_DELETE));
        HelixUtils.cancelWorkflow(workflowId, this.jobHelixManager, helixJobStopTimeoutMillis, cancelByDelete);
        LOGGER.info("Cancelled workflow: {}", deleteJobArrival.getJobName());
        //Wait until the cancelled job is complete.
        waitForJobCompletion(deleteJobArrival.getJobName());
      } else {
        LOGGER.warn("Could not find Helix Workflow Id for job: {}", deleteJobArrival.getJobName());
      }
    }
  }

  /**
   * This class is responsible for running non-scheduled jobs.
   */
  class NonScheduledJobRunner implements Runnable {
    private final Properties jobProps;
    private final GobblinHelixJobLauncherListener jobListener;
    private final Long creationTimeInMillis;

    public NonScheduledJobRunner(Properties jobProps,
                                 GobblinHelixJobLauncherListener jobListener) {

      this.jobProps = jobProps;
      this.jobListener = jobListener;
      this.creationTimeInMillis = System.currentTimeMillis();
    }

    @Override
    public void run() {
      try {
        GobblinHelixJobScheduler.this.jobSchedulerMetrics.updateTimeBeforeJobLaunching(this.jobProps);
        GobblinHelixJobScheduler.this.jobSchedulerMetrics.updateTimeBetweenJobSchedulingAndJobLaunching(this.creationTimeInMillis, System.currentTimeMillis());
        GobblinHelixJobScheduler.this.runJob(this.jobProps, this.jobListener);
      } catch (JobException je) {
        LOGGER.error("Failed to run job " + this.jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), je);
      }
    }
  }
}
