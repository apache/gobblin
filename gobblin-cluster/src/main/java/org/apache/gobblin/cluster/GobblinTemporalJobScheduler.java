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
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
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
import org.apache.gobblin.runtime.JobLauncher;
import org.apache.gobblin.runtime.listeners.JobListener;
import org.apache.gobblin.scheduler.JobScheduler;
import org.apache.gobblin.scheduler.SchedulerService;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.PropertiesUtils;


/**
 * An extension to {@link JobScheduler} that schedules and runs
 * Gobblin jobs on Temporal.
 *
 * <p> If the job should be launched from the scheduler node,
 * {@link GobblinTemporalJobLauncher} is invoked.
 *
 */
@Alpha
public class GobblinTemporalJobScheduler extends JobScheduler implements StandardMetricsBridge {

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinTemporalJobScheduler.class);
  private static final String COMMON_JOB_PROPS = "gobblin.common.job.props";

  private final Properties commonJobProperties;
  private final EventBus eventBus;
  private final Path appWorkDir;
  private final List<? extends Tag<?>> metadataTags;
  private final ConcurrentHashMap<String, Boolean> jobRunningMap;
  private final MetricContext metricContext;
  final GobblinHelixJobSchedulerMetrics jobSchedulerMetrics;
  final GobblinHelixJobLauncherMetrics launcherMetrics;
  final GobblinHelixPlanningJobLauncherMetrics planningJobLauncherMetrics;
  final HelixJobsMapping jobsMapping;
  private boolean startServicesCompleted;

  public GobblinTemporalJobScheduler(Config sysConfig,
                                  EventBus eventBus,
                                  Path appWorkDir, List<? extends Tag<?>> metadataTags,
                                  SchedulerService schedulerService) throws Exception {

    super(ConfigUtils.configToProperties(sysConfig), schedulerService);
    this.commonJobProperties = ConfigUtils.configToProperties(ConfigUtils.getConfigOrEmpty(sysConfig, COMMON_JOB_PROPS));
    this.eventBus = eventBus;
    this.jobRunningMap = new ConcurrentHashMap<>();
    this.appWorkDir = appWorkDir;
    this.metadataTags = metadataTags;
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

    this.startServicesCompleted = false;
  }

  @Override
  public Collection<StandardMetrics> getStandardMetricsCollection() {
    return ImmutableList.of(this.launcherMetrics,
                            this.jobSchedulerMetrics,
                            this.planningJobLauncherMetrics);
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
  }

  @Override
  public GobblinTemporalJobLauncher buildJobLauncher(Properties jobProps)
      throws Exception {
    Properties combinedProps = new Properties();
    combinedProps.putAll(properties);
    combinedProps.putAll(jobProps);

    return new GobblinTemporalJobLauncher(combinedProps,
        this.appWorkDir,
        this.metadataTags,
        this.jobRunningMap);
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
        GobblinHelixJobLauncherListener listener = new GobblinHelixJobLauncherListener(this.launcherMetrics);
        JobLauncher launcher = buildJobLauncher(newJobArrival.getJobConfig());
        launcher.launchJob(listener);
      }
    } catch (Exception je) {
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

    this.jobSchedulerMetrics.numCancellationStart.decrementAndGet();
  }


  private void cancelJobIfRequired(DeleteJobConfigArrivalEvent deleteJobArrival) throws InterruptedException {

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
        GobblinTemporalJobScheduler.this.jobSchedulerMetrics.updateTimeBeforeJobLaunching(this.jobProps);
        GobblinTemporalJobScheduler.this.jobSchedulerMetrics.updateTimeBetweenJobSchedulingAndJobLaunching(this.creationTimeInMillis, System.currentTimeMillis());
        GobblinTemporalJobScheduler.this.runJob(this.jobProps, this.jobListener);
      } catch (JobException je) {
        LOGGER.error("Failed to run job " + this.jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), je);
      }
    }
  }
}
