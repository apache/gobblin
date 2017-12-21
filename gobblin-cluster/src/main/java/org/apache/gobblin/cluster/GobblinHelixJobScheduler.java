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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.Path;
import org.apache.helix.HelixManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.cluster.event.DeleteJobConfigArrivalEvent;
import org.apache.gobblin.cluster.event.NewJobConfigArrivalEvent;
import org.apache.gobblin.cluster.event.UpdateJobConfigArrivalEvent;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.instrumented.StandardMetricsBridge;
import org.apache.gobblin.metrics.ContextAwareCounter;
import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.ContextAwareTimer;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.runtime.api.JobExecutionLauncher;
import org.apache.gobblin.runtime.api.MutableJobCatalog;
import org.apache.gobblin.runtime.JobException;
import org.apache.gobblin.runtime.JobLauncher;
import org.apache.gobblin.runtime.JobContext;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.listeners.AbstractJobListener;
import org.apache.gobblin.runtime.listeners.JobListener;
import org.apache.gobblin.scheduler.JobScheduler;
import org.apache.gobblin.scheduler.SchedulerService;


import javax.annotation.Nonnull;


/**
 * An extension to {@link JobScheduler} that schedules and runs Gobblin jobs on Helix using
 * {@link GobblinHelixJobLauncher}s.
 *
 * @author Yinan Li
 */
@Alpha
public class GobblinHelixJobScheduler extends JobScheduler implements StandardMetricsBridge{

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinHelixJobScheduler.class);

  static final String HELIX_MANAGER_KEY = "helixManager";
  static final String APPLICATION_WORK_DIR_KEY = "applicationWorkDir";
  static final String METADATA_TAGS = "metadataTags";
  static final String JOB_RUNNING_MAP = "jobRunningMap";

  private final Properties properties;
  private final HelixManager helixManager;
  private final EventBus eventBus;
  private final Path appWorkDir;
  private final List<? extends Tag<?>> metadataTags;
  private final ConcurrentHashMap<String, Boolean> jobRunningMap;
  private final MutableJobCatalog jobCatalog;
  private final MetricContext metricContext;
  private final Metrics metrics;

  public GobblinHelixJobScheduler(Properties properties, HelixManager helixManager, EventBus eventBus,
      Path appWorkDir, List<? extends Tag<?>> metadataTags, SchedulerService schedulerService,
      MutableJobCatalog jobCatalog) throws Exception {
    super(properties, schedulerService);
    this.properties = properties;
    this.helixManager = helixManager;
    this.eventBus = eventBus;
    this.jobRunningMap = new ConcurrentHashMap<>();
    this.appWorkDir = appWorkDir;
    this.metadataTags = metadataTags;
    this.jobCatalog = jobCatalog;
    this.metricContext = Instrumented.getMetricContext(new org.apache.gobblin.configuration.State(properties), this.getClass());
    this.metrics = new Metrics(this.metricContext);
  }

  @Nonnull
  @Override
  public MetricContext getMetricContext() {
    return this.metricContext;
  }

  @Override
  public boolean isInstrumentationEnabled() {
    return GobblinMetrics.isEnabled(this.properties);
  }

  @Override
  public List<Tag<?>> generateTags(org.apache.gobblin.configuration.State state) {
    return null;
  }

  @Override
  public void switchMetricContext(List<Tag<?>> tags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void switchMetricContext(MetricContext context) {
    throw new UnsupportedOperationException();
  }

  @Override
  public StandardMetrics getStandardMetrics() {
    return metrics;
  }

  private class Metrics extends StandardMetrics {

    private final AtomicLong totalJobsLaunched;
    private final AtomicLong totalJobsCompleted;
    private final AtomicLong totalJobsCommitted;
    private final AtomicLong totalJobsFailed;
    private final AtomicLong totalJobsCancelled;
    private final ContextAwareGauge<Long> numJobsLaunched;
    private final ContextAwareGauge<Long> numJobsCompleted;
    private final ContextAwareGauge<Long> numJobsCommitted;
    private final ContextAwareGauge<Long> numJobsFailed;
    private final ContextAwareGauge<Long> numJobsCancelled;
    private final ContextAwareGauge<Integer> numJobsRunning;

    private final ContextAwareTimer timeForJobCompletion;
    private final ContextAwareTimer timeForJobFailure;
    private final ContextAwareTimer timeBeforeJobScheduling;
    private final ContextAwareTimer timeBeforeJobLaunching;

    public Metrics(final MetricContext metricContext) {
      // All historical counters
      this.totalJobsLaunched = new AtomicLong(0);
      this.totalJobsCompleted = new AtomicLong(0);
      this.totalJobsCommitted = new AtomicLong(0);
      this.totalJobsFailed = new AtomicLong(0);
      this.totalJobsCancelled = new AtomicLong(0);

      this.numJobsLaunched = metricContext.newContextAwareGauge(JobExecutionLauncher.StandardMetrics.NUM_JOBS_LAUNCHED, ()->this.totalJobsLaunched.get());
      this.numJobsCompleted = metricContext.newContextAwareGauge(JobExecutionLauncher.StandardMetrics.NUM_JOBS_COMPLETED, ()->this.totalJobsCompleted.get());
      this.numJobsCommitted = metricContext.newContextAwareGauge(JobExecutionLauncher.StandardMetrics.NUM_JOBS_COMMITTED, ()->this.totalJobsCommitted.get());
      this.numJobsFailed = metricContext.newContextAwareGauge(JobExecutionLauncher.StandardMetrics.NUM_JOBS_FAILED, ()->this.totalJobsFailed.get());
      this.numJobsCancelled = metricContext.newContextAwareGauge(JobExecutionLauncher.StandardMetrics.NUM_JOBS_CANCELLED, ()->this.totalJobsCancelled.get());
      this.numJobsRunning = metricContext.newContextAwareGauge(JobExecutionLauncher.StandardMetrics.NUM_JOBS_RUNNING,
          ()->(int)(Metrics.this.totalJobsLaunched.get() - Metrics.this.totalJobsCompleted.get()));

      this.timeForJobCompletion = metricContext.contextAwareTimer(JobExecutionLauncher.StandardMetrics.TIMER_FOR_JOB_COMPLETION, 1, TimeUnit.MINUTES);
      this.timeForJobFailure = metricContext.contextAwareTimer(JobExecutionLauncher.StandardMetrics.TIMER_FOR_JOB_FAILURE,1, TimeUnit.MINUTES);
      this.timeBeforeJobScheduling = metricContext.contextAwareTimer(JobExecutionLauncher.StandardMetrics.TIMER_BEFORE_JOB_SCHEDULING, 1, TimeUnit.MINUTES);
      this.timeBeforeJobLaunching = metricContext.contextAwareTimer(JobExecutionLauncher.StandardMetrics.TIMER_BEFORE_JOB_LAUNCHING, 1, TimeUnit.MINUTES);
    }

    private void updateTimeBeforeJobScheduling (Properties jobConfig) {
      long jobCreationTime = Long.parseLong(jobConfig.getProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "0"));
      Instrumented.updateTimer(Optional.of(timeBeforeJobScheduling), System.currentTimeMillis() - jobCreationTime, TimeUnit.MILLISECONDS);
    }

    private void updateTimeBeforeJobLaunching (Properties jobConfig) {
      long jobCreationTime = Long.parseLong(jobConfig.getProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "0"));
      Instrumented.updateTimer(Optional.of(timeBeforeJobLaunching), System.currentTimeMillis() - jobCreationTime, TimeUnit.MILLISECONDS);
    }

    @Override
    public String getName() {
      return GobblinHelixJobScheduler.class.getName();
    }

    @Override
    public Collection<ContextAwareGauge<?>> getGauges() {
      return ImmutableList.of(numJobsRunning, numJobsLaunched, numJobsCompleted, numJobsCommitted, numJobsFailed, numJobsCancelled);
    }

    @Override
    public Collection<ContextAwareCounter> getCounters() {
      return ImmutableList.of();
    }

    @Override
    public Collection<ContextAwareTimer> getTimers() {
      return ImmutableList.of(timeForJobCompletion, timeForJobFailure, timeBeforeJobScheduling, timeBeforeJobLaunching);
    }
  }

  private class MetricsTrackingListener extends AbstractJobListener {
    private final Metrics metrics;
    private static final String START_TIME = "startTime";
    MetricsTrackingListener(Metrics metrics) {
      this.metrics = metrics;
    }

    @Override
    public void onJobPrepare(JobContext jobContext)
        throws Exception {
      super.onJobPrepare(jobContext);
      jobContext.getJobState().setProp(START_TIME, Long.toString(System.nanoTime()));
      if (GobblinHelixJobScheduler.this.isInstrumentationEnabled()) {
        metrics.totalJobsLaunched.incrementAndGet();
      }
    }

    @Override
    public void onJobCompletion(JobContext jobContext)
        throws Exception {
      super.onJobCompletion(jobContext);
      long startTime = jobContext.getJobState().getPropAsLong(START_TIME);
      if (GobblinHelixJobScheduler.this.isInstrumentationEnabled()) {
        metrics.totalJobsCompleted.incrementAndGet();
        Instrumented.updateTimer(Optional.of(metrics.timeForJobCompletion), System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        if (jobContext.getJobState().getState() == JobState.RunningState.FAILED) {
            metrics.totalJobsFailed.incrementAndGet();
            Instrumented.updateTimer(Optional.of(metrics.timeForJobFailure), System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        } else {
            metrics.totalJobsCommitted.incrementAndGet();
        }
      }
    }

    @Override
    public void onJobCancellation(JobContext jobContext)
        throws Exception {
      super.onJobCancellation(jobContext);
      if (GobblinHelixJobScheduler.this.isInstrumentationEnabled()) {
        metrics.totalJobsCancelled.incrementAndGet();
      }
    }

  }

  @Override
  protected void startUp() throws Exception {
    this.eventBus.register(this);
    super.startUp();
  }

  @Override
  public void scheduleJob(Properties jobProps, JobListener jobListener) throws JobException {
    Map<String, Object> additionalJobDataMap = Maps.newHashMap();
    additionalJobDataMap.put(HELIX_MANAGER_KEY, this.helixManager);
    additionalJobDataMap.put(APPLICATION_WORK_DIR_KEY, this.appWorkDir);
    additionalJobDataMap.put(METADATA_TAGS, this.metadataTags);
    additionalJobDataMap.put(JOB_RUNNING_MAP, this.jobRunningMap);
    try {
      scheduleJob(jobProps, jobListener, additionalJobDataMap, GobblinHelixJob.class);
    } catch (Exception e) {
      throw new JobException("Failed to schedule job " + jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), e);
    }
  }

  @Override
  protected void startServices() throws Exception {
  }

  @Override
  public void runJob(Properties jobProps, JobListener jobListener) throws JobException {
    try {
      JobLauncher jobLauncher = buildGobblinHelixJobLauncher(jobProps);
      runJob(jobProps, jobListener, jobLauncher);
    } catch (Exception e) {
      throw new JobException("Failed to run job " + jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), e);
    }
  }

  private GobblinHelixJobLauncher buildGobblinHelixJobLauncher(Properties jobProps)
      throws Exception {
    return new GobblinHelixJobLauncher(jobProps, this.helixManager, this.appWorkDir, this.metadataTags, this.jobRunningMap);
  }

  @Subscribe
  public void handleNewJobConfigArrival(NewJobConfigArrivalEvent newJobArrival) {
    LOGGER.info("Received new job configuration of job " + newJobArrival.getJobName());
    try {
      Properties jobConfig = new Properties();
      jobConfig.putAll(this.properties);
      jobConfig.putAll(newJobArrival.getJobConfig());

      metrics.updateTimeBeforeJobScheduling(jobConfig);

      if (jobConfig.containsKey(ConfigurationKeys.JOB_SCHEDULE_KEY)) {
        LOGGER.info("Scheduling job " + newJobArrival.getJobName());
        scheduleJob(jobConfig, new MetricsTrackingListener(metrics));
      } else {
        LOGGER.info("No job schedule found, so running job " + newJobArrival.getJobName());
        this.jobExecutor.execute(new NonScheduledJobRunner(newJobArrival.getJobName(), jobConfig, new MetricsTrackingListener(metrics)));
      }
    } catch (JobException je) {
      LOGGER.error("Failed to schedule or run job " + newJobArrival.getJobName(), je);
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

  @Subscribe
  public void handleDeleteJobConfigArrival(DeleteJobConfigArrivalEvent deleteJobArrival) {
    LOGGER.info("Received delete for job configuration of job " + deleteJobArrival.getJobName());
    try {
      unscheduleJob(deleteJobArrival.getJobName());
    } catch (JobException je) {
      LOGGER.error("Failed to unschedule job " + deleteJobArrival.getJobName());
    }
  }

  /**
   * This class is responsible for running non-scheduled jobs.
   */
  class NonScheduledJobRunner implements Runnable {

    private final String jobUri;
    private final Properties jobConfig;
    private final JobListener jobListener;

    public NonScheduledJobRunner(String jobUri, Properties jobConfig, JobListener jobListener) {
      this.jobUri = jobUri;
      this.jobConfig = jobConfig;
      this.jobListener = jobListener;
    }

    @Override
    public void run() {
      try {
        ((MetricsTrackingListener)jobListener).metrics.updateTimeBeforeJobLaunching(this.jobConfig);
        GobblinHelixJobScheduler.this.runJob(this.jobConfig, this.jobListener);

        // remove non-scheduled job catalog once done so it won't be re-executed
        if (GobblinHelixJobScheduler.this.jobCatalog != null) {
          try {
            GobblinHelixJobScheduler.this.jobCatalog.remove(new URI(jobUri));
          } catch (URISyntaxException e) {
            LOGGER.error("Failed to remove job with bad uri " + jobUri, e);
          }
        }
      } catch (JobException je) {
        LOGGER.error("Failed to run job " + this.jobConfig.getProperty(ConfigurationKeys.JOB_NAME_KEY), je);
      }
    }
  }
}
