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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.fs.Path;
import org.apache.helix.HelixManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.google.common.collect.Lists;
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
import org.apache.gobblin.metrics.ContextAwareHistogram;
import org.apache.gobblin.metrics.ContextAwareMeter;
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
import lombok.AllArgsConstructor;


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
  private final InnerStandardMetrics metrics;

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
    this.metricContext = getDefaultMetricContext(properties);
    this.metrics = new InnerStandardMetrics(this);
  }

  public MetricContext getDefaultMetricContext(Properties properties) {
    org.apache.gobblin.configuration.State fakeState =
        new org.apache.gobblin.configuration.State(properties);
    List<Tag<?>> tags = new ArrayList<>();
    MetricContext res = Instrumented.getMetricContext(fakeState, GobblinHelixJobScheduler.class, tags);
    return res;
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
  public StandardMetricsBridge.StandardMetrics getStandardMetrics() {
    return metrics;
  }

  private class InnerStandardMetrics implements StandardMetrics {

    private final ContextAwareCounter numJobsLaunched;
    private final ContextAwareCounter numJobsCompleted;
    private final ContextAwareCounter numJobsCommitted;
    private final ContextAwareCounter numJobsFailed;
    private final ContextAwareCounter numJobsCancelled;
    private final ContextAwareGauge<Integer> numJobsRunning;

    public InnerStandardMetrics(final GobblinHelixJobScheduler jobScheduler) {
      this.numJobsLaunched = jobScheduler.getMetricContext().contextAwareCounter(JobExecutionLauncher.StandardMetrics.NUM_JOBS_LAUNCHED_COUNTER);
      this.numJobsCompleted = jobScheduler.getMetricContext().contextAwareCounter(JobExecutionLauncher.StandardMetrics.NUM_JOBS_COMPLETED_COUNTER);
      this.numJobsCommitted = jobScheduler.getMetricContext().contextAwareCounter(JobExecutionLauncher.StandardMetrics.NUM_JOBS_COMMITTED_COUNTER);
      this.numJobsFailed = jobScheduler.getMetricContext().contextAwareCounter(JobExecutionLauncher.StandardMetrics.NUM_JOBS_FAILED_COUNTER);
      this.numJobsCancelled = jobScheduler.getMetricContext().contextAwareCounter(JobExecutionLauncher.StandardMetrics.NUM_JOBS_CANCELLED_COUNTER);
      this.numJobsRunning = jobScheduler.getMetricContext().newContextAwareGauge(JobExecutionLauncher.StandardMetrics.NUM_JOBS_RUNNING_GAUGE,
          new Gauge<Integer>() {
            @Override public Integer getValue() {
              return (int)(InnerStandardMetrics.this.numJobsLaunched.getCount() -
                  InnerStandardMetrics.this.numJobsCompleted.getCount());
            }
          });
    }

    @Override
    public String getName() {
      return GobblinHelixJobScheduler.class.getName();
    }

    @Override
    public Collection<ContextAwareGauge<?>> getGauges() {
      return Collections.singleton(numJobsRunning);
    }

    @Override
    public Collection<ContextAwareCounter> getCounters() {
      List<ContextAwareCounter> counters = Lists.newArrayList();
      counters.add(numJobsLaunched);
      counters.add(numJobsCompleted);
      counters.add(numJobsCommitted);
      counters.add(numJobsFailed);
      counters.add(numJobsCancelled);
      return counters;
    }

    @Override
    public Collection<ContextAwareMeter> getMeters() {
      return null;
    }

    @Override
    public Collection<ContextAwareTimer> getTimers() {
      return null;
    }

    @Override
    public Collection<ContextAwareHistogram> getHistograms() {
      return null;
    }
  }

  @AllArgsConstructor
  private class MetricsTrackingListener extends AbstractJobListener {
    private final InnerStandardMetrics metrics;

    @Override
    public void onJobPrepare(JobContext jobContext)
        throws Exception {
      super.onJobPrepare(jobContext);
      if (GobblinHelixJobScheduler.this.isInstrumentationEnabled()) {
        metrics.numJobsLaunched.inc();
      }
    }

    @Override
    public void onJobCompletion(JobContext jobContext)
        throws Exception {
      super.onJobCompletion(jobContext);
      if (GobblinHelixJobScheduler.this.isInstrumentationEnabled()) {
        metrics.numJobsCompleted.inc();
        if (jobContext.getJobState().getState() == JobState.RunningState.FAILED) {
            metrics.numJobsFailed.inc();
        } else {
            metrics.numJobsCommitted.inc();
        }
      }
    }

    @Override
    public void onJobCancellation(JobContext jobContext)
        throws Exception {
      super.onJobCancellation(jobContext);
      if (GobblinHelixJobScheduler.this.isInstrumentationEnabled()) {
        metrics.numJobsCancelled.inc();
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
