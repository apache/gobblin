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
package org.apache.gobblin.runtime.scheduler;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.quartz.CronScheduleBuilder;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.slf4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.GobblinInstanceEnvironment;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.JobSpecSchedule;
import org.apache.gobblin.runtime.api.JobSpecScheduler;
import org.apache.gobblin.scheduler.SchedulerService;
import org.apache.gobblin.util.service.StandardServiceConfig;

import lombok.Data;

/**
 * A {@link JobSpecScheduler} using Quartz.
 */
public class QuartzJobSpecScheduler extends AbstractJobSpecScheduler {
  public static final Config DEFAULT_CFG =
      ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(StandardServiceConfig.STARTUP_TIMEOUT_MS_PROP,  1 * 1000)  // 1 second
          .put(StandardServiceConfig.SHUTDOWN_TIMEOUT_MS_PROP,  1 * 60 * 1000) // 1 minute
          .build());

  protected static final String JOB_SPEC_KEY = "jobSpec";
  protected static final String JOB_RUNNABLE_KEY = "jobRunnable";

  // A Quartz scheduler
  @VisibleForTesting final SchedulerService _scheduler;
  private final StandardServiceConfig _cfg;

  public QuartzJobSpecScheduler(Optional<Logger> log, Config cfg, Optional<SchedulerService> scheduler) {
    super(log);
    _scheduler = scheduler.isPresent() ? scheduler.get() : createDefaultSchedulerService(cfg);
    _cfg = new StandardServiceConfig(cfg.withFallback(DEFAULT_CFG));
  }

  public QuartzJobSpecScheduler() {
    this(Optional.<Logger>absent(), ConfigFactory.empty(), Optional.<SchedulerService>absent());
  }

  public QuartzJobSpecScheduler(Logger log) {
    this(Optional.of(log), ConfigFactory.empty(), Optional.<SchedulerService>absent());
  }

  public QuartzJobSpecScheduler(Logger log, Config cfg) {
    this(Optional.of(log), cfg, Optional.<SchedulerService>absent());
  }

  public QuartzJobSpecScheduler(GobblinInstanceEnvironment env) {
    this(Optional.of(env.getLog()), env.getSysConfig().getConfig(),
         Optional.<SchedulerService>absent());
  }

  protected static SchedulerService createDefaultSchedulerService(Config cfg) {
    return new SchedulerService(cfg);
  }

  /** {@inheritDoc} */
  @Override protected JobSpecSchedule doScheduleJob(JobSpec jobSpec, Runnable jobRunnable) {

    // Build a data map that gets passed to the job
    JobDataMap jobDataMap = new JobDataMap();
    jobDataMap.put(JOB_SPEC_KEY, jobSpec);
    jobDataMap.put(JOB_RUNNABLE_KEY, jobRunnable);

    // Build a Quartz job
    JobDetail job = JobBuilder.newJob(QuartzJob.class)
        .withIdentity(jobSpec.getUri().toString())
        .withDescription(Strings.nullToEmpty(jobSpec.getDescription()))
        .usingJobData(jobDataMap)
        .build();

    Trigger jobTrigger = createTrigger(job.getKey(), jobSpec);
    QuartzJobSchedule jobSchedule = new QuartzJobSchedule(jobSpec, jobRunnable, jobTrigger);
    try {
      _scheduler.getScheduler().scheduleJob(job, jobTrigger);
      getLog().info(String.format("Scheduled job %s next two fire times: %s , %s.", jobSpec, jobTrigger.getNextFireTime(),
          jobTrigger.getFireTimeAfter(jobTrigger.getNextFireTime())));
    } catch (SchedulerException e) {
      throw new RuntimeException("Scheduling failed for " + jobSpec + ":" + e, e);
    }

    return jobSchedule;
  }

  /** {@inheritDoc} */
  @Override protected void doUnschedule(JobSpecSchedule existingSchedule) {
    Preconditions.checkNotNull(existingSchedule);
    Preconditions.checkArgument(existingSchedule instanceof QuartzJobSchedule);

    QuartzJobSchedule quartzSchedule = (QuartzJobSchedule)existingSchedule;
    try {
      _scheduler.getScheduler().deleteJob(quartzSchedule.getQuartzTrigger().getJobKey());
    } catch (SchedulerException e) {
      throw new RuntimeException("Unscheduling failed for " + existingSchedule.getJobSpec()
                                  + ":" + e, e);
    }
  }

  @Override protected void startUp() throws TimeoutException {
    super.startUp();
    _scheduler.startAsync();
    // Start-up should not take long
    getLog().info("Waiting QuartzJobSpecScheduler to run. Timeout is {} ms", _cfg.getStartUpTimeoutMs());
    long startTime = System.currentTimeMillis();
    _scheduler.awaitRunning(_cfg.getStartUpTimeoutMs(), TimeUnit.MILLISECONDS);
    getLog().info("QuartzJobSpecScheduler runs. Time waited is {} ms", System.currentTimeMillis() - startTime);
  }

  @Override protected void shutDown() throws TimeoutException {
    super.shutDown();
    _scheduler.stopAsync();
    _scheduler.awaitTerminated(_cfg.getShutDownTimeoutMs(), TimeUnit.MILLISECONDS);
  }

  /**
   * Create a {@link Trigger} from the given {@link JobSpec}
   */
  private Trigger createTrigger(JobKey jobKey, JobSpec jobSpec) {
    // Build a trigger for the job with the given cron-style schedule
    return TriggerBuilder.newTrigger()
        .withIdentity("Cron for " + jobSpec.getUri())
        .forJob(jobKey)
        .withSchedule(CronScheduleBuilder.cronSchedule(
                         jobSpec.getConfig().getString(ConfigurationKeys.JOB_SCHEDULE_KEY)))
        .build();
  }

  @Data
  static class QuartzJobSchedule implements JobSpecSchedule {
    private final JobSpec jobSpec;
    private final Runnable jobRunnable;
    private final Trigger quartzTrigger;

    @Override
    public Optional<Long> getNextRunTimeMillis() {
      Date nextRuntime = this.quartzTrigger.getNextFireTime();
      return null != nextRuntime ? Optional.<Long>of(nextRuntime.getTime()) : Optional.<Long>absent();
    }

  }

  /**
   * A Gobblin job to be run inside quartz.
   */
  @DisallowConcurrentExecution
  public static class QuartzJob implements Job {

    @Override
    public void execute(JobExecutionContext context)
        throws JobExecutionException {
      JobDataMap dataMap = context.getJobDetail().getJobDataMap();
      JobSpec jobSpec = (JobSpec) dataMap.get(JOB_SPEC_KEY);
      Runnable jobRunnable = (Runnable) dataMap.get(JOB_RUNNABLE_KEY);

      try {
        jobRunnable.run();
      } catch (Throwable t) {
        throw new JobExecutionException("Job runable for " + jobSpec + " failed.", t);
      }
    }
  }

}
