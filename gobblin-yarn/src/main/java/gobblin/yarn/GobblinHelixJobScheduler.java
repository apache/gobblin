/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.yarn;

import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.helix.HelixManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.JobException;
import gobblin.runtime.JobLauncher;
import gobblin.runtime.JobListener;
import gobblin.scheduler.JobScheduler;
import gobblin.yarn.event.NewJobConfigArrivalEvent;


/**
 * An extension to {@link JobScheduler} that schedules and runs Gobblin jobs on Helix using
 * {@link GobblinHelixJobLauncher}s.
 *
 * @author ynli
 */
public class GobblinHelixJobScheduler extends JobScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinHelixJobScheduler.class);

  static final String HELIX_MANAGER_KEY = "helixManager";
  static final String APPLICATION_WORK_DIR_KEY = "applicationWorkDir";

  private final Properties properties;
  private final HelixManager helixManager;
  private final Path appWorkDir;

  public GobblinHelixJobScheduler(Properties properties, HelixManager helixManager, EventBus eventBus,
      Path appWorkDir) throws Exception {
    super(properties);
    this.properties = properties;
    this.helixManager = helixManager;
    this.appWorkDir = appWorkDir;
    eventBus.register(this);
  }

  @Override
  public void scheduleJob(Properties jobProps, JobListener jobListener) throws JobException {
    Map<String, Object> additionalJobDataMap = Maps.newHashMap();
    additionalJobDataMap.put(HELIX_MANAGER_KEY, this.helixManager);
    additionalJobDataMap.put(APPLICATION_WORK_DIR_KEY, this.appWorkDir);

    try {
      scheduleJob(jobProps, jobListener, additionalJobDataMap, GobblinHelixJob.class);
    } catch (Exception e) {
      throw new JobException("Failed to schedule job " + jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), e);
    }
  }

  @Override
  public void runJob(Properties jobProps, JobListener jobListener) throws JobException {
    try {
      JobLauncher jobLauncher = new GobblinHelixJobLauncher(jobProps, this.helixManager, this.appWorkDir);
      runJob(jobProps, jobListener, jobLauncher);
    } catch (Exception e) {
      throw new JobException("Failed to run job " + jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), e);
    }
  }

  @SuppressWarnings("unused")
  @Subscribe
  public void handleNewJobConfigArrival(NewJobConfigArrivalEvent newJobArrival) {
    LOGGER.info("Received new job configuration of job " + newJobArrival.getJobName());
    try {
      Properties jobConfig = new Properties();
      jobConfig.putAll(this.properties);
      jobConfig.putAll(newJobArrival.getJobConfig());
      if (jobConfig.containsKey(ConfigurationKeys.JOB_SCHEDULE_KEY)) {
        LOGGER.info("Scheduling new job " + newJobArrival.getJobName());
        scheduleJob(newJobArrival.getJobConfig(), null);
      } else {
        this.jobExecutor.submit(new NonScheduledJobRunner(jobConfig, null));
      }
    } catch (JobException je) {
      LOGGER.error("Failed to schedule job " + newJobArrival.getJobName());
    }
  }

  class NonScheduledJobRunner implements Runnable {

    private final Properties jobConfig;
    private final JobListener jobListener;

    public NonScheduledJobRunner(Properties jobConfig, JobListener jobListener) {
      this.jobConfig = jobConfig;
      this.jobListener = jobListener;
    }

    @Override
    public void run() {
      try {
        GobblinHelixJobScheduler.this.runJob(this.jobConfig, this.jobListener);
      } catch (JobException je) {
        LOGGER.error("Failed to run job " + this.jobConfig.getProperty(ConfigurationKeys.JOB_NAME_KEY), je);
      }
    }
  }
}
