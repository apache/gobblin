/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
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

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.helix.HelixManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.Tag;
import gobblin.runtime.JobException;
import gobblin.runtime.JobLauncher;
import gobblin.runtime.listeners.JobListener;
import gobblin.scheduler.JobScheduler;
import gobblin.yarn.event.NewJobConfigArrivalEvent;


/**
 * An extension to {@link JobScheduler} that schedules and runs Gobblin jobs on Helix using
 * {@link GobblinHelixJobLauncher}s.
 *
 * @author Yinan Li
 */
public class GobblinHelixJobScheduler extends JobScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinHelixJobScheduler.class);

  static final String HELIX_MANAGER_KEY = "helixManager";
  static final String APPLICATION_WORK_DIR_KEY = "applicationWorkDir";
  static final String METADATA_TAGS = "metadataTags";
  static final String FILE_SYSTEM = "fileSystem";

  private final Properties properties;
  private final HelixManager helixManager;
  private final EventBus eventBus;
  private final Path appWorkDir;
  private final List<? extends Tag<?>> metadataTags;
  private final FileSystem fs;

  public GobblinHelixJobScheduler(Properties properties, HelixManager helixManager, EventBus eventBus,
      Path appWorkDir, List<? extends Tag<?>> metadataTags) throws Exception {
    super(properties);
    this.properties = properties;
    this.helixManager = helixManager;
    this.eventBus = eventBus;

    this.appWorkDir = appWorkDir;
    this.metadataTags = metadataTags;

    URI fsUri = URI.create(properties.getProperty(ConfigurationKeys.FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI));
    this.fs = FileSystem.get(fsUri, new Configuration());
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
    additionalJobDataMap.put(FILE_SYSTEM, this.fs);

    try {
      scheduleJob(jobProps, jobListener, additionalJobDataMap, GobblinHelixJob.class);
    } catch (Exception e) {
      throw new JobException("Failed to schedule job " + jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), e);
    }
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
    return new GobblinHelixJobLauncher(jobProps, this.helixManager, this.fs, this.appWorkDir, this.metadataTags);
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
        LOGGER.info("No job schedule found, so running new job " + newJobArrival.getJobName());
        this.jobExecutor.execute(new NonScheduledJobRunner(jobConfig, null));
      }
    } catch (JobException je) {
      LOGGER.error("Failed to schedule or run job " + newJobArrival.getJobName());
    }
  }

  /**
   * This class is responsible for running non-scheduled jobs.
   */
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
