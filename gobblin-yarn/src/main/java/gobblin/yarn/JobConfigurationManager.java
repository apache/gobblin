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

import java.io.File;
import java.util.List;
import java.util.Properties;

import com.google.common.base.Optional;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.AbstractIdleService;

import org.apache.hadoop.yarn.api.ApplicationConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.yarn.event.NewJobConfigArrivalEvent;
import gobblin.util.SchedulerUtils;


/**
 * A class for managing Gobblin job configurations.
 *
 * <p>
 *   Currently this class reads all-at-once at startup all the job configuration files found
 *   in the directory uncompressed from the job configuration file package and have them all
 *   scheduled by the {@link GobblinHelixJobScheduler} by posting a
 *   {@link NewJobConfigArrivalEvent} for each job configuration file.
 * </p>
 *
 * <p>
 *   In the future, we may add the ability to accept new job configuration files or updates
 *   to existing configuration files at runtime to this class.
 * </p>
 *
 * @author Yinan Li
 */
public class JobConfigurationManager extends AbstractIdleService {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobConfigurationManager.class);

  private final EventBus eventBus;
  private final Optional<String> jobConfDirPath;

  public JobConfigurationManager(EventBus eventBus, Optional<String> jobConfDirPath) {
    this.eventBus = eventBus;
    this.jobConfDirPath = jobConfDirPath;
  }

  @Override
  // TODO : Modify this 'File-approach'
  protected void startUp() throws Exception {
    if (this.jobConfDirPath.isPresent()) {
      File path = new File(this.jobConfDirPath.get());
      String pwd = System.getenv().get(ApplicationConstants.Environment.PWD.key());
      File jobConfigDir = new File(pwd, path.getName() + GobblinYarnConfigurationKeys.TAR_GZ_FILE_SUFFIX);

      if (jobConfigDir.exists()) {
        LOGGER.info("Loading job configurations from " + jobConfigDir);
        Properties properties = new Properties();
        properties.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY, jobConfigDir.getAbsolutePath());
        List<Properties> jobConfigs = SchedulerUtils.loadGenericJobConfigs(properties);
        LOGGER.info("Loaded " + jobConfigs.size() + " job configuration(s)");
        for (Properties config : jobConfigs) {
          postNewJobConfigArrival(config.getProperty(ConfigurationKeys.JOB_NAME_KEY), config);
        }
      } else {
        LOGGER.warn("Job configuration directory " + jobConfigDir + " not found");
      }
    }
  }

  @Override
  protected void shutDown() throws Exception {
    // Nothing to do
  }

  private void postNewJobConfigArrival(String jobName, Properties jobConfig) {
    this.eventBus.post(new NewJobConfigArrivalEvent(jobName, jobConfig));
  }
}
