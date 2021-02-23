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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.gobblin.cluster.event.CancelJobConfigArrivalEvent;
import org.apache.gobblin.runtime.job_spec.JobSpecResolver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;

import javax.annotation.Nullable;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.cluster.event.DeleteJobConfigArrivalEvent;
import org.apache.gobblin.cluster.event.NewJobConfigArrivalEvent;
import org.apache.gobblin.cluster.event.UpdateJobConfigArrivalEvent;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.StandardMetricsBridge;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.SchedulerUtils;


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
@Alpha
public class JobConfigurationManager extends AbstractIdleService implements StandardMetricsBridge {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobConfigurationManager.class);

  private Optional<Pattern> jobsToRun;
  protected final EventBus eventBus;
  protected final Config config;
  protected Optional<String> jobConfDirPath;
  protected final JobSpecResolver jobSpecResolver;

  public JobConfigurationManager(EventBus eventBus, Config config) {
    this.eventBus = eventBus;
    this.config = config;

    this.jobConfDirPath =
        config.hasPath(GobblinClusterConfigurationKeys.JOB_CONF_PATH_KEY) ? Optional
            .of(config.getString(GobblinClusterConfigurationKeys.JOB_CONF_PATH_KEY)) : Optional.<String>absent();
    String jobsToRunRegex = ConfigUtils.getString(config, GobblinClusterConfigurationKeys.JOBS_TO_RUN, "");
    try {
      this.jobsToRun = !Strings.isNullOrEmpty(jobsToRunRegex) ? Optional.of(Pattern.compile(config.getString(GobblinClusterConfigurationKeys.JOBS_TO_RUN))) : Optional.absent();
    } catch (PatternSyntaxException e) {
      LOGGER.error("Invalid regex pattern: {}, Exception: {}", jobsToRunRegex, e);
      this.jobsToRun = Optional.absent();
    }
    try {
      this.jobSpecResolver = JobSpecResolver.builder(config).build();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  protected void startUp() throws Exception {
    if (this.jobConfDirPath.isPresent()) {
      File path = new File(this.jobConfDirPath.get());

      File jobConfigDir = path;
      // Backward compatibility: Previous impl was forcing users to look for jobConf within ${user.dir}
      // .. so if jobConfigDir does not exists, try to resolve config path via legacy route for backward
      // .. compatibility
      if (!path.exists()) {
        String pwd = System.getProperty("user.dir");
        jobConfigDir = new File(pwd, path.getName() + GobblinClusterConfigurationKeys.TAR_GZ_FILE_SUFFIX);
      }

      if (jobConfigDir.exists()) {
        LOGGER.info("Loading job configurations from " + jobConfigDir);
        Properties properties = ConfigUtils.configToProperties(this.config);
        properties.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY, "file://" + jobConfigDir.getAbsolutePath());
        List<Properties> jobConfigs = SchedulerUtils.loadGenericJobConfigs(properties, this.jobSpecResolver);
        LOGGER.info("Loaded " + jobConfigs.size() + " job configuration(s)");
        for (Properties config : jobConfigs) {
          if (!jobsToRun.isPresent() || shouldRun(jobsToRun.get(), config)) {
            postNewJobConfigArrival(config.getProperty(ConfigurationKeys.JOB_NAME_KEY), config);
          } else {
            LOGGER.warn("Job {} has been filtered and will not be run in the cluster.", config.getProperty(ConfigurationKeys.JOB_NAME_KEY));
          }
        }
      } else {
        LOGGER.warn("Job configuration directory " + jobConfigDir + " not found");
      }
    }
  }

  @VisibleForTesting
  /**
   * A helper method to determine if a given job should be submitted to cluster for execution based on the
   * regex defining the jobs to run.
   */
  protected static boolean shouldRun(Pattern jobsToRun, Properties jobConfig) {
    Matcher matcher = jobsToRun.matcher(jobConfig.getProperty(ConfigurationKeys.JOB_NAME_KEY));
    return matcher.matches();
  }

  @Override
  protected void shutDown() throws Exception {
    // Nothing to do
  }

  protected void postNewJobConfigArrival(String jobName, Properties jobConfig) {
    LOGGER.info(String.format("Posting new JobConfig with name: %s and config: %s", jobName, jobConfig));
    this.eventBus.post(new NewJobConfigArrivalEvent(jobName, jobConfig));
  }

  protected void postUpdateJobConfigArrival(String jobName, Properties jobConfig) {
    LOGGER.info(String.format("Posting update JobConfig with name: %s and config: %s", jobName, jobConfig));
    this.eventBus.post(new UpdateJobConfigArrivalEvent(jobName, jobConfig));
  }

  protected void postDeleteJobConfigArrival(String jobName, @Nullable Properties jobConfig) {
    LOGGER.info(String.format("Posting delete JobConfig with name: %s and config: %s", jobName, jobConfig));
    this.eventBus.post(new DeleteJobConfigArrivalEvent(jobName, jobConfig));
  }

  protected void postCancelJobConfigArrival(String jobUri) {
    LOGGER.info(String.format("Posting cancel JobConfig with name: %s", jobUri));
    this.eventBus.post(new CancelJobConfigArrivalEvent(jobUri));
  }
}
