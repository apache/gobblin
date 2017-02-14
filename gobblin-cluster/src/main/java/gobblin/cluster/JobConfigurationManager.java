
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

package gobblin.cluster;

import java.io.File;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;

import gobblin.annotation.Alpha;
import gobblin.cluster.event.NewJobConfigArrivalEvent;
import gobblin.configuration.ConfigurationKeys;
import gobblin.util.ConfigUtils;
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
@Alpha
public class JobConfigurationManager extends AbstractIdleService {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobConfigurationManager.class);

  protected final EventBus eventBus;
  protected final Config config;
  protected Optional<String> jobConfDirPath;

  public JobConfigurationManager(EventBus eventBus, Config config) {
    this.eventBus = eventBus;
    this.config = config;

    this.jobConfDirPath =
        config.hasPath(GobblinClusterConfigurationKeys.JOB_CONF_PATH_KEY) ? Optional
            .of(config.getString(GobblinClusterConfigurationKeys.JOB_CONF_PATH_KEY)) : Optional.<String>absent();
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

  protected void postNewJobConfigArrival(String jobName, Properties jobConfig) {
    this.eventBus.post(new NewJobConfigArrivalEvent(jobName, jobConfig));
  }
}
