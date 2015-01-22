/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.uif.azkaban;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.google.common.base.Strings;

import azkaban.jobExecutor.AbstractJob;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.runtime.EmailNotificationJobListener;
import com.linkedin.uif.runtime.JobLauncher;
import com.linkedin.uif.runtime.JobLauncherFactory;


/**
 * A utility class for launching a Gobblin Hadoop MR job through Azkaban.
 *
 * @author ynli
 */
@SuppressWarnings("unused")
public class AzkabanJobLauncher extends AbstractJob {

  private static final Logger LOG = Logger.getLogger(AzkabanJobLauncher.class);

  private static final String HADOOP_FS_DEFAULT_NAME = "fs.default.name";
  private static final String AZKABAN_LINK_JOBEXEC_URL = "azkaban.link.jobexec.url";

  private final Properties properties;
  private final JobLauncher jobLauncher;

  public AzkabanJobLauncher(String jobId, Properties props)
      throws Exception {
    super(jobId, LOG);

    this.properties = new Properties();
    this.properties.putAll(props);
    Configuration conf = new Configuration();

    String fsUri = conf.get(HADOOP_FS_DEFAULT_NAME);
    if (!Strings.isNullOrEmpty(fsUri)) {
      if (!this.properties.containsKey(ConfigurationKeys.FS_URI_KEY)) {
        this.properties.setProperty(ConfigurationKeys.FS_URI_KEY, fsUri);
      }
      if (!this.properties.containsKey(ConfigurationKeys.STATE_STORE_FS_URI_KEY)) {
        this.properties.setProperty(ConfigurationKeys.STATE_STORE_FS_URI_KEY, fsUri);
      }
    }

    // Set the job tracking URL to point to the Azkaban job execution link URL
    this.properties
        .setProperty(ConfigurationKeys.JOB_TRACKING_URL_KEY, Strings.nullToEmpty(conf.get(AZKABAN_LINK_JOBEXEC_URL)));

    // Create a JobLauncher instance depending on the configuration
    this.jobLauncher = JobLauncherFactory.newJobLauncher(this.properties);
  }

  @Override
  public void run()
      throws Exception {
    this.jobLauncher.launchJob(this.properties, new EmailNotificationJobListener());
  }

  @Override
  public void cancel()
      throws Exception {
    this.jobLauncher.cancelJob(this.properties);
  }
}
