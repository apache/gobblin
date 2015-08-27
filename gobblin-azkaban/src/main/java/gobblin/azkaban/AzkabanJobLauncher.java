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

package gobblin.azkaban;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import azkaban.jobExecutor.AbstractJob;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.Tag;
import gobblin.runtime.EmailNotificationJobListener;
import gobblin.runtime.JobLauncher;
import gobblin.runtime.JobLauncherFactory;
import gobblin.runtime.JobListener;
import gobblin.runtime.util.JobMetrics;


/**
 * A utility class for launching a Gobblin Hadoop MR job through Azkaban.
 *
 * <p>
 *   By default, this class will use the {@link gobblin.runtime.mapreduce.MRJobLauncher} to launch and run
 *   the Gobblin job unless a different job launcher type is explicitly specified in the job configuration
 *   using {@link ConfigurationKeys#JOB_LAUNCHER_TYPE_KEY}.
 * </p>
 *
 * @author ynli
 */
public class AzkabanJobLauncher extends AbstractJob {

  private static final Logger LOG = Logger.getLogger(AzkabanJobLauncher.class);

  private static final String HADOOP_FS_DEFAULT_NAME = "fs.default.name";
  private static final String AZKABAN_LINK_JOBEXEC_URL = "azkaban.link.jobexec.url";
  private static final String HADOOP_TOKEN_FILE_LOCATION = "HADOOP_TOKEN_FILE_LOCATION";
  private static final String MAPREDUCE_JOB_CREDENTIALS_BINARY = "mapreduce.job.credentials.binary";

  private static final ImmutableMap<String, String> PROPERTIES_TO_TAGS_MAP =
      new ImmutableMap.Builder<String, String>()
          .put("azkaban.flow.projectname", "azkabanProjectName")
          .put("azkaban.flow.flowid", "azkabanFlowId")
          .put("azkaban.job.id", "azkabanJobId")
          .put("azkaban.flow.execid", "azkabanExecId")
          .build();

  private final Closer closer = Closer.create();
  private final JobLauncher jobLauncher;
  private final JobListener jobListener = new EmailNotificationJobListener();

  public AzkabanJobLauncher(String jobId, Properties props)
      throws Exception {
    super(jobId, LOG);

    Properties properties = new Properties();
    properties.putAll(props);
    Configuration conf = new Configuration();

    String fsUri = conf.get(HADOOP_FS_DEFAULT_NAME);
    if (!Strings.isNullOrEmpty(fsUri)) {
      if (!properties.containsKey(ConfigurationKeys.FS_URI_KEY)) {
        properties.setProperty(ConfigurationKeys.FS_URI_KEY, fsUri);
      }
      if (!properties.containsKey(ConfigurationKeys.STATE_STORE_FS_URI_KEY)) {
        properties.setProperty(ConfigurationKeys.STATE_STORE_FS_URI_KEY, fsUri);
      }
    }

    // Set the job tracking URL to point to the Azkaban job execution link URL
    properties.setProperty(
        ConfigurationKeys.JOB_TRACKING_URL_KEY, Strings.nullToEmpty(conf.get(AZKABAN_LINK_JOBEXEC_URL)));

    // Necessary for compatibility with Azkaban's hadoopJava job type
    // http://azkaban.github.io/azkaban/docs/2.5/#hadoopjava-type
    if (System.getenv(HADOOP_TOKEN_FILE_LOCATION) != null) {
      properties.setProperty(MAPREDUCE_JOB_CREDENTIALS_BINARY, System.getenv(HADOOP_TOKEN_FILE_LOCATION));
    }

    JobMetrics.addCustomTagsToProperties(properties, getAzkabanTags());

    // If the job launcher type is not specified in the job configuration,
    // override the default to use the MAPREDUCE launcher.
    if (!properties.containsKey(ConfigurationKeys.JOB_LAUNCHER_TYPE_KEY)) {
      properties.setProperty(ConfigurationKeys.JOB_LAUNCHER_TYPE_KEY,
          JobLauncherFactory.JobLauncherType.MAPREDUCE.toString());
    }

    // Create a JobLauncher instance depending on the configuration. The same properties object is
    // used for both system and job configuration properties because Azkaban puts configuration
    // properties in the .job file and in the .properties file into the same Properties object.
    this.jobLauncher = this.closer.register(JobLauncherFactory.newJobLauncher(properties, properties));
  }

  private List<Tag<?>> getAzkabanTags() {
    Configuration conf = new Configuration();
    List<Tag<?>> tags = Lists.newArrayList();

    for (Map.Entry<String, String> entry : PROPERTIES_TO_TAGS_MAP.entrySet()) {
      if (StringUtils.isNotBlank(conf.get(entry.getKey()))) {
        tags.add(new Tag<String>(entry.getValue(), conf.get(entry.getKey())));
      } else {
        LOG.warn(String.format("No config value found for azkaban config %s. Metrics will not have tag %s",
            entry.getKey(), entry.getValue()));
      }
    }
    return tags;
  }

  @Override
  public void run()
      throws Exception {
    try {
      this.jobLauncher.launchJob(this.jobListener);
    } finally {
      this.closer.close();
    }
  }

  @Override
  public void cancel()
      throws Exception {
    try {
      this.jobLauncher.cancelJob(this.jobListener);
    } finally {
      this.closer.close();
    }
  }
}
