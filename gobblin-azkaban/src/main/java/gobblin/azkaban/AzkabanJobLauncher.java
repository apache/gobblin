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

package gobblin.azkaban;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import azkaban.jobExecutor.AbstractJob;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.Tag;
import gobblin.runtime.JobLauncher;
import gobblin.runtime.JobLauncherFactory;
import gobblin.runtime.listeners.EmailNotificationJobListener;
import gobblin.runtime.listeners.JobListener;
import gobblin.runtime.util.JobMetrics;
import gobblin.util.TimeRangeChecker;


/**
 * A utility class for launching a Gobblin Hadoop MR job through Azkaban.
 *
 * <p>
 *   By default, this class will use the {@link gobblin.runtime.mapreduce.MRJobLauncher} to launch and run
 *   the Gobblin job unless a different job launcher type is explicitly specified in the job configuration
 *   using {@link ConfigurationKeys#JOB_LAUNCHER_TYPE_KEY}.
 * </p>
 *
 * @author Yinan Li
 */
public class AzkabanJobLauncher extends AbstractJob {

  private static final Logger LOG = Logger.getLogger(AzkabanJobLauncher.class);

  private static final String HADOOP_FS_DEFAULT_NAME = "fs.default.name";
  private static final String AZKABAN_LINK_JOBEXEC_URL = "azkaban.link.jobexec.url";
  private static final String HADOOP_TOKEN_FILE_LOCATION = "HADOOP_TOKEN_FILE_LOCATION";
  private static final String MAPREDUCE_JOB_CREDENTIALS_BINARY = "mapreduce.job.credentials.binary";

  private final Closer closer = Closer.create();
  private final JobLauncher jobLauncher;
  private final JobListener jobListener = new EmailNotificationJobListener();
  private final Properties props;

  public AzkabanJobLauncher(String jobId, Properties props)
      throws Exception {
    super(jobId, LOG);

    this.props = new Properties();
    this.props.putAll(props);

    Configuration conf = new Configuration();

    String fsUri = conf.get(HADOOP_FS_DEFAULT_NAME);
    if (!Strings.isNullOrEmpty(fsUri)) {
      if (!this.props.containsKey(ConfigurationKeys.FS_URI_KEY)) {
        this.props.setProperty(ConfigurationKeys.FS_URI_KEY, fsUri);
      }
      if (!this.props.containsKey(ConfigurationKeys.STATE_STORE_FS_URI_KEY)) {
        this.props.setProperty(ConfigurationKeys.STATE_STORE_FS_URI_KEY, fsUri);
      }
    }

    // Set the job tracking URL to point to the Azkaban job execution link URL
    this.props.setProperty(
        ConfigurationKeys.JOB_TRACKING_URL_KEY, Strings.nullToEmpty(conf.get(AZKABAN_LINK_JOBEXEC_URL)));

    // Necessary for compatibility with Azkaban's hadoopJava job type
    // http://azkaban.github.io/azkaban/docs/2.5/#hadoopjava-type
    if (System.getenv(HADOOP_TOKEN_FILE_LOCATION) != null) {
      this.props.setProperty(MAPREDUCE_JOB_CREDENTIALS_BINARY, System.getenv(HADOOP_TOKEN_FILE_LOCATION));
    }

    List<Tag<?>> tags = Lists.newArrayList();
    tags.addAll(Tag.fromMap(AzkabanTags.getAzkabanTags()));
    JobMetrics.addCustomTagsToProperties(this.props, tags);

    // If the job launcher type is not specified in the job configuration,
    // override the default to use the MAPREDUCE launcher.
    if (!this.props.containsKey(ConfigurationKeys.JOB_LAUNCHER_TYPE_KEY)) {
      this.props.setProperty(ConfigurationKeys.JOB_LAUNCHER_TYPE_KEY,
          JobLauncherFactory.JobLauncherType.MAPREDUCE.toString());
    }

    // Create a JobLauncher instance depending on the configuration. The same properties object is
    // used for both system and job configuration properties because Azkaban puts configuration
    // properties in the .job file and in the .properties file into the same Properties object.
    this.jobLauncher = this.closer.register(JobLauncherFactory.newJobLauncher(this.props, this.props));
  }

  @Override
  public void run()
      throws Exception {
    try {
      if (isCurrentTimeInRange()) {
        this.jobLauncher.launchJob(this.jobListener);
      }
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

  /**
   * Uses the properties {@link ConfigurationKeys#AZKABAN_EXECUTION_DAYS_LIST},
   * {@link ConfigurationKeys#AZKABAN_EXECUTION_TIME_RANGE}, and
   * {@link TimeRangeChecker#isTimeInRange(List, String, String, DateTime)} to determine if the current job should
   * continue its execution based on the extra scheduled parameters defined in the config.
   *
   * @return true if this job should be launched, false otherwise.
   */
  private boolean isCurrentTimeInRange() {
    Splitter splitter = Splitter.on(",").omitEmptyStrings().trimResults();

    if (this.props.contains(ConfigurationKeys.AZKABAN_EXECUTION_DAYS_LIST)
        && this.props.contains(ConfigurationKeys.AZKABAN_EXECUTION_TIME_RANGE)) {

      List<String> executionTimeRange =
          splitter.splitToList(this.props.getProperty(ConfigurationKeys.AZKABAN_EXECUTION_TIME_RANGE));
      List<String> executionDays =
          splitter.splitToList(this.props.getProperty(ConfigurationKeys.AZKABAN_EXECUTION_DAYS_LIST));
      Preconditions.checkArgument(executionTimeRange.size() == 2, "The property "
          + ConfigurationKeys.AZKABAN_EXECUTION_DAYS_LIST + " should be a comma separated list of two entries");

      return TimeRangeChecker.isTimeInRange(executionDays, executionTimeRange.get(0), executionTimeRange.get(1),
          new DateTime(DateTimeZone.forID(ConfigurationKeys.PST_TIMEZONE_NAME)));
    }

    return true;
  }
}
