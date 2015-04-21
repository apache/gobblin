/*
 * (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime.util;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.JmxReporter;
import com.google.common.base.Optional;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.MetricContext;
import gobblin.metrics.OutputStreamReporter;
import gobblin.metrics.Tag;
import gobblin.runtime.JobState;


public class JobMetrics extends GobblinMetrics {

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinMetrics.class);

  protected String jobName;

  // File metric reporter
  private Optional<OutputStreamReporter> fileReporter = Optional.absent();
  // JMX metric reporter
  private Optional<JmxReporter> jmxReporter = Optional.absent();

  protected JobMetrics(String jobName, String jobId) {
    super(jobId);
    this.jobName = jobName;
    List<Tag<?>> tags = new ArrayList<Tag<?>>();
    tags.add(new Tag<String>("jobName", jobName == null ? "" : jobName));
    tags.add(new Tag<String>("jobId", jobId));
    this.metricContext = new MetricContext.Builder("gobblin.metrics.job." + jobId).
        addTags(tags).
        build();
  }

  /**
   * Get a new {@link GobblinMetrics} instance for a given job.
   *
   * @param jobName job name
   * @param jobId job ID
   * @return a new {@link GobblinMetrics} instance for the given job
   */
  public static JobMetrics get(String jobName, String jobId) {
    if(!METRICS_MAP.containsKey(jobId)) {
      METRICS_MAP.putIfAbsent(jobId, new JobMetrics(jobName, jobId));
    }
    return (JobMetrics)METRICS_MAP.get(jobId);
  }

  public static JobMetrics get(JobState job) {
    return get(job.getJobName(), job.getJobId());
  }

  /**
   * Start metric reporting.
   *
   * @param properties configuration properties
   */
  public void startMetricReporting(Properties properties) {
    buildFileMetricReporter(properties);
    long reportInterval = Long.parseLong(properties.getProperty(ConfigurationKeys.METRICS_REPORT_INTERVAL_KEY,
        ConfigurationKeys.DEFAULT_METRICS_REPORT_INTERVAL));
    if (this.fileReporter.isPresent()) {
      this.fileReporter.get().start(reportInterval, TimeUnit.MILLISECONDS);
    }

    buildJmxMetricReporter(properties);
    if (this.jmxReporter.isPresent()) {
      this.jmxReporter.get().start();
    }
  }

  /**
   * Stop the metric reporting.
   */
  public void stopMetricReporting() {
    if (this.fileReporter.isPresent()) {
      this.fileReporter.get().stop();
    }

    if (this.jmxReporter.isPresent()) {
      this.jmxReporter.get().stop();
    }

    try {
      this.closer.close();
    } catch (IOException ioe) {
      LOGGER.error("Failed to close metric output stream for job " + this.id, ioe);
    }
  }

  private void buildFileMetricReporter(Properties properties) {
    if (!Boolean.valueOf(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_FILE_ENABLED_KEY,
        ConfigurationKeys.DEFAULT_METRICS_REPORTING_FILE_ENABLED))) {
      LOGGER.info("Not reporting metrics to log files");
      return;
    }

    if (!properties.containsKey(ConfigurationKeys.METRICS_LOG_DIR_KEY)) {
      LOGGER.error(
          "Not reporting metrics to log files because " + ConfigurationKeys.METRICS_LOG_DIR_KEY + " is undefined");
      return;
    }

    try {
      String fsUri = properties.getProperty(ConfigurationKeys.FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI);
      FileSystem fs = FileSystem.get(URI.create(fsUri), new Configuration());

      // Each job gets its own metric log subdirectory
      Path metricsLogDir = new Path(properties.getProperty(ConfigurationKeys.METRICS_LOG_DIR_KEY), this.jobName);
      if (!fs.exists(metricsLogDir) && !fs.mkdirs(metricsLogDir)) {
        LOGGER.error("Failed to create metric log directory for job " + this.jobName);
        return;
      }

      // Each job run gets its own metric log file
      Path metricLogFile = new Path(metricsLogDir, this.id + ".metrics.log");
      boolean append = false;
      // Append to the metric file if it already exists
      if (fs.exists(metricLogFile)) {
        LOGGER.info(String.format("Metric log file %s already exists, appending to it", metricLogFile));
        append = true;
      }

      this.fileReporter = Optional.
          of(closer.register(OutputStreamReporter.
              forContext(this.metricContext).
              outputTo(append ? fs.append(metricLogFile) : fs.create(metricLogFile, true)).
              build()));
    } catch (IOException ioe) {
      LOGGER.error("Failed to build file metric reporter for job " + this.id, ioe);
    }
  }

  private void buildJmxMetricReporter(Properties properties) {
    if (!Boolean.valueOf(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_JMX_ENABLED_KEY,
        ConfigurationKeys.DEFAULT_METRICS_REPORTING_JMX_ENABLED))) {
      LOGGER.info("Not reporting metrics to JMX");
      return;
    }

    this.jmxReporter = Optional.of(closer.register(JmxReporter.forRegistry(this.metricContext).convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS).build()));
  }
}
