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

package gobblin.metrics;

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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;


/**
 * A class that represents a set of metrics associated with a given name.
 *
 * @author ynli
 */
public class GobblinMetrics {

  /**
   * Enumeration of metric types.
   */
  public enum MetricType {
    COUNTER, METER, GAUGE
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinMetrics.class);

  /**
   * Check whether metrics collection and reporting are enabled or not.
   *
   * @param properties Configuration properties
   * @return whether metrics collection and reporting are enabled
   */
  public static boolean isEnabled(Properties properties) {
    return Boolean.valueOf(
        properties.getProperty(ConfigurationKeys.METRICS_ENABLED_KEY, ConfigurationKeys.DEFAULT_METRICS_ENABLED));
  }

  /**
   * Check whether metrics collection and reporting are enabled or not.
   *
   * @param state a {@link State} object containing configuration properties
   * @return whether metrics collection and reporting are enabled
   */
  public static boolean isEnabled(State state) {
    return Boolean
        .valueOf(state.getProp(ConfigurationKeys.METRICS_ENABLED_KEY, ConfigurationKeys.DEFAULT_METRICS_ENABLED));
  }

  public static GobblinMetrics get(String id) {
    return get(id, null);
  }

  public static GobblinMetrics get(String id, MetricContext parentContext) {
    return get(id, parentContext, new ArrayList<Tag<?>>());
  }

  public static GobblinMetrics get(String id, MetricContext parentContext, List<Tag<?>> tags) {
    GobblinMetricsRegistry registry = GobblinMetricsRegistry.getInstance();
    if (!registry.containsKey(id)) {
      registry.putIfAbsent(id, new GobblinMetrics(id, parentContext, tags));
    }
    return registry.get(id);
  }

  protected final String id;
  protected final MetricContext metricContext;
  // Closer for closing the metric output stream
  protected final Closer closer = Closer.create();
  // File metric reporter
  private Optional<OutputStreamReporter> fileReporter = Optional.absent();
  // JMX metric reporter
  private Optional<JmxReporter> jmxReporter = Optional.absent();

  protected GobblinMetrics(String id, MetricContext parentContext, List<Tag<?>> tags) {
    this.id = id;
    this.metricContext = parentContext == null ?
        new MetricContext.Builder(id).addTags(tags).build() :
        parentContext.childBuilder(id).addTags(tags).build();
  }

  /**
   * Get the wrapped {@link com.codahale.metrics.MetricRegistry} instance.
   *
   * @return wrapped {@link com.codahale.metrics.MetricRegistry} instance
   */
  public MetricContext getMetricContext() {
    return this.metricContext;
  }

  /**
   * Get the job ID of this metrics set.
   *
   * @return job ID of this metrics set
   */
  public String getId() {
    return this.id;
  }

  public String getName() {
    return this.id;
  }

  public Meter getMeter(String prefix, String... suffixes) {
    return this.metricContext.meter(MetricRegistry.name(prefix, suffixes));
  }

  public Counter getCounter(String prefix, String... suffixes) {
    return this.metricContext.counter(MetricRegistry.name(prefix, suffixes));
  }

  public Histogram getHistogram(String prefix, String... suffixes) {
    return this.metricContext.histogram(MetricRegistry.name(prefix, suffixes));
  }

  public Timer getTimer(String prefix, String... suffixes) {
    return this.metricContext.timer(MetricRegistry.name(prefix, suffixes));
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
      Path metricsLogDir = new Path(properties.getProperty(ConfigurationKeys.METRICS_LOG_DIR_KEY), this.getName());
      if (!fs.exists(metricsLogDir) && !fs.mkdirs(metricsLogDir)) {
        LOGGER.error("Failed to create metric log directory for metrics " + this.getName());
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
