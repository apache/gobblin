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

package org.apache.gobblin.metrics;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.jmx.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.typesafe.config.Config;

import lombok.Getter;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metrics.graphite.GraphiteConnectionType;
import org.apache.gobblin.metrics.graphite.GraphiteEventReporter;
import org.apache.gobblin.metrics.graphite.GraphiteReporter;
import org.apache.gobblin.metrics.influxdb.InfluxDBConnectionType;
import org.apache.gobblin.metrics.influxdb.InfluxDBEventReporter;
import org.apache.gobblin.metrics.influxdb.InfluxDBReporter;
import org.apache.gobblin.metrics.reporter.FileFailureEventReporter;
import org.apache.gobblin.metrics.reporter.OutputStreamEventReporter;
import org.apache.gobblin.metrics.reporter.OutputStreamReporter;
import org.apache.gobblin.metrics.reporter.ScheduledReporter;
import org.apache.gobblin.password.PasswordManager;
import org.apache.gobblin.util.PropertiesUtils;


/**
 * A class that represents a set of metrics associated with a given name.
 *
 * @author Yinan Li
 */
public class GobblinMetrics {

  public static final String METRICS_ID_PREFIX = "gobblin.metrics.";
  public static final String METRICS_STATE_CUSTOM_TAGS = "metrics.state.custom.tags";

  @Getter
  protected static final GobblinMetricsRegistry GOBBLIN_METRICS_REGISTRY = GobblinMetricsRegistry.getInstance();

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
    return PropertiesUtils
        .getPropAsBoolean(properties, ConfigurationKeys.METRICS_ENABLED_KEY, ConfigurationKeys.DEFAULT_METRICS_ENABLED);
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

  /**
   * Check whether metrics collection and reporting are enabled or not.
   *
   * @param cfg a {@link State} object containing configuration properties
   * @return whether metrics collection and reporting are enabled
   */
  public static boolean isEnabled(Config cfg) {
    return cfg.hasPath(ConfigurationKeys.METRICS_ENABLED_KEY) ? cfg.getBoolean(ConfigurationKeys.METRICS_ENABLED_KEY)
        : Boolean.parseBoolean(ConfigurationKeys.DEFAULT_METRICS_ENABLED);
  }

  /**
   * Get a {@link GobblinMetrics} instance with the given ID.
   *
   * @param id the given {@link GobblinMetrics} ID
   * @return a {@link GobblinMetrics} instance
   */
  public static GobblinMetrics get(String id) {
    return get(id, null);
  }

  /**
   * Get a {@link GobblinMetrics} instance with the given ID and parent {@link MetricContext}.
   *
   * @param id the given {@link GobblinMetrics} ID
   * @param parentContext the given parent {@link MetricContext}
   * @return a {@link GobblinMetrics} instance
   */
  public static GobblinMetrics get(String id, MetricContext parentContext) {
    return get(id, parentContext, Lists.<Tag<?>>newArrayList());
  }

  /**
   * Get a {@link GobblinMetrics} instance with the given ID, parent {@link MetricContext},
   * and list of {@link Tag}s.
   *
   * @param id the given {@link GobblinMetrics} ID
   * @param parentContext the given parent {@link MetricContext}
   * @param tags the given list of {@link Tag}s
   * @return a {@link GobblinMetrics} instance
   */
  public static GobblinMetrics get(final String id, final MetricContext parentContext, final List<Tag<?>> tags) {
    return GOBBLIN_METRICS_REGISTRY.getOrCreate(id, new Callable<GobblinMetrics>() {
      @Override
      public GobblinMetrics call()
          throws Exception {
        return new GobblinMetrics(id, parentContext, tags);
      }
    });
  }

  /**
   * Remove the {@link GobblinMetrics} instance associated with the given ID.
   *
   * @param id the given {@link GobblinMetrics} ID
   */
  public static void remove(String id) {
    GOBBLIN_METRICS_REGISTRY.remove(id);
  }

  /**
   * Add a {@link List} of {@link Tag}s to a {@link org.apache.gobblin.configuration.State} with key {@link #METRICS_STATE_CUSTOM_TAGS}.
   *
   * <p>
   *   {@link org.apache.gobblin.metrics.Tag}s under this key can later be parsed using the method {@link #getCustomTagsFromState}.
   * </p>
   *
   * @param state {@link org.apache.gobblin.configuration.State} state to add the tag to.
   * @param tags list of {@link Tag}s to add.
   */
  public static void addCustomTagToState(State state, List<? extends Tag<?>> tags) {
    for (Tag<?> tag : tags) {
      state.appendToListProp(METRICS_STATE_CUSTOM_TAGS, tag.toString());
    }
  }

  /**
   * Add a {@link Tag} to a {@link org.apache.gobblin.configuration.State} with key {@link #METRICS_STATE_CUSTOM_TAGS}.
   *
   * <p>
   *   {@link org.apache.gobblin.metrics.Tag}s under this key can later be parsed using the method {@link #getCustomTagsFromState}.
   * </p>
   *
   * @param state {@link org.apache.gobblin.configuration.State} state to add the tag to.
   * @param tag {@link Tag} to add.
   */
  public static void addCustomTagToState(State state, Tag<?> tag) {
    state.appendToListProp(METRICS_STATE_CUSTOM_TAGS, tag.toString());
  }

  /**
   * Add {@link List} of {@link Tag}s to a {@link Properties} with key {@link #METRICS_STATE_CUSTOM_TAGS}.
   * <p>
   *  Also see {@link #addCustomTagToState(State, Tag)} , {@link #addCustomTagToProperties(Properties, Tag)}
   * </p>
   *
   * <p>
   *   The {@link Properties} passed can be used to build a {@link State}.
   *   {@link org.apache.gobblin.metrics.Tag}s under this key can later be parsed using the method {@link #getCustomTagsFromState}.
   * </p>
   *
   * @param properties {@link Properties} to add the tag to.
   * @param tags list of {@link Tag}s to add.
   */
  public static void addCustomTagsToProperties(Properties properties, List<Tag<?>> tags) {
    for (Tag<?> tag : tags) {
      addCustomTagToProperties(properties, tag);
    }
  }

  /**
   * Add a {@link Tag} to a {@link Properties} with key {@link #METRICS_STATE_CUSTOM_TAGS}.
   * Also see {@link #addCustomTagToState(State, Tag)}
   *
   * <p>
   *   The {@link Properties} passed can be used to build a {@link State}.
   *   {@link org.apache.gobblin.metrics.Tag}s under this key can later be parsed using the method {@link #getCustomTagsFromState}.
   * </p>
   *
   * @param properties {@link Properties} to add the tag to.
   * @param tag {@link Tag} to add.
   */
  public static void addCustomTagToProperties(Properties properties, Tag<?> tag) {
    // Build a state wrapper to add custom tag to property
    State state = new State(properties);
    addCustomTagToState(state, tag);
  }

  /**
   * Parse custom {@link org.apache.gobblin.metrics.Tag}s from property {@link #METRICS_STATE_CUSTOM_TAGS}
   * in the input {@link org.apache.gobblin.configuration.State}.
   * @param state {@link org.apache.gobblin.configuration.State} possibly containing custom tags.
   * @return List of {@link org.apache.gobblin.metrics.Tag} parsed from input.
   */
  public static List<Tag<?>> getCustomTagsFromState(State state) {
    List<Tag<?>> tags = Lists.newArrayList();
    for (String tagKeyValue : state.getPropAsList(METRICS_STATE_CUSTOM_TAGS, "")) {
      Tag<?> tag = Tag.fromString(tagKeyValue);
      if (tag != null) {
        tags.add(tag);
      }
    }
    return tags;
  }

  protected final String id;
  protected final MetricContext metricContext;

  // Closer for closing the metric output stream
  protected final Closer codahaleReportersCloser = Closer.create();

  // JMX metric reporter
  private Optional<JmxReporter> jmxReporter = Optional.absent();

  // Custom metric reporters instantiated through reflection
  private final List<com.codahale.metrics.ScheduledReporter> codahaleScheduledReporters = Lists.newArrayList();

  // A flag telling whether metric reporting has started or not
  private volatile boolean metricsReportingStarted = false;

  protected GobblinMetrics(String id, MetricContext parentContext, List<Tag<?>> tags) {
    this.id = id;
    this.metricContext = parentContext == null ? new MetricContext.Builder(id).addTags(tags).build()
        : parentContext.childBuilder(id).addTags(tags).build();
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
   * Get the ID of this {@link GobblinMetrics}.
   *
   * @return ID of this {@link GobblinMetrics}
   */
  public String getId() {
    return this.id;
  }

  /**
   * Get the name of this {@link GobblinMetrics}.
   *
   * <p>
   *   This method is currently equivalent to {@link #getId()}.
   * </p>
   *
   * @return name of this {@link GobblinMetrics}
   */
  public String getName() {
    return this.id;
  }

  /**
   * Get a {@link Meter} with the given name prefix and suffixes.
   *
   * @param prefix the given name prefix
   * @param suffixes the given name suffixes
   * @return a {@link Meter} with the given name prefix and suffixes
   */
  public Meter getMeter(String prefix, String... suffixes) {
    return this.metricContext.meter(MetricRegistry.name(prefix, suffixes));
  }

  /**
   * Get a {@link Counter} with the given name prefix and suffixes.
   *
   * @param prefix the given name prefix
   * @param suffixes the given name suffixes
   * @return a {@link Counter} with the given name prefix and suffixes
   */
  public Counter getCounter(String prefix, String... suffixes) {
    return this.metricContext.counter(MetricRegistry.name(prefix, suffixes));
  }

  /**
   * Get a {@link Histogram} with the given name prefix and suffixes.
   *
   * @param prefix the given name prefix
   * @param suffixes the given name suffixes
   * @return a {@link Histogram} with the given name prefix and suffixes
   */
  public Histogram getHistogram(String prefix, String... suffixes) {
    return this.metricContext.histogram(MetricRegistry.name(prefix, suffixes));
  }

  /**
   * Get a {@link Timer} with the given name prefix and suffixes.
   *
   * @param prefix the given name prefix
   * @param suffixes the given name suffixes
   * @return a {@link Timer} with the given name prefix and suffixes
   */
  public Timer getTimer(String prefix, String... suffixes) {
    return this.metricContext.timer(MetricRegistry.name(prefix, suffixes));
  }

  /**
   * Starts metric reporting and appends the given metrics file suffix to the current value of
   * {@link ConfigurationKeys#METRICS_FILE_SUFFIX}.
   */
  public void startMetricReportingWithFileSuffix(State state, String metricsFileSuffix)
      throws MultiReporterException {
    Properties metricsReportingProps = new Properties();
    metricsReportingProps.putAll(state.getProperties());

    String oldMetricsFileSuffix =
        state.getProp(ConfigurationKeys.METRICS_FILE_SUFFIX, ConfigurationKeys.DEFAULT_METRICS_FILE_SUFFIX);
    if (Strings.isNullOrEmpty(oldMetricsFileSuffix)) {
      oldMetricsFileSuffix = metricsFileSuffix;
    } else {
      oldMetricsFileSuffix += "." + metricsFileSuffix;
    }
    metricsReportingProps.setProperty(ConfigurationKeys.METRICS_FILE_SUFFIX, oldMetricsFileSuffix);
    startMetricReporting(metricsReportingProps);
  }

  /**
   * Start metric reporting.
   *
   * @param configuration configuration properties
   */
  public void startMetricReporting(Configuration configuration)
      throws MultiReporterException {
    Properties props = new Properties();
    for (Map.Entry<String, String> entry : configuration) {
      props.put(entry.getKey(), entry.getValue());
    }
    startMetricReporting(props);
  }

  /**
   * Start metric reporting.
   *
   * @param properties configuration properties
   */
  public void startMetricReporting(Properties properties) throws MultiReporterException {
    if (this.metricsReportingStarted) {
      LOGGER.warn("Metric reporting has already started");
      return;
    }

    TimeUnit reportTimeUnit = TimeUnit.MILLISECONDS;
    long reportInterval = Long.parseLong(properties
        .getProperty(ConfigurationKeys.METRICS_REPORT_INTERVAL_KEY, ConfigurationKeys.DEFAULT_METRICS_REPORT_INTERVAL));
    ScheduledReporter.setReportingInterval(properties, reportInterval, reportTimeUnit);

    long startTime = System.currentTimeMillis();

    List<MetricReporterException> reporterExceptions = Lists.newArrayList();

    try {
      for (ReporterSinkType sinkType: ReporterSinkType.values()) {
        if (sinkType.equals(ReporterSinkType.CUSTOM)) {
          buildCustomMetricReporters(properties, reporterExceptions);
        } else {
          try {
            buildReporter(properties, sinkType);
          } catch (MultiReporterException e) {
            reporterExceptions.addAll(e.getExceptions());
          }
        }
      }

      // Start reporters that implement org.apache.gobblin.metrics.report.ScheduledReporter
      RootMetricContext.get().startReporting();

      // Start reporters that implement com.codahale.metrics.ScheduledReporter
      for (com.codahale.metrics.ScheduledReporter scheduledReporter : this.codahaleScheduledReporters) {
        scheduledReporter.start(reportInterval, reportTimeUnit);
      }
    } catch (Exception e) {
      LOGGER.error("Metrics reporting cannot be started due to {}", ExceptionUtils.getFullStackTrace(e));
      throw e;
    }

    this.metricsReportingStarted = true;
    LOGGER.info("Metrics reporting has been started in {} ms: GobblinMetrics {}",
        System.currentTimeMillis() - startTime, this.toString());

    if (!reporterExceptions.isEmpty()) {
      throw new MultiReporterException("Could not create one or more reporters", reporterExceptions);
    }
  }

  private void buildReporter(Properties properties, ReporterSinkType sinkType) throws MultiReporterException {
    switch (sinkType) {
      case JMX:
        buildJmxMetricReporter(properties);
        break;
      case FILE:
        buildFileMetricReporter(properties);
        break;
      case KAFKA:
        buildKafkaMetricReporter(properties);
        break;
      case GRAPHITE:
        buildGraphiteMetricReporter(properties);
        break;
      case INFLUXDB:
        buildInfluxDBMetricReporter(properties);
        break;
      case FILE_FAILURE:
        buildFileFailureEventReporter(properties);
        break;
      default:
        LOGGER.error("Unknown reporter sink type: {}", sinkType.name());
        break;
    }
  }

  /**
   * Stop metric reporting.
   */
  public void stopMetricsReporting() {
    LOGGER.info("Metrics reporting will be stopped: GobblinMetrics {}", this.toString());

    if (!this.metricsReportingStarted) {
      LOGGER.warn("Metric reporting has not started yet");
      return;
    }

    // Stop the JMX reporter
    if (this.jmxReporter.isPresent()) {
      this.jmxReporter.get().stop();
    }

    // Trigger and stop reporters that implement org.apache.gobblin.metrics.report.ScheduledReporter
    RootMetricContext.get().stopReporting();

    // Trigger and stop reporters that implement com.codahale.metrics.ScheduledReporter

    for (com.codahale.metrics.ScheduledReporter scheduledReporter : this.codahaleScheduledReporters) {
      scheduledReporter.report();
    }

    try {
      this.codahaleReportersCloser.close();
    } catch (IOException ioe) {
      LOGGER.error("Failed to close metric output stream for job " + this.id, ioe);
    } catch (Exception e) {
      LOGGER.error("Failed to close metric output stream for job {} due to {}", this.id, ExceptionUtils.getFullStackTrace(e));
      throw e;
    }

    this.metricsReportingStarted = false;
    // Remove from the cache registry
    GobblinMetrics.remove(id);
    LOGGER.info("Metrics reporting stopped successfully");
  }

  private void buildFileMetricReporter(Properties properties)
      throws MultiReporterException {
    if (!Boolean.valueOf(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_FILE_ENABLED_KEY,
        ConfigurationKeys.DEFAULT_METRICS_REPORTING_FILE_ENABLED))) {
      return;
    }
    LOGGER.info("Reporting metrics to log files");

    if (!properties.containsKey(ConfigurationKeys.METRICS_LOG_DIR_KEY)) {
      MetricReporterException e = new MetricReporterException(
          "Not reporting metrics to log files because " + ConfigurationKeys.METRICS_LOG_DIR_KEY + " is undefined", ReporterType.METRIC, ReporterSinkType.FILE);
      throw new MultiReporterException("Failed to create file metric reporter", Lists.newArrayList(e));
    }

    try {
      String fsUri = properties.getProperty(ConfigurationKeys.FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI);
      FileSystem fs = FileSystem.get(URI.create(fsUri), new Configuration());

      // Each job gets its own metric log subdirectory
      Path metricsLogDir = new Path(properties.getProperty(ConfigurationKeys.METRICS_LOG_DIR_KEY), this.getName());
      if (!fs.exists(metricsLogDir) && !fs.mkdirs(metricsLogDir)) {
        throw new MetricReporterException("Failed to create metric log directory for metrics " + this.getName(), ReporterType.METRIC, ReporterSinkType.FILE);
      }

      // Add a suffix to file name if specified in properties.
      String metricsFileSuffix =
          properties.getProperty(ConfigurationKeys.METRICS_FILE_SUFFIX, ConfigurationKeys.DEFAULT_METRICS_FILE_SUFFIX);
      if (!Strings.isNullOrEmpty(metricsFileSuffix) && !metricsFileSuffix.startsWith(".")) {
        metricsFileSuffix = "." + metricsFileSuffix;
      }

      // Each job run gets its own metric log file
      Path metricLogFile =
          new Path(metricsLogDir, this.id + metricsFileSuffix + ".metrics.log");
      boolean append = false;
      // Append to the metric file if it already exists
      if (fs.exists(metricLogFile)) {
        LOGGER.info(String.format("Metric log file %s already exists, appending to it", metricLogFile));
        append = true;
      }

      OutputStream output = append ? fs.append(metricLogFile) : fs.create(metricLogFile, true);
      // Add metrics reporter
      OutputStreamReporter.Factory.newBuilder().outputTo(output).build(properties);
      // Set up events reporter at the same time!!
      this.codahaleScheduledReporters.add(this.codahaleReportersCloser
          .register(OutputStreamEventReporter.forContext(RootMetricContext.get()).outputTo(output).build()));

      LOGGER.info("Will start reporting metrics to directory " + metricsLogDir);
    } catch (IOException ioe) {
      MetricReporterException e = new MetricReporterException("Failed to build file metric reporter for job " + this.id, ioe, ReporterType.METRIC, ReporterSinkType.FILE);
      throw new MultiReporterException("Failed to create file metric reporter", Lists.newArrayList(e));
    }
  }

  private void buildFileFailureEventReporter(Properties properties)
      throws MultiReporterException {
    if ((!Boolean.valueOf(properties.getProperty(ConfigurationKeys.FAILURE_REPORTING_FILE_ENABLED_KEY,
        ConfigurationKeys.DEFAULT_FAILURE_REPORTING_FILE_ENABLED)) || !properties.containsKey(ConfigurationKeys.FAILURE_LOG_DIR_KEY))) {
      return;
    }

    LOGGER.info("Reporting failure to log files");

    try {
      String fsUri = properties.getProperty(ConfigurationKeys.FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI);
      FileSystem fs = FileSystem.get(URI.create(fsUri), new Configuration());

      // Each job gets its own log subdirectory
      Path failureLogDir = new Path(properties.getProperty(ConfigurationKeys.FAILURE_LOG_DIR_KEY), this.getName());
      if (!fs.exists(failureLogDir) && !fs.mkdirs(failureLogDir)) {
        throw new MetricReporterException("Failed to create failure log directory for metrics " + this.getName(), ReporterType.EVENT, ReporterSinkType.FILE);
      }

      // Add a suffix to file name if specified in properties.
      String metricsFileSuffix =
          properties.getProperty(ConfigurationKeys.METRICS_FILE_SUFFIX, ConfigurationKeys.DEFAULT_METRICS_FILE_SUFFIX);
      if (!Strings.isNullOrEmpty(metricsFileSuffix) && !metricsFileSuffix.startsWith(".")) {
        metricsFileSuffix = "." + metricsFileSuffix;
      }

      // Each job run gets its own failure log file
      Path failureLogFile =
          new Path(failureLogDir, this.id + metricsFileSuffix + ".failure.log");
      this.codahaleScheduledReporters.add(this.codahaleReportersCloser
          .register(new FileFailureEventReporter(RootMetricContext.get(), fs, failureLogFile)));

      LOGGER.info("Will start reporting failure to directory " + failureLogDir);
    } catch (IOException ioe) {
      MetricReporterException e = new MetricReporterException("Failed to build file failure event reporter for job " + this.id, ioe, ReporterType.EVENT, ReporterSinkType.FILE);
      throw new MultiReporterException("Failed to create failure file event reporter", Lists.newArrayList(e));
    }
  }

  private void buildJmxMetricReporter(Properties properties) {
    if (!Boolean.valueOf(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_JMX_ENABLED_KEY,
        ConfigurationKeys.DEFAULT_METRICS_REPORTING_JMX_ENABLED))) {
      return;
    }
    LOGGER.info("Reporting metrics to JMX");

    this.jmxReporter = Optional.of(codahaleReportersCloser.register(JmxReporter.forRegistry(RootMetricContext.get()).
        convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build()));
  }

  private void buildKafkaMetricReporter(Properties properties)
      throws MultiReporterException {
    List<MetricReporterException> reporterExceptions = Lists.newArrayList();
    if (!Boolean.parseBoolean(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_KAFKA_ENABLED_KEY,
        ConfigurationKeys.DEFAULT_METRICS_REPORTING_KAFKA_ENABLED))) {
      return;
    }

    if (Boolean.parseBoolean(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_KAFKA_METRICS_ENABLED_KEY,
        Boolean.toString(true)))) {
      try {
        buildScheduledReporter(properties, ConfigurationKeys.DEFAULT_METRICS_REPORTING_KAFKA_REPORTER_CLASS);
      } catch (MetricReporterException e) {
        reporterExceptions.add(e);
      }
    }
    if (Boolean.parseBoolean(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_KAFKA_EVENTS_ENABLED_KEY,
        Boolean.toString(true)))) {
      try {
        buildScheduledReporter(properties, ConfigurationKeys.DEFAULT_EVENTS_REPORTING_KAFKA_REPORTER_CLASS);
      } catch (MetricReporterException e) {
        reporterExceptions.add(e);
      }
    }
    if (!reporterExceptions.isEmpty()) {
      throw new MultiReporterException("Failed to start one or more Kafka reporters", reporterExceptions);
    }
  }

  private void buildGraphiteMetricReporter(Properties properties)
      throws MultiReporterException {
    List<MetricReporterException> reporterExceptionList = Lists.newArrayList();

    boolean metricsEnabled = PropertiesUtils
        .getPropAsBoolean(properties, ConfigurationKeys.METRICS_REPORTING_GRAPHITE_METRICS_ENABLED_KEY,
            ConfigurationKeys.DEFAULT_METRICS_REPORTING_GRAPHITE_METRICS_ENABLED);
    if (metricsEnabled) {
      LOGGER.info("Reporting metrics to Graphite");
    }

    boolean eventsEnabled = PropertiesUtils
        .getPropAsBoolean(properties, ConfigurationKeys.METRICS_REPORTING_GRAPHITE_EVENTS_ENABLED_KEY,
            ConfigurationKeys.DEFAULT_METRICS_REPORTING_GRAPHITE_EVENTS_ENABLED);
    if (eventsEnabled) {
      LOGGER.info("Reporting events to Graphite");
    }

    if (!metricsEnabled && !eventsEnabled) {
      return;
    }

    try {
      Preconditions.checkArgument(properties.containsKey(ConfigurationKeys.METRICS_REPORTING_GRAPHITE_HOSTNAME),
          "Graphite hostname is missing.");
    } catch (IllegalArgumentException exception) {
      reporterExceptionList.add(new MetricReporterException("Missing Graphite configuration(s).", exception, ReporterType.METRIC_EVENT, ReporterSinkType.GRAPHITE));
      throw new MultiReporterException("Failed to start one or more Graphite reporters", reporterExceptionList);
    }

    String hostname = properties.getProperty(ConfigurationKeys.METRICS_REPORTING_GRAPHITE_HOSTNAME);
    int port = Integer.parseInt(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_GRAPHITE_PORT,
        ConfigurationKeys.DEFAULT_METRICS_REPORTING_GRAPHITE_PORT));

    GraphiteConnectionType connectionType;
    String type = properties.getProperty(ConfigurationKeys.METRICS_REPORTING_GRAPHITE_SENDING_TYPE,
        ConfigurationKeys.DEFAULT_METRICS_REPORTING_GRAPHITE_SENDING_TYPE).toUpperCase();
    String prefix = properties.getProperty(ConfigurationKeys.METRICS_REPORTING_GRAPHITE_PREFIX,
        ConfigurationKeys.DEFAULT_METRICS_REPORTING_GRAPHITE_PREFIX);
    try {
      connectionType = GraphiteConnectionType.valueOf(type);
    } catch (IllegalArgumentException exception) {
      LOGGER
          .warn("Graphite Reporter connection type " + type + " not recognized. Will use TCP for sending.", exception);
      connectionType = GraphiteConnectionType.TCP;
    }

    if (metricsEnabled) {
      try {
        GraphiteReporter.Factory.newBuilder().withConnectionType(connectionType)
            .withConnection(hostname, port).withMetricContextName(
            this.metricContext.getName()) //contains the current job id
            .withMetricsPrefix(prefix)
            .build(properties);
      } catch (IOException e) {
        reporterExceptionList.add(new MetricReporterException("Failed to create Graphite metrics reporter.", e, ReporterType.METRIC, ReporterSinkType.GRAPHITE));
      }
    }

    if (eventsEnabled) {
      boolean emitValueAsKey = PropertiesUtils
          .getPropAsBoolean(properties, ConfigurationKeys.METRICS_REPORTING_GRAPHITE_EVENTS_VALUE_AS_KEY,
              ConfigurationKeys.DEFAULT_METRICS_REPORTING_GRAPHITE_EVENTS_VALUE_AS_KEY);
      String eventsPortProp = properties.getProperty(ConfigurationKeys.METRICS_REPORTING_GRAPHITE_EVENTS_PORT);
      int eventsPort = (eventsPortProp == null) ? (metricsEnabled ? port
          : Integer.parseInt(ConfigurationKeys.METRICS_REPORTING_GRAPHITE_PORT)) : Integer.parseInt(eventsPortProp);
      try {
        GraphiteEventReporter eventReporter =
            GraphiteEventReporter.Factory.forContext(RootMetricContext.get())
              .withConnectionType(connectionType)
              .withConnection(hostname, eventsPort)
              .withPrefix(prefix)
              .withEmitValueAsKey(emitValueAsKey)
              .build();
        this.codahaleScheduledReporters.add(this.codahaleReportersCloser.register(eventReporter));
      }
      catch (IOException e) {
        reporterExceptionList.add(new MetricReporterException("Failed to create Graphite event reporter.", e, ReporterType.EVENT, ReporterSinkType.GRAPHITE));
      }
    }

    if (!reporterExceptionList.isEmpty()) {
      throw new MultiReporterException("Failed to create one or more Graphite Reporters", reporterExceptionList);
    }
  }

  private void buildInfluxDBMetricReporter(Properties properties)
      throws MultiReporterException {
    List<MetricReporterException> reporterExceptionList = Lists.newArrayList();

    boolean metricsEnabled = PropertiesUtils
        .getPropAsBoolean(properties, ConfigurationKeys.METRICS_REPORTING_INFLUXDB_METRICS_ENABLED_KEY,
            ConfigurationKeys.DEFAULT_METRICS_REPORTING_INFLUXDB_METRICS_ENABLED);
    if (metricsEnabled) {
      LOGGER.info("Reporting metrics to InfluxDB");
    }

    boolean eventsEnabled = PropertiesUtils
        .getPropAsBoolean(properties, ConfigurationKeys.METRICS_REPORTING_INFLUXDB_EVENTS_ENABLED_KEY,
            ConfigurationKeys.DEFAULT_METRICS_REPORTING_INFLUXDB_EVENTS_ENABLED);
    if (eventsEnabled) {
      LOGGER.info("Reporting events to InfluxDB");
    }

    if (!metricsEnabled && !eventsEnabled) {
      return;
    }

    try {
      Preconditions.checkArgument(properties.containsKey(ConfigurationKeys.METRICS_REPORTING_INFLUXDB_DATABASE),
          "InfluxDB database name is missing.");
    } catch (IllegalArgumentException exception) {
      reporterExceptionList.add(new MetricReporterException("Missing InfluxDB configuration(s)", exception, ReporterType.METRIC_EVENT, ReporterSinkType.INFLUXDB));
      throw new MultiReporterException("Failed to start one or more InfluxDB reporters", reporterExceptionList);
    }

    String url = properties.getProperty(ConfigurationKeys.METRICS_REPORTING_INFLUXDB_URL);
    String username = properties.getProperty(ConfigurationKeys.METRICS_REPORTING_INFLUXDB_USER);
    String password = PasswordManager.getInstance(properties)
        .readPassword(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_INFLUXDB_PASSWORD));
    String database = properties.getProperty(ConfigurationKeys.METRICS_REPORTING_INFLUXDB_DATABASE);

    InfluxDBConnectionType connectionType;
    String type = properties.getProperty(ConfigurationKeys.METRICS_REPORTING_INFLUXDB_SENDING_TYPE,
        ConfigurationKeys.DEFAULT_METRICS_REPORTING_INFLUXDB_SENDING_TYPE).toUpperCase();
    try {
      connectionType = InfluxDBConnectionType.valueOf(type);
    } catch (IllegalArgumentException exception) {
      LOGGER
          .warn("InfluxDB Reporter connection type " + type + " not recognized. Will use TCP for sending.", exception);
      connectionType = InfluxDBConnectionType.TCP;
    }

    if (metricsEnabled) {
      try {
        InfluxDBReporter.Factory.newBuilder().withConnectionType(connectionType)
            .withConnection(url, username, password, database).withMetricContextName(
            this.metricContext.getName()) // contains the current job id
            .build(properties);
      } catch (IOException e) {
        reporterExceptionList.add(new MetricReporterException("Failed to create InfluxDB metrics reporter.", e, ReporterType.METRIC, ReporterSinkType.INFLUXDB));
      }
    }

    if (eventsEnabled) {
      String eventsDbProp = properties.getProperty(ConfigurationKeys.METRICS_REPORTING_INFLUXDB_EVENTS_DATABASE);
      String eventsDatabase = (eventsDbProp == null) ? (metricsEnabled ? database : null) : eventsDbProp;
      try {
        InfluxDBEventReporter eventReporter =
            InfluxDBEventReporter.Factory.forContext(RootMetricContext.get())
              .withConnectionType(connectionType)
              .withConnection(url, username, password, eventsDatabase)
              .build();
        this.codahaleScheduledReporters.add(this.codahaleReportersCloser.register(eventReporter));
      }
      catch (IOException e) {
        reporterExceptionList.add(new MetricReporterException("Failed to create InfluxDB event reporter.", e, ReporterType.EVENT, ReporterSinkType.INFLUXDB));
      }
    }

    if (!reporterExceptionList.isEmpty()) {
      throw new MultiReporterException("Failed to create one or more InfluxDB reporters", reporterExceptionList);
    }
  }

  /**
   * Build scheduled metrics reporters by reflection from the property
   * {@link org.apache.gobblin.configuration.ConfigurationKeys#METRICS_CUSTOM_BUILDERS}. This allows users to specify custom
   * reporters for Gobblin runtime without having to modify the code.
   */
  private void buildCustomMetricReporters(Properties properties, List<MetricReporterException> reporterExceptions) {
    String reporterClasses = properties.getProperty(ConfigurationKeys.METRICS_CUSTOM_BUILDERS);

    if (Strings.isNullOrEmpty(reporterClasses)) {
      return;
    }

    for (String reporterClass : Splitter.on(",").split(reporterClasses)) {
      try {
        buildScheduledReporter(properties, reporterClass);
      } catch (MetricReporterException e) {
        reporterExceptions.add(e);
      }
    }
  }

  private void buildScheduledReporter(Properties properties, String reporterClass)
      throws MetricReporterException {
    try {
      Class<?> clazz = Class.forName(reporterClass);

      if (CustomCodahaleReporterFactory.class.isAssignableFrom(clazz)) {
        CustomCodahaleReporterFactory customCodahaleReporterFactory =
            ((CustomCodahaleReporterFactory) clazz.getConstructor().newInstance());
        com.codahale.metrics.ScheduledReporter scheduledReporter =
            customCodahaleReporterFactory.newScheduledReporter(RootMetricContext.get(), properties);
        if (scheduledReporter == null) {
          LOGGER.warn("Factory {} returns a null scheduledReporter", clazz.getSimpleName());
          return;
        }
        this.codahaleReportersCloser.register(scheduledReporter);
        LOGGER.info("Will start reporting metrics using " + reporterClass);
        this.codahaleScheduledReporters.add(scheduledReporter);
      } else if (CustomReporterFactory.class.isAssignableFrom(clazz)) {
        CustomReporterFactory customReporterFactory = ((CustomReporterFactory) clazz.getConstructor().newInstance());
        customReporterFactory.newScheduledReporter(properties);
        LOGGER.info("Will start reporting metrics using " + reporterClass);
      } else {
        throw new MetricReporterException("Class " + reporterClass +
            " specified by key " + ConfigurationKeys.METRICS_CUSTOM_BUILDERS + " must implement: "
            + CustomCodahaleReporterFactory.class + " or " + CustomReporterFactory.class, ReporterType.CUSTOM, ReporterSinkType.CUSTOM);
      }
    } catch (ClassNotFoundException exception) {
      throw new MetricReporterException(String
          .format("Failed to create metric reporter: requested CustomReporterFactory %s not found.", reporterClass),
          exception, ReporterType.CUSTOM, ReporterSinkType.CUSTOM);
    } catch (NoSuchMethodException exception) {
      throw new MetricReporterException(String.format("Failed to create metric reporter: requested CustomReporterFactory %s "
          + "does not have parameterless constructor.", reporterClass), exception, ReporterType.CUSTOM, ReporterSinkType.CUSTOM);
    } catch (MetricReporterException exception) {
      throw exception;
    } catch (Exception exception) {
      throw new MetricReporterException("Could not create metric reporter from builder " + reporterClass + ".", exception, ReporterType.CUSTOM, ReporterSinkType.CUSTOM);
    }
  }
}
