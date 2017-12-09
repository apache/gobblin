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

package org.apache.gobblin.metrics.reporter;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Timer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.InnerMetricContext;
import org.apache.gobblin.metrics.context.ReportableContext;
import org.apache.gobblin.metrics.metric.filter.MetricFilters;
import org.apache.gobblin.metrics.metric.filter.MetricNameRegexFilter;
import org.apache.gobblin.metrics.metric.filter.MetricTypeFilter;
import org.apache.gobblin.util.ExecutorsUtils;


/**
 * A {@link ContextAwareReporter} that reports on a schedule.
 */
@Slf4j
public abstract class ScheduledReporter extends ContextAwareReporter {

  /**
   * Interval at which metrics are reported. Format: hours, minutes, seconds. Examples: 1h, 1m, 10s, 1h30m, 2m30s, ...
   */
  public static final String REPORTING_INTERVAL =
      ConfigurationKeys.METRICS_CONFIGURATIONS_PREFIX + "reporting.interval";
  public static final String DEFAULT_REPORTING_INTERVAL_PERIOD = "1M";

  public static final PeriodFormatter PERIOD_FORMATTER = new PeriodFormatterBuilder().
      appendHours().appendSuffix("H").
      appendMinutes().appendSuffix("M").
      appendSeconds().appendSuffix("S").toFormatter();

  private static final String METRIC_FILTER_NAME_REGEX = "metric.filter.name.regex";
  private static final String METRIC_FILTER_TYPE_LIST = "metric.filter.type.list";

  @VisibleForTesting
  static int parsePeriodToSeconds(String periodStr) {
    try {
      return Period.parse(periodStr.toUpperCase(), PERIOD_FORMATTER).toStandardSeconds().getSeconds();
    } catch(ArithmeticException ae) {
      throw new RuntimeException(String.format("Reporting interval is too long. Max: %d seconds.", Integer.MAX_VALUE));
    }
  }

  public static void setReportingInterval(Properties props, long reportingInterval, TimeUnit reportingIntervalUnit) {
    long seconds = TimeUnit.SECONDS.convert(reportingInterval, reportingIntervalUnit);
    if (seconds > Integer.MAX_VALUE) {
      throw new RuntimeException(String.format("Reporting interval is too long. Max: %d seconds.", Integer.MAX_VALUE));
    }
    props.setProperty(REPORTING_INTERVAL, Long.toString(seconds) + "S");
  }

  public static Config setReportingInterval(Config config, long reportingInterval, TimeUnit reportingIntervalUnit) {
    long seconds = TimeUnit.SECONDS.convert(reportingInterval, reportingIntervalUnit);
    if (seconds > Integer.MAX_VALUE) {
      throw new RuntimeException(String.format("Reporting interval is too long. Max: %d seconds.", Integer.MAX_VALUE));
    }
    return config.withValue(REPORTING_INTERVAL, ConfigValueFactory.fromAnyRef(seconds + "S"));
  }

  private ScheduledExecutorService executor;
  private MetricFilter metricFilter;

  private Optional<ScheduledFuture> scheduledTask;
  private int reportingPeriodSeconds;

  public ScheduledReporter(String name, Config config) {
    super(name, config);
    ensureMetricFilterIsInitialized(config);
  }

  private synchronized void ensureMetricFilterIsInitialized(Config config) {
    if (this.metricFilter == null) {
      this.metricFilter = createMetricFilter(config);
    }
  }

  private MetricFilter createMetricFilter(Config config) {
    if (config.hasPath(METRIC_FILTER_NAME_REGEX) && config.hasPath(METRIC_FILTER_TYPE_LIST)) {
      return MetricFilters.and(new MetricNameRegexFilter(config.getString(METRIC_FILTER_NAME_REGEX)),
          new MetricTypeFilter(config.getString(METRIC_FILTER_TYPE_LIST)));
    }
    if (config.hasPath(METRIC_FILTER_NAME_REGEX)) {
      return new MetricNameRegexFilter(config.getString(METRIC_FILTER_NAME_REGEX));
    }
    if (config.hasPath(METRIC_FILTER_TYPE_LIST)) {
      return new MetricTypeFilter(config.getString(METRIC_FILTER_TYPE_LIST));
    }
    return MetricFilter.ALL;
  }

  @Override
  public void startImpl() {
    this.executor = Executors.newSingleThreadScheduledExecutor(
            ExecutorsUtils.newDaemonThreadFactory(Optional.of(log), Optional.of("metrics-" + name + "-scheduler")));
    this.reportingPeriodSeconds = parsePeriodToSeconds(
            config.hasPath(REPORTING_INTERVAL) ? config.getString(REPORTING_INTERVAL) : DEFAULT_REPORTING_INTERVAL_PERIOD);
    ensureMetricFilterIsInitialized(config);
    this.scheduledTask = Optional.<ScheduledFuture>of(this.executor.scheduleAtFixedRate(new Runnable() {
      @Override public void run() {
        report();
      }
    }, 0, this.reportingPeriodSeconds, TimeUnit.SECONDS));
  }

  @Override
  public void stopImpl() {
    this.scheduledTask.get().cancel(false);
    this.scheduledTask = Optional.absent();
    ExecutorsUtils.shutdownExecutorService(this.executor, Optional.of(log), 10, TimeUnit.SECONDS);

    // Report metrics one last time - this ensures any metrics values updated between intervals are reported
    report(true);
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

  @Override
  protected void removedMetricContext(InnerMetricContext context) {
    if (shouldReportInnerMetricContext(context)) {
      report(context, true);
    }
    super.removedMetricContext(context);
  }

  /**
   * Trigger emission of a report.
   */
  public void report() {
    report(false);
  }

  /***
   * @param isFinal true if this is the final time report will be called for this reporter, false otherwise
   * @see #report()
   */
  protected void report(boolean isFinal) {
    for (ReportableContext metricContext : getMetricContextsToReport()) {
      report(metricContext, isFinal);
    }
  }

  /**
   * Report as {@link InnerMetricContext}.
   *
   * <p>
   *   This method is marked as final because it is not directly invoked from the framework, so this method should not
   *   be overloaded. Overload {@link #report(ReportableContext, boolean)} instead.
   * </p>
   *
   * @param context {@link InnerMetricContext} to report.
   * @see #report(ReportableContext, boolean)
   */
  protected final void report(ReportableContext context) {
    report(context, false);
  }

  /**
   * @param context {@link InnerMetricContext} to report.
   * @param isFinal true if this is the final time report will be called for the given context, false otherwise
   * @see #report(ReportableContext)
   */
  protected void report(ReportableContext context, boolean isFinal) {
    report(context.getGauges(this.metricFilter), context.getCounters(this.metricFilter),
        context.getHistograms(this.metricFilter), context.getMeters(this.metricFilter),
        context.getTimers(this.metricFilter), context.getTagMap(), isFinal);
  }

  /**
   * Report the input metrics. The input tags apply to all input metrics.
   *
   * <p>
   *   The default implementation of this method is to ignore the value of isFinal. Sub-classes that are interested in
   *   using the value of isFinal should override this method as well as
   *    {@link #report(SortedMap, SortedMap, SortedMap, SortedMap, SortedMap, Map)}. If they are not interested in the
   *    value of isFinal, they should just override
   *    {@link #report(SortedMap, SortedMap, SortedMap, SortedMap, SortedMap, Map)}.
   * </p>
   *
   * @param isFinal true if this is the final time report will be called, false otherwise
   */
  protected void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers,
      Map<String, Object> tags, boolean isFinal) {
    report(gauges, counters, histograms, meters, timers, tags);
  }

  /**
   * @see #report(SortedMap, SortedMap, SortedMap, SortedMap, SortedMap, Map)
   */
  protected abstract void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers,
      Map<String, Object> tags);
}
