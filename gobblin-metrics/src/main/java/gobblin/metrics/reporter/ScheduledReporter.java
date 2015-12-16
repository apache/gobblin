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

package gobblin.metrics.reporter;

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

import gobblin.metrics.InnerMetricContext;
import gobblin.metrics.context.ReportableContext;
import gobblin.util.ExecutorsUtils;


/**
 * A {@link ContextAwareReporter} that reports on a schedule.
 */
@Slf4j
public abstract class ScheduledReporter extends ContextAwareReporter {

  /**
   * Interval at which metrics are reported. Format: hours, minutes, seconds. Examples: 1h, 1m, 10s, 1h30m, 2m30s, ...
   */
  public static final String REPORTING_INTERVAL = "reporting.interval";
  public static final String DEFAULT_REPORTING_INTERVAL_PERIOD = "1M";

  public static final PeriodFormatter PERIOD_FORMATTER = new PeriodFormatterBuilder().
      appendHours().appendSuffix("H").
      appendMinutes().appendSuffix("M").
      appendSeconds().appendSuffix("S").toFormatter();

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

  private final ScheduledExecutorService executor;
  private Optional<ScheduledFuture> scheduledTask;
  private int reportingPeriodSeconds;

  public ScheduledReporter(String name, Config config) {
    super(name, config);
    this.executor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(log), Optional.of("metrics-" + name + "-scheduler")));
    this.reportingPeriodSeconds = parsePeriodToSeconds(
        config.hasPath(REPORTING_INTERVAL) ? config.getString(REPORTING_INTERVAL) : DEFAULT_REPORTING_INTERVAL_PERIOD);
  }

  @Override
  public void startImpl() {
    this.scheduledTask = Optional.<ScheduledFuture>of(this.executor.scheduleAtFixedRate(new Runnable() {
      @Override public void run() {
        report();
      }
    }, 0, this.reportingPeriodSeconds, TimeUnit.SECONDS));
  }

  @Override
  public void stopImpl() {
    // Report metrics before stopping - this ensures any metrics values updated between intervals are reported
    report();
    this.scheduledTask.get().cancel(false);
    this.scheduledTask = Optional.absent();
  }

  @Override
  public void close() throws IOException {
    ExecutorsUtils.shutdownExecutorService(this.executor, Optional.of(log), 10, TimeUnit.SECONDS);
    super.close();
  }

  @Override
  protected void removedMetricContext(InnerMetricContext context) {
    if (shouldReportInnerMetricContext(context)) {
      report(context);
    }
    super.removedMetricContext(context);
  }

  /**
   * Trigger emission of a report.
   */
  public void report() {
    for (ReportableContext metricContext : getMetricContextsToReport()) {
      report(metricContext);
    }
  }

  /**
   * Report as {@link InnerMetricContext}.
   *
   * @param context {@link InnerMetricContext} to report.
   */
  protected void report(ReportableContext context) {
    report(context.getGauges(MetricFilter.ALL), context.getCounters(MetricFilter.ALL),
        context.getHistograms(MetricFilter.ALL), context.getMeters(MetricFilter.ALL),
        context.getTimers(MetricFilter.ALL), context.getTagMap());
  }

  /**
   * Report the input metrics. The input tags apply to all input metrics.
   */
  public abstract void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers,
      Map<String, Object> tags);
}
