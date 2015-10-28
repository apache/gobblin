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

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import gobblin.metrics.InnerMetricContext;
import gobblin.metrics.context.ReportableContext;
import gobblin.util.ExecutorsUtils;


/**
 * A {@link ContextAwareReporter} that reports on a schedule.
 */
@Slf4j
public abstract class ScheduledReporter extends ContextAwareReporter {

  public static final String REPORTING_INTERVAL_PERIOD = "reporting.interval.period";
  public static final long DEFAULT_REPORTING_INTERVAL_PERIOD = 1;
  public static final String REPORTING_INTERVAL_UNIT = "reporting.interval.unit";
  public static final String DEFAULT_REPORTING_INTERVAL_UNIT = "MINUTES";

  public static Config setReportingInterval(Config config, long reportingInterval, TimeUnit reportingIntervalUnit) {
    return config.withValue(REPORTING_INTERVAL_PERIOD, ConfigValueFactory.fromAnyRef(reportingInterval)).
        withValue(REPORTING_INTERVAL_UNIT, ConfigValueFactory.fromAnyRef(reportingIntervalUnit.toString()));
  }

  private final ScheduledExecutorService executor;
  private Optional<ScheduledFuture> scheduledTask;
  private long reportingInterval;
  private TimeUnit reportingIntervalUnit;

  public ScheduledReporter(String name, Config config) {
    super(name, config);
    this.executor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(log), Optional.of("metrics-" + name + "-scheduler")));
    this.reportingInterval = config.hasPath(REPORTING_INTERVAL_PERIOD) ? config.getLong(REPORTING_INTERVAL_PERIOD) :
        DEFAULT_REPORTING_INTERVAL_PERIOD;
    this.reportingIntervalUnit = TimeUnit.valueOf(
        config.hasPath(REPORTING_INTERVAL_UNIT) ? config.getString(REPORTING_INTERVAL_UNIT).toUpperCase() :
        DEFAULT_REPORTING_INTERVAL_UNIT);
  }

  @Override public void startImpl() {
    this.scheduledTask = Optional.<ScheduledFuture>of(this.executor.scheduleAtFixedRate(new Runnable() {
      @Override public void run() {
        report();
      }
    }, 0, this.reportingInterval, this.reportingIntervalUnit));
  }

  @Override public void stopImpl() {
    this.scheduledTask.get().cancel(false);
    this.scheduledTask = Optional.absent();
  }

  @Override public void close() throws IOException {
    this.executor.shutdown();
    try {
      // Wait a while for existing tasks to terminate
      if (!this.executor.awaitTermination(10, TimeUnit.SECONDS)) {
        this.executor.shutdownNow(); // Cancel currently executing tasks
        // Wait a while for tasks to respond to being cancelled
        if (!this.executor.awaitTermination(10, TimeUnit.SECONDS)) {
          System.err.println(getClass().getSimpleName() + ": ScheduledExecutorService did not terminate");
        }
      }
    } catch (InterruptedException ie) {
      // (Re-)Cancel if current thread also interrupted
      this.executor.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
    super.close();
  }

  @Override protected void removedMetricContext(InnerMetricContext context) {
    if(shouldReportInnerMetricContext(context)) {
      report(context);
    }
    super.removedMetricContext(context);
  }

  /**
   * Trigger emission of a report.
   */
  public void report() {
    for(ReportableContext metricContext : getMetricContextsToReport()) {
      report(metricContext);
    }
  }

  /**
   * Report as {@link InnerMetricContext}.
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
