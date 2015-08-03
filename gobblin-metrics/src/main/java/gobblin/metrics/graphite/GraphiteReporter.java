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

package gobblin.metrics.graphite;

import java.io.IOException;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.codahale.metrics.graphite.GraphiteSender;

import gobblin.metrics.reporter.ContextAwareScheduledReporter;
import gobblin.metrics.Measurements;
import gobblin.metrics.MetricContext;


/**
 * An implementation of {@link gobblin.metrics.reporter.ContextAwareScheduledReporter} that reports
 * metrics to Graphite.
 *
 * @see <a href="http://graphite.wikidot.com/">Graphite - Scalable Realtime Graphing</a>.
 *
 * <p>
 *   The name of the {@link MetricContext} a {@link GraphiteReporter} is associated to will
 *   be included as the prefix in the metric names, which may or may not include the tags
 *   of the {@link MetricContext} depending on if the {@link MetricContext} is configured to
 *   report fully-qualified metric names or not using the method
 *   {@link MetricContext.Builder#reportFullyQualifiedNames(boolean)}.
 * </p>
 *
 * @author ynli
 */
public class GraphiteReporter extends ContextAwareScheduledReporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(GraphiteReporter.class);

  private final GraphiteSender graphiteSender;

  protected GraphiteReporter(MetricContext context, String name, MetricFilter filter,
      TimeUnit rateUnit, TimeUnit durationUnit, GraphiteSender graphiteSender) {
    super(context, name, filter, rateUnit, durationUnit);
    this.graphiteSender = graphiteSender;
  }

  @Override
  protected void reportInContext(MetricContext context,
                                 SortedMap<String, Gauge> gauges,
                                 SortedMap<String, Counter> counters,
                                 SortedMap<String, Histogram> histograms,
                                 SortedMap<String, Meter> meters,
                                 SortedMap<String, Timer> timers) {
    try {
      if (!this.graphiteSender.isConnected()) {
        this.graphiteSender.connect();
      }

      // UNIX epoch time
      long timeStamp = System.currentTimeMillis() / 1000l;

      for (SortedMap.Entry<String, Gauge> entry : gauges.entrySet()) {
        reportGauge(context, entry.getKey(), entry.getValue(), timeStamp);
      }

      for (SortedMap.Entry<String, Counter> entry : counters.entrySet()) {
        reportCounter(context, entry.getKey(), entry.getValue(), timeStamp);
      }

      for (SortedMap.Entry<String, Histogram> entry : histograms.entrySet()) {
        reportHistogram(context, entry.getKey(), entry.getValue(), timeStamp);
      }

      for (SortedMap.Entry<String, Meter> entry : meters.entrySet()) {
        reportMetered(context, entry.getKey(), entry.getValue(), timeStamp);
      }

      for (SortedMap.Entry<String, Timer> entry : timers.entrySet()) {
        reportTimer(context, entry.getKey(), entry.getValue(), timeStamp);
      }

      this.graphiteSender.flush();
    } catch (IOException ioe) {
      LOGGER.error("Error sending metrics to Graphite", ioe);
      try {
        this.graphiteSender.close();
      } catch (IOException innerIoe) {
        LOGGER.error("Error closing the Graphite sender", innerIoe);
      }
    }
  }

  @Override
  public void stop() {
    try {
      super.stop();
    } finally {
      // Stop the Graphite sender and disconnect from Graphite
      try {
        this.graphiteSender.close();
      } catch (IOException ioe) {
        LOGGER.error("Error closing the Graphite sender", ioe);
      }
    }
  }

  /**
   * Create a new {@link gobblin.metrics.graphite.GraphiteReporter.Builder} that uses
   * the simple name of {@link GraphiteReporter} as the reporter name.
   *
   * @param graphiteSender a {@link com.codahale.metrics.graphite.GraphiteSender} to
   *                       send metrics to Graphite
   * @return a new {@link gobblin.metrics.graphite.GraphiteReporter.Builder}
   */
  public static Builder builder(GraphiteSender graphiteSender) {
    return builder(GraphiteReporter.class.getName(), graphiteSender);
  }

  /**
   * Create a new {@link gobblin.metrics.graphite.GraphiteReporter.Builder} that uses
   * a given reporter name.
   *
   * @param name the given reporter name
   * @param graphiteSender a {@link com.codahale.metrics.graphite.GraphiteSender}
   *                       to send metrics to Graphite
   * @return a new {@link gobblin.metrics.graphite.GraphiteReporter.Builder}
   */
  public static Builder builder(String name, GraphiteSender graphiteSender) {
    return new Builder(name, graphiteSender);
  }

  private void reportGauge(MetricContext context, String name, Gauge gauge, long timeStamp)
      throws IOException {
    this.graphiteSender.send(
        MetricRegistry.name(context.getName(), name),
        gauge.getValue().toString(),
        timeStamp);
  }

  private void reportCounter(MetricContext context, String name, Counter counter, long timeStamp)
      throws IOException {
    this.graphiteSender.send(
        MetricRegistry.name(context.getName(), name, Measurements.COUNT.getName()),
        Long.toString(counter.getCount()),
        timeStamp);
  }

  private void reportHistogram(MetricContext context, String name, Histogram histogram, long timeStamp)
      throws IOException {
    this.graphiteSender.send(
        MetricRegistry.name(context.getName(), name, Measurements.COUNT.getName()),
        Long.toString(histogram.getCount()),
        timeStamp);
    reportSnapshot(context, name, histogram.getSnapshot(), timeStamp, false);
  }

  private void reportTimer(MetricContext context, String name, Timer timer, long timeStamp) throws IOException {
    reportSnapshot(context, name, timer.getSnapshot(), timeStamp, true);
    reportMetered(context, name, timer, timeStamp);
  }

  private void reportSnapshot(MetricContext context, String name, Snapshot snapshot, long timeStamp,
      boolean convertDuration) throws IOException {
    this.graphiteSender.send(
        MetricRegistry.name(context.getName(), name, Measurements.MIN.getName()),
        convertDuration ?
            Double.toString(convertDuration(snapshot.getMin())) : Long.toString(snapshot.getMin()),
        timeStamp);
    this.graphiteSender.send(
        MetricRegistry.name(context.getName(), name, Measurements.MAX.getName()),
        convertDuration ?
            Double.toString(convertDuration(snapshot.getMax())) : Long.toString(snapshot.getMax()),
        timeStamp);
    this.graphiteSender.send(
        MetricRegistry.name(context.getName(), name, Measurements.MEAN.getName()),
        Double.toString(convertDuration ? convertDuration(snapshot.getMean()) : snapshot.getMean()),
        timeStamp);
    this.graphiteSender.send(
        MetricRegistry.name(context.getName(), name, Measurements.STDDEV.getName()),
        Double.toString(convertDuration ? convertDuration(snapshot.getStdDev()) : snapshot.getStdDev()),
        timeStamp);
    this.graphiteSender.send(
        MetricRegistry.name(context.getName(), name, Measurements.MEDIAN.getName()),
        Double.toString(convertDuration ? convertDuration(snapshot.getMedian()) : snapshot.getMedian()),
        timeStamp);
    this.graphiteSender.send(
        MetricRegistry.name(context.getName(), name, Measurements.PERCENTILE_75TH.getName()),
        Double.toString(convertDuration ? convertDuration(snapshot.get75thPercentile()) : snapshot.get75thPercentile()),
        timeStamp);
    this.graphiteSender.send(
        MetricRegistry.name(context.getName(), name, Measurements.PERCENTILE_95TH.getName()),
        Double.toString(convertDuration ? convertDuration(snapshot.get95thPercentile()) : snapshot.get95thPercentile()),
        timeStamp);
    this.graphiteSender.send(
        MetricRegistry.name(context.getName(), name, Measurements.PERCENTILE_98TH.getName()),
        Double.toString(convertDuration ? convertDuration(snapshot.get98thPercentile()) : snapshot.get98thPercentile()),
        timeStamp);
    this.graphiteSender.send(
        MetricRegistry.name(context.getName(), name, Measurements.PERCENTILE_99TH.getName()),
        Double.toString(convertDuration ? convertDuration(snapshot.get99thPercentile()) : snapshot.get99thPercentile()),
        timeStamp);
    this.graphiteSender.send(
        MetricRegistry.name(context.getName(), name, Measurements.PERCENTILE_999TH.getName()),
        Double.toString(convertDuration ?
            convertDuration(snapshot.get999thPercentile()) : snapshot.get999thPercentile()),
        timeStamp);
  }

  private void reportMetered(MetricContext context, String name, Metered metered, long timeStamp)
      throws IOException {
    this.graphiteSender.send(
        MetricRegistry.name(context.getName(), name, Measurements.COUNT.getName()),
        Long.toString(metered.getCount()),
        timeStamp);
    this.graphiteSender.send(
        MetricRegistry.name(context.getName(), name, Measurements.RATE_1MIN.getName()),
        Double.toHexString(convertRate(metered.getOneMinuteRate())),
        timeStamp);
    this.graphiteSender.send(
        MetricRegistry.name(context.getName(), name, Measurements.RATE_5MIN.getName()),
        Double.toString(convertRate(metered.getFiveMinuteRate())),
        timeStamp);
    this.graphiteSender.send(
        MetricRegistry.name(context.getName(), name, Measurements.RATE_15MIN.getName()),
        Double.toString(convertRate(metered.getFifteenMinuteRate())),
        timeStamp);
    this.graphiteSender.send(
        MetricRegistry.name(context.getName(), name, Measurements.MEAN_RATE.getName()),
        Double.toString(convertRate(metered.getMeanRate())),
        timeStamp);
  }

  /**
   * A builder class for {@link GraphiteReporter}.
   */
  public static class Builder extends ContextAwareScheduledReporter.Builder<GraphiteReporter, Builder> {

    private final GraphiteSender graphiteSender;

    public Builder(String name, GraphiteSender graphiteSender) {
      super(name);
      this.graphiteSender = graphiteSender;
    }

    @Override
    public GraphiteReporter build(MetricContext context) {
      return new GraphiteReporter(
          context, this.name, this.filter, this.rateUnit, this.durationUnit, this.graphiteSender);
    }
  }
}
