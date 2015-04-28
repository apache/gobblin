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

package gobblin.metrics.influxdb;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Serie;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

import gobblin.metrics.ContextAwareScheduledReporter;
import gobblin.metrics.Measurements;
import gobblin.metrics.MetricContext;


/**
 * An implementation of {@link gobblin.metrics.ContextAwareScheduledReporter} that reports
 * metrics to InfluxDB.
 *
 * @see <a href="http://influxdb.com/">InfluxDB</a>.
 *
 * <p>
 *   The name of the {@link MetricContext} a {@link InfluxDBReporter} is associated to will
 *   be included as the prefix in the metric names, which may or may not include the tags
 *   of the {@link MetricContext} depending on if the {@link MetricContext} is configured to
 *   report fully-qualified metric names or not using the method
 *   {@link MetricContext.Builder#reportFullyQualifiedNames(boolean)}.
 * </p>
 *
 * @author ynli
 */
public class InfluxDBReporter extends ContextAwareScheduledReporter {

  protected static final String VALUE = "value";
  protected static final String TIMESTAMP = "timestamp";

  private final InfluxDB influxDB;
  private final String database;
  private final TimeUnit timeUnit;

  protected InfluxDBReporter(MetricContext context, String name, MetricFilter filter, TimeUnit rateUnit,
      TimeUnit durationUnit, InfluxDB influxDB, String database, TimeUnit timeUnit) {
    super(context, name, filter, rateUnit, durationUnit);
    this.influxDB = influxDB;
    this.database = database;
    this.timeUnit = timeUnit;
  }

  protected InfluxDBReporter(MetricContext context, String name, MetricFilter filter, TimeUnit rateUnit,
      TimeUnit durationUnit, String url, String user, String password, String database, TimeUnit timeUnit) {
    this(context, name, filter, rateUnit, durationUnit, InfluxDBFactory.connect(url, user, password),
        database, timeUnit);
  }

  @Override
  protected void reportInContext(MetricContext context,
                                 SortedMap<String, Gauge> gauges,
                                 SortedMap<String, Counter> counters,
                                 SortedMap<String, Histogram> histograms,
                                 SortedMap<String, Meter> meters,
                                 SortedMap<String, Timer> timers) {

    long timeStamp = System.currentTimeMillis();

    for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
      reportGauge(context, entry.getKey(), entry.getValue(), timeStamp);
    }

    for (Map.Entry<String, Counter> entry : counters.entrySet()) {
      reportCounter(context, entry.getKey(), entry.getValue(), timeStamp);
    }

    for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
      reportHistogram(context, entry.getKey(), entry.getValue(), timeStamp);
    }

    for (Map.Entry<String, Meter> entry : meters.entrySet()) {
      reportMetered(context, entry.getKey(), entry.getValue(), timeStamp);
    }

    for (Map.Entry<String, Timer> entry : timers.entrySet()) {
      reportTimer(context, entry.getKey(), entry.getValue(), timeStamp);
    }
  }

  /**
   * Create a new {@link gobblin.metrics.influxdb.InfluxDBReporter.Builder} that uses
   * the simple name of {@link InfluxDBReporter} as the reporter name.
   *
   * @return a new {@link gobblin.metrics.influxdb.InfluxDBReporter.Builder}
   */
  public static Builder builder() {
    return new Builder(InfluxDBReporter.class.getSimpleName());
  }

  /**
   * Create a new {@link gobblin.metrics.influxdb.InfluxDBReporter.Builder} that uses
   * a given reporter name.
   *
   * @param name the given reporter name
   * @return a new {@link gobblin.metrics.influxdb.InfluxDBReporter.Builder}
   */
  public static Builder builder(String name) {
    return new Builder(name);
  }

  private void reportGauge(MetricContext context, String name, Gauge gauge, long timeStamp) {
    this.influxDB.write(
        this.database,
        this.timeUnit,
        getSerieBuilder(MetricRegistry.name(context.getName(), name)).values(timeStamp, gauge.getValue())
            .build());
  }

  private void reportCounter(MetricContext context, String name, Counter counter, long timeStamp) {
    this.influxDB.write(
        this.database,
        this.timeUnit,
        getSerieBuilder(MetricRegistry.name(context.getName(), name, Measurements.COUNT.getName()))
            .values(timeStamp, counter.getCount()).build());
  }

  private void reportHistogram(MetricContext context, String name, Histogram histogram, long timeStamp) {
    this.influxDB.write(
        this.database,
        this.timeUnit,
        getSerieBuilder(MetricRegistry.name(context.getName(), name, Measurements.COUNT.getName()))
            .values(timeStamp, histogram.getCount()).build());
    reportSnapshot(context, name, histogram.getSnapshot(), timeStamp, false);
  }

  private void reportTimer(MetricContext context, String name, Timer timer, long timeStamp) {
    reportMetered(context, name, timer, timeStamp);
    reportSnapshot(context, name, timer.getSnapshot(), timeStamp, true);
  }

  private void reportMetered(MetricContext context, String name, Metered metered, long timeStamp) {
    this.influxDB.write(
        this.database,
        this.timeUnit,
        getSerieBuilder(MetricRegistry.name(context.getName(), name, Measurements.COUNT.getName()))
            .values(timeStamp, metered.getCount()).build());
    this.influxDB.write(
        this.database,
        this.timeUnit,
        getSerieBuilder(MetricRegistry.name(context.getName(), name, Measurements.MEAN_RATE.getName()))
            .values(timeStamp, convertRate(metered.getMeanRate())).build());
    this.influxDB.write(
        this.database,
        this.timeUnit,
        getSerieBuilder(MetricRegistry.name(context.getName(), name, Measurements.RATE_1MIN.getName()))
            .values(timeStamp, convertRate(metered.getOneMinuteRate())).build());
    this.influxDB.write(
        this.database,
        this.timeUnit,
        getSerieBuilder(MetricRegistry.name(context.getName(), name, Measurements.RATE_5MIN.getName()))
            .values(timeStamp, convertRate(metered.getFiveMinuteRate())).build());
    this.influxDB.write(
        this.database,
        this.timeUnit,
        getSerieBuilder(MetricRegistry.name(context.getName(), name, Measurements.RATE_15MIN.getName()))
            .values(timeStamp, convertRate(metered.getFifteenMinuteRate())).build());
  }

  private void reportSnapshot(MetricContext context, String name, Snapshot snapshot, long timeStamp,
      boolean convertDuration) {
    this.influxDB.write(
        this.database,
        this.timeUnit,
        getSerieBuilder(MetricRegistry.name(context.getName(), name, Measurements.MIN.getName()))
            .values(timeStamp, convertDuration ? convertDuration(snapshot.getMin()) : snapshot.getMin())
            .build());
    this.influxDB.write(
        this.database,
        this.timeUnit,
        getSerieBuilder(MetricRegistry.name(context.getName(), name, Measurements.MAX.getName()))
            .values(timeStamp, convertDuration ? convertDuration(snapshot.getMax()) : snapshot.getMax())
            .build());
    this.influxDB.write(
        this.database,
        this.timeUnit,
        getSerieBuilder(MetricRegistry.name(context.getName(), name, Measurements.MEAN.getName()))
            .values(timeStamp, convertDuration ? convertDuration(snapshot.getMean()) : snapshot.getMean())
            .build());
    this.influxDB.write(
        this.database,
        this.timeUnit,
        getSerieBuilder(MetricRegistry.name(context.getName(), name, Measurements.STDDEV.getName()))
            .values(timeStamp, convertDuration ? convertDuration(snapshot.getStdDev()) : snapshot.getStdDev())
            .build());
    this.influxDB.write(
        this.database,
        this.timeUnit,
        getSerieBuilder(MetricRegistry.name(context.getName(), name, Measurements.MEDIAN.getName()))
            .values(timeStamp, convertDuration ? convertDuration(snapshot.getMedian()) : snapshot.getMedian())
            .build());
    this.influxDB.write(this.database, this.timeUnit,
        getSerieBuilder(MetricRegistry.name(context.getName(), name, Measurements.PERCENTILE_75TH.getName()))
            .values(timeStamp,
                convertDuration ? convertDuration(snapshot.get75thPercentile()) : snapshot.get75thPercentile())
            .build());
    this.influxDB.write(
        this.database,
        this.timeUnit,
        getSerieBuilder(MetricRegistry.name(context.getName(), name, Measurements.PERCENTILE_95TH.getName()))
            .values(timeStamp,
                convertDuration ? convertDuration(snapshot.get95thPercentile()) : snapshot.get95thPercentile())
            .build());
    this.influxDB.write(
        this.database,
        this.timeUnit,
        getSerieBuilder(MetricRegistry.name(context.getName(), name, Measurements.PERCENTILE_98TH.getName()))
            .values(timeStamp,
                convertDuration ? convertDuration(snapshot.get98thPercentile()) : snapshot.get98thPercentile())
            .build());
    this.influxDB.write(
        this.database,
        this.timeUnit,
        getSerieBuilder(MetricRegistry.name(context.getName(), name, Measurements.PERCENTILE_99TH.getName()))
            .values(timeStamp,
                convertDuration ? convertDuration(snapshot.get99thPercentile()) : snapshot.get99thPercentile())
            .build());
    this.influxDB.write(
        this.database,
        this.timeUnit,
        getSerieBuilder(MetricRegistry.name(context.getName(), name, Measurements.PERCENTILE_999TH.getName()))
            .values(timeStamp,
                convertDuration ? convertDuration(snapshot.get999thPercentile()) : snapshot.get999thPercentile())
            .build());
  }

  private Serie.Builder getSerieBuilder(String serieName) {
    return new Serie.Builder(serieName).columns(TIMESTAMP, VALUE);
  }

  /**
   * A builder class for {@link InfluxDBReporter}.
   */
  public static class Builder extends ContextAwareScheduledReporter.Builder<InfluxDBReporter, Builder> {

    private String url = "http://localhost:8086";
    private String user = "root";
    private String password = "root";
    private String database = "gobblin_metrics";
    private TimeUnit timeUnit = TimeUnit.MILLISECONDS;

    public Builder(String name) {
      super(name);
    }

    /**
     * Configure the InfluxDB URL to write metric data to.
     *
     * @param url the InfluxDB URL to write metric data to
     * @return {@code this}
     */
    public Builder useUrl(String url) {
      this.url = url;
      return this;
    }

    /**
     * Configure the InfluxDB user name used to write metric data.
     *
     * @param user the InfluxDB user name used to write metric data
     * @return {@code this}
     */
    public Builder useUsername(String user) {
      this.user = user;
      return this;
    }

    /**
     * Configure the InfluxDB password used to write metric data.
     *
     * @param password the InfluxDB password used to write metric data
     * @return {@code this}
     */
    public Builder userPassword(String password) {
      this.password = password;
      return this;
    }

    /**
     * Configure the InfluxDB database to write metric data into.
     *
     * @param database the InfluxDB database to write metric data into
     * @return {@code this}
     */
    public Builder writeTo(String database) {
      this.database = database;
      return this;
    }

    /**
     * Configure the {@link java.util.concurrent.TimeUnit} used for timestamps.
     *
     * @param timeUnit the {@link java.util.concurrent.TimeUnit} used for timestamps
     * @return {@code this}
     */
    public Builder useTimeUnit(TimeUnit timeUnit) {
      this.timeUnit = timeUnit;
      return this;
    }

    @Override
    public InfluxDBReporter build(MetricContext context) {
      return new InfluxDBReporter(context, this.name, this.filter, this.rateUnit, this.durationUnit,
          this.url, this.user, this.password, this.database, this.timeUnit);
    }
  }
}
