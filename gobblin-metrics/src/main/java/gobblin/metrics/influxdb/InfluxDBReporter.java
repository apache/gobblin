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
 * @author ynli
 */
public class InfluxDBReporter extends ContextAwareScheduledReporter {

  protected static final String NAME = "name";
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
    Serie.Builder builder = new Serie.Builder(context.getName()).columns(TIMESTAMP, NAME, VALUE);

    for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
      builder = addGauge(entry.getKey(), entry.getValue(), timeStamp, builder);
    }

    for (Map.Entry<String, Counter> entry : counters.entrySet()) {
      builder = addCounter(entry.getKey(), entry.getValue(), timeStamp, builder);
    }

    for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
      builder = addHistogram(entry.getKey(), entry.getValue(), timeStamp, builder);
    }

    for (Map.Entry<String, Meter> entry : meters.entrySet()) {
      builder = addMetered(entry.getKey(), entry.getValue(), timeStamp, builder);
    }

    for (Map.Entry<String, Timer> entry : timers.entrySet()) {
      builder = addTimer(entry.getKey(), entry.getValue(), timeStamp, builder);
    }

    this.influxDB.write(this.database, this.timeUnit, builder.build());
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

  private Serie.Builder addGauge(String name, Gauge gauge, long timeStamp, Serie.Builder builder) {
    return builder.values(timeStamp, name, gauge.getValue());
  }

  private Serie.Builder addCounter(String name, Counter counter, long timeStamp, Serie.Builder builder) {
    return builder.values(timeStamp, MetricRegistry.name(name, Measurements.COUNT.getName()), counter.getCount());
  }

  private Serie.Builder addHistogram(String name, Histogram histogram, long timeStamp, Serie.Builder builder) {
    builder = builder.values(timeStamp, MetricRegistry.name(name, Measurements.COUNT.getName()), histogram.getCount());
    return addSnapshot(name, histogram.getSnapshot(), timeStamp, builder);
  }

  private Serie.Builder addTimer(String name, Timer timer, long timeStamp, Serie.Builder builder) {
    builder = addMetered(name, timer, timeStamp, builder);
    return addSnapshot(name, timer.getSnapshot(), timeStamp, builder);
  }

  private Serie.Builder addMetered(String name, Metered metered, long timeStamp, Serie.Builder builder) {
    return builder
        .values(timeStamp, MetricRegistry.name(name, Measurements.COUNT.getName()), metered.getCount())
        .values(timeStamp, MetricRegistry.name(name, Measurements.RATE_1MIN.getName()), metered.getOneMinuteRate())
        .values(timeStamp, MetricRegistry.name(name, Measurements.RATE_5MIN.getName()), metered.getFiveMinuteRate())
        .values(timeStamp, MetricRegistry.name(name, Measurements.RATE_15MIN.getName()), metered.getFifteenMinuteRate())
        .values(timeStamp, MetricRegistry.name(name, Measurements.MEAN_RATE.getName()), metered.getMeanRate());
  }

  private Serie.Builder addSnapshot(String name, Snapshot snapshot, long timeStamp, Serie.Builder builder) {
    return builder
        .values(timeStamp, MetricRegistry.name(name, Measurements.MIN.getName()), snapshot.getMin())
        .values(timeStamp, MetricRegistry.name(name, Measurements.MAX.getName()), snapshot.getMax())
        .values(timeStamp, MetricRegistry.name(name, Measurements.MEAN.getName()), snapshot.getMean())
        .values(timeStamp, MetricRegistry.name(name, Measurements.STDDEV.getName()), snapshot.getStdDev())
        .values(timeStamp, MetricRegistry.name(name, Measurements.MEDIAN.getName()), snapshot.getMedian())
        .values(timeStamp, MetricRegistry.name(name, Measurements.PERCENTILE_75TH.getName()),
            snapshot.get75thPercentile())
        .values(timeStamp, MetricRegistry.name(name, Measurements.PERCENTILE_95TH.getName()),
            snapshot.get95thPercentile())
        .values(timeStamp, MetricRegistry.name(name, Measurements.PERCENTILE_98TH.getName()),
            snapshot.get98thPercentile())
        .values(timeStamp, MetricRegistry.name(name, Measurements.PERCENTILE_99TH.getName()),
            snapshot.get99thPercentile())
        .values(timeStamp, MetricRegistry.name(name, Measurements.PERCENTILE_999TH.getName()),
            snapshot.get999thPercentile());
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
