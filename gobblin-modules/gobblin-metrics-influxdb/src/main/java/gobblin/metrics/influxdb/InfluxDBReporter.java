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

package gobblin.metrics.influxdb;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Counting;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.Measurements;
import gobblin.metrics.reporter.ConfiguredScheduledReporter;
import gobblin.util.ConfigUtils;
import static gobblin.metrics.Measurements.*;


/**
 * InfluxDB reporter for metrics
 *
 * @author Lorand Bendig
 *
 */
public class InfluxDBReporter extends ConfiguredScheduledReporter {

  private final InfluxDBPusher influxDBPusher;

  private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBReporter.class);

  public InfluxDBReporter(Builder<?> builder, Config config) {
    super(builder, config);
    if (builder.influxDBPusher.isPresent()) {
      this.influxDBPusher = builder.influxDBPusher.get();
    } else {
      this.influxDBPusher =
          new InfluxDBPusher.Builder(builder.url, builder.username, builder.password, builder.database,
              builder.connectionType).build();
    }
  }

  /**
   * A static factory class for obtaining new {@link gobblin.metrics.influxdb.InfluxDBReporter.Builder}s
   *
   * @see gobblin.metrics.influxdb.InfluxDBReporter.Builder
   */
  public static class Factory {

    public static BuilderImpl newBuilder() {
      return new BuilderImpl();
    }
  }

  public static class BuilderImpl extends Builder<BuilderImpl> {

    @Override
    protected BuilderImpl self() {
      return this;
    }
  }

  /**
   * Builder for {@link InfluxDBReporter}. Defaults to no filter, reporting rates in seconds and times in
   * milliseconds using TCP sending type
   */
  public static abstract class Builder<T extends ConfiguredScheduledReporter.Builder<T>> extends
      ConfiguredScheduledReporter.Builder<T> {

    protected MetricFilter filter;
    protected String url;
    protected String username;
    protected String password;
    protected String database;
    protected InfluxDBConnectionType connectionType;
    protected Optional<InfluxDBPusher> influxDBPusher;

    protected Builder() {
      super();
      this.name = "InfluxDBReporter";
      this.influxDBPusher = Optional.absent();
      this.filter = MetricFilter.ALL;
      this.connectionType = InfluxDBConnectionType.TCP;
    }

    /**
     * Set {@link gobblin.metrics.influxdb.InfluxDBPusher} to use.
     */
    public T withInfluxDBPusher(InfluxDBPusher pusher) {
      this.influxDBPusher = Optional.of(pusher);
      return self();
    }

    /**
     * Set connection parameters for the {@link gobblin.metrics.influxdb.InfluxDBPusher} creation
     */
    public T withConnection(String url, String username, String password, String database) {
      this.url = url;
      this.username = username;
      this.password = password;
      this.database = database;
      return self();
    }

    /**
     * Set {@link gobblin.metrics.influxdb.InfluxDBConnectionType} to use.
     */
    public T withConnectionType(InfluxDBConnectionType connectionType) {
      this.connectionType = connectionType;
      return self();
    }

    /**
     * Only report metrics which match the given filter.
     *
     * @param filter a {@link MetricFilter}
     * @return {@code this}
     */
    public T filter(MetricFilter filter) {
      this.filter = filter;
      return self();
    }

    /**
     * Builds and returns {@link InfluxDBReporter}.
     *
     * @return InfluxDBReporter
     */
    public InfluxDBReporter build(Properties props) throws IOException {
      return new InfluxDBReporter(this, ConfigUtils.propertiesToConfig(props,
          Optional.of(ConfigurationKeys.METRICS_CONFIGURATIONS_PREFIX)));
    }
  }

  @Override
  protected void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers,
      Map<String, Object> tags) {

    String prefix = getMetricNamePrefix(tags);
    long timestamp = System.currentTimeMillis();
    List<Point> points = Lists.newArrayList();

    try {
      for (Map.Entry<String, Gauge> gauge : gauges.entrySet()) {
        reportGauge(points, prefix, gauge.getKey(), gauge.getValue(), timestamp);
      }

      for (Map.Entry<String, Counter> counter : counters.entrySet()) {
        reportCounter(points, prefix, counter.getKey(), counter.getValue(), timestamp);
      }

      for (Map.Entry<String, Histogram> histogram : histograms.entrySet()) {
        reportHistogram(points, prefix, histogram.getKey(), histogram.getValue(), timestamp);
      }

      for (Map.Entry<String, Meter> meter : meters.entrySet()) {
        reportMetered(points, prefix, meter.getKey(), meter.getValue(), timestamp);
      }

      for (Map.Entry<String, Timer> timer : timers.entrySet()) {
        reportTimer(points, prefix, timer.getKey(), timer.getValue(), timestamp);
      }

      influxDBPusher.push(points);

    } catch (IOException ioe) {
      LOGGER.error("Error sending metrics to InfluxDB", ioe);
    }
  }

  private void reportGauge(List<Point> points, String prefix, String name, Gauge gauge, long timestamp)
      throws IOException {
    String metricName = getKey(prefix, name);
    points.add(buildMetricAsPoint(metricName, gauge.getValue(), timestamp));
  }

  private void reportCounter(List<Point> points, String prefix, String name, Counting counter, long timestamp)
      throws IOException {
    String metricName = getKey(prefix, name, COUNT.getName());
    points.add(buildMetricAsPoint(metricName, counter.getCount(), false, timestamp));
  }

  private void reportHistogram(List<Point> points, String prefix, String name, Histogram histogram, long timestamp)
      throws IOException {
    reportCounter(points, prefix, name, histogram, timestamp);
    reportSnapshot(points, prefix, name, histogram.getSnapshot(), timestamp, false);
  }

  private void reportTimer(List<Point> points, String prefix, String name, Timer timer, long timestamp)
      throws IOException {
    reportSnapshot(points, prefix, name, timer.getSnapshot(), timestamp, true);
    reportMetered(points, prefix, name, timer, timestamp);
  }

  private void reportSnapshot(List<Point> points, String prefix, String name, Snapshot snapshot, long timestamp,
      boolean convertDuration) throws IOException {
    String baseMetricName = getKey(prefix, name);
    points.add(buildMetricAsPoint(getKey(baseMetricName, MIN), snapshot.getMin(), convertDuration, timestamp));
    points.add(buildMetricAsPoint(getKey(baseMetricName, MAX), snapshot.getMax(), convertDuration, timestamp));
    points.add(buildMetricAsPoint(getKey(baseMetricName, MEAN), snapshot.getMean(), convertDuration, timestamp));
    points.add(buildMetricAsPoint(getKey(baseMetricName, STDDEV), snapshot.getStdDev(), convertDuration, timestamp));
    points.add(buildMetricAsPoint(getKey(baseMetricName, MEDIAN), snapshot.getMedian(), convertDuration, timestamp));
    points.add(buildMetricAsPoint(getKey(baseMetricName, PERCENTILE_75TH), snapshot.get75thPercentile(), convertDuration, timestamp));
    points.add(buildMetricAsPoint(getKey(baseMetricName, PERCENTILE_95TH), snapshot.get95thPercentile(), convertDuration, timestamp));
    points.add(buildMetricAsPoint(getKey(baseMetricName, PERCENTILE_98TH), snapshot.get98thPercentile(), convertDuration, timestamp));
    points.add(buildMetricAsPoint(getKey(baseMetricName, PERCENTILE_99TH), snapshot.get99thPercentile(), convertDuration, timestamp));
    points.add(buildMetricAsPoint(getKey(baseMetricName, PERCENTILE_999TH), snapshot.get999thPercentile(), convertDuration, timestamp));
  }

  private void reportMetered(List<Point> points, String prefix, String name, Metered metered, long timestamp)
      throws IOException {
    reportCounter(points,prefix, name, metered, timestamp);
    String baseMetricName = getKey(prefix, name);
    points.add(buildRateAsPoint(getKey(baseMetricName, RATE_1MIN), metered.getOneMinuteRate(), timestamp));
    points.add(buildRateAsPoint(getKey(baseMetricName, RATE_5MIN), metered.getFiveMinuteRate(), timestamp));
    points.add(buildRateAsPoint(getKey(baseMetricName, RATE_15MIN), metered.getFifteenMinuteRate(), timestamp));
    points.add(buildRateAsPoint(getKey(baseMetricName, MEAN_RATE), metered.getMeanRate(), timestamp));
  }

  private Point buildMetricAsPoint(String metricName, Number value, boolean toDuration, long timestamp)
      throws IOException {
    Number metricValue = toDuration ? convertDuration(value.doubleValue()) : value;
    return buildMetricAsPoint(metricName, metricValue, timestamp);
  }

  private Point buildRateAsPoint(String metricName, double value, long timestamp)
      throws IOException {
    return buildMetricAsPoint(metricName, convertRate(value), timestamp);
  }

  private Point buildMetricAsPoint(String name, Object value, long timestamp) throws IOException {
    return Point.measurement(name).field("value", value).time(timestamp, TimeUnit.MILLISECONDS).build();
  }

  private String getKey(String baseName, Measurements measurements) {
    return getKey(baseName, measurements.getName());
  }

  private String getKey(String... keys) {
    return JOINER.join(keys);
  }

}
