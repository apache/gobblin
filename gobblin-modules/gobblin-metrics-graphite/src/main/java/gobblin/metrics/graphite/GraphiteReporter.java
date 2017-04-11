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

package gobblin.metrics.graphite;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;

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
import com.typesafe.config.Config;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.Measurements;
import gobblin.metrics.reporter.ConfiguredScheduledReporter;
import gobblin.util.ConfigUtils;
import static gobblin.metrics.Measurements.*;

/**
 * Graphite reporter for metrics
 *
 * @author Lorand Bendig
 *
 */
public class GraphiteReporter extends ConfiguredScheduledReporter {

  private final GraphitePusher graphitePusher;

  private static final Logger LOGGER = LoggerFactory.getLogger(GraphiteReporter.class);

  public GraphiteReporter(Builder<?> builder, Config config) throws IOException {
    super(builder, config);
    if (builder.graphitePusher.isPresent()) {
      this.graphitePusher = builder.graphitePusher.get();
    } else {
      this.graphitePusher = this.closer.register(new GraphitePusher(builder.hostname, builder.port, builder.connectionType));
    }
  }

  /**
   * A static factory class for obtaining new {@link gobblin.metrics.graphite.GraphiteReporter.Builder}s
   *
   * @see gobblin.metrics.graphite.GraphiteReporter.Builder
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
   * Builder for {@link GraphiteReporter}. Defaults to no filter, reporting rates in seconds and times in
   * milliseconds using TCP sending type
   */
  public static abstract class Builder<T extends ConfiguredScheduledReporter.Builder<T>> extends
      ConfiguredScheduledReporter.Builder<T> {

    protected MetricFilter filter;
    protected String hostname;
    protected int port;
    protected GraphiteConnectionType connectionType;
    protected Optional<GraphitePusher> graphitePusher;

    protected Builder() {
      super();
      this.name = "GraphiteReporter";
      this.graphitePusher = Optional.absent();
      this.filter = MetricFilter.ALL;
      this.connectionType = GraphiteConnectionType.TCP;
    }

    /**
     * Set {@link gobblin.metrics.graphite.GraphitePusher} to use.
     */
    public T withGraphitePusher(GraphitePusher pusher) {
      this.graphitePusher = Optional.of(pusher);
      return self();
    }

    /**
     * Set connection parameters for the {@link gobblin.metrics.graphite.GraphitePusher} creation
     */
    public T withConnection(String hostname, int port) {
      this.hostname = hostname;
      this.port = port;
      return self();
    }

    /**
     * Set {@link gobblin.metrics.graphite.GraphiteConnectionType} to use.
     */
    public T withConnectionType(GraphiteConnectionType connectionType) {
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
     * Builds and returns {@link GraphiteReporter}.
     *
     * @param props metrics properties
     * @return GraphiteReporter
     */
    public GraphiteReporter build(Properties props) throws IOException {
      return new GraphiteReporter(this, ConfigUtils.propertiesToConfig(props,
          Optional.of(ConfigurationKeys.METRICS_CONFIGURATIONS_PREFIX)));
    }
  }

  @Override
  protected void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers,
      Map<String, Object> tags) {

    String prefix = getMetricNamePrefix(tags);
    long timestamp = System.currentTimeMillis() / 1000l;

    try {
      for (Map.Entry<String, Gauge> gauge : gauges.entrySet()) {
        reportGauge(prefix, gauge.getKey(), gauge.getValue(), timestamp);
      }

      for (Map.Entry<String, Counter> counter : counters.entrySet()) {
        reportCounter(prefix, counter.getKey(), counter.getValue(), timestamp);
      }

      for (Map.Entry<String, Histogram> histogram : histograms.entrySet()) {
        reportHistogram(prefix, histogram.getKey(), histogram.getValue(), timestamp);
      }

      for (Map.Entry<String, Meter> meter : meters.entrySet()) {
        reportMetered(prefix, meter.getKey(), meter.getValue(), timestamp);
      }

      for (Map.Entry<String, Timer> timer : timers.entrySet()) {
        reportTimer(prefix, timer.getKey(), timer.getValue(), timestamp);
      }

      this.graphitePusher.flush();

    } catch (IOException ioe) {
      LOGGER.error("Error sending metrics to Graphite", ioe);
      try {
        this.graphitePusher.close();
      } catch (IOException innerIoe) {
        LOGGER.error("Error closing the Graphite sender", innerIoe);
      }
    }
  }

  private void reportGauge(String prefix, String name, Gauge gauge, long timestamp) throws IOException {
    String metricName = getKey(prefix, name);
    pushMetric(metricName, gauge.getValue().toString(), timestamp);
  }

  private void reportCounter(String prefix, String name, Counting counter, long timestamp) throws IOException {
    String metricName = getKey(prefix, name, COUNT.getName());
    pushMetric(metricName, counter.getCount(), false, timestamp);
  }

  private void reportHistogram(String prefix, String name, Histogram histogram, long timestamp) throws IOException {
    reportCounter(prefix, name, histogram, timestamp);
    reportSnapshot(prefix, name, histogram.getSnapshot(), timestamp, false);
  }

  private void reportTimer(String prefix, String name, Timer timer, long timestamp) throws IOException {
    reportSnapshot(prefix, name, timer.getSnapshot(), timestamp, true);
    reportMetered(prefix, name, timer, timestamp);
  }

  private void reportSnapshot(String prefix, String name, Snapshot snapshot, long timestamp, boolean convertDuration)
      throws IOException {
    String baseMetricName = getKey(prefix, name);
    pushMetric(getKey(baseMetricName, MIN), snapshot.getMin(), convertDuration, timestamp);
    pushMetric(getKey(baseMetricName, MAX), snapshot.getMax(), convertDuration, timestamp);
    pushMetric(getKey(baseMetricName, MEAN), snapshot.getMean(), convertDuration, timestamp);
    pushMetric(getKey(baseMetricName, STDDEV), snapshot.getStdDev(), convertDuration, timestamp);
    pushMetric(getKey(baseMetricName, MEDIAN), snapshot.getMedian(), convertDuration, timestamp);
    pushMetric(getKey(baseMetricName, PERCENTILE_75TH), snapshot.get75thPercentile(), convertDuration, timestamp);
    pushMetric(getKey(baseMetricName, PERCENTILE_95TH), snapshot.get95thPercentile(), convertDuration, timestamp);
    pushMetric(getKey(baseMetricName, PERCENTILE_98TH), snapshot.get98thPercentile(), convertDuration, timestamp);
    pushMetric(getKey(baseMetricName, PERCENTILE_99TH), snapshot.get99thPercentile(), convertDuration, timestamp);
    pushMetric(getKey(baseMetricName, PERCENTILE_999TH), snapshot.get999thPercentile(), convertDuration, timestamp);
  }

  private void reportMetered(String prefix, String name, Metered metered, long timestamp) throws IOException {
    reportCounter(prefix, name, metered, timestamp);
    String baseMetricName = getKey(prefix, name);
    pushMetricRate(getKey(baseMetricName, RATE_1MIN), metered.getOneMinuteRate(), timestamp);
    pushMetricRate(getKey(baseMetricName, RATE_5MIN), metered.getFiveMinuteRate(), timestamp);
    pushMetricRate(getKey(baseMetricName, RATE_15MIN), metered.getFifteenMinuteRate(), timestamp);
    pushMetricRate(getKey(baseMetricName, MEAN_RATE), metered.getMeanRate(), timestamp);
  }

  private void pushMetric(String metricName, Number value, boolean toDuration, long timestamp) throws IOException {
    String metricValue = toDuration ? getValue(convertDuration(value.doubleValue())) : getValue(value);
    pushMetric(metricName, metricValue, timestamp);
  }

  private void pushMetricRate(String metricName, double value, long timestamp)
      throws IOException {
    pushMetric(metricName, getValue(convertRate(value)), timestamp);
  }

  private void pushMetric(String name, String value, long timestamp) throws IOException {
    this.graphitePusher.push(name, value, timestamp);
  }

  private String getValue(Number value) {
    return value.toString();
  }

  private String getKey(String baseName, Measurements measurements) {
    return getKey(baseName, measurements.getName());
  }

  private String getKey(String... keys) {
    return JOINER.join(keys);
  }

}
