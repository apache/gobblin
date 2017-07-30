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

package gobblin.metrics.reporter;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Counting;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;

import gobblin.metrics.Measurements;
import gobblin.metrics.Metric;
import gobblin.metrics.MetricReport;


/**
 * Scheduled reporter based on {@link gobblin.metrics.MetricReport}.
 *
 * <p>
 *   This class will generate a metric report, and call {@link #emitReport} to actually emit the metrics.
 * </p>
 */
public abstract class MetricReportReporter extends ConfiguredScheduledReporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricReportReporter.class);

  public MetricReportReporter(Builder<?> builder, Config config) {
    super(builder, config);
  }

  /**
   * Builder for {@link MetricReportReporter}. Defaults to no filter, reporting rates in seconds and times in
   * milliseconds.
   */
  public static abstract class Builder<T extends ConfiguredScheduledReporter.Builder<T>> extends
      ConfiguredScheduledReporter.Builder<T> {

    protected MetricFilter filter;

    protected Builder() {
      super();
      this.name = "MetricReportReporter";
      this.filter = MetricFilter.ALL;
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
  }

  /**
   * Serializes metrics and pushes the byte arrays to Kafka. Uses the serialize* methods in {@link MetricReportReporter}.
   *
   * @param gauges map of {@link com.codahale.metrics.Gauge} to report and their name.
   * @param counters map of {@link com.codahale.metrics.Counter} to report and their name.
   * @param histograms map of {@link com.codahale.metrics.Histogram} to report and their name.
   * @param meters map of {@link com.codahale.metrics.Meter} to report and their name.
   * @param timers map of {@link com.codahale.metrics.Timer} to report and their name.
   */
  @Override
  protected void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers,
      Map<String, Object> tags) {

    List<Metric> metrics = Lists.newArrayList();

    for (Map.Entry<String, Gauge> gauge : gauges.entrySet()) {
      metrics.addAll(serializeGauge(gauge.getKey(), gauge.getValue()));
    }

    for (Map.Entry<String, Counter> counter : counters.entrySet()) {
      metrics.addAll(serializeCounter(counter.getKey(), counter.getValue()));
    }

    for (Map.Entry<String, Histogram> histogram : histograms.entrySet()) {
      metrics.addAll(serializeSnapshot(histogram.getKey(), histogram.getValue().getSnapshot()));
      metrics.addAll(serializeCounter(histogram.getKey(), histogram.getValue()));
    }

    for (Map.Entry<String, Meter> meter : meters.entrySet()) {
      metrics.addAll(serializeMetered(meter.getKey(), meter.getValue()));
    }

    for (Map.Entry<String, Timer> timer : timers.entrySet()) {
      metrics.addAll(serializeSnapshot(timer.getKey(), timer.getValue().getSnapshot()));
      metrics.addAll(serializeMetered(timer.getKey(), timer.getValue()));
      metrics.addAll(serializeSingleValue(timer.getKey(), timer.getValue().getCount(), Measurements.COUNT.getName()));
    }

    Map<String, Object> allTags = Maps.newHashMap();
    allTags.putAll(tags);
    allTags.putAll(this.tags);

    Map<String, String> allTagsString = Maps.transformValues(allTags, new Function<Object, String>() {
      @Nullable
      @Override
      public String apply(Object input) {
        return input.toString();
      }
    });

    MetricReport report = new MetricReport(allTagsString, System.currentTimeMillis(), metrics);

    emitReport(report);
  }

  /**
   * Emit the {@link gobblin.metrics.MetricReport} to the metrics sink.
   *
   * @param report metric report to emit.
   */
  protected abstract void emitReport(MetricReport report);

  /**
   * Extracts metrics from {@link com.codahale.metrics.Gauge}.
   *
   * @param name name of the {@link com.codahale.metrics.Gauge}.
   * @param gauge instance of {@link com.codahale.metrics.Gauge} to serialize.
   * @return a list of {@link gobblin.metrics.Metric}.
   */
  protected List<Metric> serializeGauge(String name, Gauge gauge) {
    List<Metric> metrics = Lists.newArrayList();
    try {
      metrics.add(new Metric(name, Double.parseDouble(gauge.getValue().toString())));
    } catch(NumberFormatException exception) {
      LOGGER.info("Failed to serialize gauge metric. Not compatible with double value.", exception);
    }
    return metrics;
  }

  /**
   * Extracts metrics from {@link com.codahale.metrics.Counter}.
   *
   * @param name name of the {@link com.codahale.metrics.Counter}.
   * @param counter instance of {@link com.codahale.metrics.Counter} to serialize.
   * @return a list of {@link gobblin.metrics.Metric}.
   */
  protected List<Metric> serializeCounter(String name, Counting counter) {
    return Lists.newArrayList(
        serializeValue(name, counter.getCount(), Measurements.COUNT.name())
    );
  }

  /**
   * Extracts metrics from {@link com.codahale.metrics.Metered}.
   *
   * @param name name of the {@link com.codahale.metrics.Metered}.
   * @param meter instance of {@link com.codahale.metrics.Metered} to serialize.
   * @return a list of {@link gobblin.metrics.Metric}.
   */
  protected List<Metric> serializeMetered(String name, Metered meter) {
    return Lists.newArrayList(
      serializeValue(name, meter.getCount(), Measurements.COUNT.name()),
      serializeValue(name, meter.getMeanRate(), Measurements.MEAN_RATE.name()),
      serializeValue(name, meter.getOneMinuteRate(), Measurements.RATE_1MIN.name()),
      serializeValue(name, meter.getFiveMinuteRate(), Measurements.RATE_5MIN.name()),
      serializeValue(name, meter.getFifteenMinuteRate(), Measurements.RATE_15MIN.name())
    );
  }

  /**
   * Extracts metrics from {@link com.codahale.metrics.Snapshot}.
   *
   * @param name name of the {@link com.codahale.metrics.Snapshot}.
   * @param snapshot instance of {@link com.codahale.metrics.Snapshot} to serialize.
   * @return a list of {@link gobblin.metrics.Metric}.
   */
  protected List<Metric> serializeSnapshot(String name, Snapshot snapshot) {
    return Lists.newArrayList(
      serializeValue(name, snapshot.getMean(), Measurements.MEAN.name()),
      serializeValue(name, snapshot.getMin(), Measurements.MIN.name()),
      serializeValue(name, snapshot.getMax(), Measurements.MAX.name()),
      serializeValue(name, snapshot.getMedian(), Measurements.MEDIAN.name()),
      serializeValue(name, snapshot.get75thPercentile(), Measurements.PERCENTILE_75TH.name()),
      serializeValue(name, snapshot.get95thPercentile(), Measurements.PERCENTILE_95TH.name()),
      serializeValue(name, snapshot.get99thPercentile(), Measurements.PERCENTILE_99TH.name()),
      serializeValue(name, snapshot.get999thPercentile(), Measurements.PERCENTILE_999TH.name())
    );
  }

  /**
   * Convert single value into list of {@link gobblin.metrics.Metric}.
   *
   * @param name name of the metric.
   * @param value value of the metric.
   * @param path suffixes to more precisely identify the meaning of the reported value
   * @return a Singleton list of {@link gobblin.metrics.Metric}.
   */
  protected List<Metric> serializeSingleValue(String name, Number value, String... path) {
    return Lists.newArrayList(
      serializeValue(name, value, path)
    );
  }

  /**
   * Converts a single key-value pair into a metric.
   *
   * @param name name of the metric
   * @param value value of the metric to report
   * @param path additional suffixes to further identify the meaning of the reported value
   * @return a {@link gobblin.metrics.Metric}.
   */
  protected Metric serializeValue(String name, Number value, String... path) {
    return new Metric(MetricRegistry.name(name, path), value.doubleValue());
  }
}