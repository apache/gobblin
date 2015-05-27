/*
 * (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metrics;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
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
import com.google.common.collect.Maps;
import com.google.common.io.Closer;


/**
 * Scheduled reporter based on {@link gobblin.metrics.MetricReport}.
 *
 * This class will generate a metric report, and call pushReport to actually emit the metrics.
 */
public abstract class MetricReportReporter extends RecursiveScheduledReporter {

  private final static Logger LOGGER = LoggerFactory.getLogger(MetricReportReporter.class);

  public final Map<String, String> tags;
  protected final Closer closer;

  public MetricReportReporter(Builder<?> builder) {
    super(builder.registry, builder.name, builder.filter, builder.rateUnit, builder.durationUnit);
    this.tags = builder.tags;
    this.closer = Closer.create();
  }

  /**
   * Builder for {@link gobblin.metrics.MetricReportReporter}.
   * Defaults to no filter, reporting rates in seconds and times in milliseconds.
   */
  public static abstract class Builder<T extends Builder<T>> {
    protected MetricRegistry registry;
    protected String name;
    protected MetricFilter filter;
    protected TimeUnit rateUnit;
    protected TimeUnit durationUnit;
    protected Map<String, String> tags;

    protected Builder(MetricRegistry registry) {
      this.registry = registry;
      this.name = "KafkaReporter";
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.filter = MetricFilter.ALL;
      this.tags = new LinkedHashMap<String, String>();
    }

    protected abstract T self();

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
     * Convert rates to the given time unit.
     *
     * @param rateUnit a unit of time
     * @return {@code this}
     */
    public T convertRatesTo(TimeUnit rateUnit) {
      this.rateUnit = rateUnit;
      return self();
    }

    /**
     * Convert durations to the given time unit.
     *
     * @param durationUnit a unit of time
     * @return {@code this}
     */
    public T convertDurationsTo(TimeUnit durationUnit) {
      this.durationUnit = durationUnit;
      return self();
    }

    /**
     * Add tags
     * @param tags
     * @return {@code this}
     */
    public T withTags(Map<String, String> tags) {
      this.tags.putAll(tags);
      return self();
    }

    /**
     * Add tags.
     * @param tags List of {@link gobblin.metrics.Tag}
     * @return {@code this}
     */
    public T withTags(List<Tag<?>> tags) {
      for(Tag<?> tag : tags) {
        this.tags.put(tag.getKey(), tag.getValue().toString());
      }
      return self();
    }

    /**
     * Add tag.
     * @param key tag key
     * @param value tag value
     * @return {@code this}
     */
    public T withTag(String key, String value) {
      this.tags.put(key, value);
      return self();
    }

  }


  /**
   * Serializes metrics and pushes the byte arrays to Kafka.
   * Uses the serialize* methods in {@link gobblin.metrics.MetricReportReporter}.
   * @param gauges
   * @param counters
   * @param histograms
   * @param meters
   * @param timers
   */
  @Override
  public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers,
      Map<String, String> tags) {

    List<Metric> metrics = new ArrayList<Metric>();

    for( Map.Entry<String, Gauge> gauge : gauges.entrySet()) {
      metrics.addAll(serializeGauge(gauge.getKey(), gauge.getValue()));
    }

    for ( Map.Entry<String, Counter> counter : counters.entrySet()) {
      metrics.addAll(serializeCounter(counter.getKey(), counter.getValue()));
    }

    for( Map.Entry<String, Histogram> histogram : histograms.entrySet()) {
      metrics.addAll(serializeSnapshot(histogram.getKey(), histogram.getValue().getSnapshot()));
      metrics.addAll(serializeSingleValue(histogram.getKey(), histogram.getValue().getCount(), "count"));
    }

    for ( Map.Entry<String, Meter> meter : meters.entrySet()) {
      metrics.addAll(serializeMetered(meter.getKey(), meter.getValue()));
    }

    for ( Map.Entry<String, Timer> timer : timers.entrySet()) {
      metrics.addAll(serializeSnapshot(timer.getKey(), timer.getValue().getSnapshot()));
      metrics.addAll(serializeMetered(timer.getKey(), timer.getValue()));
      metrics.addAll(serializeSingleValue(timer.getKey(), timer.getValue().getCount(), "count"));
    }

    Map<String, String> allTags = Maps.newHashMap();
    allTags.putAll(tags);
    allTags.putAll(this.tags);

    MetricReport report = new MetricReport(allTags, new DateTime().getMillis(), metrics);

    pushReport(report);

  }

  /**
   * Emit the {@link gobblin.metrics.MetricReport} to the metrics sink.
   * @param report metric report to emit.
   */
  protected abstract void pushReport(MetricReport report);

  /**
   * Extracts metrics from {@link com.codahale.metrics.Gauge}.
   * @param name
   * @param gauge
   * @return a list of {@link gobblin.metrics.Metric}.
   */
  protected List<Metric> serializeGauge(String name, Gauge gauge) {
    List<Metric> metrics = new ArrayList<Metric>();
    try {
      metrics.add(new Metric(name, Double.parseDouble(gauge.getValue().toString())));
    } catch(NumberFormatException exception) {
      LOGGER.info("Failed to serialize gauge metric. Not compatible with double value.");
    }
    return metrics;
  }

  /**
   * Extracts metrics from {@link com.codahale.metrics.Counter}.
   * @param name
   * @param counter
   * @return a list of {@link gobblin.metrics.Metric}.
   */
  protected List<Metric> serializeCounter(String name, Counter counter) {
    List<Metric> metrics = new ArrayList<Metric>();
    metrics.add(serializeValue(name, counter.getCount()));
    return metrics;
  }

  /**
   * Extracts metrics from {@link com.codahale.metrics.Metered}.
   * @param name
   * @param meter
   * @return a list of {@link gobblin.metrics.Metric}.
   */
  protected List<Metric> serializeMetered(String name, Metered meter) {
    List<Metric> metrics = new ArrayList<Metric>();

    metrics.add(serializeValue(name, meter.getCount(), "count"));
    metrics.add(serializeValue(name, meter.getMeanRate(), "rate", "mean"));
    metrics.add(serializeValue(name, meter.getOneMinuteRate(), "rate", "1m"));
    metrics.add(serializeValue(name, meter.getFiveMinuteRate(), "rate", "5m"));
    metrics.add(serializeValue(name, meter.getFifteenMinuteRate(), "rate", "15m"));

    return metrics;
  }

  /**
   * Extracts metrics from {@link com.codahale.metrics.Snapshot}.
   * @param name
   * @param snapshot
   * @return a list of {@link gobblin.metrics.Metric}.
   */
  protected List<Metric> serializeSnapshot(String name, Snapshot snapshot) {
    List<Metric> metrics = new ArrayList<Metric>();

    metrics.add(serializeValue(name, snapshot.getMean(), "mean"));
    metrics.add(serializeValue(name, snapshot.getMin(), "min"));
    metrics.add(serializeValue(name, snapshot.getMax(), "max"));
    metrics.add(serializeValue(name, snapshot.getMedian(), "median"));
    metrics.add(serializeValue(name, snapshot.get75thPercentile(), "75percentile"));
    metrics.add(serializeValue(name, snapshot.get95thPercentile(), "95percentile"));
    metrics.add(serializeValue(name, snapshot.get99thPercentile(), "99percentile"));
    metrics.add(serializeValue(name, snapshot.get999thPercentile(), "999percentile"));

    return metrics;
  }

  /**
   * Convert single value into list of Metrics.
   * @param name
   * @param value
   * @param path suffixes to more precisely identify the meaning of the reported value
   * @return a Singleton list of {@link gobblin.metrics.Metric}.
   */
  protected List<Metric> serializeSingleValue(String name, Number value, String... path) {
    List<Metric> metrics = new ArrayList<Metric>();
    metrics.add(serializeValue(name, value, path));
    return metrics;
  }

  /**
   * Converts a single key-value pair into a metric.
   * @param name name of the metric
   * @param value value of the metric to report
   * @param path additional suffixes to further identify the meaning of the reported value
   * @return a {@link gobblin.metrics.Metric}.
   */
  protected Metric serializeValue(String name, Number value, String... path) {
    return new Metric(MetricRegistry.name(name, path), value.doubleValue());
  }

  @Override
  public void close() {
    try {
      super.close();
      this.closer.close();
    } catch(Exception e) {
      LOGGER.warn("Exception when closing KafkaReporter", e);
    }
  }
}
