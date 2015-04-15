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

package gobblin.metrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.google.common.base.Strings;
import com.google.common.io.Closer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


/**
 * Kafka reporter for Codahale metrics.
 *
 * @author ibuenros
 */
public class KafkaReporter extends ScheduledReporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReporter.class);

  private final Producer<String, byte[]> producer;
  private final ProducerConfig config;
  private final String topic;
  private final ObjectMapper mapper = new ObjectMapper();

  protected long lastSerializeExceptionTime;
  protected final Closer closer;

  public final Map<String, String> tags;

  protected KafkaReporter(Builder<?> builder) {
    super(builder.registry, builder.name, builder.filter,
        builder.rateUnit, builder.durationUnit);

    this.closer = Closer.create();

    this.lastSerializeExceptionTime = 0;

    this.tags = builder.tags;

    Properties props = new Properties();

    props.put("metadata.broker.list", builder.brokers);
    props.put("serializer.class", "kafka.serializer.DefaultEncoder");
    props.put("request.required.acks", "1");

    this.config = new ProducerConfig(props);
    this.producer = closer.register(new ProducerCloseable<String, byte[]>(config));
    this.topic = builder.topic;
  }

  /**
   * Returns a new {@link gobblin.metrics.KafkaReporter.Builder} for {@link gobblin.metrics.KafkaReporter}.
   * If the registry is of type {@link gobblin.metrics.MetricContext} tags will NOT be inherited.
   * To inherit tags, use forContext method.
   *
   * @param registry the registry to report
   * @return KafkaReporter builder
   */
  public static Builder<?> forRegistry(MetricRegistry registry) {
    if(MetricContext.class.isInstance(registry)) {
      LOGGER.warn("Creating Kafka Reporter from MetricContext using forRegistry method. Will not inherit tags.");
    }
    return new BuilderImpl(registry);
  }

  /**
   * Returns a new {@link gobblin.metrics.KafkaReporter.Builder} for {@link gobblin.metrics.KafkaReporter}.
   * Will automatically add all Context tags to the reporter.
   *
   * @param context the {@link gobblin.metrics.MetricContext} to report
   * @return KafkaReporter builder
   */
  public static Builder<?> forContext(MetricContext context) {
    return new BuilderImpl(context).withTags(context.getTags());
  }

  private static class BuilderImpl extends Builder<BuilderImpl> {
    public BuilderImpl(MetricRegistry registry) {
      super(registry);
    }

    @Override
    protected BuilderImpl self() {
      return this;
    }
  }

  /**
   * Builder for {@link gobblin.metrics.KafkaReporter}.
   * Defaults to no filter, reporting rates in seconds and times in milliseconds.
   */
  public static abstract class Builder<T extends Builder<T>> {
    protected MetricRegistry registry;
    protected String name;
    protected MetricFilter filter;
    protected TimeUnit rateUnit;
    protected TimeUnit durationUnit;
    protected Map<String, String> tags;
    protected String brokers;
    protected String topic;

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

    /**
     * Builds and returns {@link gobblin.metrics.KafkaReporter}.
     *
     * @param brokers string of Kafka brokers
     * @param topic topic to send metrics to
     * @return KafkaReporter
     */
    public KafkaReporter build(String brokers, String topic) {
      this.brokers = brokers;
      this.topic = topic;
      return new KafkaReporter(this);
    }

  }

  /**
   * Class to contain a single metric.
   */
  public static class Metric {
    public String name;
    public Object value;
    public Map<String, String> tags;
    public long timestamp;
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

  /**
   * Serializes metrics and pushes the byte arrays to Kafka.
   * Uses the serialize* methods in {@link gobblin.metrics.KafkaReporter}.
   * @param gauges
   * @param counters
   * @param histograms
   * @param meters
   * @param timers
   */
  @Override
  public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {

    List<KeyedMessage<String, byte[]>> messages = new ArrayList<KeyedMessage<String, byte[]>>();

    for( Entry<String, Gauge> gauge : gauges.entrySet()) {
      messages.addAll(toKeyedMessages(serializeGauge(gauge.getKey(), gauge.getValue())));
    }

    for ( Entry<String, Counter> counter : counters.entrySet()) {
      messages.addAll(toKeyedMessages(serializeCounter(counter.getKey(), counter.getValue())));
    }

    for( Entry<String, Histogram> histogram : histograms.entrySet()) {
      messages.addAll(toKeyedMessages(serializeSnapshot(histogram.getKey(), histogram.getValue().getSnapshot())));
      messages.addAll(
          toKeyedMessages(serializeSingleValue(histogram.getKey(), histogram.getValue().getCount(), "count")));
    }

    for ( Entry<String, Meter> meter : meters.entrySet()) {
      messages.addAll(toKeyedMessages(serializeMetered(meter.getKey(), meter.getValue())));
    }

    for ( Entry<String, Timer> timer : timers.entrySet()) {
      messages.addAll(toKeyedMessages(serializeSnapshot(timer.getKey(), timer.getValue().getSnapshot())));
      messages.addAll(toKeyedMessages(serializeMetered(timer.getKey(), timer.getValue())));
      messages.addAll(toKeyedMessages(serializeSingleValue(timer.getKey(), timer.getValue().getCount(), "count")));
    }

    this.producer.send(messages);

  }

  /**
   * Serialize a {@link com.codahale.metrics.Gauge}.
   * @param name
   * @param gauge
   * @return List of byte arrays for each metric derived from the gauge
   */
  protected List<byte[]> serializeGauge(String name, Gauge gauge) {
    List<byte[]> metrics = new ArrayList<byte[]>();
    metrics.add(serializeValue(name, gauge.getValue()));
    return metrics;
  }

  /**
   * Serialize a {@link com.codahale.metrics.Counter}.
   * @param name
   * @param counter
   * @return List of byte arrays for each metric derived from the counter
   */
  protected List<byte[]> serializeCounter(String name, Counter counter) {
    List<byte[]> metrics = new ArrayList<byte[]>();
    metrics.add(serializeValue(name, counter.getCount()));
    return metrics;
  }

  /**
   * Serialize a {@link com.codahale.metrics.Metered}.
   * @param name
   * @param meter
   * @return List of byte arrays for each metric derived from the metered object
   */
  protected List<byte[]> serializeMetered(String name, Metered meter) {
    List<byte[]> metrics = new ArrayList<byte[]>();

    metrics.add(serializeValue(name, meter.getCount(), "count"));
    metrics.add(serializeValue(name, meter.getMeanRate(), "rate", "mean"));
    metrics.add(serializeValue(name, meter.getOneMinuteRate(), "rate", "1m"));
    metrics.add(serializeValue(name, meter.getFiveMinuteRate(), "rate", "5m"));
    metrics.add(serializeValue(name, meter.getFifteenMinuteRate(), "rate", "15m"));

    return metrics;
  }

  /**
   * Serialize a {@link com.codahale.metrics.Snapshot}.
   * @param name
   * @param snapshot
   * @return List of byte arrays for each metric derived from the snapshot object
   */
  protected List<byte[]> serializeSnapshot(String name, Snapshot snapshot) {
    List<byte[]> metrics = new ArrayList<byte[]>();

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
   * Serialize a single value.
   * @param name
   * @param value
   * @param path suffixes to more precisely identify the meaning of the reported value
   * @return Singleton list of byte arrays representing the value
   */
  protected List<byte[]> serializeSingleValue(String name, Object value, String... path) {
    List<byte[]> metrics = new ArrayList<byte[]>();
    metrics.add(serializeValue(name, value, path));
    return metrics;
  }

  /**
   * Serializes a single metric key-value pair to send to Kafka.
   * @param name name of the metric
   * @param value value of the metric to report
   * @param path additional suffixes to further identify the meaning of the reported value
   * @return a byte array containing the key-value pair representing the metric
   */
  protected byte[] serializeValue(String name, Object value, String... path) {
    String str = stringifyValue(name, value, path);
    if(!Strings.isNullOrEmpty(str)) {
      return str.getBytes();
    } else {
      return null;
    }
  }

  /**
   * Converts a single metric key-value pair to a string.
   * @param name name of the metric
   * @param value value of the metric to report
   * @param path additional suffixes to further identify the meaning of the reported value
   * @return a string containing the key-value pair representing the metric
   */
  protected synchronized  String stringifyValue(String name, Object value, String... path) {
    Metric metric = new Metric();
    metric.name = MetricRegistry.name(name, path);
    metric.value = value;
    metric.tags = tags;
    metric.timestamp = System.currentTimeMillis();

    try {
      return mapper.writeValueAsString(metric);
    } catch(Exception e) {
      // If there is actually something wrong with the serializer,
      // this exception would be thrown for every single metric serialized.
      // Instead, report at warn level at most every 10 seconds.
      LOGGER.trace("Could not serialize Avro record for Kafka Metrics. Exception: %s", e.getMessage());
      if(System.currentTimeMillis() - lastSerializeExceptionTime > 10000) {
        LOGGER.warn("Could not serialize Avro record for Kafka Metrics. Exception: %s", e.getMessage());
        lastSerializeExceptionTime = System.currentTimeMillis();
      }
    }

    return null;
  }

  private List<KeyedMessage<String, byte[]>> toKeyedMessages(List<byte[]> bytesArray) {
    List<KeyedMessage<String, byte[]>> messages = new ArrayList<KeyedMessage<String, byte[]>>();
    for ( byte[] bytes : bytesArray) {
      if (bytes != null) {
        messages.add(new KeyedMessage<String, byte[]>(topic, bytes));
      }
    }
    return messages;
  }

}
