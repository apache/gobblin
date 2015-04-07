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
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Kafka reporter for Codahale metrics.
 *
 * @author ibuenros
 */
public class KafkaReporter extends ScheduledReporter {

  private ProducerConfig _config;
  private Producer<String, Message> _producer;
  private String _topic;

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReporter.class);

  /**
   * Returns a new {@link gobblin.metrics.KafkaReporter.Builder} for {@link gobblin.metrics.KafkaReporter}
   *
   * @param registry the registry to report
   * @return KafkaReporter builder
   */
  public static Builder forRegistry(MetricRegistry registry) {
    return new Builder(registry);
  }

  /**
   * Builder for {@link gobblin.metrics.KafkaReporter}
   * Defaults to no filter, reporting rates in seconds and times in milliseconds
   */
  public static class Builder {
    private MetricRegistry _registry;
    private String _name;
    private MetricFilter _filter;
    private TimeUnit _rateUnit;
    private TimeUnit _durationUnit;

    protected Builder(MetricRegistry registry) {
      this._registry = registry;
      this._name = "KafkaReporter";
      this._rateUnit = TimeUnit.SECONDS;
      this._durationUnit = TimeUnit.MILLISECONDS;
      this._filter = MetricFilter.ALL;
    }

    /**
     * Only report metrics which match the given filter.
     *
     * @param filter a {@link MetricFilter}
     * @return {@code this}
     */
    public Builder filter(MetricFilter filter) {
      this._filter = filter;
      return this;
    }

    /**
     * Convert rates to the given time unit.
     *
     * @param rateUnit a unit of time
     * @return {@code this}
     */
    public Builder convertRatesTo(TimeUnit rateUnit) {
      this._rateUnit = rateUnit;
      return this;
    }

    /**
     * Convert durations to the given time unit.
     *
     * @param durationUnit a unit of time
     * @return {@code this}
     */
    public Builder convertDurationsTo(TimeUnit durationUnit) {
      this._durationUnit = durationUnit;
      return this;
    }

    /**
     * Builds and returns {@link gobblin.metrics.KafkaReporter}
     *
     * @param brokers string of Kafka brokers
     * @param topic topic to send metrics to
     * @return KafkaReporter
     */
    public KafkaReporter build(String brokers, String topic) {
      return new KafkaReporter(_registry, _name, _filter, _rateUnit, _durationUnit, brokers, topic);
    }

  }

  /**
   * Builds a metric name by appending elements of the path to the base name
   * @param name base name of metric
   * @param path elements to append to the base name
   * @return generated name
   */
  public static String makeName(String name, String... path) {
    final StringBuilder sb = new StringBuilder(name);
    for (String part: path) {
      sb.append('.').append(part);
    }
    return sb.toString();
  }

  protected KafkaReporter(MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit,
      TimeUnit durationUnit, String brokers, String topic) {
    super(registry, name, filter, rateUnit, durationUnit);

    Properties props = new Properties();

    props.put("metadata.broker.list", brokers);
    props.put("serializer.class", "kafka.serializer.DefaultEncoder");
    props.put("request.required.acks", "1");

    _config = new ProducerConfig(props);
    _producer = new Producer<String, Message>(_config);
    _topic = topic;

  }

  @Override
  public void close() {
    super.close();
    _producer.close();
  }

  /**
   * Serializes metrics and pushes the byte arrays to Kafka.
   * Uses the serialize* methods in {@link gobblin.metrics.KafkaReporter}
   * @param gauges
   * @param counters
   * @param histograms
   * @param meters
   * @param timers
   */
  @Override
  public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {

    List<KeyedMessage<String, Message>> messages = new ArrayList<KeyedMessage<String, Message>>();

    for( Entry<String, Gauge> gauge : gauges.entrySet()) {
      messages.addAll(toKeyedMessages(serializeGauge(gauge.getKey(), gauge.getValue())));
    }

    for ( Entry<String, Counter> counter : counters.entrySet()) {
      messages.addAll(toKeyedMessages(serializeCounter(counter.getKey(), counter.getValue())));
    }

    for( Entry<String, Histogram> histogram : histograms.entrySet()) {
      messages.addAll(toKeyedMessages(serializeSnapshot(histogram.getKey(), histogram.getValue().getSnapshot())));
      messages.addAll(toKeyedMessages(serializeSingleValue(histogram.getKey(), histogram.getValue().getCount())));
    }

    for ( Entry<String, Meter> meter : meters.entrySet()) {
      messages.addAll(toKeyedMessages(serializeMetered(meter.getKey(), meter.getValue())));
    }

    for ( Entry<String, Timer> timer : timers.entrySet()) {
      messages.addAll(toKeyedMessages(serializeSnapshot(timer.getKey(), timer.getValue().getSnapshot())));
      messages.addAll(toKeyedMessages(serializeMetered(timer.getKey(), timer.getValue())));
      messages.addAll(toKeyedMessages(serializeSingleValue(timer.getKey(), timer.getValue().getCount())));
    }

    _producer.send(messages);

  }

  /**
   * Serialize a {@link com.codahale.metrics.Gauge}
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
   * Serialize a {@link com.codahale.metrics.Counter}
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
   * Serialize a {@link com.codahale.metrics.Metered}
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
   * Serialize a {@link com.codahale.metrics.Snapshot}
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
   * Serialize a single value
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
   * Serializes a single metric key-value pair to send to Kafka
   * @param name name of the metric
   * @param value value of the metric to report
   * @param path additional suffixes to further identify the meaning of the reported value
   * @return a byte array containing the key-value pair representing the metric
   */
  protected byte[] serializeValue(String name, Object value, String... path) {
    return stringifyValue(name, value, path).getBytes();
  }

  /**
   * Converts a single metric key-value pair to a string
   * @param name name of the metric
   * @param value value of the metric to report
   * @param path additional suffixes to further identify the meaning of the reported value
   * @return a string containing the key-value pair representing the metric
   */
  protected String stringifyValue(String name, Object value, String... path) {
    return String.format("%s:%s", makeName(name, path), value.toString());
  }

  private List<KeyedMessage<String, Message>> toKeyedMessages(List<byte[]> bytesArray) {
    List<KeyedMessage<String, Message>> messages = new ArrayList<KeyedMessage<String, Message>>();
    for ( byte[] bytes : bytesArray) {
      if (bytes != null) {
        messages.add(new KeyedMessage<String, Message>(_topic, new Message(bytes)));
      }
    }
    return messages;
  }

}
