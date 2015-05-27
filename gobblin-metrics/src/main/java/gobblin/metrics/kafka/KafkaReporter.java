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

package gobblin.metrics.kafka;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
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
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import gobblin.metrics.Metric;
import gobblin.metrics.MetricContext;
import gobblin.metrics.MetricReport;
import gobblin.metrics.RecursiveScheduledReporter;
import gobblin.metrics.Tag;


/**
 * Kafka reporter for Codahale metrics.
 *
 * @author ibuenros
 */
public class KafkaReporter extends RecursiveScheduledReporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReporter.class);
  public static final int SCHEMA_VERSION = 1;

  private final String topic;
  private final Encoder encoder;
  private final ByteArrayOutputStream byteArrayOutputStream;
  private final DataOutputStream out;
  protected final Closer closer;

  private final Optional<ProducerCloseable<String, byte[]>> producerOpt;
  private final Optional<SpecificDatumWriter<MetricReport>> writerOpt;

  private static Optional<SpecificDatumReader<MetricReport>> READER = Optional.absent();

  public final Map<String, String> tags;
  private final boolean reportMetrics;

  protected KafkaReporter(Builder<?> builder) {
    super(builder.registry, builder.name, builder.filter,
        builder.rateUnit, builder.durationUnit);

    this.closer = Closer.create();
    this.topic = builder.topic;
    this.tags = builder.tags;

    this.byteArrayOutputStream = new ByteArrayOutputStream();
    this.out = this.closer.register(new DataOutputStream(byteArrayOutputStream));
    this.encoder = getEncoder(out);
    this.reportMetrics = this.encoder != null;

    if (this.reportMetrics) {
      Properties props = new Properties();
      props.put("metadata.broker.list", builder.brokers);
      props.put("serializer.class", "kafka.serializer.DefaultEncoder");
      props.put("request.required.acks", "1");

      ProducerConfig config = new ProducerConfig(props);

      this.writerOpt = Optional.of(new SpecificDatumWriter<MetricReport>(MetricReport.class));
      this.producerOpt = Optional.of(closer.register(new ProducerCloseable<String, byte[]>(config)));
    } else {
      this.writerOpt = Optional.absent();
      this.producerOpt = Optional.absent();
    }

  }

  /**
   * Returns a new {@link KafkaReporter.Builder} for {@link KafkaReporter}.
   * If the registry is of type {@link gobblin.metrics.MetricContext} tags will NOT be inherited.
   * To inherit tags, use forContext method.
   *
   * @param registry the registry to report
   * @return KafkaReporter builder
   */
  public static Builder<?> forRegistry(MetricRegistry registry) {
    return new BuilderImpl(registry);
  }

  /**
   * Returns a new {@link KafkaReporter.Builder} for {@link KafkaReporter}.
   * Will automatically add all Context tags to the reporter.
   *
   * @param context the {@link gobblin.metrics.MetricContext} to report
   * @return KafkaReporter builder
   */
  public static Builder<?> forContext(MetricContext context) {
    return forRegistry(context);
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
   * Builder for {@link KafkaReporter}.
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
     * Builds and returns {@link KafkaReporter}.
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
   * Get {@link org.apache.avro.io.Encoder} for serializing Avro records.
   * @param out {@link java.io.OutputStream} where records should be written.
   * @return Encoder.
   */
  protected Encoder getEncoder(OutputStream out) {
    try {
      return EncoderFactory.get().jsonEncoder(MetricReport.SCHEMA$, out);
    } catch(IOException exception) {
      LOGGER.warn("KafkaReporter serializer failed to initialize. Will not report Metrics to Kafka.", exception);
      return null;
    }
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
   * Parses a {@link gobblin.metrics.MetricReport} from a byte array.
   * @param reuse MetricReport to reuse.
   * @param bytes Input bytes.
   * @return MetricReport.
   * @throws IOException
   */
  public static MetricReport deserializeReport(MetricReport reuse, byte[] bytes) throws IOException {
    if (!READER.isPresent()) {
      READER = Optional.of(new SpecificDatumReader<MetricReport>(MetricReport.class));
    }

    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(bytes));

    // Check version byte
    if (inputStream.readInt() != SCHEMA_VERSION) {
      throw new IOException("MetricReport schema version not recognized.");
    }
    // Decode the rest
    Decoder decoder = DecoderFactory.get().jsonDecoder(MetricReport.SCHEMA$, inputStream);
    return READER.get().read(reuse, decoder);
  }

  public void reportForContext(MetricContext context) {
    report(context.getGauges(), context.getCounters(), context.getHistograms(), context.getMeters(), context.getTimers());
  }

  /**
   * Serializes metrics and pushes the byte arrays to Kafka.
   * Uses the serialize* methods in {@link KafkaReporter}.
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

    if(!this.reportMetrics || !this.producerOpt.isPresent()) {
      return;
    }

    List<Metric> metrics = new ArrayList<Metric>();

    for( Entry<String, Gauge> gauge : gauges.entrySet()) {
      metrics.addAll(serializeGauge(gauge.getKey(), gauge.getValue()));
    }

    for ( Entry<String, Counter> counter : counters.entrySet()) {
      metrics.addAll(serializeCounter(counter.getKey(), counter.getValue()));
    }

    for( Entry<String, Histogram> histogram : histograms.entrySet()) {
      metrics.addAll(serializeSnapshot(histogram.getKey(), histogram.getValue().getSnapshot()));
      metrics.addAll(serializeSingleValue(histogram.getKey(), histogram.getValue().getCount(), "count"));
    }

    for ( Entry<String, Meter> meter : meters.entrySet()) {
      metrics.addAll(serializeMetered(meter.getKey(), meter.getValue()));
    }

    for ( Entry<String, Timer> timer : timers.entrySet()) {
      metrics.addAll(serializeSnapshot(timer.getKey(), timer.getValue().getSnapshot()));
      metrics.addAll(serializeMetered(timer.getKey(), timer.getValue()));
      metrics.addAll(serializeSingleValue(timer.getKey(), timer.getValue().getCount(), "count"));
    }

    Map<String, String> allTags = Maps.newHashMap();
    allTags.putAll(tags);
    allTags.putAll(this.tags);

    MetricReport report = new MetricReport(allTags, new DateTime().getMillis(), metrics);

    byte[] serializedReport = serializeReport(report);

    if(serializedReport != null) {
      List<KeyedMessage<String, byte[]>> messages = new ArrayList<KeyedMessage<String, byte[]>>();
      messages.add(new KeyedMessage<String, byte[]>(this.topic, serializeReport(report)));
      this.producerOpt.get().send(messages);
    }

  }

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

  /**
   * Converts a {@link gobblin.metrics.MetricReport} to bytes to send through Kafka.
   * @param report MetricReport to serialize.
   * @return Serialized bytes.
   */
  protected synchronized byte[] serializeReport(MetricReport report) {
    if (!this.writerOpt.isPresent()) {
      return null;
    }

    try {
      this.byteArrayOutputStream.reset();
      // Write version number at the beginning of the message.
      this.out.writeInt(SCHEMA_VERSION);
      // Now write the report itself.
      this.writerOpt.get().write(report, this.encoder);
      this.encoder.flush();
      return this.byteArrayOutputStream.toByteArray();
    } catch(IOException exception) {
      LOGGER.warn("Could not serialize Avro record for Kafka Metrics. Exception: %s", exception.getMessage());
      return null;
    }
  }

}
