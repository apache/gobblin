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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;


/**
 * Kafka reporter for codahale metrics writing metrics in Avro format.
 *
 * @author ibuenros
 */
public class KafkaAvroReporter extends KafkaReporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAvroReporter.class);

  /**
   * Avro schema used for encoding metrics.
   * TODO: finalize metrics avro schema
   * TODO: make SCHEMA_STRING and SCHEMA not hard coded
   */
  private static final String SCHEMA_STRING = "{\n"+
      " \"type\": \"record\",\n"+
      " \"name\": \"Metric\",\n"+
      " \"namespace\":\"gobblin.metrics\",\n"+
      " \"fields\" : [\n"+
      " {\"name\": \"tags\", \"type\": {\"type\": \"map\", \"values\": \"string\"}, \"doc\": \"tags associated with the metric\"},\n"+
      " {\"name\": \"name\", \"type\": \"string\", \"doc\": \"metric name\"},\n"+
      " {\"name\": \"value\", \"type\": [\"boolean\", \"int\", \"long\", \"float\", \"double\", \"bytes\", \"string\"], \"doc\": \"metric value\"}\n"+
      " ]\n"+
      "}";

  public static final Schema SCHEMA = (new Schema.Parser()).parse(SCHEMA_STRING);
  private final GenericDatumWriter<GenericRecord> writer;
  private final Encoder encoder;
  private final ByteArrayOutputStream out;

  protected KafkaAvroReporter(Builder<?> builder) {
    super(builder);

    this.lastSerializeExceptionTime = 0;

    this.out = this.closer.register(new ByteArrayOutputStream());
    this.encoder = EncoderFactory.get().binaryEncoder(this.out, null);
    this.writer = new GenericDatumWriter<GenericRecord>(SCHEMA);
  }

  /**
   * Serializes a metric key-value pair into an avro object.
   * Calls _serializeValue, which is synchronized because avro serializer is not thread-safe.
   *
   * @param name name of the metric
   * @param value value of the metric to report
   * @param path additional suffixes to further identify the meaning of the reported value
   * @return
   */
  @Override
  protected synchronized byte[] serializeValue(String name, Object value, String... path) {
    GenericRecord record = new GenericData.Record(SCHEMA);

    record.put("name", MetricRegistry.name(name, path));
    record.put("value", value);
    record.put("tags", this.tags);

    try {
      this.out.reset();
      this.writer.write(record, this.encoder);
      this.encoder.flush();
      return out.toByteArray();
    } catch(IOException e) {
      // If there is actually something wrong with the serializer,
      // this exception would be thrown for every single metric serialized.
      // Instead, report at warn level at most every 10 seconds.
      LOGGER.trace("Could not serialize Avro record for Kafka Metrics. Exception: %s", e.getMessage());
      if(System.currentTimeMillis() - this.lastSerializeExceptionTime > 10000) {
        LOGGER.warn("Could not serialize Avro record for Kafka Metrics. Exception: %s", e.getMessage());
        this.lastSerializeExceptionTime = System.currentTimeMillis();
      }
    }

    return null;
  }

  /**
   * Returns a new {@link gobblin.metrics.KafkaAvroReporter.Builder} for {@link gobblin.metrics.KafkaAvroReporter}.
   * If the registry is of type {@link gobblin.metrics.MetricContext} tags will NOT be inherited.
   * To inherit tags, use forContext method.
   *
   * @param registry the registry to report
   * @return KafkaAvroReporter builder
   */
  public static Builder<?> forRegistry(MetricRegistry registry) {
    if(MetricContext.class.isInstance(registry)) {
      LOGGER.warn("Creating Kafka Avro Reporter from MetricContext using forRegistry method. Will not inherit tags.");
    }
    return new BuilderImpl(registry);
  }

  /**
   * Returns a new {@link gobblin.metrics.KafkaAvroReporter.Builder} for {@link gobblin.metrics.KafkaAvroReporter}.
   *
   * @param context the {@link gobblin.metrics.MetricContext} to report
   * @return KafkaAvroReporter builder
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
   * Builder for {@link gobblin.metrics.KafkaAvroReporter}.
   * Defaults to no filter, reporting rates in seconds and times in milliseconds.
   */
  public static abstract class Builder<T extends Builder<T>> extends KafkaReporter.Builder<T> {

    private Builder(MetricRegistry registry) {
      super(registry);
    }

    /**
     * Builds and returns {@link gobblin.metrics.KafkaAvroReporter}.
     *
     * @param brokers string of Kafka brokers
     * @param topic topic to send metrics to
     * @return KafkaAvroReporter
     */
    public KafkaAvroReporter build(String brokers, String topic) {
      this.brokers = brokers;
      this.topic = topic;
      return new KafkaAvroReporter(this);
    }

  }
}
