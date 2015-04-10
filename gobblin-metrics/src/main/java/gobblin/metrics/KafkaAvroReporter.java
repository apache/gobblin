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

import com.codahale.metrics.MetricRegistry;
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


/**
 * Kafka reporter for codahale metrics writing metrics in Avro format
 *
 * @author ibuenros
 */
public class KafkaAvroReporter extends KafkaReporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAvroReporter.class);

  /**
   * Avro schema used for encoding metrics
   * TODO: finalize metrics avro schema
   */
  private static final String SchemaString = "{\n"+
      " \"type\": \"record\",\n"+
      " \"name\": \"Metric\",\n"+
      " \"namespace\":\"<TBD>\",\n"+
      " \"fields\" : [\n"+
      " {\"name\": \"hostname\", \"type\": [\"null\", \"string\"], \"doc\": \"hostname emitting the metric\"},\n"+
      " {\"name\": \"environment\", \"type\": [\"null\", \"string\"], \"doc\": \"environment where metric was emitted\"},\n"+
      " {\"name\": \"tags\", \"type\": {\"type\": \"array\", \"items\": \"string\"}, \"doc\": \"tags associated with the metric\"},\n"+
      " {\"name\": \"name\", \"type\": \"string\", \"doc\": \"metric name\"},\n"+
      " {\"name\": \"value\", \"type\": [\"boolean\", \"int\", \"long\", \"float\", \"double\", \"bytes\", \"string\"], \"doc\": \"metric value\"}\n"+
      " ]\n"+
      "}";

  public static final Schema Schema = (new Schema.Parser()).parse(SchemaString);
  private GenericDatumWriter<GenericRecord> writer;
  private Encoder encoder;
  private ByteArrayOutputStream out;
  private long lastSerializeExceptionTime;

  protected KafkaAvroReporter(Builder<?> builder) {
    super(builder);

    lastSerializeExceptionTime = 0;

    out = new ByteArrayOutputStream();
    encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer = new GenericDatumWriter<GenericRecord>(Schema);
  }

  /**
   * Serializes a metric key-value pair into an avro object
   * Calls _serializeValue, which is synchronized because avro serializer is not thread-safe
   *
   * @param name name of the metric
   * @param value value of the metric to report
   * @param path additional suffixes to further identify the meaning of the reported value
   * @return
   */
  @Override
  protected synchronized byte[] serializeValue(String name, Object value, String... path) {
    GenericRecord record = new GenericData.Record(Schema);

    record.put("name", makeName(name, path));
    record.put("value", value);
    record.put("hostname", host);
    record.put("environment", env);
    record.put("tags", tags);

    try {
      out.reset();
      writer.write(record, encoder);
      encoder.flush();
      return out.toByteArray();
    } catch(IOException e) {
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

  /**
   * Returns a new {@link gobblin.metrics.KafkaAvroReporter.Builder} for {@link gobblin.metrics.KafkaAvroReporter}
   *
   * @param registry the registry to report
   * @return KafkaAvroReporter builder
   */
  public static Builder<?> forRegistry(MetricRegistry registry) {
    return new BuilderImpl(registry);
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
   * Builder for {@link gobblin.metrics.KafkaAvroReporter}
   * Defaults to no filter, reporting rates in seconds and times in milliseconds
   */
  public static abstract class Builder<T extends Builder<T>> extends KafkaReporter.Builder<T> {

    private Builder(MetricRegistry registry) {
      super(registry);
    }

    /**
     * Builds and returns {@link gobblin.metrics.KafkaAvroReporter}
     *
     * @param brokers string of Kafka brokers
     * @param topic topic to send metrics to
     * @return KafkaAvroReporter
     */
    public KafkaAvroReporter build(String brokers, String topic) {
      return new KafkaAvroReporter(this);
    }

  }
}
