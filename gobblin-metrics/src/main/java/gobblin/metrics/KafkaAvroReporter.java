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

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
  static String SchemaString = "{\n"+
      " \"type\": \"record\",\n"+
      " \"name\": \"Metric\",\n"+
      " \"namespace\":\"<TBD>\",\n"+
      " \"fields\" : [\n"+
      " {\"name\": \"cluster\", \"type\": [\"null\", \"string\"], \"doc\": \"name of the cluster where the metric is collected\"},\n"+
      " {\"name\": \"container\", \"type\": [\"null\", \"string\"], \"doc\": \"ID of the container where the metric is collected\"},\n"+
      " {\"name\": \"jobid\", \"type\": [\"null\", \"string\"], \"doc\": \"ID of the job that collects the metric\"},\n"+
      " {\"name\": \"taskid\", \"type\": [\"null\", \"string\"], \"doc\": \"ID of the task that collects the metric\"},\n"+
      " {\"name\": \"timestamp\", \"type\": \"long\", \"doc\": \"timestamp the metric is read\"},\n"+
      " {\"name\": \"name\", \"type\": \"string\", \"doc\": \"metric name\"},\n"+
      " {\"type\": \"enum\", \"name\": \"type\", \"symbols\": [\"COUNTER\", \"GAUGE\"], \"doc\": \"metric type\"}\n"+
      " {\"name\": \"group\", \"type\": \"string\", \"doc\": \"metric group name\"},\n"+
      " {\"name\": \"value\", \"type\": [\"boolean\", \"int\", \"long\", \"float\", \"double\", \"bytes\", \"string\"], \"doc\": \"metric value\"}\n"+
      " ]\n"+
      "}";

  Schema _schema;
  private GenericDatumWriter<GenericRecord> w;
  private Encoder e;
  private ByteArrayOutputStream out;
  private int exceptionCount;

  protected KafkaAvroReporter(MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit,
      TimeUnit durationUnit, String brokers, String topic, String host, String env, Set<String> tags) {
    super(registry, name, filter, rateUnit, durationUnit, brokers, topic, host, env, tags);

    Schema.Parser parser = new Schema.Parser();
    _schema = parser.parse(SchemaString);

    exceptionCount = 0;

    out = new ByteArrayOutputStream();
    e = EncoderFactory.get().binaryEncoder(out, null);
    w = new GenericDatumWriter<GenericRecord>(_schema);

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
  protected byte[] serializeValue(String name, Object value, String... path) {
    return _serializeValue(name, value, path);
  }

  protected synchronized byte[] _serializeValue(String name, Object value, String... path) {

    GenericRecord record = new GenericData.Record(_schema);

    record.put("name", makeName(name, path));
    record.put("value", value);

    try {
      w.write(record, e);
      e.flush();
      return out.toByteArray();
    } catch(IOException e) {
      exceptionCount++;
      LOGGER.trace("Could not serialize Avro record for Kafka Metrics. Exception: %s", e.getMessage());
      if (exceptionCount % 1000 == 0) {
        LOGGER.warn("Could not serialize Avro record for Kafka Metrics. Exception: %s", e.getMessage());
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
  public static Builder forRegistry(MetricRegistry registry) {
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
      return new KafkaAvroReporter(_registry, _name, _filter, _rateUnit, _durationUnit,
          brokers, topic, _host, _env, _tags);
    }

  }
}
