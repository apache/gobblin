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

package org.apache.gobblin.metrics.kafka;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.commons.lang3.tuple.Pair;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.MetricReport;
import org.apache.gobblin.metrics.reporter.util.AvroBinarySerializer;
import org.apache.gobblin.metrics.reporter.util.AvroSerializer;
import org.apache.gobblin.metrics.reporter.util.FixedSchemaVersionWriter;
import org.apache.gobblin.metrics.reporter.util.SchemaRegistryVersionWriter;
import org.apache.gobblin.metrics.reporter.util.SchemaVersionWriter;
import org.apache.gobblin.util.ConfigUtils;


@Slf4j
public class KafkaAvroMetricKeyValueReporter extends KafkaKeyValueMetricObjectReporter {

  protected AvroSerializer<MetricReport> serializer;

  protected KafkaAvroMetricKeyValueReporter(Builder<?> builder, Config config) throws IOException {

    super(builder, config);
    log.info("Using KafkaAvroMetricKeyValueReporter");
    this.serializer = this.closer.register(createSerializer(new FixedSchemaVersionWriter()));

    if(builder.registry.isPresent()) {
      Schema schema =
          new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream("MetricReport.avsc"));
      this.serializer.setSchemaVersionWriter(new SchemaRegistryVersionWriter(builder.registry.get(), builder.topic,
          Optional.of(schema)));
    }

    if (builder.kafkaPusher.isPresent()) {
      this.kafkaPusher = builder.kafkaPusher.get();
    } else {
      Config kafkaConfig = ConfigUtils.getConfigOrEmpty(config, PusherUtils.METRICS_REPORTING_KAFKA_CONFIG_PREFIX)
          .withFallback(ConfigUtils.getConfigOrEmpty(config, ConfigurationKeys.SHARED_KAFKA_CONFIG_PREFIX));

      String pusherClassName = ConfigUtils.getString(config, PusherUtils.KAFKA_PUSHER_CLASS_NAME_KEY,  PusherUtils.DEFAULT_KAFKA_PUSHER_CLASS_NAME);

      this.kafkaPusher = PusherUtils.getKeyValuePusher(pusherClassName, builder.brokers, builder.topic, Optional.of(kafkaConfig));
    }
  }

  @Override
  protected void emitReport(MetricReport report) {
    log.info("Emitting report using KeyValueMetricPlainObjectReporter");

    this.kafkaPusher.pushKeyValueMessages(Lists.newArrayList(Pair.of(buildKey(report),this.serializer.serializeRecord(report))));

  }

  protected AvroSerializer<MetricReport> createSerializer(SchemaVersionWriter schemaVersionWriter)
      throws IOException {
    return new AvroBinarySerializer<>(MetricReport.SCHEMA$, schemaVersionWriter);
  }

  private static class BuilderImpl extends Builder<BuilderImpl> {
    @Override
    protected BuilderImpl self() {
      return this;
    }
  }

  public static abstract class Factory {

    public static BuilderImpl newBuilder() {
      return new BuilderImpl();
    }
  }

  /**
   * Builder for {@link KafkaAvroMetricKeyValueReporter}.
   * Defaults to no filter, reporting rates in seconds and times in milliseconds.
   */
  public static abstract class Builder<T extends Builder<T>> extends KafkaKeyValueMetricObjectReporter.Builder<T> {
    private Optional<KafkaAvroSchemaRegistry> registry = Optional.absent();

    public T withSchemaRegistry(KafkaAvroSchemaRegistry registry) {
      this.registry = Optional.of(registry);
      return self();
    }

    /**
     * Builds and returns {@link KafkaAvroReporter}.
     *
     * @param brokers string of Kafka brokers
     * @param topic topic to send metrics to
     * @return KafkaAvroReporter
     */
    public KafkaAvroMetricKeyValueReporter build(String brokers, String topic, Properties props) throws IOException {
      this.brokers=brokers;
      this.topic=topic;
      return new KafkaAvroMetricKeyValueReporter(this, KafkaReporter.getKafkaAndMetricsConfigFromProperties(props));
    }
  }
}
