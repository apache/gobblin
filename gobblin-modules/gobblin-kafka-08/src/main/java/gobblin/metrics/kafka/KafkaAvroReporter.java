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

package gobblin.metrics.kafka;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.MetricReport;
import gobblin.metrics.reporter.util.AvroBinarySerializer;
import gobblin.metrics.reporter.util.AvroSerializer;
import gobblin.metrics.reporter.util.SchemaRegistryVersionWriter;
import gobblin.metrics.reporter.util.SchemaVersionWriter;
import gobblin.util.ConfigUtils;

import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;

import com.google.common.base.Optional;
import com.typesafe.config.Config;


/**
 * Kafka reporter for codahale metrics writing metrics in Avro format.
 *
 * @author ibuenros
 */
public class KafkaAvroReporter extends KafkaReporter {

  protected KafkaAvroReporter(Builder<?> builder, Config config) throws IOException {
    super(builder, config);
    if (builder.registry.isPresent()) {
      Schema schema =
          new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream("MetricReport.avsc"));
      this.serializer.setSchemaVersionWriter(new SchemaRegistryVersionWriter(builder.registry.get(), builder.topic,
          Optional.of(schema)));
    }
  }

  @Override
  protected AvroSerializer<MetricReport> createSerializer(SchemaVersionWriter schemaVersionWriter)
      throws IOException {
    return new AvroBinarySerializer<>(MetricReport.SCHEMA$, schemaVersionWriter);
  }

  /**
   * A static factory class for obtaining new {@link gobblin.metrics.kafka.KafkaAvroReporter.Builder}s
   *
   * @see gobblin.metrics.kafka.KafkaAvroReporter.Builder
   */
  public static class BuilderFactory {

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
   * Builder for {@link KafkaAvroReporter}. Defaults to no filter, reporting rates in seconds and times in milliseconds.
   */
  public static abstract class Builder<T extends Builder<T>> extends KafkaReporter.Builder<T> {

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
    public KafkaAvroReporter build(String brokers, String topic, Properties props) throws IOException {
      this.brokers = brokers;
      this.topic = topic;
      return new KafkaAvroReporter(this, ConfigUtils.propertiesToConfig(props, Optional.of(ConfigurationKeys.METRICS_CONFIGURATIONS_PREFIX)));
    }
  }
}
