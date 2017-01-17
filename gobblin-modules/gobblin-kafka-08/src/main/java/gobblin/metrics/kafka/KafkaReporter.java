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
import gobblin.metrics.reporter.MetricReportReporter;
import gobblin.metrics.reporter.util.AvroJsonSerializer;
import gobblin.metrics.reporter.util.AvroSerializer;
import gobblin.metrics.reporter.util.FixedSchemaVersionWriter;
import gobblin.metrics.reporter.util.SchemaVersionWriter;
import gobblin.util.ClassAliasResolver;
import gobblin.util.ConfigUtils;

import java.io.IOException;
import java.util.Properties;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;


/**
 * Kafka reporter for metrics.
 *
 * @author ibuenros
 */
@Slf4j
public class KafkaReporter extends MetricReportReporter {

  public static final String SCHEMA_VERSION_WRITER_TYPE = "metrics.kafka.schemaVersionWriterType";

  protected final AvroSerializer<MetricReport> serializer;
  protected final KafkaPusher kafkaPusher;


  protected KafkaReporter(Builder<?> builder, Config config) throws IOException {
    super(builder, config);

    SchemaVersionWriter versionWriter;
    if (config.hasPath(SCHEMA_VERSION_WRITER_TYPE)) {
      try {
        ClassAliasResolver<SchemaVersionWriter> resolver = new ClassAliasResolver<>(SchemaVersionWriter.class);
        Class<? extends SchemaVersionWriter> klazz = resolver.resolveClass(config.getString(SCHEMA_VERSION_WRITER_TYPE));
        versionWriter = klazz.newInstance();
      } catch (ReflectiveOperationException roe) {
        throw new IOException("Could not instantiate version writer.", roe);
      }
    } else {
      versionWriter = new FixedSchemaVersionWriter();
    }

    log.info("Schema version writer: " + versionWriter.getClass().getName());
    this.serializer = this.closer.register(createSerializer(versionWriter));

    if (builder.kafkaPusher.isPresent()) {
      this.kafkaPusher = builder.kafkaPusher.get();
    } else {
      this.kafkaPusher = this.closer.register(new KafkaPusher(builder.brokers, builder.topic));
    }
  }

  protected AvroSerializer<MetricReport> createSerializer(SchemaVersionWriter schemaVersionWriter) throws IOException {
    return new AvroJsonSerializer<>(MetricReport.SCHEMA$, schemaVersionWriter);
  }

  /**
   * A static factory class for obtaining new {@link gobblin.metrics.kafka.KafkaReporter.Builder}s
   *
   * @see gobblin.metrics.kafka.KafkaReporter.Builder
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
   * Builder for {@link KafkaReporter}. Defaults to no filter, reporting rates in seconds and times in milliseconds.
   */
  public static abstract class Builder<T extends MetricReportReporter.Builder<T>>
      extends MetricReportReporter.Builder<T> {

    protected String brokers;
    protected String topic;
    protected Optional<KafkaPusher> kafkaPusher;

    protected Builder() {
      super();
      this.name = "KafkaReporter";
      this.kafkaPusher = Optional.absent();
    }

    /**
     * Set {@link gobblin.metrics.kafka.KafkaPusher} to use.
     */
    public T withKafkaPusher(KafkaPusher pusher) {
      this.kafkaPusher = Optional.of(pusher);
      return self();
    }

    /**
     * Builds and returns {@link KafkaReporter}.
     *
     * @param brokers string of Kafka brokers
     * @param topic topic to send metrics to
     * @return KafkaReporter
     */
    public KafkaReporter build(String brokers, String topic, Properties props) throws IOException {
      this.brokers = brokers;
      this.topic = topic;

      return new KafkaReporter(this, ConfigUtils.propertiesToConfig(props, Optional.of(ConfigurationKeys.METRICS_CONFIGURATIONS_PREFIX)));
    }
  }

  @Override
  protected void emitReport(MetricReport report) {
    this.kafkaPusher.pushMessages(Lists.newArrayList(this.serializer.serializeRecord(report)));
  }
}
