/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import gobblin.metrics.MetricContext;
import gobblin.metrics.MetricReport;
import gobblin.metrics.reporter.MetricReportReporter;
import gobblin.metrics.reporter.util.AvroJsonSerializer;
import gobblin.metrics.reporter.util.AvroSerializer;
import gobblin.metrics.reporter.util.FixedSchemaVersionWriter;
import gobblin.metrics.reporter.util.SchemaVersionWriter;


/**
 * Kafka reporter for Codahale metrics.
 *
 * @author ibuenros
 */
public class KafkaReporter extends MetricReportReporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReporter.class);

  protected final AvroSerializer<MetricReport> serializer;
  private final KafkaPusher kafkaPusher;

  protected KafkaReporter(Builder<?> builder) throws IOException {
    super(builder);

    this.serializer = this.closer.register(
        createSerializer(new FixedSchemaVersionWriter()));

    if(builder.kafkaPusher.isPresent()) {
      this.kafkaPusher = builder.kafkaPusher.get();
    } else {
      this.kafkaPusher = this.closer.register(new KafkaPusher(builder.brokers, builder.topic));
    }
  }

  protected AvroSerializer<MetricReport> createSerializer(SchemaVersionWriter schemaVersionWriter) throws IOException {
    return new AvroJsonSerializer<MetricReport>(MetricReport.SCHEMA$, schemaVersionWriter);
  }

  /**
   * Returns a new {@link KafkaReporter.Builder} for {@link KafkaReporter}.
   * If the registry is of type {@link gobblin.metrics.MetricContext} tags will NOT be inherited.
   * To inherit tags, use forContext method.
   *
   * @param registry the registry to report
   * @return KafkaReporter builder
   * @deprecated This method is bugged. Use {@link gobblin.metrics.kafka.KafkaReporter.Factory#forRegistry}.
   */
  @Deprecated
  public static Builder<? extends Builder> forRegistry(MetricRegistry registry) {
    return new BuilderImpl(registry);
  }

  /**
   * Returns a new {@link KafkaReporter.Builder} for {@link KafkaReporter}.
   * Will automatically add all Context tags to the reporter.
   *
   * @param context the {@link gobblin.metrics.MetricContext} to report
   * @return KafkaReporter builder
   * @deprecated This method is bugged. Use {@link gobblin.metrics.kafka.KafkaReporter.Factory#forContext}.
   */
  @Deprecated
  public static Builder<?> forContext(MetricContext context) {
    return forRegistry(context);
  }

  public static class Factory {
    /**
     * Returns a new {@link KafkaReporter.Builder} for {@link KafkaReporter}.
     * If the registry is of type {@link gobblin.metrics.MetricContext} tags will NOT be inherited.
     * To inherit tags, use forContext method.
     *
     * @param registry the registry to report
     * @return KafkaReporter builder
     */
    public static BuilderImpl forRegistry(MetricRegistry registry) {
      return new BuilderImpl(registry);
    }

    /**
     * Returns a new {@link KafkaReporter.Builder} for {@link KafkaReporter}.
     * Will automatically add all Context tags to the reporter.
     *
     * @param context the {@link gobblin.metrics.MetricContext} to report
     * @return KafkaReporter builder
     */
    public static BuilderImpl forContext(MetricContext context) {
      return forRegistry(context);
    }
  }

  public static class BuilderImpl extends Builder<BuilderImpl> {
    private BuilderImpl(MetricRegistry registry) {
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
  public static abstract class Builder<T extends MetricReportReporter.Builder<T>>
      extends MetricReportReporter.Builder<T> {
    protected String brokers;
    protected String topic;
    protected Optional<KafkaPusher> kafkaPusher;

    protected Builder(MetricRegistry registry) {
      super(registry);
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
    public KafkaReporter build(String brokers, String topic) throws IOException {
      this.brokers = brokers;
      this.topic = topic;
      return new KafkaReporter(this);
    }

  }

  @Override
  protected void emitReport(MetricReport report) {
    this.kafkaPusher.pushMessages(Lists.newArrayList(this.serializer.serializeRecord(report)));
  }

}
