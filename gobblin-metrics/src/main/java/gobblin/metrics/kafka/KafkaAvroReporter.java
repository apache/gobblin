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

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;

import gobblin.metrics.MetricContext;
import gobblin.metrics.MetricReport;
import gobblin.metrics.reporter.util.AvroBinarySerializer;
import gobblin.metrics.reporter.util.AvroSerializer;
import gobblin.metrics.reporter.util.SchemaRegistryVersionWriter;
import gobblin.metrics.reporter.util.SchemaVersionWriter;


/**
 * Kafka reporter for codahale metrics writing metrics in Avro format.
 *
 * @author ibuenros
 */
public class KafkaAvroReporter extends KafkaReporter {

  protected KafkaAvroReporter(Builder<?> builder) throws IOException {
    super(builder);
    if(builder.registry.isPresent()) {
      this.serializer.setSchemaVersionWriter(new SchemaRegistryVersionWriter(builder.registry.get(), builder.topic));
    }
  }

  @Override
  protected AvroSerializer<MetricReport> createSerializer(SchemaVersionWriter schemaVersionWriter)
      throws IOException {
    return new AvroBinarySerializer<MetricReport>(MetricReport.SCHEMA$, schemaVersionWriter);
  }

  /**
   * Returns a new {@link KafkaAvroReporter.Builder} for {@link KafkaAvroReporter}.
   * If the registry is of type {@link gobblin.metrics.MetricContext} tags will NOT be inherited.
   * To inherit tags, use forContext method.
   *
   * @param registry the registry to report
   * @return KafkaAvroReporter builder
   * @deprecated this method is bugged. Use {@link KafkaAvroReporter.Factory#forRegistry} instead.
   */
  @Deprecated
  public static Builder<? extends Builder<?>> forRegistry(MetricRegistry registry) {
    return new BuilderImpl(registry);
  }

  /**
   * Returns a new {@link KafkaAvroReporter.Builder} for {@link KafkaAvroReporter}.
   *
   * @param context the {@link gobblin.metrics.MetricContext} to report
   * @return KafkaAvroReporter builder
   * @deprecated this method is bugged. Use {@link KafkaAvroReporter.Factory#forContext} instead.
   */
  @Deprecated
  public static Builder<? extends Builder<?>> forContext(MetricContext context) {
    return forRegistry(context);
  }

  public static class Factory {
    /**
     * Returns a new {@link KafkaAvroReporter.Builder} for {@link KafkaAvroReporter}.
     * If the registry is of type {@link gobblin.metrics.MetricContext} tags will NOT be inherited.
     * To inherit tags, use forContext method.
     *
     * @param registry the registry to report
     * @return KafkaAvroReporter builder
     */
    public static BuilderImpl forRegistry(MetricRegistry registry) {
      return new BuilderImpl(registry);
    }

    /**
     * Returns a new {@link KafkaAvroReporter.Builder} for {@link KafkaAvroReporter}.
     *
     * @param context the {@link gobblin.metrics.MetricContext} to report
     * @return KafkaAvroReporter builder
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
   * Builder for {@link KafkaAvroReporter}.
   * Defaults to no filter, reporting rates in seconds and times in milliseconds.
   */
  public static abstract class Builder<T extends Builder<T>> extends KafkaReporter.Builder<T> {

    private Optional<KafkaAvroSchemaRegistry> registry = Optional.absent();

    private Builder(MetricRegistry registry) {
      super(registry);
    }

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
    public KafkaAvroReporter build(String brokers, String topic) throws IOException {
      this.brokers = brokers;
      this.topic = topic;
      return new KafkaAvroReporter(this);
    }

  }

}
