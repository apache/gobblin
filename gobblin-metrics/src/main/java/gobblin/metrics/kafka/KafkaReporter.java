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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import gobblin.metrics.MetricContext;
import gobblin.metrics.reporter.SerializedMetricReportReporter;


/**
 * Kafka reporter for Codahale metrics.
 *
 * @author ibuenros
 */
public class KafkaReporter extends SerializedMetricReportReporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReporter.class);
  public static final int SCHEMA_VERSION = 1;

  private final String topic;
  private final Optional<ProducerCloseable<String, byte[]>> producerOpt;

  protected KafkaReporter(Builder<?> builder) {
    super(builder);

    this.topic = builder.topic;

    if (this.reportMetrics) {
      Properties props = new Properties();
      props.put("metadata.broker.list", builder.brokers);
      props.put("serializer.class", "kafka.serializer.DefaultEncoder");
      props.put("request.required.acks", "1");

      ProducerConfig config = new ProducerConfig(props);

      this.producerOpt = Optional.of(this.closer.register(new ProducerCloseable<String, byte[]>(config)));
    } else {
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
  public static Builder<? extends Builder> forRegistry(MetricRegistry registry) {
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
  public static abstract class Builder<T extends SerializedMetricReportReporter.Builder<T>>
      extends SerializedMetricReportReporter.Builder<T> {
    protected String brokers;
    protected String topic;

    protected Builder(MetricRegistry registry) {
      super(registry);
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

  @Override
  protected void pushSerializedReport(byte[] serializedReport) {
    if (this.producerOpt.isPresent()) {

      List<KeyedMessage<String, byte[]>> messages = new ArrayList<KeyedMessage<String, byte[]>>();
      messages.add(new KeyedMessage<String, byte[]>(this.topic, serializedReport));
      this.producerOpt.get().send(messages);
    }
  }
}
