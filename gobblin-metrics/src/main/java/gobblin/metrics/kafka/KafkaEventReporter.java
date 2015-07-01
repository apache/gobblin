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
import java.util.List;
import java.util.Queue;

import com.google.common.collect.Lists;

import gobblin.metrics.GobblinTrackingEvent;
import gobblin.metrics.MetricContext;
import gobblin.metrics.reporter.EventReporter;
import gobblin.metrics.reporter.util.AvroJsonSerializer;
import gobblin.metrics.reporter.util.AvroSerializer;
import gobblin.metrics.reporter.util.FixedSchemaVersionWriter;
import gobblin.metrics.reporter.util.SchemaVersionWriter;


public class KafkaEventReporter extends EventReporter {

  protected final AvroSerializer<GobblinTrackingEvent> serializer;
  private final KafkaPusher kafkaPusher;

  public KafkaEventReporter(Builder builder) throws IOException {
    super(builder);

    this.serializer = this.closer.register(
        createSerializer(new FixedSchemaVersionWriter()));
    this.kafkaPusher = this.closer.register(new KafkaPusher(builder.brokers, builder.topic));

  }

  @Override
  public void reportEventQueue(Queue<GobblinTrackingEvent> queue) {
    GobblinTrackingEvent nextEvent;
    List<byte[]> events = Lists.newArrayList();

    while(null != (nextEvent = queue.poll())) {
      events.add(this.serializer.serializeRecord(nextEvent));
    }

    this.kafkaPusher.pushMessages(events);
  }

  protected AvroSerializer<GobblinTrackingEvent> createSerializer(SchemaVersionWriter schemaVersionWriter) throws IOException {
    return new AvroJsonSerializer<GobblinTrackingEvent>(GobblinTrackingEvent.SCHEMA$, schemaVersionWriter);
  }

  /**
   * Returns a new {@link KafkaEventReporter.Builder} for {@link KafkaEventReporter}.
   * Will automatically add all Context tags to the reporter.
   *
   * @param context the {@link gobblin.metrics.MetricContext} to report
   * @return KafkaReporter builder
   */
  public static Builder<? extends Builder> forContext(MetricContext context) {
    return new BuilderImpl(context);
  }

  private static class BuilderImpl extends Builder<BuilderImpl> {
    public BuilderImpl(MetricContext context) {
      super(context);
    }

    @Override
    protected BuilderImpl self() {
      return this;
    }
  }

  /**
   * Builder for {@link KafkaEventReporter}.
   * Defaults to no filter, reporting rates in seconds and times in milliseconds.
   */
  public static abstract class Builder<T extends EventReporter.Builder<T>>
      extends EventReporter.Builder<T> {
    protected String brokers;
    protected String topic;

    protected Builder(MetricContext context) {
      super(context);
    }

    /**
     * Builds and returns {@link KafkaEventReporter}.
     *
     * @param brokers string of Kafka brokers
     * @param topic topic to send metrics to
     * @return KafkaReporter
     */
    public KafkaEventReporter build(String brokers, String topic) throws IOException {
      this.brokers = brokers;
      this.topic = topic;
      return new KafkaEventReporter(this);
    }

  }
}
