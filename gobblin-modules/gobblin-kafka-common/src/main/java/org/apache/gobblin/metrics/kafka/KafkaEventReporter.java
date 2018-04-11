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

import java.io.IOException;
import java.util.List;
import java.util.Queue;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.reporter.EventReporter;
import org.apache.gobblin.metrics.reporter.util.AvroJsonSerializer;
import org.apache.gobblin.metrics.reporter.util.AvroSerializer;
import org.apache.gobblin.metrics.reporter.util.FixedSchemaVersionWriter;
import org.apache.gobblin.metrics.reporter.util.SchemaVersionWriter;


/**
 * Reports {@link GobblinTrackingEvent} to a Kafka topic serialized as JSON.
 */
public class KafkaEventReporter extends EventReporter {

  protected final AvroSerializer<GobblinTrackingEvent> serializer;
  private final Pusher kafkaPusher;

  public KafkaEventReporter(Builder<?> builder) throws IOException {
    super(builder);

    this.serializer = this.closer.register(
        createSerializer(new FixedSchemaVersionWriter()));

    if(builder.kafkaPusher.isPresent()) {
      this.kafkaPusher = builder.kafkaPusher.get();
    } else {
        String pusherClassName = builder.pusherClassName.or(PusherUtils.DEFAULT_KAFKA_PUSHER_CLASS_NAME);
        this.kafkaPusher = PusherUtils.getPusher(pusherClassName, builder.brokers, builder.topic, builder.config);
    }
    this.closer.register(this.kafkaPusher);
  }

  @Override
  public void reportEventQueue(Queue<GobblinTrackingEvent> queue) {
    GobblinTrackingEvent nextEvent;
    List<byte[]> events = Lists.newArrayList();

    while(null != (nextEvent = queue.poll())) {
      events.add(this.serializer.serializeRecord(nextEvent));
    }

    if (!events.isEmpty()) {
      this.kafkaPusher.pushMessages(events);
    }

  }

  protected AvroSerializer<GobblinTrackingEvent> createSerializer(SchemaVersionWriter schemaVersionWriter) throws IOException {
    return new AvroJsonSerializer<GobblinTrackingEvent>(GobblinTrackingEvent.SCHEMA$, schemaVersionWriter);
  }

  /**
   * Returns a new {@link Builder} for {@link KafkaEventReporter}.
   * Will automatically add all Context tags to the reporter.
   *
   * @param context the {@link MetricContext} to report
   * @return KafkaReporter builder
   * @deprecated this method is bugged. Use {@link Factory#forContext} instead.
   */
  @Deprecated
  public static Builder<? extends Builder> forContext(MetricContext context) {
    return new BuilderImpl(context);
  }

  public static class BuilderImpl extends Builder<BuilderImpl> {
    private BuilderImpl(MetricContext context) {
      super(context);
    }

    @Override
    protected BuilderImpl self() {
      return this;
    }
  }

  public static class Factory {
    /**
     * Returns a new {@link Builder} for {@link KafkaEventReporter}.
     * Will automatically add all Context tags to the reporter.
     *
     * @param context the {@link MetricContext} to report
     * @return KafkaReporter builder
     */
    public static BuilderImpl forContext(MetricContext context) {
      return new BuilderImpl(context);
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
    protected Optional<Pusher> kafkaPusher;
    protected Optional<Config> config = Optional.absent();
    protected Optional<String> pusherClassName = Optional.absent();

    protected Builder(MetricContext context) {
      super(context);
      this.kafkaPusher = Optional.absent();
    }

    /**
     * Set {@link Pusher} to use.
     */
    public T withKafkaPusher(Pusher pusher) {
      this.kafkaPusher = Optional.of(pusher);
      return self();
    }

    /**
     * Set additional configuration.
     */
    public T withConfig(Config config) {
      this.config = Optional.of(config);
      return self();
    }

    /**
     * Set a {@link Pusher} class name
     */
    public T withPusherClassName(String pusherClassName) {
      this.pusherClassName = Optional.of(pusherClassName);
      return self();
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
