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

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;

/**
 * {@link org.apache.gobblin.metrics.reporter.EventReporter} that emits events to Kafka as serialized Avro records with a key.
 * Key for these kafka messages is obtained from values of properties provided via {@link ConfigurationKeys#METRICS_REPORTING_EVENTS_KAFKAPUSHERKEYS}.
 * If the GobblinTrackingEvent does not contain any of the required property, key is set to null. In that case, this reporter
 * will act like a {@link org.apache.gobblin.metrics.kafka.KafkaAvroEventReporter}
 */
@Slf4j
public class KafkaEventKeyValueReporter extends KafkaEventReporter {
  private Optional<List<String>> keys = Optional.absent();

  protected KafkaEventKeyValueReporter(Builder<?> builder) throws IOException {
    super(builder);
    if (builder.keys.size() > 0) {
      this.keys = Optional.of(builder.keys);
    } else {
      log.warn("Cannot find keys for key-value reporter. Please set it with property {}",
          ConfigurationKeys.METRICS_REPORTING_EVENTS_KAFKAPUSHERKEYS);
    }
  }

  @Override
  public void reportEventQueue(Queue<GobblinTrackingEvent> queue) {
    GobblinTrackingEvent nextEvent;
    List<Pair<String, byte[]>> events = Lists.newArrayList();

    while(null != (nextEvent = queue.poll())) {
      StringBuilder sb = new StringBuilder();
      String key = null;
      if (keys.isPresent()) {
        for (String keyPart : keys.get()) {
          if (nextEvent.getMetadata().containsKey(keyPart)) {
            sb.append(nextEvent.getMetadata().get(keyPart));
          } else {
            log.debug("{} not found in the GobblinTrackingEvent. Setting key to null.", keyPart);
            sb = null;
            break;
          }
        }
        key = (sb == null) ? null : sb.toString();
      }
      events.add(Pair.of(key, this.serializer.serializeRecord(nextEvent)));
    }

    if (!events.isEmpty()) {
      this.kafkaPusher.pushMessages(events);
    }
  }

  private static class BuilderImpl extends Builder<BuilderImpl> {
    private BuilderImpl(MetricContext context) {
      super(context);
    }

    @Override
    protected BuilderImpl self() {
      return this;
    }
  }

  public static abstract class Factory {
    /**
     * Returns a new {@link Builder} for {@link KafkaEventKeyValueReporter}.
     *
     * @param context the {@link MetricContext} to report
     * @return KafkaAvroReporter builder
     */
    public static BuilderImpl forContext(MetricContext context) {
      return new BuilderImpl(context);
    }
  }

  /**
   * Builder for {@link KafkaEventKeyValueReporter}.
   * Defaults to no filter, reporting rates in seconds and times in milliseconds.
   */
  public static abstract class Builder<T extends Builder<T>> extends KafkaEventReporter.Builder<T> {
    private List<String> keys = Lists.newArrayList();

    protected Builder(MetricContext context) {
      super(context);
    }

    public T withKeys(List<String> keys) {
      this.keys = keys;
      return self();
    }

    /**
     * Builds and returns {@link KafkaAvroEventReporter}.
     *
     * @param brokers string of Kafka brokers
     * @param topic topic to send metrics to
     * @return KafkaAvroReporter
     */
    public KafkaEventKeyValueReporter build(String brokers, String topic) throws IOException {
      this.brokers = brokers;
      this.topic = topic;
      return new KafkaEventKeyValueReporter(this);
    }
  }
}
