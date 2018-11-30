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

import avro.shaded.com.google.common.base.Optional;
import avro.shaded.com.google.common.collect.Lists;
import com.google.common.base.Splitter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Queue;

/**
 * {@link org.apache.gobblin.metrics.reporter.EventReporter} that emits events to Kafka as serialized Avro records with a key.
 * Key for these kafka messages is obtained from values of properties provided via {@link #keyPropertyName}.
 * If the GobblinTrackingEvent does not contain any of the required property, key is set to null. In that case, this reporter
 * will act like a {@link org.apache.gobblin.metrics.kafka.KafkaAvroEventReporter}
 */
@Slf4j
public class KafkaAvroEventKeyValueReporter extends KafkaAvroEventReporter {
  private static final Splitter COMMA_SEPARATOR = Splitter.on(",").omitEmptyStrings().trimResults();
  private Optional<List<String>> keyName = Optional.absent();
  public static final String keyPropertyName = "metrics.reporting.kafkaPusherKeys";

  protected KafkaAvroEventKeyValueReporter(BuilderImpl builder) throws IOException {
    super(builder);
    if (builder.keyName.size() > 0) {
      this.keyName = Optional.of(builder.keyName);
    }
  }

  @Override
  public void reportEventQueue(Queue<GobblinTrackingEvent> queue) {
    GobblinTrackingEvent nextEvent;
    List<Pair<String, byte[]>> events = com.google.common.collect.Lists.newArrayList();

    while(null != (nextEvent = queue.poll())) {
      StringBuilder sb = new StringBuilder();
      String key = null;
      if (keyName.isPresent()) {
        try {
          for (String keyPart : keyName.get()) {
            if (nextEvent.getMetadata().containsKey(keyPart)) {
              sb.append(nextEvent.getMetadata().get(keyPart));
            } else {
              throw new KeyPartNotFoundException(keyPart);
            }
          }
          key = sb.toString();
        } catch (KeyPartNotFoundException e) {
          log.error("{} not found in the GobblinTrackingEvent." +
              "Setting key to null." + e.expectedKey);
          key = null;
        }
      }
      events.add(Pair.of(key, this.serializer.serializeRecord(nextEvent)));
    }

    if (!events.isEmpty()) {
      this.kafkaPusher.pushMessages(events);
    }

  }

  public static class BuilderImpl extends Builder<KafkaAvroEventKeyValueReporter.BuilderImpl> {
    private List<String> keyName = Lists.newArrayList();

    public BuilderImpl(MetricContext context, Properties properties) {
      super(context);
      if (properties.containsKey(keyPropertyName)) {
         this.keyName = COMMA_SEPARATOR.splitToList(properties.getProperty(keyPropertyName));
      } else {
        log.warn("Keys for KafkaAvroEventKeyValueReporter are not provided. Without keys, it will act like a KafkaAvroEventReporter.");
      }
    }

    @Override
    public KafkaAvroEventReporter build(String brokers, String topic) throws IOException {
      this.brokers = brokers;
      this.topic = topic;
      return new KafkaAvroEventKeyValueReporter(this);
    }

    @Override
    protected KafkaAvroEventKeyValueReporter.BuilderImpl self() {
      return this;
    }
  }

  private static class KeyPartNotFoundException extends IllegalArgumentException {
    private final String expectedKey;
    KeyPartNotFoundException(String key) {
      this.expectedKey = key;
    }
  }
}
