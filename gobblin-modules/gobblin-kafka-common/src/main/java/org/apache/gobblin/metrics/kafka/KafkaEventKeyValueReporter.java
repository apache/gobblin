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
import java.util.Properties;
import java.util.Queue;
import org.apache.commons.lang3.tuple.Pair;
import com.google.common.base.Splitter;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;


@Slf4j
public abstract class KafkaEventKeyValueReporter extends KafkaEventReporter {
  private static final Splitter COMMA_SEPARATOR = Splitter.on(",").omitEmptyStrings().trimResults();
  private Optional<List<String>> keyName = Optional.absent();

  public KafkaEventKeyValueReporter(BuilderImpl builder) throws IOException {
    super(builder);
    if (builder.keyName.size() > 0) {
      this.keyName = Optional.of(builder.keyName);
    }
  }

  public static abstract class BuilderImpl extends KafkaEventReporter.BuilderImpl {
    protected List<String> keyName = Lists.newArrayList();

    public BuilderImpl(MetricContext context, Properties properties) {
      super(context);
      if (properties.containsKey(ConfigurationKeys.METRICS_REPORTING_EVENTS_KAFKAPUSHERKEYS)) {
        this.keyName = COMMA_SEPARATOR.splitToList(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_EVENTS_KAFKAPUSHERKEYS));
      } else {
        log.warn("Keys for KafkaEventKeyValueReporter are not provided. Without keys, it will act like a KafkaAvroEventReporter.");
      }
    }

    @Override
    public abstract KafkaEventReporter build(String brokers, String topic) throws IOException;

    @Override
    protected KafkaEventKeyValueReporter.BuilderImpl self() {
      return this;
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
        for (String keyPart : keyName.get()) {
          if (nextEvent.getMetadata().containsKey(keyPart)) {
            sb.append(nextEvent.getMetadata().get(keyPart));
          } else {
            log.error("{} not found in the GobblinTrackingEvent. Setting key to null.", keyPart);
            sb = null;
            break;
          }
        }
        key = sb == null ? null : sb.toString();
      }
      events.add(Pair.of(key, this.serializer.serializeRecord(nextEvent)));
    }

    if (!events.isEmpty()) {
      this.kafkaPusher.pushMessages(events);
    }
  }
}
