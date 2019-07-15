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

package org.apache.gobblin.metrics.reporter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.StringJoiner;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.kafka.PusherUtils;
import org.apache.gobblin.util.AvroUtils;
import org.apache.gobblin.util.ConfigUtils;


/**
 * This is a raw event (GobblinTrackingEvent) key value reporter that reports events as GenericRecords without serialization
 * Configuration for this reporter start with the prefix "metrics.reporting.events"
 */
@Slf4j
public class KeyValueEventObjectReporter extends EventReporter {
  private static final String PUSHER_CONFIG = "pusherConfig";
  private static final String PUSHER_CLASS = "pusherClass";
  private static final String PUSHER_KEYS = "pusherKeys";
  private static final String KEY_DELIMITER = ",";
  private static final String KEY_SIZE_KEY = "keySize";

  protected List<String> keys;
  protected final String randomKey;
  protected KeyValuePusher pusher;
  protected final Schema schema;

  public KeyValueEventObjectReporter(Builder builder) {
    super(builder);

    Config config = builder.config.get();
    Config pusherConfig = ConfigUtils.getConfigOrEmpty(config, PUSHER_CONFIG).withFallback(config);
    String pusherClassName =
        ConfigUtils.getString(config, PUSHER_CLASS, PusherUtils.DEFAULT_KEY_VALUE_PUSHER_CLASS_NAME);
    this.pusher = (KeyValuePusher) PusherUtils
        .getPusher(pusherClassName, builder.brokers, builder.topic, Optional.of(pusherConfig));
    this.closer.register(this.pusher);

    randomKey = String.valueOf(
        new Random().nextInt(ConfigUtils.getInt(config, KEY_SIZE_KEY, ConfigurationKeys.DEFAULT_REPORTER_KEY_SIZE)));
    if (config.hasPath(PUSHER_KEYS)) {
      List<String> keys = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(config.getString(PUSHER_KEYS));
      this.keys = keys;
    } else {
      log.info(
          "Key not assigned from config. Please set it with property {} Using randomly generated number {} as key ",
          ConfigurationKeys.METRICS_REPORTING_EVENTS_PUSHERKEYS, randomKey);
    }

    schema = AvroUtils.overrideNameAndNamespace(GobblinTrackingEvent.getClassSchema(), builder.topic, builder.namespaceOverride);
  }

  @Override
  public void reportEventQueue(Queue<GobblinTrackingEvent> queue) {

    List<Pair<String, GenericRecord>> events = Lists.newArrayList();
    GobblinTrackingEvent event;

    while (null != (event = queue.poll())) {

      GenericRecord record=event;
      try {
        record = AvroUtils.convertRecordSchema(event, schema);
      } catch (IOException e){
        log.error("Unable to generate generic data record", e);
      }
      events.add(Pair.of(buildKey(record), record));
    }

    if (!events.isEmpty()) {
      this.pusher.pushKeyValueMessages(events);
    }
  }

  private String buildKey(GenericRecord record) {

    String key = randomKey;
    if (this.keys != null && this.keys.size() > 0) {
      StringJoiner joiner = new StringJoiner(KEY_DELIMITER);
      for (String keyPart : keys) {
        Optional value = AvroUtils.getFieldValue(record, keyPart);
        if (value.isPresent()) {
          joiner.add(value.get().toString());
        } else {
          log.info("{} not found in the GobblinTrackingEvent. Setting key to {}", keyPart, key);
          return key;
        }
      }

      key = joiner.toString();
    }

    return key;
  }

  public static class Builder extends EventReporter.Builder<Builder> {

    protected String brokers;
    protected String topic;
    protected Optional<Config> config = Optional.absent();
    protected Optional<Map<String, String>> namespaceOverride = Optional.absent();

    public Builder(MetricContext context) {
      super(context);
    }

    @Override
    protected Builder self() {
      return this;
    }

    /**
     * Set additional configuration.
     */
    public Builder withConfig(Config config) {
      this.config = Optional.of(config);
      return self();
    }

    public Builder namespaceOverride(Optional<Map<String, String>> namespaceOverride) {
      this.namespaceOverride = namespaceOverride;
      return self();
    }

    public KeyValueEventObjectReporter build(String brokers, String topic) {
      this.brokers = brokers;
      this.topic = topic;
      return new KeyValueEventObjectReporter(this);
    }
  }
}
