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

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Optional;
import com.typesafe.config.Config;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.reporter.KeyValuePusher;
import org.apache.gobblin.util.ConfigUtils;

import lombok.extern.slf4j.Slf4j;


/**
 * This is a {@Pusher} class that logs the messages
 * @param <V> message type
 */
@Slf4j
public class LoggingPusher<K, V> implements KeyValuePusher<K, V> {
  private final String brokers;
  private final String topic;
  private static final String KAFKA_TOPIC = "kafka.topic";
  private static final String NO_BROKERS = "NoBrokers";
  private static final String NO_TOPIC = "NoTopic";

  public LoggingPusher() {
    this(NO_BROKERS, NO_TOPIC, Optional.absent());
  }

  public LoggingPusher(Config config) {
    this.brokers = ConfigUtils.getString(config, ConfigurationKeys.KAFKA_BROKERS, NO_BROKERS);
    this.topic = ConfigUtils.getString(config, KAFKA_TOPIC, NO_TOPIC);
  }

  /**
   * Constructor like the one in KafkaProducerPusher for compatibility
   */
  public LoggingPusher(String brokers, String topic, Optional<Config> kafkaConfig) {
    this.brokers = brokers;
    this.topic = topic;
  }

  @Override
  public void close()
      throws IOException {
  }

  @Override
  public void pushKeyValueMessages(List<Pair<K, V>> messages) {
    for (Pair<K, V> message : messages) {
      log.info("Pushing to {}:{}: {} - {}", this.brokers, this.topic, message.getKey(), message.getValue().toString());
    }
  }

  @Override
  public void pushMessages(List<V> messages) {
    for (V message : messages) {
      log.info("Pushing to {}:{}: {}", this.brokers, this.topic, message.toString());
    }
  }
}
