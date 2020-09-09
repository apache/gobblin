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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.common.base.Optional;
import com.google.common.io.Closer;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.util.ConfigUtils;


/**
 * Establishes a connection to a Kafka cluster and push keyed messages to a specified topic.
 * @param <K> key type
 * @param <V> value type
 */
@Slf4j
public class KafkaKeyValueProducerPusher<K, V> implements Pusher<Pair<K, V>> {
  private final String topic;
  private final KafkaProducer<K, V> producer;
  private final Closer closer;

  public KafkaKeyValueProducerPusher(String brokers, String topic, Optional<Config> kafkaConfig) {
    this.closer = Closer.create();

    this.topic = topic;

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.RETRIES_CONFIG, 3);
    //To guarantee ordered delivery, the maximum in flight requests must be set to 1.
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

    // add the kafka scoped config. if any of the above are specified then they are overridden
    if (kafkaConfig.isPresent()) {
      props.putAll(ConfigUtils.configToProperties(kafkaConfig.get()));
    }

    this.producer = createProducer(props);
  }

  public KafkaKeyValueProducerPusher(String brokers, String topic) {
    this(brokers, topic, Optional.absent());
  }

  /**
   * Push all keyed messages to the Kafka topic.
   * @param messages List of keyed messages to push to Kakfa.
   */
  public void pushMessages(List<Pair<K, V>> messages) {
    for (Pair<K, V> message: messages) {
      this.producer.send(new ProducerRecord<>(topic, message.getKey(), message.getValue()), (recordMetadata, e) -> {
        if (e != null) {
          log.error("Failed to send message to topic {} due to exception: ", topic, e);
        }
      });
    }
  }

  @Override
  public void close()
      throws IOException {
    //Call flush() before invoking close() to ensure any buffered messages are immediately sent. This is required
    //since close() only guarantees delivery of in-flight messages.
    log.info("Flushing records from producer buffer");
    this.producer.flush();
    this.closer.close();
  }

  /**
   * Create the Kafka producer.
   */
  protected KafkaProducer<K, V> createProducer(Properties props) {
    return this.closer.register(new KafkaProducer<K, V>(props));
  }
}
