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
 * Establish a connection to a Kafka cluster and push byte messages to a specified topic.
 */
@Slf4j
public class KafkaProducerPusher implements Pusher<byte[]> {

  private final String topic;
  private final KafkaProducer<String, byte[]> producer;
  private final Closer closer;

  public KafkaProducerPusher(String brokers, String topic, Optional<Config> kafkaConfig) {
    this.closer = Closer.create();

    this.topic = topic;

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.RETRIES_CONFIG, 3);

    // add the kafka scoped config. if any of the above are specified then they are overridden
    if (kafkaConfig.isPresent()) {
      props.putAll(ConfigUtils.configToProperties(kafkaConfig.get()));
    }

    this.producer = createProducer(props);
  }

  public KafkaProducerPusher(String brokers, String topic) {
    this(brokers, topic, Optional.absent());
  }

  /**
   * Push all byte array messages to the Kafka topic.
   * @param messages List of byte array messages to push to Kakfa.
   */
  public void pushMessages(List<byte[]> messages) {
    for (byte[] message: messages) {
      producer.send(new ProducerRecord<>(topic, message), (recordMetadata, e) -> {
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
  protected KafkaProducer<String, byte[]> createProducer(Properties props) {
    return this.closer.register(new KafkaProducer<String, byte[]>(props));
  }
}
