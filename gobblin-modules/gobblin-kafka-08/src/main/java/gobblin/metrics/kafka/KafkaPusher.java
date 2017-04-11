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

package gobblin.metrics.kafka;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import javax.annotation.Nullable;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


/**
 * Establishes a connection to a Kafka cluster and pushed byte messages to a specified topic.
 */
public class KafkaPusher implements Closeable {

  private final String topic;
  private final ProducerCloseable<String, byte[]> producer;
  private final Closer closer;

  public KafkaPusher(String brokers, String topic) {
    this.closer = Closer.create();

    this.topic = topic;

    Properties props = new Properties();
    props.put("metadata.broker.list", brokers);
    props.put("serializer.class", "kafka.serializer.DefaultEncoder");
    props.put("request.required.acks", "1");

    ProducerConfig config = new ProducerConfig(props);
    this.producer = createProducer(config);
  }

  /**
   * Push all mbyte array messages to the Kafka topic.
   * @param messages List of byte array messages to push to Kakfa.
   */
  public void pushMessages(List<byte[]> messages) {
    List<KeyedMessage<String, byte[]>> keyedMessages = Lists.transform(messages,
        new Function<byte[], KeyedMessage<String, byte[]>>() {
          @Nullable
          @Override
          public KeyedMessage<String, byte[]> apply(byte[] bytes) {
            return new KeyedMessage<String, byte[]>(topic, bytes);
          }
        });
    this.producer.send(keyedMessages);
  }

  @Override
  public void close()
      throws IOException {
    this.closer.close();
  }

  /**
   * Actually creates the Kafka producer.
   */
  protected ProducerCloseable<String, byte[]> createProducer(ProducerConfig config) {
    return this.closer.register(new ProducerCloseable<String, byte[]>(config));
  }
}
