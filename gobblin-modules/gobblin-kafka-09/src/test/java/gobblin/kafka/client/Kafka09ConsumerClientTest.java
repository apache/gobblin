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
package gobblin.kafka.client;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.source.extractor.extract.kafka.KafkaPartition;
import gobblin.kafka.client.Kafka09ConsumerClient.Kafka09ConsumerRecord;


public class Kafka09ConsumerClientTest {

  @Test
  public void testConsume() throws Exception {
    Config testConfig = ConfigFactory.parseMap(ImmutableMap.of(ConfigurationKeys.KAFKA_BROKERS, "test"));
    MockConsumer<String, String> consumer = new MockConsumer<String, String>(OffsetResetStrategy.NONE);
    consumer.assign(Arrays.asList(new TopicPartition("test_topic", 0)));

    HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
    beginningOffsets.put(new TopicPartition("test_topic", 0), 0L);
    consumer.updateBeginningOffsets(beginningOffsets);

    ConsumerRecord<String, String> record0 = new ConsumerRecord<>("test_topic", 0, 0L, "key", "value0");
    ConsumerRecord<String, String> record1 = new ConsumerRecord<>("test_topic", 0, 1L, "key", "value1");
    ConsumerRecord<String, String> record2 = new ConsumerRecord<>("test_topic", 0, 2L, "key", "value2");

    consumer.addRecord(record0);
    consumer.addRecord(record1);
    consumer.addRecord(record2);

    try (Kafka09ConsumerClient<String, String> kafka09Client = new Kafka09ConsumerClient<>(testConfig, consumer);) {

      // Consume from 0 offset
      Set<KafkaConsumerRecord> consumedRecords =
          Sets.newHashSet(kafka09Client.consume(new KafkaPartition.Builder().withId(0).withTopicName("test_topic")
              .build(), 0l, 100l));

      Set<Kafka09ConsumerRecord<String, String>> expected =
          ImmutableSet.<Kafka09ConsumerRecord<String, String>> of(new Kafka09ConsumerRecord<>(record0),
              new Kafka09ConsumerRecord<>(record1), new Kafka09ConsumerRecord<>(record2));
      Assert.assertEquals(consumedRecords, expected);

    }

  }
}
