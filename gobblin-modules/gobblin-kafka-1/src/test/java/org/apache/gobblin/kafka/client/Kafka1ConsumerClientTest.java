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
package org.apache.gobblin.kafka.client;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaPartition;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;


public class Kafka1ConsumerClientTest {

  @Test
  public void testConsume() throws Exception {
    Config testConfig = ConfigFactory.parseMap(ImmutableMap.of(ConfigurationKeys.KAFKA_BROKERS, "test"));
    MockConsumer<String, String> consumer = new MockConsumer<String, String>(OffsetResetStrategy.NONE);
    consumer.assign(Arrays.asList(new TopicPartition("test_topic", 0)));

    HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
    beginningOffsets.put(new TopicPartition("test_topic", 0), 0L);
    consumer.updateBeginningOffsets(beginningOffsets);


    ConsumerRecord<String, String> record0 = new ConsumerRecord<>("test_topic", 0, 0L, 10L, TimestampType.CREATE_TIME, 0L, 3, 6, "key", "value0");
    ConsumerRecord<String, String> record1 = new ConsumerRecord<>("test_topic", 0, 1L, 11L, TimestampType.LOG_APPEND_TIME, 1L, 3, 6, "key", "value1");
    ConsumerRecord<String, String> record2 = new ConsumerRecord<>("test_topic", 0, 2L, 12L, TimestampType.LOG_APPEND_TIME, 2L, 3, 6, "key", "value2");

    consumer.addRecord(record0);
    consumer.addRecord(record1);
    consumer.addRecord(record2);

    try (Kafka1ConsumerClient<String, String> kafka1Client = new Kafka1ConsumerClient<>(testConfig, consumer);) {

      // Consume from 0 offset
      Set<KafkaConsumerRecord> consumedRecords =
          Sets.newHashSet(kafka1Client.consume(new KafkaPartition.Builder().withId(0).withTopicName("test_topic")
              .build(), 0l, 100l));

      Set<Kafka1ConsumerClient.Kafka1ConsumerRecord<String, String>> expected =
          ImmutableSet.of(new Kafka1ConsumerClient.Kafka1ConsumerRecord<>(record0),
              new Kafka1ConsumerClient.Kafka1ConsumerRecord<>(record1), new Kafka1ConsumerClient.Kafka1ConsumerRecord<>(record2));
      Assert.assertEquals(consumedRecords, expected);

      Kafka1ConsumerClient.Kafka1ConsumerRecord expected0 = expected.iterator().next();
      Assert.assertEquals(record0.timestamp(), expected0.getTimestamp());
      Assert.assertEquals(record0.timestampType() == TimestampType.LOG_APPEND_TIME, expected0.isTimestampLogAppend());
      Assert.assertEquals(record0.timestampType(), expected0.getTimestampType());
    }

  }
}
