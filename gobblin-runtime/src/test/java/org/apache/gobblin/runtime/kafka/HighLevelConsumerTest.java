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

package org.apache.gobblin.runtime.kafka;

import java.io.File;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.api.client.util.Lists;
import com.google.common.base.Optional;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.kafka.client.Kafka09ConsumerClient;
import org.apache.gobblin.kafka.client.KafkaConsumerRecord;
import org.apache.gobblin.kafka.client.MockBatchKafkaConsumerClient;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaPartition;
import org.apache.gobblin.util.ConfigUtils;

@Slf4j
public class HighLevelConsumerTest {

  public HighLevelConsumerTest()
      throws InterruptedException, RuntimeException {
  }

  public static Config getSimpleConfig(Optional<String> prefix) {
    Properties properties = new Properties();
    properties.put(getConfigKey(prefix, ConfigurationKeys.KAFKA_BROKERS), "localhost:1234");
    properties.put(getConfigKey(prefix, Kafka09ConsumerClient.GOBBLIN_CONFIG_VALUE_DESERIALIZER_CLASS_KEY), Kafka09ConsumerClient.KAFKA_09_DEFAULT_KEY_DESERIALIZER);
    properties.put(getConfigKey(prefix, "zookeeper.connect"), "zookeeper");
    properties.put(ConfigurationKeys.STATE_STORE_ENABLED, "true");
    File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();
    properties.put(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, tmpDir.toString());

    return ConfigFactory.parseProperties(properties);
  }

  private static String getConfigKey(Optional<String> prefix, String key) {
    return prefix.isPresent() ? prefix.get() + "." + key : key;
  }

  @Test
  public void testConsumer() throws Exception {
    String topic = this.getClass().getSimpleName();
    Properties properties = new Properties();
    properties.put(ConfigurationKeys.KAFKA_BROKERS, "dummy_broker");
    int numPartitions = 5;
    int msgsPerPartition = 10;
    int consumeBatchSize = 10;
    List<KafkaConsumerRecord> records = createMessages(topic, numPartitions, msgsPerPartition);
    MockedHighLevelConsumer consumer = new MockedHighLevelConsumer(topic, ConfigUtils.propertiesToConfig(properties), numPartitions, records, consumeBatchSize);
    consumer.startAsync();
    consumer.awaitRunning();

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
    }

    Assert.assertEquals(consumer.getMessages().size(), numPartitions * msgsPerPartition);
    consumer.shutDown();
  }

  @Test
  public void testManualOffsetCommit() throws Exception {
    String topic = this.getClass().getSimpleName();
    Properties properties = new Properties();
    properties.put(ConfigurationKeys.KAFKA_BROKERS, "dummy_broker");
    properties.put("enable.auto.commit", false);
    // Setting this to a second to make sure we are committing offsets frequently
    properties.put(HighLevelConsumer.OFFSET_COMMIT_TIME_THRESHOLD_SECS_KEY, 1);
    int numPartitions = 3;
    int msgsPerPartition = 10;
    int consumeBatchSize = 10;
    List<KafkaConsumerRecord> records = createMessages(topic, numPartitions, msgsPerPartition);
    MockedHighLevelConsumer consumer = new MockedHighLevelConsumer(topic, ConfigUtils.propertiesToConfig(properties), numPartitions, records, consumeBatchSize);
    consumer.startAsync();
    consumer.awaitRunning();

    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {

    }

    Assert.assertEquals(consumer.getMessages().size(), numPartitions * msgsPerPartition);
    MockBatchKafkaConsumerClient client  = (MockBatchKafkaConsumerClient) consumer.getGobblinKafkaConsumerClient();
    Assert.assertEquals(client.getCommittedOffsets().size(), numPartitions);
    for(int i=0;i< numPartitions;i++) {
      Assert.assertTrue(client.getCommittedOffsets().get(new KafkaPartition.Builder().withId(i).withTopicName(topic).build()) == msgsPerPartition - 1);
    }
    consumer.shutDown();
  }

  private List<KafkaConsumerRecord> createMessages(String topic, int numPartitions, int msgsPerPartition) {
    List<KafkaConsumerRecord> records = Lists.newArrayList();

    for(int i=0; i<numPartitions; i++) {
      for(int j=0; j<msgsPerPartition; j++) {
        KafkaConsumerRecord record = new Kafka09ConsumerClient.Kafka09ConsumerRecord<>(new ConsumerRecord<>(topic, i, j, null, "partition_" + i + "_msg_" + j));
        records.add(record);
      }
    }

    return records;
  }
}
