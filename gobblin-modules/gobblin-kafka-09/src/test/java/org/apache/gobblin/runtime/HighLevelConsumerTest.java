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

package org.apache.gobblin.runtime;

import java.io.File;
import java.util.List;
import java.util.Properties;

import org.mockito.Mockito;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import com.google.api.client.util.Lists;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.kafka.KafkaTestBase;
import org.apache.gobblin.kafka.client.AbstractBaseKafkaConsumerClient;
import org.apache.gobblin.kafka.client.Kafka09ConsumerClient;
import org.apache.gobblin.kafka.writer.Kafka09DataWriter;
import org.apache.gobblin.kafka.writer.KafkaWriterConfigurationKeys;
import org.apache.gobblin.runtime.kafka.HighLevelConsumer;
import org.apache.gobblin.runtime.kafka.MockedHighLevelConsumer;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaPartition;
import org.apache.gobblin.test.TestUtils;
import org.apache.gobblin.testing.AssertWithBackoff;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.writer.AsyncDataWriter;
import org.apache.gobblin.writer.WriteCallback;

@Test
@Slf4j
public class HighLevelConsumerTest extends KafkaTestBase {
  private static final String BOOTSTRAP_SERVERS_KEY = "bootstrap.servers";
  private static final String KAFKA_AUTO_OFFSET_RESET_KEY = "auto.offset.reset";
  private static final String SOURCE_KAFKA_CONSUMERCONFIG_KEY_WITH_DOT = AbstractBaseKafkaConsumerClient.CONFIG_NAMESPACE + "." + AbstractBaseKafkaConsumerClient.CONSUMER_CONFIG + ".";
  private static final String TOPIC = HighLevelConsumerTest.class.getSimpleName();
  private static final int NUM_PARTITIONS = 2;
  private static final int NUM_MSGS = 10;

  private Closer _closer;
  private String _kafkaBrokers;

  public HighLevelConsumerTest()
      throws InterruptedException, RuntimeException {
    super();
    _kafkaBrokers = "127.0.0.1:" + this.getKafkaServerPort();
  }

  @BeforeSuite
  public void beforeSuite()
      throws Exception {
    startServers();
    _closer = Closer.create();
    Properties producerProps = new Properties();
    producerProps.setProperty(KafkaWriterConfigurationKeys.KAFKA_TOPIC, TOPIC);
    producerProps
        .setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + BOOTSTRAP_SERVERS_KEY, _kafkaBrokers);
    producerProps.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX
            + KafkaWriterConfigurationKeys.VALUE_SERIALIZER_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProps.setProperty(KafkaWriterConfigurationKeys.CLUSTER_ZOOKEEPER, this.getZkConnectString());
    producerProps.setProperty(KafkaWriterConfigurationKeys.PARTITION_COUNT, String.valueOf(NUM_PARTITIONS));
    producerProps.setProperty(KafkaWriterConfigurationKeys.DELETE_TOPIC_IF_EXISTS, String.valueOf(true));
    AsyncDataWriter<byte[]> dataWriter = _closer.register(new Kafka09DataWriter<byte[], byte[]>(producerProps));

    List<byte[]> records = createByteArrayMessages();
    WriteCallback mock = Mockito.mock(WriteCallback.class);
    for (byte[] record : records) {
      dataWriter.write(record, mock);
    }
    dataWriter.flush();
  }

  public static Config getSimpleConfig(Optional<String> prefix) {
    Properties properties = new Properties();
    properties.put(getConfigKey(prefix, ConfigurationKeys.KAFKA_BROKERS), "127.0.0.1:" + TestUtils.findFreePort());
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
  public void testConsumerAutoOffsetCommit() throws Exception {
    Properties consumerProps = new Properties();
    consumerProps.setProperty(ConfigurationKeys.KAFKA_BROKERS, _kafkaBrokers);
    consumerProps.setProperty(Kafka09ConsumerClient.GOBBLIN_CONFIG_VALUE_DESERIALIZER_CLASS_KEY, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumerProps.setProperty(SOURCE_KAFKA_CONSUMERCONFIG_KEY_WITH_DOT + KAFKA_AUTO_OFFSET_RESET_KEY, "earliest");
    //Generate a brand new consumer group id to ensure there are no previously committed offsets for this group id
    String consumerGroupId = Joiner.on("-").join(TOPIC, "auto", System.currentTimeMillis());
    consumerProps.setProperty(SOURCE_KAFKA_CONSUMERCONFIG_KEY_WITH_DOT + HighLevelConsumer.GROUP_ID_KEY, consumerGroupId);
    consumerProps.setProperty(HighLevelConsumer.ENABLE_AUTO_COMMIT_KEY, "true");
    MockedHighLevelConsumer consumer = new MockedHighLevelConsumer(TOPIC, ConfigUtils.propertiesToConfig(consumerProps), NUM_PARTITIONS);
    consumer.startAsync().awaitRunning();

    consumer.awaitExactlyNMessages(NUM_MSGS, 10000);
    consumer.shutDown();
  }

  @Test
  public void testConsumerManualOffsetCommit() throws Exception {
    Properties consumerProps = new Properties();
    consumerProps.setProperty(ConfigurationKeys.KAFKA_BROKERS, _kafkaBrokers);
    consumerProps.setProperty(Kafka09ConsumerClient.GOBBLIN_CONFIG_VALUE_DESERIALIZER_CLASS_KEY, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumerProps.setProperty(SOURCE_KAFKA_CONSUMERCONFIG_KEY_WITH_DOT + KAFKA_AUTO_OFFSET_RESET_KEY, "earliest");
    //Generate a brand new consumer group id to ensure there are no previously committed offsets for this group id
    String consumerGroupId = Joiner.on("-").join(TOPIC, "manual", System.currentTimeMillis());
    consumerProps.setProperty(SOURCE_KAFKA_CONSUMERCONFIG_KEY_WITH_DOT + HighLevelConsumer.GROUP_ID_KEY, consumerGroupId);
    // Setting this to a second to make sure we are committing offsets frequently
    consumerProps.put(HighLevelConsumer.OFFSET_COMMIT_TIME_THRESHOLD_SECS_KEY, 1);

    MockedHighLevelConsumer consumer = new MockedHighLevelConsumer(TOPIC, ConfigUtils.propertiesToConfig(consumerProps),
        NUM_PARTITIONS);
    consumer.startAsync().awaitRunning();

    consumer.awaitExactlyNMessages(NUM_MSGS, 10000);

    for(int i=0; i< NUM_PARTITIONS; i++) {
      KafkaPartition partition = new KafkaPartition.Builder().withTopicName(TOPIC).withId(i).build();
      AssertWithBackoff.assertTrue(input -> consumer.getCommittedOffsets().containsKey(partition),
          5000, "waiting for committing offsets", log, 2, 1000);
    }
    consumer.shutDown();
  }

  private List<byte[]> createByteArrayMessages() {
    List<byte[]> records = Lists.newArrayList();

    for(int i=0; i<NUM_MSGS; i++) {
      byte[] msg = ("msg_" + i).getBytes();
      records.add(msg);
    }
    return records;
  }

  @AfterSuite
  public void afterSuite() {
    try {
      _closer.close();
    } catch (Exception e) {
      System.out.println("Failed to close data writer." +  e);
    } finally {
      try {
        close();
      } catch (Exception e) {
        System.out.println("Failed to close Kafka server."+ e);
      }
    }
  }
}
