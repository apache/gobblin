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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaOffsetRetrievalFailureException;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaPartition;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaTopic;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;


/**
 * A {@link GobblinKafkaConsumerClient} that uses kafka 1.1 consumer client. Use {@link Factory#create(Config)} to create
 * new Kafka1.1ConsumerClients. The {@link Config} used to create clients must have required key {@value #GOBBLIN_CONFIG_VALUE_DESERIALIZER_CLASS_KEY}
 *
 * @param <K> Message key type
 * @param <V> Message value type
 */
public class Kafka1ConsumerClient<K, V> extends AbstractBaseKafkaConsumerClient {

  private static final String CLIENT_BOOTSTRAP_SERVERS_KEY = "bootstrap.servers";
  private static final String CLIENT_ENABLE_AUTO_COMMIT_KEY = "enable.auto.commit";
  private static final String CLIENT_SESSION_TIMEOUT_KEY = "session.timeout.ms";
  private static final String CLIENT_KEY_DESERIALIZER_CLASS_KEY = "key.deserializer";
  private static final String CLIENT_VALUE_DESERIALIZER_CLASS_KEY = "value.deserializer";
  private static final String CLIENT_GROUP_ID = "group.id";

  private static final String DEFAULT_ENABLE_AUTO_COMMIT = Boolean.toString(false);
  private static final String DEFAULT_KEY_DESERIALIZER =
      "org.apache.kafka.common.serialization.StringDeserializer";
  private static final String DEFAULT_GROUP_ID = "kafka1";

  public static final String GOBBLIN_CONFIG_KEY_DESERIALIZER_CLASS_KEY = CONFIG_PREFIX
      + CLIENT_KEY_DESERIALIZER_CLASS_KEY;
  public static final String GOBBLIN_CONFIG_VALUE_DESERIALIZER_CLASS_KEY = CONFIG_PREFIX
      + CLIENT_VALUE_DESERIALIZER_CLASS_KEY;

  private static final Config FALLBACK =
      ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(CLIENT_ENABLE_AUTO_COMMIT_KEY, DEFAULT_ENABLE_AUTO_COMMIT)
          .put(CLIENT_KEY_DESERIALIZER_CLASS_KEY, DEFAULT_KEY_DESERIALIZER)
          .put(CLIENT_GROUP_ID, DEFAULT_GROUP_ID)
          .build());

  private final Consumer<K, V> consumer;

  private Kafka1ConsumerClient(Config config) {
    super(config);
    Preconditions.checkArgument(config.hasPath(GOBBLIN_CONFIG_VALUE_DESERIALIZER_CLASS_KEY),
        "Missing required property " + GOBBLIN_CONFIG_VALUE_DESERIALIZER_CLASS_KEY);

    Properties props = new Properties();
    props.put(CLIENT_BOOTSTRAP_SERVERS_KEY, Joiner.on(",").join(super.brokers));
    props.put(CLIENT_SESSION_TIMEOUT_KEY, super.socketTimeoutMillis);

    // grab all the config under "source.kafka" and add the defaults as fallback.
    Config baseConfig = ConfigUtils.getConfigOrEmpty(config, CONFIG_NAMESPACE).withFallback(FALLBACK);
    // get the "source.kafka.consumerConfig" config for extra config to pass along to Kafka with a fallback to the
    // shared config that start with "gobblin.kafka.sharedConfig"
    Config specificConfig = ConfigUtils.getConfigOrEmpty(baseConfig, CONSUMER_CONFIG).withFallback(
        ConfigUtils.getConfigOrEmpty(config, ConfigurationKeys.SHARED_KAFKA_CONFIG_PREFIX));
    // The specific config overrides settings in the base config
    Config scopedConfig = specificConfig.withFallback(baseConfig.withoutPath(CONSUMER_CONFIG));
    props.putAll(ConfigUtils.configToProperties(scopedConfig));

    this.consumer = new KafkaConsumer<>(props);
  }

  public Kafka1ConsumerClient(Config config, Consumer<K, V> consumer) {
    super(config);
    this.consumer = consumer;
  }

  @Override
  public List<KafkaTopic> getTopics() {
    return FluentIterable.from(this.consumer.listTopics().entrySet())
        .transform(new Function<Entry<String, List<PartitionInfo>>, KafkaTopic>() {
          @Override
          public KafkaTopic apply(Entry<String, List<PartitionInfo>> filteredTopicEntry) {
            return new KafkaTopic(filteredTopicEntry.getKey(), Lists.transform(filteredTopicEntry.getValue(),
                PARTITION_INFO_TO_KAFKA_PARTITION));
          }
        }).toList();
  }

  @Override
  public long getEarliestOffset(KafkaPartition partition) throws KafkaOffsetRetrievalFailureException {
    TopicPartition topicPartition = new TopicPartition(partition.getTopicName(), partition.getId());
    List<TopicPartition> topicPartitionList = Collections.singletonList(topicPartition);
    this.consumer.assign(topicPartitionList);
    this.consumer.seekToBeginning(topicPartitionList);

    return this.consumer.position(topicPartition);
  }

  @Override
  public long getLatestOffset(KafkaPartition partition) throws KafkaOffsetRetrievalFailureException {
    TopicPartition topicPartition = new TopicPartition(partition.getTopicName(), partition.getId());
    List<TopicPartition> topicPartitionList = Collections.singletonList(topicPartition);
    this.consumer.assign(topicPartitionList);
    this.consumer.seekToEnd(topicPartitionList);

    return this.consumer.position(topicPartition);
  }

  @Override
  public Iterator<KafkaConsumerRecord> consume(KafkaPartition partition, long nextOffset, long maxOffset) {

    if (nextOffset > maxOffset) {
      return null;
    }

    this.consumer.assign(Lists.newArrayList(new TopicPartition(partition.getTopicName(), partition.getId())));
    this.consumer.seek(new TopicPartition(partition.getTopicName(), partition.getId()), nextOffset);
    ConsumerRecords<K, V> consumerRecords = consumer.poll(super.fetchTimeoutMillis);
    return Iterators.transform(consumerRecords.iterator(), new Function<ConsumerRecord<K, V>, KafkaConsumerRecord>() {

      @Override
      public KafkaConsumerRecord apply(ConsumerRecord<K, V> input) {
        return new Kafka1ConsumerRecord<>(input);
      }
    });
  }

  @Override
  public void close() throws IOException {
    this.consumer.close();
  }

  private static final Function<PartitionInfo, KafkaPartition> PARTITION_INFO_TO_KAFKA_PARTITION =
      new Function<PartitionInfo, KafkaPartition>() {
        @Override
        public KafkaPartition apply(@Nonnull PartitionInfo partitionInfo) {
          return new KafkaPartition.Builder().withId(partitionInfo.partition()).withTopicName(partitionInfo.topic())
              .withLeaderId(partitionInfo.leader().id())
              .withLeaderHostAndPort(partitionInfo.leader().host(), partitionInfo.leader().port()).build();
        }
      };

  /**
   * A factory class to instantiate {@link Kafka1ConsumerClient}
   */
  public static class Factory implements GobblinKafkaConsumerClientFactory {
    @SuppressWarnings("rawtypes")
    @Override
    public GobblinKafkaConsumerClient create(Config config) {
      return new Kafka1ConsumerClient(config);
    }
  }

  /**
   * A record returned by {@link Kafka1ConsumerClient}
   *
   * @param <K> Message key type
   * @param <V> Message value type
   */
  @EqualsAndHashCode(callSuper = true)
  @ToString
  public static class Kafka1ConsumerRecord<K, V> extends BaseKafkaConsumerRecord implements
      DecodeableKafkaRecord<K, V> {
    private final ConsumerRecord<K, V> consumerRecord;

    public Kafka1ConsumerRecord(ConsumerRecord<K, V> consumerRecord) {
      // Kafka 09 consumerRecords do not provide value size.
      // Only 08 and 11 versions provide them.
      super(consumerRecord.offset(), consumerRecord.serializedValueSize() , consumerRecord.topic(), consumerRecord.partition());
      this.consumerRecord = consumerRecord;
    }

    @Override
    public K getKey() {
      return this.consumerRecord.key();
    }

    @Override
    public V getValue() {
      return this.consumerRecord.value();
    }
  }
}
