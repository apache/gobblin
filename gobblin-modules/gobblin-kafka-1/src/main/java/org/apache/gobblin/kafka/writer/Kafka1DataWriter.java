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

package org.apache.gobblin.kafka.writer;

import com.google.common.base.Throwables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gobblin.configuration.ConfigurationException;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.writer.*;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.*;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;


/**
 * Implementation of KafkaWriter that wraps a {@link KafkaProducer}.
 * This provides at-least once semantics.
 * Applications should expect data to be possibly written to Kafka even if the overall Gobblin job fails.
 */
@Slf4j
public class Kafka1DataWriter<K, V> implements KafkaDataWriter<K, V> {

  public static final WriteResponseMapper<RecordMetadata> WRITE_RESPONSE_WRAPPER =
      new WriteResponseMapper<RecordMetadata>() {

        @Override
        public WriteResponse wrap(final RecordMetadata recordMetadata) {
          return new WriteResponse<RecordMetadata>() {
            @Override
            public RecordMetadata getRawResponse() {
              return recordMetadata;
            }

            @Override
            public String getStringResponse() {
              return recordMetadata.toString();
            }

            @Override
            public long bytesWritten() {
              return -1;
            }
          };
        }
      };

  private final Producer<K, V> producer;
  private final String topic;
  private final KafkaWriterCommonConfig commonConfig;

  public static Producer getKafkaProducer(Properties props) {
    Object producerObject = KafkaWriterHelper.getKafkaProducer(props);
    try {
      Producer producer = (Producer) producerObject;
      return producer;
    } catch (ClassCastException e) {
      log.error("Failed to instantiate Kafka producer " + producerObject.getClass().getName()
          + " as instance of Producer.class", e);
      throw Throwables.propagate(e);
    }
  }

  public Kafka1DataWriter(Properties props)
      throws ConfigurationException {
    this(getKafkaProducer(props), ConfigFactory.parseProperties(props));
  }

  public Kafka1DataWriter(Producer producer, Config config)
      throws ConfigurationException {
    this.topic = config.getString(KafkaWriterConfigurationKeys.KAFKA_TOPIC);
    provisionTopic(topic, config);
    this.producer = producer;
    this.commonConfig = new KafkaWriterCommonConfig(config);
  }

  @Override
  public void close()
      throws IOException {
    log.debug("Close called");
    this.producer.close();
  }

  @Override
  public Future<WriteResponse> write(final V record, final WriteCallback callback) {
    try {
      Pair<K, V> keyValuePair = KafkaWriterHelper.getKeyValuePair(record, this.commonConfig);
      return write(keyValuePair, callback);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create a Kafka write request", e);
    }
  }

  public Future<WriteResponse> write(Pair<K, V> keyValuePair, final WriteCallback callback) {
    try {
      return new WriteResponseFuture<>(this.producer
          .send(new ProducerRecord<>(topic, keyValuePair.getKey(), keyValuePair.getValue()), new Callback() {
            @Override
            public void onCompletion(final RecordMetadata metadata, Exception exception) {
              if (exception != null) {
                callback.onFailure(exception);
              } else {
                callback.onSuccess(WRITE_RESPONSE_WRAPPER.wrap(metadata));
              }
            }
          }), WRITE_RESPONSE_WRAPPER);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create a Kafka write request", e);
    }
  }

  @Override
  public void flush()
      throws IOException {
    this.producer.flush();
  }

  private void provisionTopic(String topicName, Config config) {
    String zooKeeperPropKey = KafkaWriterConfigurationKeys.CLUSTER_ZOOKEEPER;
    if (!config.hasPath(zooKeeperPropKey)) {
      log.debug("Topic " + topicName + " is configured without the partition and replication");
      return;
    }
    String zookeeperConnect = config.getString(zooKeeperPropKey);
    int sessionTimeoutMs = ConfigUtils.getInt(config,
        KafkaWriterConfigurationKeys.ZOOKEEPER_SESSION_TIMEOUT,
        KafkaWriterConfigurationKeys.ZOOKEEPER_SESSION_TIMEOUT_DEFAULT);
    int connectionTimeoutMs = ConfigUtils.getInt(config,
        KafkaWriterConfigurationKeys.ZOOKEEPER_CONNECTION_TIMEOUT,
        KafkaWriterConfigurationKeys.ZOOKEEPER_CONNECTION_TIMEOUT_DEFAULT);

    // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then
    // createTopic() will only seem to work (it will return without error).  The topic will exist in
    // only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the
    // topic.
    ZkClient zkClient =
        new ZkClient(zookeeperConnect, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);
    // Security for Kafka was added in Kafka 0.9.0.0
    ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), false);
    int partitions = ConfigUtils.getInt(config,
        KafkaWriterConfigurationKeys.PARTITION_COUNT,
        KafkaWriterConfigurationKeys.PARTITION_COUNT_DEFAULT);
    int replication = ConfigUtils.getInt(config,
        KafkaWriterConfigurationKeys.REPLICATION_COUNT,
        KafkaWriterConfigurationKeys.PARTITION_COUNT_DEFAULT);
    Properties topicConfig = new Properties();
    if (AdminUtils.topicExists(zkUtils, topicName)) {
      log.debug("Topic {} already exists with replication: {} and partitions: {}", topicName, replication, partitions);
      boolean deleteTopicIfExists = ConfigUtils.getBoolean(config, KafkaWriterConfigurationKeys.DELETE_TOPIC_IF_EXISTS,
          KafkaWriterConfigurationKeys.DEFAULT_DELETE_TOPIC_IF_EXISTS);
      if (!deleteTopicIfExists) {
        return;
      } else {
        log.debug("Deleting topic {}", topicName);
        AdminUtils.deleteTopic(zkUtils, topicName);
      }
    }
    AdminUtils.createTopic(zkUtils, topicName, partitions, replication, topicConfig, RackAwareMode.Disabled$.MODULE$);
    log.info("Created topic {} with replication: {} and partitions : {}", topicName, replication, partitions);
  }
}
