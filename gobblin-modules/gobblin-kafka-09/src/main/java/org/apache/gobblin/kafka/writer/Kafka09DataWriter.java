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

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.google.common.base.Throwables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import kafka.utils.ZkUtils;
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import lombok.extern.slf4j.Slf4j;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.writer.AsyncDataWriter;
import org.apache.gobblin.writer.WriteCallback;
import org.apache.gobblin.writer.WriteResponse;
import org.apache.gobblin.writer.WriteResponseFuture;
import org.apache.gobblin.writer.WriteResponseMapper;


/**
 * Implementation of KafkaWriter that wraps a {@link KafkaProducer}.
 * This provides at-least once semantics.
 * Applications should expect data to be possibly written to Kafka even if the overall Gobblin job fails.
 *
 */
@Slf4j
public class Kafka09DataWriter<D> implements AsyncDataWriter<D> {

  
  private static final WriteResponseMapper<RecordMetadata> WRITE_RESPONSE_WRAPPER =
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

  private final Producer<String, D> producer;
  private final String topic;

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

  public Kafka09DataWriter(Properties props) {
    this(getKafkaProducer(props), ConfigFactory.parseProperties(props));
  }

  public Kafka09DataWriter(Producer producer, Config config) {
    this.topic = config.getString(KafkaWriterConfigurationKeys.KAFKA_TOPIC);
    provisionTopic(topic,config);
    this.producer = producer;
  }

  @Override
  public void close()
      throws IOException {
    log.debug("Close called");
    this.producer.close();
  }

  @Override
  public Future<WriteResponse> write(final D record, final WriteCallback callback) {
    return new WriteResponseFuture<>(this.producer.send(new ProducerRecord<String, D>(topic, record), new Callback() {
      @Override
      public void onCompletion(final RecordMetadata metadata, Exception exception) {
        if (exception != null) {
          callback.onFailure(exception);
        } else {
          callback.onSuccess(WRITE_RESPONSE_WRAPPER.wrap(metadata));
        }
      }
    }), WRITE_RESPONSE_WRAPPER);
  }

  @Override
  public void flush()
      throws IOException {
	  this.producer.flush();
  }
  
  private void provisionTopic(String topicName,Config config) {
    String zooKeeperPropKey = KafkaWriterConfigurationKeys.CLUSTER_ZOOKEEPER;
    if(!config.hasPath(zooKeeperPropKey)) {
     log.debug("Topic "+topicName+" is configured without the partition and replication");
     return;
    }
    String zookeeperConnect = config.getString(zooKeeperPropKey);
    int sessionTimeoutMs = ConfigUtils.getInt(config, KafkaWriterConfigurationKeys.ZOOKEEPER_SESSION_TIMEOUT, KafkaWriterConfigurationKeys.ZOOKEEPER_SESSION_TIMEOUT_DEFAULT);
    int connectionTimeoutMs = ConfigUtils.getInt(config, KafkaWriterConfigurationKeys.ZOOKEEPER_CONNECTION_TIMEOUT, KafkaWriterConfigurationKeys.ZOOKEEPER_CONNECTION_TIMEOUT_DEFAULT);
    // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then
    // createTopic() will only seem to work (it will return without error).  The topic will exist in
    // only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the
    // topic.
    ZkClient zkClient = new ZkClient(zookeeperConnect, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);
    // Security for Kafka was added in Kafka 0.9.0.0
    ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), false);
    int partitions = ConfigUtils.getInt(config, KafkaWriterConfigurationKeys.PARTITION_COUNT, KafkaWriterConfigurationKeys.PARTITION_COUNT_DEFAULT);
    int replication = ConfigUtils.getInt(config, KafkaWriterConfigurationKeys.REPLICATION_COUNT, KafkaWriterConfigurationKeys.PARTITION_COUNT_DEFAULT);
    Properties topicConfig = new Properties(); 
    if(AdminUtils.topicExists(zkUtils, topicName)) {
	   log.debug("Topic"+topicName+" already Exists with replication: "+replication+" and partitions :"+partitions);
       return;
    } 
    try {
       AdminUtils.createTopic(zkUtils, topicName, partitions, replication, topicConfig);
    } catch (RuntimeException e) {
       throw new RuntimeException(e);
    }
       log.info("Created Topic "+topicName+" with replication: "+replication+" and partitions :"+partitions);
    }
}
