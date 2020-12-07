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

package org.apache.gobblin.kafka;

import com.google.common.collect.ImmutableMap;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.ZkClient;
import org.apache.gobblin.test.TestUtils;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * A private class for starting a suite of servers for Kafka
 * Calls to start and shutdown are reference counted, so that the suite is started and shutdown in pairs.
 * A suite of servers (Zk, Kafka etc) will be started just once per process
 */
@Slf4j
class KafkaServerSuite {
  static KafkaServerSuite _instance;
  private final int _kafkaServerPort;
  private final AtomicInteger _numStarted;
  private int _brokerId = 0;
  private EmbeddedZookeeper _zkServer;
  private ZkClient _zkClient;
  private KafkaServer _kafkaServer;
  private String _zkConnectString;
  private KafkaServerSuite() {
    _kafkaServerPort = TestUtils.findFreePort();
    _zkConnectString = "UNINITIALIZED_HOST_PORT";
    _numStarted = new AtomicInteger(0);
  }

  static KafkaServerSuite getInstance() {
    if (null == _instance) {
      _instance = new KafkaServerSuite();
      return _instance;
    } else {
      return _instance;
    }
  }

  public ZkClient getZkClient() {
    return _zkClient;
  }

  public KafkaServer getKafkaServer() {
    return _kafkaServer;
  }

  public int getKafkaServerPort() {
    return _kafkaServerPort;
  }

  public String getZkConnectString() {
    return _zkConnectString;
  }

  void start()
      throws RuntimeException {
    if (_numStarted.incrementAndGet() == 1) {
      log.warn("Starting up Kafka server suite. Zk at " + _zkConnectString + "; Kafka server at " + _kafkaServerPort);
      _zkServer = new EmbeddedZookeeper();
      _zkConnectString = "127.0.0.1:" + _zkServer.port();
      _zkClient = new ZkClient(_zkConnectString, 30000, 30000, ZKStringSerializer$.MODULE$);

      Properties props = kafka.utils.TestUtils.createBrokerConfig(
          _brokerId,
          _zkConnectString,
          kafka.utils.TestUtils.createBrokerConfig$default$3(),
          kafka.utils.TestUtils.createBrokerConfig$default$4(),
          _kafkaServerPort,
          kafka.utils.TestUtils.createBrokerConfig$default$6(),
          kafka.utils.TestUtils.createBrokerConfig$default$7(),
          kafka.utils.TestUtils.createBrokerConfig$default$8(),
          kafka.utils.TestUtils.createBrokerConfig$default$9(),
          kafka.utils.TestUtils.createBrokerConfig$default$10(),
          kafka.utils.TestUtils.createBrokerConfig$default$11(),
          kafka.utils.TestUtils.createBrokerConfig$default$12(),
          kafka.utils.TestUtils.createBrokerConfig$default$13(),
          kafka.utils.TestUtils.createBrokerConfig$default$14(),
          kafka.utils.TestUtils.createBrokerConfig$default$15(),
          kafka.utils.TestUtils.createBrokerConfig$default$16(),
          kafka.utils.TestUtils.createBrokerConfig$default$17(),
          kafka.utils.TestUtils.createBrokerConfig$default$18()
      );


      KafkaConfig config = new KafkaConfig(props);
      Time mock = new MockTime();
      _kafkaServer = kafka.utils.TestUtils.createServer(config, mock);
    } else {
      log.info("Kafka server suite already started... continuing");
    }
  }


  void shutdown() {
    if (_numStarted.decrementAndGet() == 0) {
      log.info("Shutting down Kafka server suite");
      _kafkaServer.shutdown();
      _zkClient.close();
      _zkServer.shutdown();
    } else {
      log.info("Kafka server suite still in use ... not shutting down yet");
    }
  }

}

class KafkaConsumerSuite {
  private final ConsumerConnector _consumer;
  private final KafkaStream<byte[], byte[]> _stream;
  private final ConsumerIterator<byte[], byte[]> _iterator;
  private final String _topic;

  KafkaConsumerSuite(String zkConnectString, String topic) {
    _topic = topic;
    Properties consumeProps = new Properties();
    consumeProps.put("zookeeper.connect", zkConnectString);
    consumeProps.put("group.id", _topic + "-" + System.nanoTime());
    consumeProps.put("zookeeper.session.timeout.ms", "10000");
    consumeProps.put("zookeeper.sync.time.ms", "10000");
    consumeProps.put("auto.commit.interval.ms", "10000");
    consumeProps.put("_consumer.timeout.ms", "10000");

    _consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumeProps));

    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
        _consumer.createMessageStreams(ImmutableMap.of(this._topic, 1));
    List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(this._topic);
    _stream = streams.get(0);
    _iterator = _stream.iterator();
  }

  void shutdown() {
    _consumer.shutdown();
  }

  public ConsumerIterator<byte[], byte[]> getIterator() {
    return _iterator;
  }
}

/**
 * A Helper class for testing against Kafka
 * A suite of servers (Zk, Kafka etc) will be started just once per process
 * Consumer and iterator will be created per instantiation and is one instance per topic.
 */
public class KafkaTestBase implements Closeable {

  private final KafkaServerSuite _kafkaServerSuite;
  private final Map<String, KafkaConsumerSuite> _topicConsumerMap;

  public KafkaTestBase() throws InterruptedException, RuntimeException {

    this._kafkaServerSuite = KafkaServerSuite.getInstance();
    this._topicConsumerMap = new HashMap<>();
  }

  public synchronized void startServers() {
    _kafkaServerSuite.start();
  }

  public void stopServers() {
    _kafkaServerSuite.shutdown();
  }

  public void start() {
    startServers();
  }

  public void stopClients() throws IOException {
    for (Map.Entry<String, KafkaConsumerSuite> consumerSuiteEntry : _topicConsumerMap.entrySet()) {
      consumerSuiteEntry.getValue().shutdown();
      AdminUtils.deleteTopic(ZkUtils.apply(_kafkaServerSuite.getZkClient(), false),
          consumerSuiteEntry.getKey());
    }
  }

  @Override
  public void close() throws IOException {
    stopClients();
    stopServers();
  }

  public void provisionTopic(String topic) {
    if (_topicConsumerMap.containsKey(topic)) {
      // nothing to do: return
    } else {
      // provision topic
      AdminUtils.createTopic(ZkUtils.apply(_kafkaServerSuite.getZkClient(), false),
          topic, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);

      List<KafkaServer> servers = new ArrayList<>();
      servers.add(_kafkaServerSuite.getKafkaServer());
      kafka.utils.TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), topic, 0, 5000);
      KafkaConsumerSuite consumerSuite = new KafkaConsumerSuite(_kafkaServerSuite.getZkConnectString(), topic);
      _topicConsumerMap.put(topic, consumerSuite);
    }
  }


  public ConsumerIterator<byte[], byte[]> getIteratorForTopic(String topic) {
    if (_topicConsumerMap.containsKey(topic)) {
      return _topicConsumerMap.get(topic).getIterator();
    } else {
      throw new IllegalStateException("Could not find provisioned topic" + topic + ": call provisionTopic before");
    }
  }

  public int getKafkaServerPort() {
    return _kafkaServerSuite.getKafkaServerPort();
  }

  public String getZkConnectString() {
    return this._kafkaServerSuite.getZkConnectString();
  }

}
