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

package gobblin.metrics.reporter;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;

import kafka.admin.AdminUtils;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.Time;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;


/**
 * Base for tests requiring a Kafka server
 *
 * Server will be created automatically at "localhost:" + kafkaPort
 * {@link kafka.consumer.ConsumerIterator} for the specified topic will be created at iterator
 *
 * @author ibuenros
 */
public class KafkaTestBase implements Closeable {

  private static int brokerId = 0;
  static int kafkaPort = 0;
  static String zkConnect = "";
  static EmbeddedZookeeper zkServer = null;
  static ZkClient zkClient = null;
  static KafkaServer kafkaServer = null;
  static boolean serverStarted = false;
  static boolean serverClosed = false;

  public static void startServer() throws RuntimeException {
    if (serverStarted && serverClosed) {
      throw new RuntimeException("Kafka test server has already been closed. Cannot generate Kafka server twice.");
    }
    if (!serverStarted) {
      serverStarted = true;
      zkConnect = TestZKUtils.zookeeperConnect();
      zkServer = new EmbeddedZookeeper(zkConnect);
      zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);

      kafkaPort = TestUtils.choosePort();
      Properties props = TestUtils.createBrokerConfig(brokerId, kafkaPort, true);

      KafkaConfig config = new KafkaConfig(props);
      Time mock = new MockTime();
      kafkaServer = TestUtils.createServer(config, mock);
    }
  }

  public static void closeServer() {
    if (serverStarted && !serverClosed) {
      serverClosed = true;
      kafkaServer.shutdown();
      zkClient.close();
      zkServer.shutdown();
    }
  }

  protected String topic;
  protected ConsumerConnector consumer;
  protected KafkaStream<byte[], byte[]> stream;
  protected ConsumerIterator<byte[], byte[]> iterator;

  public KafkaTestBase(String topic) throws InterruptedException, RuntimeException {

    startServer();

    this.topic = topic;

    AdminUtils.createTopic(zkClient, topic, 1, 1, new Properties());

    List<KafkaServer> servers = new ArrayList<>();
    servers.add(kafkaServer);
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), topic, 0, 5000);

    Properties consumeProps = new Properties();
    consumeProps.put("zookeeper.connect", zkConnect);
    consumeProps.put("group.id", "testConsumer");
    consumeProps.put("zookeeper.session.timeout.ms", "10000");
    consumeProps.put("zookeeper.sync.time.ms", "10000");
    consumeProps.put("auto.commit.interval.ms", "10000");
    consumeProps.put("consumer.timeout.ms", "10000");

    consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumeProps));

    Map<String, Integer> topicCountMap = new HashMap<>();
    topicCountMap.put(this.topic, 1);
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(this.topic);
    stream = streams.get(0);

    iterator = stream.iterator();
  }

  @Override
  public void close() throws IOException {
    consumer.shutdown();
  }
}
