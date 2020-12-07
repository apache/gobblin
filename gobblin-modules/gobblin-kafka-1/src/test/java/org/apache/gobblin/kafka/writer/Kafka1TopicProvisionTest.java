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

import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.kafka.KafkaClusterTestBase;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


@Slf4j
public class Kafka1TopicProvisionTest {

  private final KafkaClusterTestBase _kafkaTestHelper;
  private int testClusterCount = 5;

  public Kafka1TopicProvisionTest()
      throws InterruptedException, RuntimeException {
    _kafkaTestHelper = new KafkaClusterTestBase(testClusterCount);
  }

  @BeforeSuite(alwaysRun = true)
  public void beforeSuite() {
    log.info("Process id = " + ManagementFactory.getRuntimeMXBean().getName());
    _kafkaTestHelper.startCluster();
  }

  @AfterSuite(alwaysRun = true)
  public void afterSuite()
      throws IOException {
    _kafkaTestHelper.stopCluster();
  }

  @Test(enabled = false)
  public void testCluster()
      throws IOException, InterruptedException, KeeperException {
    int clusterCount = _kafkaTestHelper.getClusterCount();
    Assert.assertEquals(clusterCount, testClusterCount);
    int zkPort = _kafkaTestHelper.getZookeeperPort();
    String kafkaBrokerPortList = _kafkaTestHelper.getKafkaBrokerPortList().toString();
    System.out.println("kafkaBrokerPortList : " + kafkaBrokerPortList);
    ZooKeeper zk = new ZooKeeper("localhost:" + zkPort, 11000, new ByPassWatcher());
    List<Integer> brokerPortList = new ArrayList<Integer>();
    List<String> ids = zk.getChildren("/brokers/ids", false);
    for (String id : ids) {
      String brokerInfo = new String(zk.getData("/brokers/ids/" + id, false, null));
      JSONObject obj = new JSONObject(brokerInfo);
      int brokerPort = obj.getInt("port");
      System.out.println(brokerPort);
      brokerPortList.add(brokerPort);
    }
    Assert.assertTrue(_kafkaTestHelper.getKafkaBrokerPortList().equals(brokerPortList));
  }

  @Test(enabled = false)
  public void testTopicPartitionCreationCount()
      throws IOException, InterruptedException, ExecutionException {
    String topic = "topicPartition4";
    int clusterCount = _kafkaTestHelper.getClusterCount();
    int partionCount = clusterCount / 2;
    int zkPort = _kafkaTestHelper.getZookeeperPort();
    Properties props = new Properties();

    //	Setting Topic Properties
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_TOPIC, topic);
    props.setProperty(KafkaWriterConfigurationKeys.REPLICATION_COUNT, String.valueOf(clusterCount));
    props.setProperty(KafkaWriterConfigurationKeys.PARTITION_COUNT, String.valueOf(partionCount));
    props.setProperty(KafkaWriterConfigurationKeys.CLUSTER_ZOOKEEPER, "localhost:" + zkPort);

    // Setting Producer Properties
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + "bootstrap.servers", _kafkaTestHelper.getBootServersList());
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + "value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Kafka1DataWriter<String, String> kafka1DataWriter = new Kafka1DataWriter<>(props);

    String zookeeperConnect = "localhost:" + _kafkaTestHelper.getZookeeperPort();
    int sessionTimeoutMs = 10 * 1000;
    int connectionTimeoutMs = 8 * 1000;
    // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then
    // createTopic() will only seem to work (it will return without error).  The topic will exist in
    // only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the
    // topic.
    ZkClient zkClient = new ZkClient(
        zookeeperConnect,
        sessionTimeoutMs,
        connectionTimeoutMs,
        ZKStringSerializer$.MODULE$);
    boolean isSecureKafkaCluster = false;
    ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), isSecureKafkaCluster);

    Integer partitionCount = (Integer) zkUtils.getTopicPartitionCount(topic).get();
    Assert.assertEquals(partitionCount.intValue(), partionCount);

  }

  @Test(enabled = false)
  public void testLiveTopicPartitionCreationCount()
      throws IOException, InterruptedException, ExecutionException {
    String liveClusterCount = System.getProperty("live.cluster.count");
    String liveZookeeper = System.getProperty("live.zookeeper");
    String liveBroker = System.getProperty("live.broker");
    String topic = System.getProperty("live.newtopic");
    String topicReplicationCount = System.getProperty("live.newtopic.replicationCount");
    String topicPartitionCount = System.getProperty("live.newtopic.partitionCount");
    if (StringUtils.isEmpty(liveClusterCount)) {
      Assert.assertTrue(true);
      return;
    }
    if (StringUtils.isEmpty(topicPartitionCount)) {
      int clusterCount = Integer.parseInt(liveClusterCount);
      clusterCount--;
      int partionCount = clusterCount / 2;
      topicReplicationCount = String.valueOf(clusterCount);
      topicPartitionCount = String.valueOf(partionCount);
    }

    Properties props = new Properties();
    //	Setting Topic Properties
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_TOPIC, topic);
    props.setProperty(KafkaWriterConfigurationKeys.REPLICATION_COUNT, topicReplicationCount);
    props.setProperty(KafkaWriterConfigurationKeys.PARTITION_COUNT, topicPartitionCount);
    props.setProperty(KafkaWriterConfigurationKeys.CLUSTER_ZOOKEEPER, liveZookeeper);
    // Setting Producer Properties
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + "bootstrap.servers", liveBroker);
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + "value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Kafka1DataWriter<String, String> kafka1DataWriter = new Kafka1DataWriter<>(props);
    int sessionTimeoutMs = 10 * 1000;
    int connectionTimeoutMs = 8 * 1000;
    // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then
    // createTopic() will only seem to work (it will return without error).  The topic will exist in
    // only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the
    // topic.
    ZkClient zkClient = new ZkClient(
        liveZookeeper,
        sessionTimeoutMs,
        connectionTimeoutMs,
        ZKStringSerializer$.MODULE$);
    boolean isSecureKafkaCluster = false;
    ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(liveZookeeper), isSecureKafkaCluster);

    Properties config = new Properties();
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, _kafkaTestHelper.getBootServersList());
    AdminClient adminClient = AdminClient.create(config);
    DescribeTopicsResult describer = adminClient.describeTopics(Collections.singletonList(topic));

    // Note: AdminUtils.fetchTopicMetadataFromZk is deprecated after 0.10.0. Please consider using AdminClient
    // to fetch topic config, or using ZKUtils.
    Assert.assertEquals(describer.values().get(topic).get().partitions().size(), Integer.parseInt(topicPartitionCount));

  }

}
