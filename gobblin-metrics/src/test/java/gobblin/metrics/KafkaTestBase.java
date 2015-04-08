package gobblin.metrics;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
import org.I0Itec.zkclient.ZkClient;


public class KafkaTestBase implements Closeable {

  private int brokerId = 0;
  String _topic;
  int kafkaPort;
  String zkConnect;
  EmbeddedZookeeper zkServer;
  ZkClient zkClient;
  KafkaServer kafkaServer;
  ConsumerConnector consumer;
  KafkaStream<byte[], byte[]> stream;
  ConsumerIterator<byte[],byte[]> iterator;

  public KafkaTestBase(String topic) throws InterruptedException {

    _topic = topic;

    zkConnect = TestZKUtils.zookeeperConnect();
    zkServer = new EmbeddedZookeeper(zkConnect);
    zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);

    kafkaPort = TestUtils.choosePort();
    Properties props = TestUtils.createBrokerConfig(brokerId, kafkaPort, true);

    KafkaConfig config = new KafkaConfig(props);
    Time mock = new MockTime();
    kafkaServer = TestUtils.createServer(config, mock);

    AdminUtils.createTopic(zkClient, topic, 1, 1, new Properties());

    List<KafkaServer> servers = new ArrayList<KafkaServer>();
    servers.add(kafkaServer);
    TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), topic, 0, 5000);

    Properties consumeProps = new Properties();
    consumeProps.put("zookeeper.connect", zkConnect);
    consumeProps.put("group.id", "testConsumer");
    consumeProps.put("zookeeper.session.timeout.ms", "400");
    consumeProps.put("zookeeper.sync.time.ms", "200");
    consumeProps.put("auto.commit.interval.ms", "1000");

    consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumeProps));

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(_topic, 1);
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    List<KafkaStream<byte[],byte[]>> streams = consumerMap.get(_topic);
    stream = streams.get(0);

    iterator = stream.iterator();
  }

  @Override
  public void close()
      throws IOException {
    consumer.shutdown();
    kafkaServer.shutdown();
    zkClient.close();
    zkServer.shutdown();
  }
}

