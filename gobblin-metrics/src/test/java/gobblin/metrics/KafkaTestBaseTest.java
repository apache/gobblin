package gobblin.metrics;

import java.util.Iterator;
import java.util.Properties;
import kafka.consumer.ConsumerIterator;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = {"gobblin.metrics"})
public class KafkaTestBaseTest extends KafkaTestBase {

  public KafkaTestBaseTest()
      throws InterruptedException {
    super("test");
  }

  @Test
  public void test() {

    Properties props = new Properties();
    props.put("metadata.broker.list", "localhost:" + kafkaPort);
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("request.required.acks", "1");

    ProducerConfig config = new ProducerConfig(props);

    Producer<String, String> producer = new Producer<String, String>(config);

    producer.send(new KeyedMessage<String, String>("test", "testMessage"));

    try {
      Thread.sleep(100);
    } catch(InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    assert(iterator.hasNext());
    Assert.assertEquals(new String(iterator.next().message()), "testMessage");

    producer.close();
  }

  @AfterClass
  public void after() {
    try {
      close();
    } catch(Exception e) {
      System.err.println("Failed to close Kafka server.");
    }
  }
}
