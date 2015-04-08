package gobblin.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;


@Test(groups = {"gobblin.metrics"})
public class KafkaReporterTest extends KafkaTestBase {

  static String Topic = "KafkaReporterTest";

  public KafkaReporterTest(String topic)
      throws InterruptedException {
    super(Topic);
  }

  @Test
  public void testReporter(){
    MetricRegistry registry = new MetricRegistry();
    Counter counter = registry.counter("com.linkedin.example.counter");

    KafkaReporter kafkaReporter = KafkaReporter.forRegistry(registry).build("localhost:" + kafkaPort, Topic);

    counter.inc();

    kafkaReporter.report();

    try {
      Thread.sleep(100);
    } catch(InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    System.out.println("******************");
    System.out.println(new String(iterator.next().message()));
    System.out.println("******************");
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
