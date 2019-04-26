package org.apache.gobblin.metrics.reporter;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.kafka.schemareg.KafkaSchemaRegistryConfigurationKeys;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.kafka.KafkaKeyValueMetricObjectReporter;
import org.apache.gobblin.metrics.kafka.KeyValuePusher;
import org.apache.gobblin.metrics.reporter.util.KafkaAvroReporterUtil;


public class KafkaKeyValueMetricObjectReporterTest {

  private static final String TOPIC = KafkaKeyValueMetricObjectReporterTest.class.getSimpleName();
  /**
   * Get builder for KafkaKeyValueMetricObjectReporter
   * @return KafkaKeyValueMetricObjectReporter builder
   */
  public KafkaKeyValueMetricObjectReporter.Builder getBuilder(KeyValuePusher pusher, Properties props) {
    return KafkaKeyValueMetricObjectReporter.Factory.newBuilder().withKafkaPusher(pusher).namespaceOverride(KafkaAvroReporterUtil.extractOverrideNamespace(props));
  }

  @Test
  public void testKafkaKeyValueMetricObjectReporter() throws IOException {
    MetricContext metricContext = MetricContext.builder(this.getClass().getCanonicalName() + ".testKafkaReporter").build();

    String namespace = "org.apache.gobblin.metrics:gobblin.metrics.test";
    String name = TOPIC;
    Properties properties = new Properties();
    properties.put(KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_OVERRIDE_NAMESPACE, namespace);

    MockKafkaKeyValPusherNew pusher = new MockKafkaKeyValPusherNew();
    KafkaKeyValueMetricObjectReporter kafkaReporter = getBuilder(pusher, properties).build("localhost:0000", TOPIC, properties);

    kafkaReporter.report(metricContext);

    try {
      Thread.sleep(1000);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    Pair<String,GenericRecord> retrievedEvent = nextKVReport(pusher.messageIterator());

    Assert.assertEquals(retrievedEvent.getValue().getSchema().getNamespace(), "gobblin.metrics.test");
    Assert.assertEquals(retrievedEvent.getValue().getSchema().getName(), name);
    int partition = Integer.parseInt(retrievedEvent.getKey());
    Assert.assertTrue((0 <= partition && partition <= 99));

    kafkaReporter.close();

  }

  /**
   * Extract the next metric from the Kafka iterator
   * Assumes existence of the metric has already been checked.
   * @param it Kafka ConsumerIterator
   * @return next metric in the stream
   * @throws IOException
   */
  protected Pair<String,GenericRecord> nextKVReport(Iterator<Pair<String, GenericRecord>> it){
    Assert.assertTrue(it.hasNext());
    return it.next();
  }

}
