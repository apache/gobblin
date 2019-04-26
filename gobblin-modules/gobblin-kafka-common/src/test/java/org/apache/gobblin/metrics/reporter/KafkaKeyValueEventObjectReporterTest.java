package org.apache.gobblin.metrics.reporter;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;

import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.kafka.KafkaKeyValueEventObjectReporter;
import org.apache.gobblin.metrics.kafka.KeyValuePusher;


public class KafkaKeyValueEventObjectReporterTest{

  /**
   * Get builder for KafkaKeyValueEventObjectReporter
   * @return KafkaKeyValueEventObjectReporter builder
   */
  public KafkaKeyValueEventObjectReporter.Builder getBuilder(MetricContext context, KeyValuePusher pusher) {
    return KafkaKeyValueEventObjectReporter.Factory.forContext(context).withKafkaPusher(pusher);
  }

  @Test
  public void testKafkaKeyValueEventObjectReporter() throws IOException {
    MetricContext context = MetricContext.builder("context").build();

    MockKafkaKeyValPusherNew pusher = new MockKafkaKeyValPusherNew();
    KafkaKeyValueEventObjectReporter kafkaReporter = getBuilder(context, pusher).build("localhost:0000", "topic");

    String namespace = "gobblin.metrics.test";
    String eventName = "testEvent";

    GobblinTrackingEvent event = new GobblinTrackingEvent();
    event.setName(eventName);
    event.setNamespace(namespace);
    Map<String, String> metadata = Maps.newHashMap();
    event.setMetadata(metadata);
    context.submitEvent(event);

    try {
      Thread.sleep(100);
    } catch(InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    kafkaReporter.report();

    try {
      Thread.sleep(100);
    } catch(InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    Pair<String,GenericRecord> retrievedEvent = nextKVEvent(pusher.messageIterator());

    Assert.assertEquals(retrievedEvent.getValue().get("namespace"), namespace);
    Assert.assertEquals(retrievedEvent.getValue().get("name"), eventName);
    int partition = Integer.parseInt(retrievedEvent.getKey());
    Assert.assertTrue((0 <= partition && partition <= 99));

  }

  private Pair<String, GenericRecord> nextKVEvent(Iterator<Pair<String, GenericRecord>> it) {
    Assert.assertTrue(it.hasNext());
    Pair<String, GenericRecord> event = it.next();
    return Pair.of(event.getKey(), event.getValue());
  }
}
