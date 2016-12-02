/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metrics.reporter;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;

import gobblin.metrics.GobblinTrackingEvent;
import gobblin.metrics.MetricContext;
import gobblin.metrics.Tag;
import gobblin.metrics.reporter.util.EventUtils;
import gobblin.metrics.kafka.KafkaEventReporter;
import gobblin.metrics.kafka.KafkaPusher;


@Test(groups = {"gobblin.metrics"})
public class KafkaEventReporterTest {

  /**
   * Get builder for KafkaReporter (override if testing an extension of KafkaReporter)
   * @param context metricregistry
   * @return KafkaReporter builder
   */
  public KafkaEventReporter.Builder<? extends KafkaEventReporter.Builder> getBuilder(MetricContext context,
      KafkaPusher pusher) {
    return KafkaEventReporter.Factory.forContext(context).withKafkaPusher(pusher);
  }


  @Test
  public void testKafkaEventReporter() throws IOException {
    MetricContext context = MetricContext.builder("context").build();

    MockKafkaPusher pusher = new MockKafkaPusher();
    KafkaEventReporter kafkaReporter = getBuilder(context, pusher).build("localhost:0000", "topic");

    String namespace = "gobblin.metrics.test";
    String eventName = "testEvent";

    GobblinTrackingEvent event = new GobblinTrackingEvent();
    event.setName(eventName);
    event.setNamespace(namespace);
    Map<String, String> metadata = Maps.newHashMap();
    metadata.put("m1", "v1");
    metadata.put("m2", null);
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

    GobblinTrackingEvent retrievedEvent = nextEvent(pusher.messageIterator());
    Assert.assertEquals(retrievedEvent.getNamespace(), namespace);
    Assert.assertEquals(retrievedEvent.getName(), eventName);
    Assert.assertEquals(retrievedEvent.getMetadata().size(), 4);

  }

  @Test
  public void testTagInjection() throws IOException {

    String tag1 = "tag1";
    String value1 = "value1";
    String metadataValue1 = "metadata1";
    String tag2 = "tag2";
    String value2 = "value2";

    MetricContext context = MetricContext.builder("context").addTag(new Tag<String>(tag1, value1)).
        addTag(new Tag<String>(tag2, value2)).build();

    MockKafkaPusher pusher = new MockKafkaPusher();
    KafkaEventReporter kafkaReporter = getBuilder(context, pusher).build("localhost:0000", "topic");

    String namespace = "gobblin.metrics.test";
    String eventName = "testEvent";

    GobblinTrackingEvent event = new GobblinTrackingEvent();
    event.setName(eventName);
    event.setNamespace(namespace);
    Map<String, String> metadata = Maps.newHashMap();
    metadata.put(tag1, metadataValue1);
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

    GobblinTrackingEvent retrievedEvent = nextEvent(pusher.messageIterator());
    Assert.assertEquals(retrievedEvent.getNamespace(), namespace);
    Assert.assertEquals(retrievedEvent.getName(), eventName);
    Assert.assertEquals(retrievedEvent.getMetadata().size(), 4);
    Assert.assertEquals(retrievedEvent.getMetadata().get(tag1), metadataValue1);
    Assert.assertEquals(retrievedEvent.getMetadata().get(tag2), value2);
  }

  /**
   * Extract the next metric from the Kafka iterator
   * Assumes existence of the metric has already been checked.
   * @param it Kafka ConsumerIterator
   * @return next metric in the stream
   * @throws java.io.IOException
   */
  protected GobblinTrackingEvent nextEvent(Iterator<byte[]> it) throws IOException {
    Assert.assertTrue(it.hasNext());
    return EventUtils.deserializeReportFromJson(new GobblinTrackingEvent(), it.next());
  }
}
