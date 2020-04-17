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

package org.apache.gobblin.metrics.reporter;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.kafka.KafkaAvroEventKeyValueReporter;
import org.apache.gobblin.metrics.kafka.KafkaAvroSchemaRegistry;
import org.apache.gobblin.metrics.kafka.KafkaEventReporter;
import org.apache.gobblin.metrics.kafka.Pusher;
import org.apache.gobblin.metrics.reporter.util.EventUtils;


public class KafkaAvroEventKeyValueReporterTest extends KafkaAvroEventReporterTest {
  private static final int SCHEMA_ID_LENGTH_BYTES = 20;

  private String schemaId;

  @BeforeClass
  public void setUp() throws IOException {
    Schema schema =
        new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream("GobblinTrackingEvent.avsc"));
    this.schemaId = DigestUtils.sha1Hex(schema.toString().getBytes());
  }

  @Override
  public KafkaEventReporter.Builder<? extends KafkaEventReporter.Builder> getBuilder(MetricContext context,
                                                                                     Pusher pusher) {
    KafkaAvroEventKeyValueReporter.Builder<?> builder = KafkaAvroEventKeyValueReporter.Factory.forContext(context);
    return builder.withKafkaPusher(pusher).withKeys(Lists.newArrayList("k1", "k2", "k3"));
  }

  private Pair<String, GobblinTrackingEvent> nextKVEvent(Iterator<Pair<String, byte[]>> it, boolean isSchemaIdEnabled) throws IOException {
    Assert.assertTrue(it.hasNext());
    Pair<String, byte[]> event = it.next();
    return isSchemaIdEnabled ? Pair.of(event.getKey(), EventUtils
        .deserializeEventFromAvroSerialization(new GobblinTrackingEvent(), event.getValue(), schemaId)) : Pair.of(event.getKey(),
        EventUtils.deserializeEventFromAvroSerialization(new GobblinTrackingEvent(), event.getValue()));
  }

  private GobblinTrackingEvent getEvent(boolean isMessageKeyed) {
    String namespace = "gobblin.metrics.test";
    String eventName = "testEvent";

    GobblinTrackingEvent event = new GobblinTrackingEvent();
    event.setName(eventName);
    event.setNamespace(namespace);
    Map<String, String> metadata = Maps.newHashMap();
    metadata.put("m1", "v1");
    metadata.put("m2", null);
    if (isMessageKeyed) {
      metadata.put("k1", "v1");
      metadata.put("k2", "v2");
      metadata.put("k3", "v3");
    }

    event.setMetadata(metadata);
    return event;
  }

  @Test
  public void testKafkaEventReporter() throws IOException {
    MetricContext context = MetricContext.builder("context").build();

    MockKafkaKeyValuePusher pusher = new MockKafkaKeyValuePusher();
    KafkaEventReporter kafkaReporter = getBuilder(context, pusher).build("localhost:0000", "topic");

    context.submitEvent(getEvent(false));

    kafkaReporter.report();

    Pair<String, GobblinTrackingEvent> retrievedEvent = nextKVEvent(pusher.messageIterator(), false);
    Assert.assertNull(retrievedEvent.getKey());

    context.submitEvent(getEvent(true));

    kafkaReporter.report();

    retrievedEvent = nextKVEvent(pusher.messageIterator(), false);
    Assert.assertEquals(retrievedEvent.getKey(), "v1v2v3");
  }

  @Test
  public void testKafkaEventReporterWithSchemaRegistry() throws IOException {
    MetricContext context = MetricContext.builder("context").build();
    MockKafkaKeyValuePusher pusher = new MockKafkaKeyValuePusher();

    Schema schema =
        new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream("GobblinTrackingEvent.avsc"));
    String schemaId = DigestUtils.sha1Hex(schema.toString().getBytes());

    KafkaAvroEventKeyValueReporter.Builder<?> builder = KafkaAvroEventKeyValueReporter.Factory.forContext(context);
    KafkaAvroEventKeyValueReporter kafkaReporter =
        builder.withKafkaPusher(pusher).withKeys(Lists.newArrayList("k1", "k2", "k3"))
            .withSchemaRegistry(Mockito.mock(KafkaAvroSchemaRegistry.class)).withSchemaId(schemaId)
            .build("localhost:0000", "topic");

    context.submitEvent(getEvent(true));

    kafkaReporter.report();

    Pair<String, GobblinTrackingEvent> retrievedEvent = nextKVEvent(pusher.messageIterator(), true);
    Assert.assertEquals(retrievedEvent.getKey(), "v1v2v3");
  }

  @Test (enabled=false)
  public void testTagInjection() throws IOException {
    // This test is not applicable for testing KafkaAvroEventKeyValueReporter
  }
}
