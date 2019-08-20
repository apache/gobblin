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
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;

import org.apache.gobblin.kafka.schemareg.KafkaSchemaRegistryConfigurationKeys;
import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.reporter.util.KafkaAvroReporterUtil;
import org.apache.gobblin.util.ConfigUtils;


public class KeyValueEventObjectReporterTest extends KeyValueEventObjectReporter {

  public KeyValueEventObjectReporterTest(Builder builder) {
    super(builder);
  }

  public MockKeyValuePusher getPusher() {
    return (MockKeyValuePusher) pusher;
  }

  public static class Builder extends KeyValueEventObjectReporter.Builder {

    protected Builder(MetricContext context) {
      super(context);
    }

    public KeyValueEventObjectReporterTest build(String brokers, String topic) {
      this.brokers = brokers;
      this.topic = topic;
      return new KeyValueEventObjectReporterTest(this);
    }
  }

  /**
   * Get builder for KeyValueEventObjectReporter
   * @return KeyValueEventObjectReporter builder
   */
  public static KeyValueEventObjectReporterTest.Builder getBuilder(MetricContext context, Properties props) {
    KeyValueEventObjectReporterTest.Builder builder = new KeyValueEventObjectReporterTest.Builder(context);
    builder.namespaceOverride(KafkaAvroReporterUtil.extractOverrideNamespace(props))
        .withConfig(ConfigUtils.propertiesToConfig(props));
    return builder;
  }

  @Test
  public static void testKafkaKeyValueEventObjectReporter()
      throws IOException {
    MetricContext context = MetricContext.builder("context").build();
    String namespace = "org.apache.gobblin.metrics:gobblin.metrics.test";

    Properties properties = new Properties();
    properties.put(KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_OVERRIDE_NAMESPACE, namespace);
    properties.put("pusherClass", "org.apache.gobblin.metrics.reporter.MockKeyValuePusher");

    KeyValueEventObjectReporterTest reporter = getBuilder(context, properties).build("localhost:0000", "topic");

    String eventName = "testEvent";

    GobblinTrackingEvent event = new GobblinTrackingEvent();
    event.setName(eventName);
    event.setNamespace(namespace);
    Map<String, String> metadata = Maps.newHashMap();
    event.setMetadata(metadata);
    context.submitEvent(event);

    try {
      Thread.sleep(100);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    reporter.report();

    MockKeyValuePusher pusher = reporter.getPusher();
    Pair<String, GenericRecord> retrievedEvent = nextKVEvent(pusher.messageIterator());

    Assert.assertEquals(retrievedEvent.getValue().get("namespace"), namespace);
    Assert.assertEquals(retrievedEvent.getValue().get("name"), eventName);
    int partition = Integer.parseInt(retrievedEvent.getKey());
    Assert.assertTrue((0 <= partition && partition <= 99));
    Assert.assertTrue(retrievedEvent.getValue().getSchema() == reporter.schema);
  }

  private static Pair<String, GenericRecord> nextKVEvent(Iterator<Pair<String, GenericRecord>> it) {
    Assert.assertTrue(it.hasNext());
    Pair<String, GenericRecord> event = it.next();
    return Pair.of(event.getKey(), event.getValue());
  }
}
