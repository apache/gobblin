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
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;

import org.apache.gobblin.kafka.schemareg.KafkaSchemaRegistryConfigurationKeys;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.reporter.util.KafkaReporterUtils;
import org.apache.gobblin.util.ConfigUtils;


public class KeyValueMetricObjectReporterTest extends KeyValueMetricObjectReporter {

  private static final String TOPIC = KeyValueMetricObjectReporterTest.class.getSimpleName();

  public KeyValueMetricObjectReporterTest(Builder builder, Config config) {
    super(builder, config);
  }

  public MockKeyValuePusher getPusher() {
    return (MockKeyValuePusher) pusher;
  }

  public static class Builder extends KeyValueMetricObjectReporter.Builder {

    public KeyValueMetricObjectReporterTest build(String brokers, String topic, Config config)
        throws IOException {
      this.brokers = brokers;
      this.topic = topic;
      return new KeyValueMetricObjectReporterTest(this, config);
    }
  }

  /**
   * Get builder for KeyValueMetricObjectReporter
   * @return KeyValueMetricObjectReporter builder
   */
  public static KeyValueMetricObjectReporterTest.Builder getBuilder(Properties props) {
    KeyValueMetricObjectReporterTest.Builder builder = new KeyValueMetricObjectReporterTest.Builder();
    builder.namespaceOverride(KafkaReporterUtils.extractOverrideNamespace(props));
    return builder;
  }

  @Test
  public static void testKafkaKeyValueMetricObjectReporter()
      throws IOException {
    MetricContext metricContext = MetricContext.builder("context").build();

    String namespace = "org.apache.gobblin.metrics:gobblin.metrics.test";
    String name = TOPIC;
    Properties properties = new Properties();
    properties.put(KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_OVERRIDE_NAMESPACE, namespace);
    properties.put("pusherClass", "org.apache.gobblin.metrics.reporter.MockKeyValuePusher");

    KeyValueMetricObjectReporterTest reporter =
        getBuilder(properties).build("localhost:0000", TOPIC, ConfigUtils.propertiesToConfig(properties));

    reporter.report(metricContext);

    try {
      Thread.sleep(1000);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    MockKeyValuePusher pusher = reporter.getPusher();
    Pair<String, GenericRecord> retrievedEvent = nextKVReport(pusher.messageIterator());

    Assert.assertEquals(retrievedEvent.getValue().getSchema().getNamespace(), "gobblin.metrics.test");
    Assert.assertEquals(retrievedEvent.getValue().getSchema().getName(), name);
    int partition = Integer.parseInt(retrievedEvent.getKey());
    Assert.assertTrue((0 <= partition && partition <= 99));
    Assert.assertTrue(retrievedEvent.getValue().getSchema() == reporter.schema);

    reporter.close();
  }

  /**
   * Extract the next metric from the Kafka iterator
   * Assumes existence of the metric has already been checked.
   * @param it Kafka ConsumerIterator
   * @return next metric in the stream
   * @throws IOException
   */
  protected static Pair<String, GenericRecord> nextKVReport(Iterator<Pair<String, GenericRecord>> it) {
    Assert.assertTrue(it.hasNext());
    return it.next();
  }
}
