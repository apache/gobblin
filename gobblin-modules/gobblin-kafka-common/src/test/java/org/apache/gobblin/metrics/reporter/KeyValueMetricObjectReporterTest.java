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

import org.apache.gobblin.kafka.schemareg.KafkaSchemaRegistryConfigurationKeys;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.kafka.KeyValueMetricObjectReporter;
import org.apache.gobblin.metrics.kafka.KeyValuePusher;
import org.apache.gobblin.metrics.reporter.util.KafkaAvroReporterUtil;
import org.apache.gobblin.util.ConfigUtils;


public class KeyValueMetricObjectReporterTest {

  private static final String TOPIC = KeyValueMetricObjectReporterTest.class.getSimpleName();
  /**
   * Get builder for KeyValueMetricObjectReporter
   * @return KeyValueMetricObjectReporter builder
   */
  public KeyValueMetricObjectReporter.Builder getBuilder(KeyValuePusher pusher, Properties props) {
    return KeyValueMetricObjectReporter.Factory.newBuilder().withPusher(pusher).namespaceOverride(KafkaAvroReporterUtil.extractOverrideNamespace(props));
  }

  @Test
  public void testKafkaKeyValueMetricObjectReporter() throws IOException {
    MetricContext metricContext = MetricContext.builder(this.getClass().getCanonicalName() + ".testKafkaReporter").build();

    String namespace = "org.apache.gobblin.metrics:gobblin.metrics.test";
    String name = TOPIC;
    Properties properties = new Properties();
    properties.put(KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_OVERRIDE_NAMESPACE, namespace);

    MockKafkaKeyValPusherNew pusher = new MockKafkaKeyValPusherNew();
    KeyValueMetricObjectReporter kafkaReporter = getBuilder(pusher, properties).build("localhost:0000", TOPIC,
        ConfigUtils.propertiesToConfig(properties));

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
