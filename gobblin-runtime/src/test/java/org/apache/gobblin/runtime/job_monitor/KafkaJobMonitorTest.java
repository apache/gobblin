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

package org.apache.gobblin.runtime.job_monitor;

import java.net.URI;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.kafka.client.Kafka09ConsumerClient;
import org.apache.gobblin.kafka.client.KafkaConsumerRecord;
import org.apache.gobblin.kafka.client.MockBatchKafkaConsumerClient;
import org.apache.gobblin.runtime.kafka.HighLevelConsumerTest;
import org.apache.gobblin.util.ConfigUtils;


public class KafkaJobMonitorTest {

  @Test
  public void test() throws Exception {

    Properties consumerProps = new Properties();
    consumerProps.put(ConfigurationKeys.KAFKA_BROKERS, "localhost:1234");
    consumerProps.put(Kafka09ConsumerClient.GOBBLIN_CONFIG_VALUE_DESERIALIZER_CLASS_KEY, "org.apache.kafka.common.serialization.StringDeserializer");
    MockBatchKafkaConsumerClient consumerClient = new MockBatchKafkaConsumerClient(ConfigUtils.propertiesToConfig(consumerProps), Lists.newArrayList(), 1);

    Config config = HighLevelConsumerTest.getSimpleConfig(Optional.of(KafkaJobMonitor.KAFKA_JOB_MONITOR_PREFIX));

    MockedKafkaJobMonitor monitor = MockedKafkaJobMonitor.create(config, Optional.of(consumerClient));
    monitor.startAsync();

    consumerClient.addToRecords(getRecord("job1:1"));
    monitor.awaitExactlyNSpecs(1);
    Assert.assertTrue(monitor.getJobSpecs().containsKey(new URI("job1")));
    Assert.assertEquals(monitor.getJobSpecs().get(new URI("job1")).getVersion(), "1");

    consumerClient.addToRecords(getRecord("job2:1"));
    monitor.awaitExactlyNSpecs(2);
    Assert.assertTrue(monitor.getJobSpecs().containsKey(new URI("job2")));
    Assert.assertEquals(monitor.getJobSpecs().get(new URI("job2")).getVersion(), "1");

    consumerClient.addToRecords(getRecord(MockedKafkaJobMonitor.REMOVE + ":job1"));
    monitor.awaitExactlyNSpecs(1);
    Assert.assertFalse(monitor.getJobSpecs().containsKey(new URI("job1")));
    Assert.assertTrue(monitor.getJobSpecs().containsKey(new URI("job2")));

    consumerClient.addToRecords(getRecord("job2:2,job1:2"));
    monitor.awaitExactlyNSpecs(2);
    Assert.assertTrue(monitor.getJobSpecs().containsKey(new URI("job1")));
    Assert.assertEquals(monitor.getJobSpecs().get(new URI("job1")).getVersion(), "2");
    Assert.assertTrue(monitor.getJobSpecs().containsKey(new URI("job2")));
    Assert.assertEquals(monitor.getJobSpecs().get(new URI("job2")).getVersion(), "2");

    monitor.shutDown();
  }

  private KafkaConsumerRecord getRecord(String message) {
    return new Kafka09ConsumerClient.Kafka09ConsumerRecord<>(new ConsumerRecord<>("topic",0,0,null, message.getBytes(
        Charsets.UTF_8)));
  }
}
