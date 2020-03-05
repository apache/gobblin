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

package org.apache.gobblin.runtime;

import java.net.URI;
import java.util.Properties;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import com.google.common.io.Closer;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.kafka.KafkaTestBase;
import org.apache.gobblin.kafka.client.AbstractBaseKafkaConsumerClient;
import org.apache.gobblin.kafka.client.Kafka09ConsumerClient;
import org.apache.gobblin.kafka.writer.Kafka09DataWriter;
import org.apache.gobblin.kafka.writer.KafkaWriterConfigurationKeys;
import org.apache.gobblin.runtime.job_monitor.KafkaJobMonitor;
import org.apache.gobblin.runtime.job_monitor.MockedKafkaJobMonitor;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.writer.AsyncDataWriter;
import org.apache.gobblin.writer.WriteCallback;


public class KafkaJobMonitorTest extends KafkaTestBase {

  private static final String BOOTSTRAP_SERVERS_KEY = "bootstrap.servers";
  private static final String KAFKA_AUTO_OFFSET_RESET_KEY = "auto.offset.reset";
  private static final String SOURCE_KAFKA_CONSUMERCONFIG_KEY_WITH_DOT = AbstractBaseKafkaConsumerClient.CONFIG_NAMESPACE + "." + AbstractBaseKafkaConsumerClient.CONSUMER_CONFIG + ".";
  private static final String TOPIC = KafkaJobMonitorTest.class.getSimpleName();
  private static final int NUM_PARTITIONS = 2;

  private Closer _closer;
  private String _kafkaBrokers;
  private AsyncDataWriter dataWriter;

  public KafkaJobMonitorTest()
      throws InterruptedException, RuntimeException {
    super();
    _kafkaBrokers = "localhost:" + this.getKafkaServerPort();
  }

  @BeforeSuite
  public void beforeSuite() throws Exception {
    startServers();
    _closer = Closer.create();
    Properties producerProps = new Properties();
    producerProps.setProperty(KafkaWriterConfigurationKeys.KAFKA_TOPIC, TOPIC);
    producerProps.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + BOOTSTRAP_SERVERS_KEY, _kafkaBrokers);
    producerProps.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + KafkaWriterConfigurationKeys.VALUE_SERIALIZER_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProps.setProperty(KafkaWriterConfigurationKeys.CLUSTER_ZOOKEEPER, this.getZkConnectString());
    producerProps.setProperty(KafkaWriterConfigurationKeys.PARTITION_COUNT, String.valueOf(NUM_PARTITIONS));
    dataWriter = _closer.register(new Kafka09DataWriter(producerProps));
  }

  @Test
  public void test() throws Exception {

    Properties consumerProps = new Properties();
    consumerProps.put(KafkaJobMonitor.KAFKA_JOB_MONITOR_PREFIX + "." + ConfigurationKeys.KAFKA_BROKERS, _kafkaBrokers);
    consumerProps.put(KafkaJobMonitor.KAFKA_JOB_MONITOR_PREFIX + "." + Kafka09ConsumerClient.GOBBLIN_CONFIG_VALUE_DESERIALIZER_CLASS_KEY, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumerProps.setProperty(KafkaJobMonitor.KAFKA_JOB_MONITOR_PREFIX + "." + SOURCE_KAFKA_CONSUMERCONFIG_KEY_WITH_DOT + KAFKA_AUTO_OFFSET_RESET_KEY, "earliest");

    MockedKafkaJobMonitor monitor = MockedKafkaJobMonitor.create(TOPIC, ConfigUtils.propertiesToConfig(consumerProps));
    monitor.startAsync().awaitRunning();

    WriteCallback mockCallback = Mockito.mock(WriteCallback.class);
    dataWriter.write("job1:1".getBytes(), mockCallback);
    monitor.awaitExactlyNSpecs(1);
    Assert.assertTrue(monitor.getJobSpecs().containsKey(new URI("job1")));
    Assert.assertEquals(monitor.getJobSpecs().get(new URI("job1")).getVersion(), "1");

    dataWriter.write("job2:1".getBytes(), mockCallback);
    monitor.awaitExactlyNSpecs(2);
    Assert.assertTrue(monitor.getJobSpecs().containsKey(new URI("job2")));
    Assert.assertEquals(monitor.getJobSpecs().get(new URI("job2")).getVersion(), "1");

    dataWriter.write((MockedKafkaJobMonitor.REMOVE + ":job1").getBytes(), mockCallback);
    monitor.awaitExactlyNSpecs(1);
    Assert.assertFalse(monitor.getJobSpecs().containsKey(new URI("job1")));
    Assert.assertTrue(monitor.getJobSpecs().containsKey(new URI("job2")));

    dataWriter.write(("job2:2,job1:2").getBytes(), mockCallback);
    monitor.awaitExactlyNSpecs(2);
    Assert.assertTrue(monitor.getJobSpecs().containsKey(new URI("job1")));
    Assert.assertEquals(monitor.getJobSpecs().get(new URI("job1")).getVersion(), "2");
    Assert.assertTrue(monitor.getJobSpecs().containsKey(new URI("job2")));
    Assert.assertEquals(monitor.getJobSpecs().get(new URI("job2")).getVersion(), "2");

    monitor.shutDown();
  }

  @AfterSuite
  public void afterSuite() {
    try {
      _closer.close();
    } catch (Exception e) {
      System.out.println("Failed to close data writer." +  e);
    } finally {
      try {
        close();
      } catch (Exception e) {
        System.out.println("Failed to close Kafka server."+ e);
      }
    }
  }

}
