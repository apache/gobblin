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

package org.apache.gobblin.service;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import com.google.common.io.Closer;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.kafka.KafkaTestBase;
import org.apache.gobblin.kafka.client.Kafka09ConsumerClient;
import org.apache.gobblin.kafka.writer.KafkaWriterConfigurationKeys;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.job_catalog.NonObservingFSJobCatalog;
import org.apache.gobblin.runtime.job_monitor.KafkaJobMonitor;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.writer.WriteResponse;


@Slf4j
public class StreamingKafkaSpecExecutorTest extends KafkaTestBase {

  public static final String TOPIC = StreamingKafkaSpecExecutorTest.class.getSimpleName();

  private Closer _closer;
  private Properties _properties;
  private SimpleKafkaSpecProducer _seip;
  private StreamingKafkaSpecConsumer _seic;
  private NonObservingFSJobCatalog _jobCatalog;
  private String _kafkaBrokers;
  private static final String _TEST_DIR_PATH = "/tmp/StreamingKafkaSpecExecutorTest";
  private static final String _JOBS_DIR_PATH = _TEST_DIR_PATH + "/jobs";
  String flowSpecUriString = "/flowgroup/flowname/spec";
  Spec flowSpec = initJobSpecWithFlowExecutionId(flowSpecUriString, "12345");
  String specUriString = "/foo/bar/spec";
  Spec spec = initJobSpec(specUriString);


  @BeforeSuite
  public void beforeSuite() {
    log.info("Process id = " + ManagementFactory.getRuntimeMXBean().getName());
    startServers();
  }

  public StreamingKafkaSpecExecutorTest()
      throws InterruptedException, RuntimeException {
    super();
    _kafkaBrokers = "localhost:" + this.getKafkaServerPort();
    log.info("Going to use Kakfa broker: " + _kafkaBrokers);

    cleanupTestDir();
  }

  private void cleanupTestDir() {
    File testDir = new File(_TEST_DIR_PATH);

    if (testDir.exists()) {
      try {
        FileUtils.deleteDirectory(testDir);
      } catch (IOException e) {
        throw new RuntimeException("Could not delete test directory", e);
      }
    }
  }
  @BeforeClass
  public void setup() throws Exception {
    _closer = Closer.create();
    _properties = new Properties();

    // Properties for Producer
    _properties.setProperty(KafkaWriterConfigurationKeys.KAFKA_TOPIC, TOPIC);
    _properties.setProperty("spec.kafka.dataWriterClass", "org.apache.gobblin.kafka.writer.Kafka09DataWriter");
    _properties.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + "bootstrap.servers", _kafkaBrokers);
    _properties.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX+"value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

    // Properties for Consumer
    _properties.setProperty(KafkaJobMonitor.KAFKA_JOB_MONITOR_PREFIX + "." + ConfigurationKeys.KAFKA_BROKERS, _kafkaBrokers);
    _properties.setProperty(KafkaJobMonitor.KAFKA_JOB_MONITOR_PREFIX + "." + Kafka09ConsumerClient.GOBBLIN_CONFIG_VALUE_DESERIALIZER_CLASS_KEY, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    _properties.setProperty(SimpleKafkaSpecExecutor.SPEC_KAFKA_TOPICS_KEY, TOPIC);
    _properties.setProperty("gobblin.cluster.jobconf.fullyQualifiedPath", _JOBS_DIR_PATH);
    _properties.setProperty(KafkaJobMonitor.KAFKA_JOB_MONITOR_PREFIX + "." + Kafka09ConsumerClient.CONFIG_PREFIX + Kafka09ConsumerClient.CONSUMER_CONFIG + ".auto.offset.reset", "earliest");

    Config config = ConfigUtils.propertiesToConfig(_properties);

    // SEI Producer
    _seip = _closer.register(new SimpleKafkaSpecProducer(config));

    _jobCatalog = new NonObservingFSJobCatalog(config.getConfig("gobblin.cluster"));
    _jobCatalog.startAsync().awaitRunning();

    // SEI Consumer
    _seic = _closer.register(new StreamingKafkaSpecConsumer(config, _jobCatalog));
    _seic.startAsync().awaitRunning();

  }

  @Test
  public void testAddSpec() throws Exception {
    WriteResponse writeResponse = (WriteResponse) _seip.addSpec(spec).get();
    log.info("WriteResponse: " + writeResponse);

    List<Pair<SpecExecutor.Verb, Spec>> consumedEvent = _seic.changedSpecs().get();
    Assert.assertTrue(consumedEvent.size() == 1, "Consumption did not match production");

    Map.Entry<SpecExecutor.Verb, Spec> consumedSpecAction = consumedEvent.get(0);
    Assert.assertTrue(consumedSpecAction.getKey().equals(SpecExecutor.Verb.ADD), "Verb did not match");
    Assert.assertTrue(consumedSpecAction.getValue().getUri().toString().equals(specUriString), "Expected URI did not match");
    Assert.assertTrue(consumedSpecAction.getValue() instanceof JobSpec, "Expected JobSpec");
  }

  @Test (dependsOnMethods = "testAddSpec")
  public void testUpdateSpec() throws Exception {
    // update is only treated as an update for existing job specs
    WriteResponse writeResponse = (WriteResponse) _seip.updateSpec(spec).get();
    log.info("WriteResponse: " + writeResponse);

    List<Pair<SpecExecutor.Verb, Spec>> consumedEvent = _seic.changedSpecs().get();
    Assert.assertTrue(consumedEvent.size() == 1, "Consumption did not match production");

    Map.Entry<SpecExecutor.Verb, Spec> consumedSpecAction = consumedEvent.get(0);
    Assert.assertTrue(consumedSpecAction.getKey().equals(SpecExecutor.Verb.UPDATE), "Verb did not match");
    Assert.assertTrue(consumedSpecAction.getValue().getUri().toString().equals(specUriString), "Expected URI did not match");
    Assert.assertTrue(consumedSpecAction.getValue() instanceof JobSpec, "Expected JobSpec");
  }

  @Test (dependsOnMethods = "testUpdateSpec")
  public void testDeleteSpec() throws Exception {
    // delete needs to be on a job spec that exists to get notification
    WriteResponse writeResponse = (WriteResponse) _seip.deleteSpec(new URI(specUriString)).get();
    log.info("WriteResponse: " + writeResponse);

    List<Pair<SpecExecutor.Verb, Spec>> consumedEvent = _seic.changedSpecs().get();
    Assert.assertTrue(consumedEvent.size() == 1, "Consumption did not match production");

    Map.Entry<SpecExecutor.Verb, Spec> consumedSpecAction = consumedEvent.get(0);
    Assert.assertTrue(consumedSpecAction.getKey().equals(SpecExecutor.Verb.DELETE), "Verb did not match");
    Assert.assertTrue(consumedSpecAction.getValue().getUri().toString().equals(specUriString), "Expected URI did not match");
    Assert.assertTrue(consumedSpecAction.getValue() instanceof JobSpec, "Expected JobSpec");
  }

  @Test(dependsOnMethods = "testDeleteSpec")
  public void testCancelSpec() throws Exception {
    // Cancel an existing spec that was added
    _seip.addSpec(spec).get();
    WriteResponse writeResponse = (WriteResponse) _seip.cancelJob(new URI(specUriString), new Properties()).get();
    log.info("WriteResponse: " + writeResponse);

    // Wait for the cancellation to be processed
    Thread.sleep(5000);
    List<Pair<SpecExecutor.Verb, Spec>> consumedEvent = _seic.changedSpecs().get();
    Assert.assertTrue(consumedEvent.size() == 3, "Consumption did not match production");

    Map.Entry<SpecExecutor.Verb, Spec> consumedSpecAction = consumedEvent.get(2);
    log.info(consumedSpecAction.getKey().toString());
    Assert.assertTrue(consumedEvent.get(0).getKey().equals(SpecExecutor.Verb.ADD), "Verb did not match");
    Assert.assertTrue(consumedEvent.get(1).getKey().equals(SpecExecutor.Verb.DELETE), "Verb did not match");
    Assert.assertTrue(consumedSpecAction.getKey().equals(SpecExecutor.Verb.CANCEL), "Verb did not match");
    Assert.assertTrue(consumedSpecAction.getValue().getUri().toString().equals(specUriString), "Expected URI did not match");
    Assert.assertTrue(consumedSpecAction.getValue() instanceof JobSpec, "Expected JobSpec");
  }

  @Test (dependsOnMethods = "testCancelSpec")
  public void testCancelSpecNoopDefault() throws Exception {
     _seip.addSpec(flowSpec).get();
    Properties props = new Properties();
    props.setProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "54321"); // Does not match with added jobspec, so should not cancel job
    WriteResponse writeResponse = (WriteResponse) _seip.cancelJob(new URI(flowSpecUriString), props).get();
    log.info("WriteResponse: " + writeResponse);
    // Wait for the cancellation to be processed, but it should ignore the spec as flow execution IDs do not match
    Thread.sleep(5000);
    List<Pair<SpecExecutor.Verb, Spec>> consumedEvent = _seic.changedSpecs().get();
    Assert.assertTrue(consumedEvent.size() == 1, "Consumption did not match production");

    Map.Entry<SpecExecutor.Verb, Spec> consumedSpecAction = consumedEvent.get(0);
    Assert.assertTrue(consumedSpecAction.getKey().equals(SpecExecutor.Verb.ADD), "Verb did not match");
    Assert.assertTrue(consumedSpecAction.getValue().getUri().toString().equals(flowSpecUriString), "Expected URI did not match");
    Assert.assertTrue(consumedSpecAction.getValue() instanceof JobSpec, "Expected JobSpec");

    _seip.cancelJob(new URI(flowSpecUriString), new Properties()).get();
    Thread.sleep(5000);
    consumedEvent = _seic.changedSpecs().get();
    Assert.assertTrue(consumedEvent.size() == 2, "Should emit cancellation event if no flow ID provided");
    consumedSpecAction = consumedEvent.get(1);
    Assert.assertTrue(consumedEvent.get(0).getKey().equals(SpecExecutor.Verb.DELETE), "Verb did not match");
    Assert.assertTrue(consumedSpecAction.getKey().equals(SpecExecutor.Verb.CANCEL), "Verb did not match");
    Assert.assertTrue(consumedSpecAction.getValue().getUri().toString().equals(flowSpecUriString), "Expected URI did not match");
    Assert.assertTrue(consumedSpecAction.getValue() instanceof JobSpec, "Expected JobSpec");
  }

  @Test(dependsOnMethods = "testCancelSpecNoopDefault")
  public void testCancelSpecWithFlowExecutionId() throws Exception {
    _seip.addSpec(flowSpec).get();
    Properties props = new Properties();
    props.setProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "12345");
    WriteResponse writeResponse = (WriteResponse) _seip.cancelJob(new URI(flowSpecUriString), props).get();
    log.info("WriteResponse: " + writeResponse);

    // Wait for the cancellation to be processed
    Thread.sleep(5000);
    List<Pair<SpecExecutor.Verb, Spec>> consumedEvent = _seic.changedSpecs().get();
    Assert.assertTrue(consumedEvent.size() == 3, "Consumption did not match production");

    Map.Entry<SpecExecutor.Verb, Spec> consumedSpecAction = consumedEvent.get(2);
    log.info(consumedSpecAction.getKey().toString());
    Assert.assertTrue(consumedEvent.get(0).getKey().equals(SpecExecutor.Verb.ADD), "Verb did not match");
    Assert.assertTrue(consumedEvent.get(1).getKey().equals(SpecExecutor.Verb.DELETE), "Verb did not match");
    Assert.assertTrue(consumedSpecAction.getKey().equals(SpecExecutor.Verb.CANCEL), "Verb did not match");
    Assert.assertTrue(consumedSpecAction.getValue().getUri().toString().equals(flowSpecUriString), "Expected URI did not match");
    Assert.assertTrue(consumedSpecAction.getValue() instanceof JobSpec, "Expected JobSpec");
  }


  private static JobSpec initJobSpec(String specUri) {
    Properties properties = new Properties();
    return JobSpec.builder(specUri)
        .withConfig(ConfigUtils.propertiesToConfig(properties))
        .withVersion("1")
        .withDescription("Spec Description")
        .build();
  }

  private static JobSpec initJobSpecWithFlowExecutionId(String specUri, String flowExecutionId) {
    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, flowExecutionId);
    return JobSpec.builder(specUri)
        .withConfig(ConfigUtils.propertiesToConfig(properties))
        .withVersion("1")
        .withDescription("Spec Description")
        .build();
  }

  @AfterSuite
  public void after() {
    try {
      _closer.close();
    } catch(Exception e) {
      log.error("Failed to close SEIC and SEIP.", e);
    }
    try {
      close();
    } catch(Exception e) {
      log.error("Failed to close Kafka server.", e);
    }

    if (_jobCatalog != null) {
      _jobCatalog.stopAsync().awaitTerminated();
    }

    cleanupTestDir();
  }
}