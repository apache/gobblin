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

package gobblin.service;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.Test;

import com.google.common.io.Closer;
import com.typesafe.config.Config;

import gobblin.configuration.ConfigurationKeys;
import gobblin.kafka.writer.KafkaWriterConfigurationKeys;
import gobblin.metrics.reporter.KafkaTestBase;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.Spec;
import gobblin.runtime.api.SpecExecutorInstance;
import gobblin.runtime.job_catalog.NonObservingFSJobCatalog;
import gobblin.util.ConfigUtils;
import gobblin.writer.WriteResponse;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class StreamingKafkaSpecExecutorInstanceTest extends KafkaTestBase {

  public static final String TOPIC = StreamingKafkaSpecExecutorInstanceTest.class.getSimpleName();

  private Closer _closer;
  private Properties _properties;
  private SimpleKafkaSpecExecutorInstanceProducer _seip;
  private StreamingKafkaSpecExecutorInstanceConsumer _seic;
  private NonObservingFSJobCatalog _jobCatalog;
  private String _kafkaBrokers;
  private static final String _TEST_DIR_PATH = "/tmp/StreamingKafkaSpecExecutorInstanceTest";
  private static final String _JOBS_DIR_PATH = _TEST_DIR_PATH + "/jobs";

  public StreamingKafkaSpecExecutorInstanceTest()
      throws InterruptedException, RuntimeException {
    super(TOPIC);
    _kafkaBrokers = "localhost:" + kafkaPort;
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

  @Test
  public void testAddSpec() throws Exception {
    _closer = Closer.create();
    _properties = new Properties();

    // Properties for Producer
    _properties.setProperty(KafkaWriterConfigurationKeys.KAFKA_TOPIC, TOPIC);
    _properties.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + "bootstrap.servers", _kafkaBrokers);
    _properties.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX+"value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

    // Properties for Consumer
    _properties.setProperty("jobSpecMonitor.kafka.zookeeper.connect", zkConnect);
    _properties.setProperty(SimpleKafkaSpecExecutorInstanceProducer.SPEC_KAFKA_TOPICS_KEY, TOPIC);
    _properties.setProperty("gobblin.cluster.jobconf.fullyQualifiedPath", _JOBS_DIR_PATH);

    Config config = ConfigUtils.propertiesToConfig(_properties);

    // SEI Producer
    _seip = _closer.register(new SimpleKafkaSpecExecutorInstanceProducer(config));

    String addedSpecUriString = "/foo/bar/addedSpec";
    Spec spec = initJobSpec(addedSpecUriString);
    WriteResponse writeResponse = (WriteResponse) _seip.addSpec(spec).get();
    log.info("WriteResponse: " + writeResponse);

    _jobCatalog = new NonObservingFSJobCatalog(config.getConfig("gobblin.cluster"));
    _jobCatalog.startAsync().awaitRunning();

    // SEI Consumer
    _seic = _closer.register(new StreamingKafkaSpecExecutorInstanceConsumer(config, _jobCatalog));
    _seic.startAsync().awaitRunning();

    List<Pair<SpecExecutorInstance.Verb, Spec>> consumedEvent = _seic.changedSpecs().get();
    Assert.assertTrue(consumedEvent.size() == 1, "Consumption did not match production");

    Map.Entry<SpecExecutorInstance.Verb, Spec> consumedSpecAction = consumedEvent.get(0);
    Assert.assertTrue(consumedSpecAction.getKey().equals(SpecExecutorInstance.Verb.ADD), "Verb did not match");
    Assert.assertTrue(consumedSpecAction.getValue().getUri().toString().equals(addedSpecUriString), "Expected URI did not match");
    Assert.assertTrue(consumedSpecAction.getValue() instanceof JobSpec, "Expected JobSpec");
  }

  @Test (dependsOnMethods = "testAddSpec")
  public void testUpdateSpec() throws Exception {
    // update is only treated as an update for existing job specs
    String updatedSpecUriString = "/foo/bar/addedSpec";
    Spec spec = initJobSpec(updatedSpecUriString);
    WriteResponse writeResponse = (WriteResponse) _seip.updateSpec(spec).get();
    log.info("WriteResponse: " + writeResponse);

    List<Pair<SpecExecutorInstance.Verb, Spec>> consumedEvent = _seic.changedSpecs().get();
    Assert.assertTrue(consumedEvent.size() == 1, "Consumption did not match production");

    Map.Entry<SpecExecutorInstance.Verb, Spec> consumedSpecAction = consumedEvent.get(0);
    Assert.assertTrue(consumedSpecAction.getKey().equals(SpecExecutorInstance.Verb.UPDATE), "Verb did not match");
    Assert.assertTrue(consumedSpecAction.getValue().getUri().toString().equals(updatedSpecUriString), "Expected URI did not match");
    Assert.assertTrue(consumedSpecAction.getValue() instanceof JobSpec, "Expected JobSpec");
  }

  @Test (dependsOnMethods = "testUpdateSpec")
  public void testDeleteSpec() throws Exception {
    // delete needs to be on a job spec that exists to get notification
    String deletedSpecUriString = "/foo/bar/addedSpec";
    WriteResponse writeResponse = (WriteResponse) _seip.deleteSpec(new URI(deletedSpecUriString)).get();
    log.info("WriteResponse: " + writeResponse);

    List<Pair<SpecExecutorInstance.Verb, Spec>> consumedEvent = _seic.changedSpecs().get();
    Assert.assertTrue(consumedEvent.size() == 1, "Consumption did not match production");

    Map.Entry<SpecExecutorInstance.Verb, Spec> consumedSpecAction = consumedEvent.get(0);
    Assert.assertTrue(consumedSpecAction.getKey().equals(SpecExecutorInstance.Verb.DELETE), "Verb did not match");
    Assert.assertTrue(consumedSpecAction.getValue().getUri().toString().equals(deletedSpecUriString), "Expected URI did not match");
    Assert.assertTrue(consumedSpecAction.getValue() instanceof JobSpec, "Expected JobSpec");
  }

  private JobSpec initJobSpec(String specUri) {
    Properties properties = new Properties();
    return JobSpec.builder(specUri)
        .withConfig(ConfigUtils.propertiesToConfig(properties))
        .withVersion("1")
        .withDescription("Spec Description")
        .build();
  }

  @AfterClass
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

  @AfterSuite
  public void afterSuite() {
    closeServer();
  }
}
