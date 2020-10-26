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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import kafka.consumer.ConsumerIterator;
import kafka.message.MessageAndMetadata;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.kafka.KafkaTestBase;
import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;
import org.apache.gobblin.kafka.client.Kafka09ConsumerClient;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.metrics.kafka.KafkaAvroEventKeyValueReporter;
import org.apache.gobblin.metrics.kafka.KafkaEventReporter;
import org.apache.gobblin.metrics.kafka.KafkaKeyValueProducerPusher;
import org.apache.gobblin.metrics.kafka.Pusher;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;
import org.apache.gobblin.service.monitoring.KafkaAvroJobStatusMonitor;
import org.apache.gobblin.service.monitoring.KafkaJobStatusMonitor;


public class KafkaAvroJobStatusMonitorTest {
  public static final String TOPIC = KafkaAvroJobStatusMonitorTest.class.getSimpleName();

  private KafkaTestBase kafkaTestHelper;
  private String flowGroup = "myFlowGroup";
  private String flowName = "myFlowName";
  private String jobGroup = "myJobGroup";
  private String jobName = "myJobName";
  private String flowExecutionId = "1234";
  private String jobExecutionId = "1111";
  private String message = "https://myServer:8143/1234/1111";
  private String stateStoreDir = "/tmp/jobStatusMonitor/statestore";
  private MetricContext context;
  private KafkaAvroEventKeyValueReporter.Builder<?> builder;

  @BeforeClass
  public void setUp() throws Exception {
    cleanUpDir(stateStoreDir);
    kafkaTestHelper = new KafkaTestBase();
    kafkaTestHelper.startServers();
    kafkaTestHelper.provisionTopic(TOPIC);

    // Create KeyValueProducerPusher instance.
    Pusher pusher = new KafkaKeyValueProducerPusher<byte[], byte[]>("localhost:dummy", TOPIC,
        Optional.of(ConfigFactory.parseMap(ImmutableMap.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + this.kafkaTestHelper.getKafkaServerPort()))));

    //Create an event reporter instance
    context = MetricContext.builder("context").build();
    builder = KafkaAvroEventKeyValueReporter.Factory.forContext(context);
    builder = builder.withKafkaPusher(pusher).withKeys(Lists.newArrayList(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD,
        TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD));
  }

  @Test
  public void testProcessMessageForSuccessfulFlow() throws IOException, ReflectiveOperationException {
    KafkaEventReporter kafkaReporter = builder.build("localhost:0000", "topic1");

    //Submit GobblinTrackingEvents to Kafka
    GobblinTrackingEvent event1 = createFlowCompiledEvent();
    context.submitEvent(event1);
    kafkaReporter.report();

    GobblinTrackingEvent event2 = createJobOrchestratedEvent(1);
    context.submitEvent(event2);
    kafkaReporter.report();

    GobblinTrackingEvent event3 = createJobStartEvent();
    context.submitEvent(event3);
    kafkaReporter.report();

    GobblinTrackingEvent event4 = createJobSucceededEvent();
    context.submitEvent(event4);
    kafkaReporter.report();

    GobblinTrackingEvent event5 = createDummyEvent();
    context.submitEvent(event5);
    kafkaReporter.report();

    GobblinTrackingEvent event6 = createJobStartEvent();
    context.submitEvent(event6);
    kafkaReporter.report();

    try {
      Thread.sleep(1000);
    } catch(InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    Config config = ConfigFactory.empty().withValue(ConfigurationKeys.KAFKA_BROKERS, ConfigValueFactory.fromAnyRef("localhost:0000"))
        .withValue(Kafka09ConsumerClient.GOBBLIN_CONFIG_VALUE_DESERIALIZER_CLASS_KEY, ConfigValueFactory.fromAnyRef("org.apache.kafka.common.serialization.ByteArrayDeserializer"))
        .withValue(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, ConfigValueFactory.fromAnyRef(stateStoreDir))
        .withValue("zookeeper.connect", ConfigValueFactory.fromAnyRef("localhost:2121"));
    MockKafkaAvroJobStatusMonitor jobStatusMonitor =  new MockKafkaAvroJobStatusMonitor("test",config, 1);
    jobStatusMonitor.buildMetricsContextAndMetrics();

    ConsumerIterator<byte[], byte[]> iterator = this.kafkaTestHelper.getIteratorForTopic(TOPIC);

    MessageAndMetadata<byte[], byte[]> messageAndMetadata = iterator.next();
    jobStatusMonitor.processMessage(convertMessageAndMetadataToDecodableKafkaRecord(messageAndMetadata));

    StateStore stateStore = jobStatusMonitor.getStateStore();
    String storeName = KafkaJobStatusMonitor.jobStatusStoreName(flowGroup, flowName);
    String tableName = KafkaJobStatusMonitor.jobStatusTableName(this.flowExecutionId, "NA", "NA");
    List<State> stateList  = stateStore.getAll(storeName, tableName);
    Assert.assertEquals(stateList.size(), 1);
    State state = stateList.get(0);
    Assert.assertEquals(state.getProp(JobStatusRetriever.EVENT_NAME_FIELD), ExecutionStatus.COMPILED.name());

    messageAndMetadata = iterator.next();
    jobStatusMonitor.processMessage(convertMessageAndMetadataToDecodableKafkaRecord(messageAndMetadata));

    tableName = KafkaJobStatusMonitor.jobStatusTableName(this.flowExecutionId, this.jobGroup, this.jobName);
    stateList  = stateStore.getAll(storeName, tableName);
    Assert.assertEquals(stateList.size(), 1);
    state = stateList.get(0);
    Assert.assertEquals(state.getProp(JobStatusRetriever.EVENT_NAME_FIELD), ExecutionStatus.ORCHESTRATED.name());

    messageAndMetadata = iterator.next();
    jobStatusMonitor.processMessage(convertMessageAndMetadataToDecodableKafkaRecord(messageAndMetadata));

    stateList  = stateStore.getAll(storeName, tableName);
    Assert.assertEquals(stateList.size(), 1);
    state = stateList.get(0);
    Assert.assertEquals(state.getProp(JobStatusRetriever.EVENT_NAME_FIELD), ExecutionStatus.RUNNING.name());

    messageAndMetadata = iterator.next();
    jobStatusMonitor.processMessage(convertMessageAndMetadataToDecodableKafkaRecord(messageAndMetadata));

    stateList  = stateStore.getAll(storeName, tableName);
    Assert.assertEquals(stateList.size(), 1);
    state = stateList.get(0);
    Assert.assertEquals(state.getProp(JobStatusRetriever.EVENT_NAME_FIELD), ExecutionStatus.COMPLETE.name());

    messageAndMetadata = iterator.next();
    Assert.assertNull(jobStatusMonitor.parseJobStatus(convertMessageAndMetadataToDecodableKafkaRecord(messageAndMetadata)));

    // Check that state didn't get set to running since it was already complete
    messageAndMetadata = iterator.next();
    jobStatusMonitor.processMessage(convertMessageAndMetadataToDecodableKafkaRecord(messageAndMetadata));

    stateList  = stateStore.getAll(storeName, tableName);
    Assert.assertEquals(stateList.size(), 1);
    state = stateList.get(0);
    Assert.assertEquals(state.getProp(JobStatusRetriever.EVENT_NAME_FIELD), ExecutionStatus.COMPLETE.name());

    jobStatusMonitor.shutDown();
  }

  @Test
  public void testProcessMessageForFailedFlow() throws IOException, ReflectiveOperationException {
    KafkaEventReporter kafkaReporter = builder.build("localhost:0000", "topic2");

    //Submit GobblinTrackingEvents to Kafka
    GobblinTrackingEvent event1 = createFlowCompiledEvent();
    context.submitEvent(event1);
    kafkaReporter.report();

    //set maximum attempt to 2, and current attempt to 1
    GobblinTrackingEvent event2 = createJobOrchestratedEvent(1);
    context.submitEvent(event2);
    kafkaReporter.report();

    GobblinTrackingEvent event3 = createJobStartEvent();
    context.submitEvent(event3);
    kafkaReporter.report();

    GobblinTrackingEvent event4 = createJobFailedEvent();
    context.submitEvent(event4);
    kafkaReporter.report();

    //Mimic retrying - job orchestration
    //set maximum attempt to 2, and current attempt to 2
    GobblinTrackingEvent event5 = createJobOrchestratedEvent(2);
    context.submitEvent(event5);
    kafkaReporter.report();

    //Mimic retrying - job start (current attempt = 2)
    GobblinTrackingEvent event6 = createJobStartEvent();
    context.submitEvent(event6);
    kafkaReporter.report();

    //Mimic retrying - job failed again (current attempt = 2)
    GobblinTrackingEvent event7 = createJobFailedEvent();
    context.submitEvent(event7);
    kafkaReporter.report();

    try {
      Thread.sleep(1000);
    } catch(InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    Config config = ConfigFactory.empty().withValue(ConfigurationKeys.KAFKA_BROKERS, ConfigValueFactory.fromAnyRef("localhost:0000"))
        .withValue(Kafka09ConsumerClient.GOBBLIN_CONFIG_VALUE_DESERIALIZER_CLASS_KEY, ConfigValueFactory.fromAnyRef("org.apache.kafka.common.serialization.ByteArrayDeserializer"))
        .withValue(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, ConfigValueFactory.fromAnyRef(stateStoreDir))
        .withValue("zookeeper.connect", ConfigValueFactory.fromAnyRef("localhost:2121"));
    MockKafkaAvroJobStatusMonitor jobStatusMonitor = new MockKafkaAvroJobStatusMonitor("test",config, 1);
    jobStatusMonitor.buildMetricsContextAndMetrics();

    ConsumerIterator<byte[], byte[]> iterator = this.kafkaTestHelper.getIteratorForTopic(TOPIC);

    MessageAndMetadata<byte[], byte[]> messageAndMetadata = iterator.next();
    // Verify undecodeable message is skipped
    byte[] undecodeableMessage = Arrays.copyOf(messageAndMetadata.message(),
        messageAndMetadata.message().length - 1);
    ConsumerRecord undecodeableRecord = new ConsumerRecord<>(TOPIC, messageAndMetadata.partition(),
        messageAndMetadata.offset(), messageAndMetadata.key(), undecodeableMessage);
    Assert.assertEquals(jobStatusMonitor.getMessageParseFailures().getCount(), 0L);
    jobStatusMonitor.processMessage(new Kafka09ConsumerClient.Kafka09ConsumerRecord(undecodeableRecord));
    Assert.assertEquals(jobStatusMonitor.getMessageParseFailures().getCount(), 1L);
    // Test an normal event
    jobStatusMonitor.processMessage(convertMessageAndMetadataToDecodableKafkaRecord(messageAndMetadata));

    StateStore stateStore = jobStatusMonitor.getStateStore();
    String storeName = KafkaJobStatusMonitor.jobStatusStoreName(flowGroup, flowName);
    String tableName = KafkaJobStatusMonitor.jobStatusTableName(this.flowExecutionId, "NA", "NA");
    List<State> stateList  = stateStore.getAll(storeName, tableName);
    Assert.assertEquals(stateList.size(), 1);
    State state = stateList.get(0);
    Assert.assertEquals(state.getProp(JobStatusRetriever.EVENT_NAME_FIELD), ExecutionStatus.COMPILED.name());

    messageAndMetadata = iterator.next();
    jobStatusMonitor.processMessage(convertMessageAndMetadataToDecodableKafkaRecord(messageAndMetadata));

    tableName = KafkaJobStatusMonitor.jobStatusTableName(this.flowExecutionId, this.jobGroup, this.jobName);
    stateList  = stateStore.getAll(storeName, tableName);
    Assert.assertEquals(stateList.size(), 1);
    state = stateList.get(0);
    Assert.assertEquals(state.getProp(JobStatusRetriever.EVENT_NAME_FIELD), ExecutionStatus.ORCHESTRATED.name());

    messageAndMetadata = iterator.next();
    jobStatusMonitor.processMessage(convertMessageAndMetadataToDecodableKafkaRecord(messageAndMetadata));

    stateList  = stateStore.getAll(storeName, tableName);
    Assert.assertEquals(stateList.size(), 1);
    state = stateList.get(0);
    Assert.assertEquals(state.getProp(JobStatusRetriever.EVENT_NAME_FIELD), ExecutionStatus.RUNNING.name());

    messageAndMetadata = iterator.next();
    jobStatusMonitor.processMessage(convertMessageAndMetadataToDecodableKafkaRecord(messageAndMetadata));

    stateList  = stateStore.getAll(storeName, tableName);
    Assert.assertEquals(stateList.size(), 1);
    state = stateList.get(0);
    //Because the maximum attempt is set to 2, so the state is set to PENDING_RETRY after the first failure
    Assert.assertEquals(state.getProp(JobStatusRetriever.EVENT_NAME_FIELD), ExecutionStatus.PENDING_RETRY.name());
    Assert.assertEquals(state.getProp(TimingEvent.FlowEventConstants.SHOULD_RETRY_FIELD), Boolean.toString(true));

    messageAndMetadata = iterator.next();
    jobStatusMonitor.processMessage(convertMessageAndMetadataToDecodableKafkaRecord(messageAndMetadata));

    stateList  = stateStore.getAll(storeName, tableName);
    Assert.assertEquals(stateList.size(), 1);
    state = stateList.get(0);
    //Job orchestrated for retrying
    Assert.assertEquals(state.getProp(JobStatusRetriever.EVENT_NAME_FIELD), ExecutionStatus.ORCHESTRATED.name());

    messageAndMetadata = iterator.next();
    jobStatusMonitor.processMessage(convertMessageAndMetadataToDecodableKafkaRecord(messageAndMetadata));

    stateList  = stateStore.getAll(storeName, tableName);
    Assert.assertEquals(stateList.size(), 1);
    state = stateList.get(0);
    //Because the maximum attempt is set to 2, so the state is set to PENDING_RETRY after the first failure
    Assert.assertEquals(state.getProp(JobStatusRetriever.EVENT_NAME_FIELD), ExecutionStatus.RUNNING.name());

    messageAndMetadata = iterator.next();
    jobStatusMonitor.processMessage(convertMessageAndMetadataToDecodableKafkaRecord(messageAndMetadata));

    stateList  = stateStore.getAll(storeName, tableName);
    Assert.assertEquals(stateList.size(), 1);
    state = stateList.get(0);
    //Because the maximum attempt is set to 2, so the state is set to Failed after trying twice
    Assert.assertEquals(state.getProp(JobStatusRetriever.EVENT_NAME_FIELD), ExecutionStatus.FAILED.name());
    Assert.assertEquals(state.getProp(TimingEvent.FlowEventConstants.SHOULD_RETRY_FIELD), Boolean.toString(false));

    jobStatusMonitor.shutDown();
  }

  private GobblinTrackingEvent createFlowCompiledEvent() {
    String namespace = "org.apache.gobblin.metrics";
    Long timestamp = System.currentTimeMillis();
    String name = TimingEvent.FlowTimings.FLOW_COMPILED;
    Map<String, String> metadata = Maps.newHashMap();
    metadata.put(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, this.flowGroup);
    metadata.put(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, this.flowName);
    metadata.put(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, this.flowExecutionId);
    metadata.put(TimingEvent.METADATA_START_TIME, "1");
    metadata.put(TimingEvent.METADATA_END_TIME, "2");
    GobblinTrackingEvent event = new GobblinTrackingEvent(timestamp, namespace, name, metadata);
    return event;
  }

  /**
   * Create a Job Orchestrated Event with a configurable currentAttempt
   * @param currentAttempt specify the number of attempts for the JobOrchestration event
   * @return the {@link GobblinTrackingEvent}
   */
  private GobblinTrackingEvent createJobOrchestratedEvent(int currentAttempt) {
    String namespace = "org.apache.gobblin.metrics";
    Long timestamp = System.currentTimeMillis();
    String name = TimingEvent.LauncherTimings.JOB_ORCHESTRATED;
    Map<String, String> metadata = Maps.newHashMap();
    metadata.put(TimingEvent.FlowEventConstants.MAX_ATTEMPTS_FIELD, "2");
    metadata.put(TimingEvent.FlowEventConstants.CURRENT_ATTEMPTS_FIELD, String.valueOf(currentAttempt));
    metadata.put(TimingEvent.FlowEventConstants.SHOULD_RETRY_FIELD, Boolean.toString(false));
    metadata.put(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, this.flowGroup);
    metadata.put(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, this.flowName);
    metadata.put(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, this.flowExecutionId);
    metadata.put(TimingEvent.FlowEventConstants.JOB_NAME_FIELD, this.jobName);
    metadata.put(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD, this.jobGroup);
    metadata.put(TimingEvent.METADATA_START_TIME, "3");
    metadata.put(TimingEvent.METADATA_END_TIME, "4");

    GobblinTrackingEvent event = new GobblinTrackingEvent(timestamp, namespace, name, metadata);
    return event;
  }

  private GobblinTrackingEvent createJobStartEvent() {
    String namespace = "org.apache.gobblin.metrics";
    Long timestamp = System.currentTimeMillis();
    String name = TimingEvent.LauncherTimings.JOB_START;
    Map<String, String> metadata = Maps.newHashMap();
    metadata.put(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, this.flowGroup);
    metadata.put(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, this.flowName);
    metadata.put(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, this.flowExecutionId);
    metadata.put(TimingEvent.FlowEventConstants.JOB_NAME_FIELD, this.jobName);
    metadata.put(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD, this.jobGroup);
    metadata.put(TimingEvent.FlowEventConstants.JOB_EXECUTION_ID_FIELD, this.jobExecutionId);
    metadata.put(TimingEvent.METADATA_MESSAGE, this.message);
    metadata.put(TimingEvent.METADATA_START_TIME, "5");
    metadata.put(TimingEvent.METADATA_END_TIME, "6");

    GobblinTrackingEvent event = new GobblinTrackingEvent(timestamp, namespace, name, metadata);
    return event;
  }

  private GobblinTrackingEvent createJobSucceededEvent() {
    String namespace = "org.apache.gobblin.metrics";
    Long timestamp = System.currentTimeMillis();
    String name = TimingEvent.LauncherTimings.JOB_SUCCEEDED;
    Map<String, String> metadata = Maps.newHashMap();
    metadata.put(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, this.flowGroup);
    metadata.put(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, this.flowName);
    metadata.put(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, this.flowExecutionId);
    metadata.put(TimingEvent.FlowEventConstants.JOB_NAME_FIELD, this.jobName);
    metadata.put(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD, this.jobGroup);
    metadata.put(TimingEvent.FlowEventConstants.JOB_EXECUTION_ID_FIELD, this.jobExecutionId);
    metadata.put(TimingEvent.METADATA_MESSAGE, this.message);
    metadata.put(TimingEvent.METADATA_START_TIME, "7");
    metadata.put(TimingEvent.METADATA_END_TIME, "8");

    GobblinTrackingEvent event = new GobblinTrackingEvent(timestamp, namespace, name, metadata);
    return event;
  }

  private GobblinTrackingEvent createJobFailedEvent() {
    String namespace = "org.apache.gobblin.metrics";
    Long timestamp = System.currentTimeMillis();
    String name = TimingEvent.LauncherTimings.JOB_FAILED;
    Map<String, String> metadata = Maps.newHashMap();
    metadata.put(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, this.flowGroup);
    metadata.put(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, this.flowName);
    metadata.put(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, this.flowExecutionId);
    metadata.put(TimingEvent.FlowEventConstants.JOB_NAME_FIELD, this.jobName);
    metadata.put(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD, this.jobGroup);
    metadata.put(TimingEvent.FlowEventConstants.JOB_EXECUTION_ID_FIELD, this.jobExecutionId);
    metadata.put(TimingEvent.METADATA_MESSAGE, this.message);
    metadata.put(TimingEvent.METADATA_START_TIME, "7");
    metadata.put(TimingEvent.METADATA_END_TIME, "8");

    GobblinTrackingEvent event = new GobblinTrackingEvent(timestamp, namespace, name, metadata);
    return event;
  }
  /**
   *   Create a dummy event to test if it is filtered out by the consumer.
   */
  private GobblinTrackingEvent createDummyEvent() {
    String namespace = "org.apache.gobblin.metrics";
    Long timestamp = System.currentTimeMillis();
    String name = "dummy";
    Map<String, String> metadata = Maps.newHashMap();
    metadata.put("k1", "v1");
    metadata.put("k2", "v2");

    GobblinTrackingEvent event = new GobblinTrackingEvent(timestamp, namespace, name, metadata);
    return event;
  }

  private DecodeableKafkaRecord convertMessageAndMetadataToDecodableKafkaRecord(MessageAndMetadata messageAndMetadata) {
    ConsumerRecord consumerRecord = new ConsumerRecord<>(TOPIC, messageAndMetadata.partition(), messageAndMetadata.offset(), messageAndMetadata.key(), messageAndMetadata.message());
    return new Kafka09ConsumerClient.Kafka09ConsumerRecord(consumerRecord);
  }

  private void cleanUpDir(String dir) throws Exception {
    File specStoreDir = new File(dir);
    if (specStoreDir.exists()) {
      FileUtils.deleteDirectory(specStoreDir);
    }
  }

  @AfterMethod
  public void cleanUpStateStore() {
    try {
      cleanUpDir(stateStoreDir);
    } catch(Exception e) {
      System.err.println("Failed to clean up the state store.");
    }
  }

  @AfterClass
  public void tearDown() {
    try {
      this.kafkaTestHelper.close();
    } catch(Exception e) {
      System.err.println("Failed to close Kafka server.");
    }
  }

  class MockKafkaAvroJobStatusMonitor extends KafkaAvroJobStatusMonitor {

    public MockKafkaAvroJobStatusMonitor(String topic, Config config, int numThreads)
        throws IOException, ReflectiveOperationException {
      super(topic, config, numThreads);
    }

    @Override
    protected void processMessage(DecodeableKafkaRecord record) {
      super.processMessage(record);
    }

    @Override
    protected void buildMetricsContextAndMetrics() {
      super.buildMetricsContextAndMetrics();
    }
  }
}