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

import java.io.IOException;
import java.net.URI;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;
import org.apache.gobblin.kafka.client.Kafka09ConsumerClient;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.service.modules.orchestration.DagActionReminderScheduler;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagManagement;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.task.DagProcessingEngineMetrics;
import org.apache.gobblin.service.monitoring.DagActionStoreChangeEvent;
import org.apache.gobblin.service.monitoring.DagActionStoreChangeMonitor;
import org.apache.gobblin.service.monitoring.DagActionValue;
import org.apache.gobblin.service.monitoring.GenericStoreChangeEvent;
import org.apache.gobblin.service.monitoring.OperationType;

import static org.mockito.Mockito.*;


/**
 * Tests the main functionality of {@link DagActionStoreChangeMonitor} to process {@link DagActionStoreChangeEvent} type
 * events stored in a {@link org.apache.gobblin.kafka.client.KafkaConsumerRecord}. The
 * processMessage(DecodeableKafkaRecord message) function should be able to gracefully process a variety of message
 * types, even with undesired formats, without throwing exceptions.
 */
@Slf4j
public class DagActionStoreChangeMonitorTest {
  public static final String TOPIC = DagActionStoreChangeEvent.class.getSimpleName();
  private final String FLOW_GROUP = "flowGroup";
  private final String FLOW_NAME = "flowName";
  private final String FLOW_EXECUTION_ID = "123456789";
  private MockDagActionStoreChangeMonitor mockDagActionStoreChangeMonitor;
  private int txidCounter = 0;
  private static final DagActionReminderScheduler dagActionReminderScheduler = mock(DagActionReminderScheduler.class);

  private ITestMetastoreDatabase testDb;

  /**
   * Note: The class methods are wrapped in a test specific method because the original methods are package protected
   * and cannot be accessed by this class.
   */
  static class MockDagActionStoreChangeMonitor extends DagActionStoreChangeMonitor {

    public MockDagActionStoreChangeMonitor(Config config, int numThreads) {
      this(config, numThreads, mock(DagManagementStateStore.class));
    }

    public MockDagActionStoreChangeMonitor(Config config, int numThreads, DagManagementStateStore dagManagementStateStore) {
      super(config, numThreads, dagManagementStateStore, mock(DagManagement.class),
          dagActionReminderScheduler, mock(DagProcessingEngineMetrics.class));
    }

    protected void processMessageForTest(DecodeableKafkaRecord<String, DagActionStoreChangeEvent> record) {
      super.processMessage(record);
    }

    protected void startUpForTest() {
      super.startUp();
    }
  }

  MockDagActionStoreChangeMonitor createMockDagActionStoreChangeMonitor() {
    Config config = ConfigFactory.empty().withValue(ConfigurationKeys.KAFKA_BROKERS, ConfigValueFactory.fromAnyRef("localhost:0000"))
        .withValue(Kafka09ConsumerClient.GOBBLIN_CONFIG_VALUE_DESERIALIZER_CLASS_KEY, ConfigValueFactory.fromAnyRef("org.apache.kafka.common.serialization.ByteArrayDeserializer"))
        .withValue(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, ConfigValueFactory.fromAnyRef("/tmp/fakeStateStore"))
        .withValue("zookeeper.connect", ConfigValueFactory.fromAnyRef("localhost:2121"));
    return new MockDagActionStoreChangeMonitor(config, 5);
  }

  // Called at start of every test so the count of each method being called is reset to 0
  @BeforeMethod
  public void setupMockMonitor() {
     mockDagActionStoreChangeMonitor = createMockDagActionStoreChangeMonitor();
     mockDagActionStoreChangeMonitor.startUpForTest();
  }

  @BeforeClass
  public void setUp() throws Exception {
    this.testDb = TestMetastoreDatabaseFactory.get();
    doNothing().when(dagActionReminderScheduler).unscheduleReminderJob(any(), anyBoolean());
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() throws Exception {
    // `.close()` to avoid (in the aggregate, across multiple suites) - java.sql.SQLNonTransientConnectionException: Too many connections
    this.testDb.close();
  }

  /**
   * Ensure no NPE results from passing a HEARTBEAT type message with a null {@link DagActionValue} and the message is
   * filtered out since it's a heartbeat type so no methods are called.
   */
  @Test
  public void testProcessMessageWithHeartbeatAndNullDagAction() throws IOException {
    Kafka09ConsumerClient.Kafka09ConsumerRecord<String, DagActionStoreChangeEvent> consumerRecord =
        wrapDagActionStoreChangeEvent(OperationType.HEARTBEAT, "", "", "",
            DagActionStore.NO_JOB_NAME_DEFAULT, null);
    mockDagActionStoreChangeMonitor.processMessageForTest(consumerRecord);
    verify(mockDagActionStoreChangeMonitor.getDagManagement(), times(0)).addDagAction(any());
  }

  /**
   * Ensure a HEARTBEAT type message with non-empty flow information is filtered out since it's a heartbeat type so no
   * methods are called.
   */
  @Test (dependsOnMethods = "testProcessMessageWithHeartbeatAndNullDagAction")
  public void testProcessMessageWithHeartbeatAndFlowInfo() throws IOException {
    Kafka09ConsumerClient.Kafka09ConsumerRecord<String, DagActionStoreChangeEvent> consumerRecord =
        wrapDagActionStoreChangeEvent(OperationType.HEARTBEAT, FLOW_GROUP, "", "",
            DagActionStore.NO_JOB_NAME_DEFAULT, DagActionValue.RESUME);
    mockDagActionStoreChangeMonitor.processMessageForTest(consumerRecord);
    verify(mockDagActionStoreChangeMonitor.getDagManagement(), times(0)).addDagAction(any());
  }

  /**
   * Tests process message with an INSERT type message of a `launch` action
   */
  @Test (dependsOnMethods = "testProcessMessageWithHeartbeatAndFlowInfo")
  public void testProcessMessageWithInsertLaunchType() throws IOException {
    Kafka09ConsumerClient.Kafka09ConsumerRecord<String, DagActionStoreChangeEvent> consumerRecord =
        wrapDagActionStoreChangeEvent(OperationType.INSERT, FLOW_GROUP, FLOW_NAME, FLOW_EXECUTION_ID,
            DagActionStore.NO_JOB_NAME_DEFAULT, DagActionValue.LAUNCH);
    mockDagActionStoreChangeMonitor.processMessageForTest(consumerRecord);
    verify(mockDagActionStoreChangeMonitor.getDagManagement(), times(1)).addDagAction(any());
    verify(mockDagActionStoreChangeMonitor.getDagManagement(), times(1))
        .addDagAction(argThat(leaseParam -> leaseParam.getDagAction().getDagActionType() == DagActionStore.DagActionType.LAUNCH));
  }

  /**
   * Tests process message with an INSERT type message of a `resume` action. It re-uses the same flow information however
   * since it is a different tid used every time it will be considered unique and submit a kill request.
   */
  @Test (dependsOnMethods = "testProcessMessageWithInsertLaunchType")
  public void testProcessMessageWithInsertResumeType() throws IOException {
    Kafka09ConsumerClient.Kafka09ConsumerRecord<String, DagActionStoreChangeEvent> consumerRecord =
        wrapDagActionStoreChangeEvent(OperationType.INSERT, FLOW_GROUP, FLOW_NAME, FLOW_EXECUTION_ID,
            DagActionStore.NO_JOB_NAME_DEFAULT, DagActionValue.RESUME);
    mockDagActionStoreChangeMonitor.processMessageForTest(consumerRecord);
    verify(mockDagActionStoreChangeMonitor.getDagManagement(), times(1)).addDagAction(any());
    verify(mockDagActionStoreChangeMonitor.getDagManagement(), times(1))
        .addDagAction(argThat(leaseParam -> leaseParam.getDagAction().getDagActionType() == DagActionStore.DagActionType.RESUME));
  }

  /**
   * Tests process message with an INSERT type message of a `kill` action. Similar to `testProcessMessageWithInsertResumeType`.
   */
  @Test (dependsOnMethods = "testProcessMessageWithInsertResumeType")
  public void testProcessMessageWithInsertKillType() throws IOException {
    Kafka09ConsumerClient.Kafka09ConsumerRecord<String, DagActionStoreChangeEvent> consumerRecord =
        wrapDagActionStoreChangeEvent(OperationType.INSERT, FLOW_GROUP, FLOW_NAME, FLOW_EXECUTION_ID,
            DagActionStore.NO_JOB_NAME_DEFAULT,DagActionValue.KILL);
    mockDagActionStoreChangeMonitor.processMessageForTest(consumerRecord);
    verify(mockDagActionStoreChangeMonitor.getDagManagement(), times(1)).addDagAction(any());
    verify(mockDagActionStoreChangeMonitor.getDagManagement(), times(1))
        .addDagAction(argThat(leaseParam -> leaseParam.getDagAction().getDagActionType() == DagActionStore.DagActionType.KILL));
  }

  /**
   * Tests process message with an UPDATE type message of the 'launch' action above. Although processMessage does not
   * expect this message type it should handle it gracefully
   */
  @Test (dependsOnMethods = "testProcessMessageWithInsertKillType")
  public void testProcessMessageWithUpdate() throws IOException {
    Kafka09ConsumerClient.Kafka09ConsumerRecord<String, DagActionStoreChangeEvent> consumerRecord =
        wrapDagActionStoreChangeEvent(OperationType.UPDATE, FLOW_GROUP, FLOW_NAME, FLOW_EXECUTION_ID,
            DagActionStore.NO_JOB_NAME_DEFAULT, DagActionValue.LAUNCH);
    mockDagActionStoreChangeMonitor.processMessageForTest(consumerRecord);
    verify(mockDagActionStoreChangeMonitor.getDagManagement(), times(0)).addDagAction(any());
  }

  /**
   * Tests process message with a DELETE type message.
   */
  @Test (dependsOnMethods = "testProcessMessageWithUpdate")
  public void testProcessMessageWithDelete() {
    String JOB_NAME = "jobName";
    Kafka09ConsumerClient.Kafka09ConsumerRecord<String, DagActionStoreChangeEvent> consumerRecord =
        wrapDagActionStoreChangeEvent(OperationType.DELETE, FLOW_GROUP, FLOW_NAME, FLOW_EXECUTION_ID, JOB_NAME, DagActionValue.ENFORCE_JOB_START_DEADLINE);
    DagActionStore.DagAction dagAction = new DagActionStore.DagAction(FLOW_GROUP, FLOW_NAME, Long.parseLong(FLOW_EXECUTION_ID),
        JOB_NAME,
        DagActionStore.DagActionType.ENFORCE_JOB_START_DEADLINE);
    mockDagActionStoreChangeMonitor.processMessageForTest(consumerRecord);
    /* TODO: skip deadline removal for now and let them fire
    verify(mockDagManagementDagActionStoreChangeMonitor.getDagActionReminderScheduler(), times(1))
        .unscheduleReminderJob(eq(dagAction), eq(true));
    verify(mockDagManagementDagActionStoreChangeMonitor.getDagActionReminderScheduler(), times(1))
        .unscheduleReminderJob(eq(dagAction), eq(false));
     */
  }

  @Test (dependsOnMethods = "testProcessMessageWithDelete")
  public void testStartupSequenceHandlesFailures() throws Exception {
    DagManagementStateStore dagManagementStateStore = mock(DagManagementStateStore.class);

    Config monitorConfig = ConfigFactory.empty().withValue(ConfigurationKeys.KAFKA_BROKERS, ConfigValueFactory.fromAnyRef("localhost:0000"))
        .withValue(Kafka09ConsumerClient.GOBBLIN_CONFIG_VALUE_DESERIALIZER_CLASS_KEY, ConfigValueFactory.fromAnyRef("org.apache.kafka.common.serialization.ByteArrayDeserializer"))
        .withValue(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, ConfigValueFactory.fromAnyRef("/tmp/fakeStateStore"))
        .withValue("zookeeper.connect", ConfigValueFactory.fromAnyRef("localhost:2121"));
    FlowCatalog mockFlowCatalog = mock(FlowCatalog.class);
    // Throw an uncaught exception during startup sequence
    when(mockFlowCatalog.getSpecs(any(URI.class))).thenThrow(new RuntimeException("Uncaught exception"));
    mockDagActionStoreChangeMonitor =  new MockDagActionStoreChangeMonitor(monitorConfig, 5, dagManagementStateStore);
    try {
      mockDagActionStoreChangeMonitor.setActive();
    } catch (Exception e) {
      verify(mockFlowCatalog.getSpecs(), times(1));
      Assert.fail();
    }
  }

  /**
   * Util to create a general DagActionStoreChange type event
   */
  private DagActionStoreChangeEvent createDagActionStoreChangeEvent(OperationType operationType,
      String flowGroup, String flowName, String flowExecutionId, String jobName, DagActionValue dagAction) {
    String key = getKeyForFlow(flowGroup, flowName, flowExecutionId);
    GenericStoreChangeEvent genericStoreChangeEvent =
        new GenericStoreChangeEvent(key, String.valueOf(txidCounter), System.currentTimeMillis(), operationType);
    txidCounter++;
    return new DagActionStoreChangeEvent(genericStoreChangeEvent, flowGroup, flowName, flowExecutionId, jobName, dagAction);
  }

  /**
   * Form a key for events using the flow identifiers
   * @return a key formed by adding an '_' delimiter between the flow identifiers
   */
  public static String getKeyForFlow(String flowGroup, String flowName, String flowExecutionId) {
    return flowGroup + "_" + flowName + "_" + flowExecutionId;
  }

  /**
   * Util to create wrapper around DagActionStoreChangeEvent
   */
  private Kafka09ConsumerClient.Kafka09ConsumerRecord<String, DagActionStoreChangeEvent> wrapDagActionStoreChangeEvent(OperationType operationType,
      String flowGroup, String flowName, String flowExecutionId, String jobName, DagActionValue dagAction) {
    DagActionStoreChangeEvent eventToProcess = null;
    try {
      eventToProcess =
          createDagActionStoreChangeEvent(operationType, flowGroup, flowName, flowExecutionId, jobName, dagAction);
    } catch (Exception e) {
      log.error("Exception while creating event ", e);
    }
    // TODO: handle partition and offset values better
    int PARTITION = 1;
    int OFFSET = 1;
    ConsumerRecord<String, DagActionStoreChangeEvent> consumerRecord = new ConsumerRecord<>(TOPIC, PARTITION, OFFSET,
        getKeyForFlow(flowGroup, flowName, flowExecutionId), eventToProcess);
    return new Kafka09ConsumerClient.Kafka09ConsumerRecord<>(consumerRecord);
  }
}