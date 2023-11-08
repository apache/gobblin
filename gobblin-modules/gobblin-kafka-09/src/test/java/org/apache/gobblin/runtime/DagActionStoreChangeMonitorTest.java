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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.net.URI;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;
import org.apache.gobblin.kafka.client.Kafka09ConsumerClient;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.service.modules.orchestration.Orchestrator;
import org.apache.gobblin.service.monitoring.DagActionStoreChangeEvent;
import org.apache.gobblin.service.monitoring.DagActionStoreChangeMonitor;
import org.apache.gobblin.service.monitoring.DagActionValue;
import org.apache.gobblin.service.monitoring.GenericStoreChangeEvent;
import org.apache.gobblin.service.monitoring.OperationType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.testng.annotations.Test;

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
  private final int PARTITION = 1;
  private final int OFFSET = 1;
  private final String FLOW_GROUP = "flowGroup";
  private final String FLOW_NAME = "flowName";
  private final String FLOW_EXECUTION_ID = "123";
  private MockDagActionStoreChangeMonitor mockDagActionStoreChangeMonitor;
  private int txidCounter = 0;

  /**
   * Note: The class methods are wrapped in a test specific method because the original methods are package protected
   * and cannot be accessed by this class.
   */
  class MockDagActionStoreChangeMonitor extends DagActionStoreChangeMonitor {

    public MockDagActionStoreChangeMonitor(String topic, Config config, int numThreads,
        boolean isMultiActiveSchedulerEnabled) {
      super(topic, config, mock(DagActionStore.class), mock(DagManager.class), numThreads, mock(FlowCatalog.class),
          mock(Orchestrator.class), isMultiActiveSchedulerEnabled);
    }

    protected void processMessageForTest(DecodeableKafkaRecord record) {
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
    return new MockDagActionStoreChangeMonitor("dummyTopic", config, 5, true);
  }

  // Called at start of every test so the count of each method being called is reset to 0
  public void setup() {
     mockDagActionStoreChangeMonitor = createMockDagActionStoreChangeMonitor();
     mockDagActionStoreChangeMonitor.startUpForTest();
  }

  /**
   * Ensure no NPE results from passing a HEARTBEAT type message with a null {@link DagActionValue} and the message is
   * filtered out since it's a heartbeat type so no methods are called.
   */
  @Test
  public void testProcessMessageWithHeartbeatAndNullDagAction() throws SpecNotFoundException {
    setup();
    Kafka09ConsumerClient.Kafka09ConsumerRecord consumerRecord =
        wrapDagActionStoreChangeEvent(OperationType.HEARTBEAT, "", "", "", null);
    mockDagActionStoreChangeMonitor.processMessageForTest(consumerRecord);
    verify(mockDagActionStoreChangeMonitor.getDagManager(), times(0)).handleResumeFlowRequest(anyString(), anyString(), anyLong());
    verify(mockDagActionStoreChangeMonitor.getDagManager(), times(0)).handleKillFlowRequest(anyString(), anyString(), anyLong());
    // Note: Indirectly verifies submitFlowToDagManagerHelper is called which is not a mocked object so cannot be asserted
    verify(mockDagActionStoreChangeMonitor.getFlowCatalog(), times(0)).getSpecs(any(URI.class));
  }

  /**
   * Ensure a HEARTBEAT type message with non-empty flow information is filtered out since it's a heartbeat type so no
   * methods are called.
   */
  @Test (dependsOnMethods = "testProcessMessageWithHeartbeatAndNullDagAction")
  public void testProcessMessageWithHeartbeatAndFlowInfo() throws SpecNotFoundException {
    setup();
    Kafka09ConsumerClient.Kafka09ConsumerRecord consumerRecord =
        wrapDagActionStoreChangeEvent(OperationType.HEARTBEAT, FLOW_GROUP, FLOW_NAME, FLOW_EXECUTION_ID, DagActionValue.RESUME);
    mockDagActionStoreChangeMonitor.processMessageForTest(consumerRecord);
    verify(mockDagActionStoreChangeMonitor.getDagManager(), times(0)).handleResumeFlowRequest(anyString(), anyString(), anyLong());
    verify(mockDagActionStoreChangeMonitor.getDagManager(), times(0)).handleKillFlowRequest(anyString(), anyString(), anyLong());
    verify(mockDagActionStoreChangeMonitor.getFlowCatalog(), times(0)).getSpecs(any(URI.class));
  }

  /**
   * Tests process message with an INSERT type message of a `launch` action
   */
  @Test (dependsOnMethods = "testProcessMessageWithHeartbeatAndFlowInfo")
  public void testProcessMessageWithInsertLaunchType() throws SpecNotFoundException {
    setup();
    Kafka09ConsumerClient.Kafka09ConsumerRecord consumerRecord =
        wrapDagActionStoreChangeEvent(OperationType.INSERT, FLOW_GROUP, FLOW_NAME, FLOW_EXECUTION_ID, DagActionValue.LAUNCH);
    mockDagActionStoreChangeMonitor.processMessageForTest(consumerRecord);
    verify(mockDagActionStoreChangeMonitor.getDagManager(), times(0)).handleResumeFlowRequest(anyString(), anyString(), anyLong());
    verify(mockDagActionStoreChangeMonitor.getDagManager(), times(0)).handleKillFlowRequest(anyString(), anyString(), anyLong());
    verify(mockDagActionStoreChangeMonitor.getFlowCatalog(), times(1)).getSpecs(any(URI.class));
  }

  /**
   * Tests process message with an INSERT type message of a `resume` action. It re-uses the same flow information however
   * since it is a different tid used every time it will be considered unique and submit a kill request.
   */
  @Test (dependsOnMethods = "testProcessMessageWithInsertLaunchType")
  public void testProcessMessageWithInsertResumeType() throws SpecNotFoundException {
    setup();
    Kafka09ConsumerClient.Kafka09ConsumerRecord consumerRecord =
        wrapDagActionStoreChangeEvent(OperationType.INSERT, FLOW_GROUP, FLOW_NAME, FLOW_EXECUTION_ID, DagActionValue.RESUME);
    mockDagActionStoreChangeMonitor.processMessageForTest(consumerRecord);
    verify(mockDagActionStoreChangeMonitor.getDagManager(), times(1)).handleResumeFlowRequest(anyString(), anyString(), anyLong());
    verify(mockDagActionStoreChangeMonitor.getDagManager(), times(0)).handleKillFlowRequest(anyString(), anyString(), anyLong());
    verify(mockDagActionStoreChangeMonitor.getFlowCatalog(), times(0)).getSpecs(any(URI.class));
  }

  /**
   * Tests process message with an INSERT type message of a `kill` action. Similar to `testProcessMessageWithInsertResumeType`.
   */
  @Test (dependsOnMethods = "testProcessMessageWithInsertResumeType")
  public void testProcessMessageWithInsertKillType() throws SpecNotFoundException {
    setup();
    Kafka09ConsumerClient.Kafka09ConsumerRecord consumerRecord =
        wrapDagActionStoreChangeEvent(OperationType.INSERT, FLOW_GROUP, FLOW_NAME, FLOW_EXECUTION_ID, DagActionValue.KILL);
    mockDagActionStoreChangeMonitor.processMessageForTest(consumerRecord);
    verify(mockDagActionStoreChangeMonitor.getDagManager(), times(0)).handleResumeFlowRequest(anyString(), anyString(), anyLong());
    verify(mockDagActionStoreChangeMonitor.getDagManager(), times(1)).handleKillFlowRequest(anyString(), anyString(), anyLong());
    verify(mockDagActionStoreChangeMonitor.getFlowCatalog(), times(0)).getSpecs(any(URI.class));
  }

  /**
   * Tests process message with an UPDATE type message of the 'launch' action above. Although processMessage does not
   * expect this message type it should handle it gracefully
   */
  @Test (dependsOnMethods = "testProcessMessageWithInsertKillType")
  public void testProcessMessageWithUpdate() throws SpecNotFoundException {
    setup();
    Kafka09ConsumerClient.Kafka09ConsumerRecord consumerRecord =
        wrapDagActionStoreChangeEvent(OperationType.UPDATE, FLOW_GROUP, FLOW_NAME, FLOW_EXECUTION_ID, DagActionValue.LAUNCH);
    mockDagActionStoreChangeMonitor.processMessageForTest(consumerRecord);
    verify(mockDagActionStoreChangeMonitor.getDagManager(), times(0)).handleResumeFlowRequest(anyString(), anyString(), anyLong());
    verify(mockDagActionStoreChangeMonitor.getDagManager(), times(0)).handleKillFlowRequest(anyString(), anyString(), anyLong());
    verify(mockDagActionStoreChangeMonitor.getFlowCatalog(), times(0)).getSpecs(any(URI.class));
  }

  /**
   * Tests process message with a DELETE type message which should be ignored regardless of the flow information.
   */
  @Test (dependsOnMethods = "testProcessMessageWithUpdate")
  public void testProcessMessageWithDelete() throws SpecNotFoundException {
    setup();
    Kafka09ConsumerClient.Kafka09ConsumerRecord consumerRecord =
        wrapDagActionStoreChangeEvent(OperationType.DELETE, FLOW_GROUP, FLOW_NAME, FLOW_EXECUTION_ID, DagActionValue.LAUNCH);
    mockDagActionStoreChangeMonitor.processMessageForTest(consumerRecord);
    verify(mockDagActionStoreChangeMonitor.getDagManager(), times(0)).handleResumeFlowRequest(anyString(), anyString(), anyLong());
    verify(mockDagActionStoreChangeMonitor.getDagManager(), times(0)).handleKillFlowRequest(anyString(), anyString(), anyLong());
    verify(mockDagActionStoreChangeMonitor.getFlowCatalog(), times(0)).getSpecs(any(URI.class));
  }

  /**
   * Util to create a general DagActionStoreChange type event
   */
  private DagActionStoreChangeEvent createDagActionStoreChangeEvent(OperationType operationType,
      String flowGroup, String flowName, String flowExecutionId, DagActionValue dagAction) {
    String key = getKeyForFlow(flowGroup, flowName, flowExecutionId);
    GenericStoreChangeEvent genericStoreChangeEvent =
        new GenericStoreChangeEvent(key, String.valueOf(txidCounter), System.currentTimeMillis(), operationType);
    txidCounter++;
    return new DagActionStoreChangeEvent(genericStoreChangeEvent, flowGroup, flowName, flowExecutionId, dagAction);
  }

  /**
   * Form a key for events using the flow identifiers
   * @return a key formed by adding an '_' delimiter between the flow identifiers
   */
  private String getKeyForFlow(String flowGroup, String flowName, String flowExecutionId) {
    return flowGroup + "_" + flowName + "_" + flowExecutionId;
  }

  /**
   * Util to create wrapper around DagActionStoreChangeEvent
   */
  private Kafka09ConsumerClient.Kafka09ConsumerRecord wrapDagActionStoreChangeEvent(OperationType operationType, String flowGroup, String flowName,
      String flowExecutionId, DagActionValue dagAction) {
    DagActionStoreChangeEvent eventToProcess = null;
    try {
      eventToProcess =
          createDagActionStoreChangeEvent(operationType, flowGroup, flowName, flowExecutionId, dagAction);
    } catch (Exception e) {
      log.error("Exception while creating event ", e);
    }
    // TODO: handle partition and offset values better
    ConsumerRecord consumerRecord = new ConsumerRecord<>(TOPIC, PARTITION, OFFSET,
        getKeyForFlow(flowGroup, flowName, flowExecutionId), eventToProcess);
    return new Kafka09ConsumerClient.Kafka09ConsumerRecord(consumerRecord);
  }
}