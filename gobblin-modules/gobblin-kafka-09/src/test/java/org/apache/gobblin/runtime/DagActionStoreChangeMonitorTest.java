package org.apache.gobblin.runtime;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;
import org.apache.gobblin.kafka.client.Kafka09ConsumerClient;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.service.modules.orchestration.Orchestrator;
import org.apache.gobblin.service.monitoring.DagActionStoreChangeEvent;
import org.apache.gobblin.service.monitoring.DagActionStoreChangeMonitor;
import org.apache.gobblin.service.monitoring.DagActionValue;
import org.apache.gobblin.service.monitoring.GenericStoreChangeEvent;
import org.apache.gobblin.service.monitoring.OperationType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.testng.annotations.BeforeClass;
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

  class MockDagActionStoreChangeMonitor extends DagActionStoreChangeMonitor {

    public MockDagActionStoreChangeMonitor(String topic, Config config, int numThreads,
        boolean isMultiActiveSchedulerEnabled) {
      super(topic, config, mock(DagActionStore.class), mock(DagManager.class), numThreads, mock(FlowCatalog.class),
          mock(Orchestrator.class), isMultiActiveSchedulerEnabled);
    }

    @Override
    protected void processMessage(DecodeableKafkaRecord record) {
      super.processMessage(record);
    }

    @Override
    protected void startUp() {
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

  @BeforeClass
  public void setup() {
     mockDagActionStoreChangeMonitor = createMockDagActionStoreChangeMonitor();
     mockDagActionStoreChangeMonitor.startUp();
  }

  /**
   * Ensure no NPE results from passing a HEARTBEAT type message with a null {@link DagActionValue}
   */
  @Test
  public void testProcessMessageWithHeartbeat() {
    Kafka09ConsumerClient.Kafka09ConsumerRecord consumerRecord =
        wrapDagActionStoreChangeEvent(OperationType.HEARTBEAT, "", "", "", null);
    mockDagActionStoreChangeMonitor.processMessage(consumerRecord);
  }

  /**
   * Tests process message with an INSERT type message
   */
  @Test
  public void testProcessMessageWithInsert() {
    Kafka09ConsumerClient.Kafka09ConsumerRecord consumerRecord =
        wrapDagActionStoreChangeEvent(OperationType.INSERT, FLOW_GROUP, FLOW_NAME, FLOW_EXECUTION_ID, DagActionValue.LAUNCH);
    mockDagActionStoreChangeMonitor.processMessage(consumerRecord);
  }


  /**
   * Tests process message with an UPDATE type message
   */
  @Test
  public void testProcessMessageWithUpdate() {
    Kafka09ConsumerClient.Kafka09ConsumerRecord consumerRecord =
        wrapDagActionStoreChangeEvent(OperationType.UPDATE, FLOW_GROUP, FLOW_NAME, FLOW_EXECUTION_ID, DagActionValue.LAUNCH);
    mockDagActionStoreChangeMonitor.processMessage(consumerRecord);
  }

  /**
   * Tests process message with a DELETE type message
   */
  @Test
  public void testProcessMessageWithDelete() {
    Kafka09ConsumerClient.Kafka09ConsumerRecord consumerRecord =
        wrapDagActionStoreChangeEvent(OperationType.DELETE, FLOW_GROUP, FLOW_NAME, FLOW_EXECUTION_ID, DagActionValue.LAUNCH);
    mockDagActionStoreChangeMonitor.processMessage(consumerRecord);
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