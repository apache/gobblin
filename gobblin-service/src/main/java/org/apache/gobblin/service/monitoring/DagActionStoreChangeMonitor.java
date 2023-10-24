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

package org.apache.gobblin.service.monitoring;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;
import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.kafka.HighLevelConsumer;
import org.apache.gobblin.runtime.metrics.RuntimeMetrics;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.service.modules.orchestration.Orchestrator;


/**
 * A DagActionStore change monitor that uses {@link DagActionStoreChangeEvent} schema to process Kafka messages received
 * from its corresponding consumer client. This monitor responds to requests to resume or delete a flow and acts as a
 * connector between the API and execution layers of GaaS.
 */
@Slf4j
public class DagActionStoreChangeMonitor extends HighLevelConsumer {
  public static final String DAG_ACTION_CHANGE_MONITOR_PREFIX = "dagActionChangeStore";

  // Metrics
  private ContextAwareMeter killsInvoked;
  private ContextAwareMeter resumesInvoked;
  private ContextAwareMeter flowsLaunched;
  private ContextAwareMeter failedFlowLaunchSubmissions;
  private ContextAwareMeter unexpectedErrors;
  private ContextAwareMeter messageProcessedMeter;
  private ContextAwareMeter messageFilteredOutMeter;
  private ContextAwareGauge produceToConsumeDelayMillis; // Reports delay from all partitions in one gauge

  private volatile Long produceToConsumeDelayValue = -1L;

  protected CacheLoader<String, String> cacheLoader = new CacheLoader<String, String>() {
    @Override
    public String load(String key) throws Exception {
      return key;
    }
  };

  protected LoadingCache<String, String>
      dagActionsSeenCache = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build(cacheLoader);

  protected DagActionStore dagActionStore;

  protected DagManager dagManager;
  protected Orchestrator orchestrator;
  protected boolean isMultiActiveSchedulerEnabled;
  protected FlowCatalog flowCatalog;

  // Note that the topic is an empty string (rather than null to avoid NPE) because this monitor relies on the consumer
  // client itself to determine all Kafka related information dynamically rather than through the config.
  public DagActionStoreChangeMonitor(String topic, Config config, DagActionStore dagActionStore, DagManager dagManager,
      int numThreads, FlowCatalog flowCatalog, Orchestrator orchestrator, boolean isMultiActiveSchedulerEnabled) {
    // Differentiate group id for each host
    super(topic, config.withValue(GROUP_ID_KEY,
        ConfigValueFactory.fromAnyRef(DAG_ACTION_CHANGE_MONITOR_PREFIX + UUID.randomUUID().toString())),
        numThreads);
    this.dagActionStore = dagActionStore;
    this.dagManager = dagManager;
    this.flowCatalog = flowCatalog;
    this.orchestrator = orchestrator;
    this.isMultiActiveSchedulerEnabled = isMultiActiveSchedulerEnabled;
  }

  @Override
  protected void assignTopicPartitions() {
    // Expects underlying consumer to handle initializing partitions and offset for the topic -
    // subscribe to all partitions from latest offset
    return;
  }

  @Override
  /*
  This class is multithreaded and this method will be called by multiple threads, however any given message will be
  partitioned and processed by only one thread (and corresponding queue).
   */
  protected void processMessage(DecodeableKafkaRecord message) {
    // This will also include the heathCheck message so that we can rely on this to monitor the health of this Monitor
    messageProcessedMeter.mark();
    String key = (String) message.getKey();
    DagActionStoreChangeEvent value = (DagActionStoreChangeEvent) message.getValue();

    String tid = value.getChangeEventIdentifier().getTxId();
    Long produceTimestamp = value.getChangeEventIdentifier().getProduceTimestampMillis();
    String operation = value.getChangeEventIdentifier().getOperationType().name();
    String flowGroup = value.getFlowGroup();
    String flowName = value.getFlowName();
    String flowExecutionId = value.getFlowExecutionId();

    DagActionStore.FlowActionType dagActionType = DagActionStore.FlowActionType.valueOf(value.getDagAction().toString());

    produceToConsumeDelayValue = calcMillisSince(produceTimestamp);
    log.debug("Processing Dag Action message for flow group: {} name: {} executionId: {} tid: {} operation: {} lag: {}",
        flowGroup, flowName, flowExecutionId, tid, operation, produceToConsumeDelayValue);

    String changeIdentifier = tid + key;
    if (!ChangeMonitorUtils.shouldProcessMessage(changeIdentifier, dagActionsSeenCache, operation,
        produceTimestamp.toString())) {
      this.messageFilteredOutMeter.mark();
      return;
    }

    // Used to easily log information to identify the dag action
    DagActionStore.DagAction dagAction = new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId,
        dagActionType);

    // We only expect INSERT and DELETE operations done to this table. INSERTs correspond to any type of
    // {@link DagActionStore.FlowActionType} flow requests that have to be processed. DELETEs require no action.
    try {
      if (operation.equals("INSERT")) {
        log.info("DagAction change ({}) received for flow: {}", dagActionType, dagAction);
        if (dagActionType.equals(DagActionStore.FlowActionType.RESUME)) {
          dagManager.handleResumeFlowRequest(flowGroup, flowName,Long.parseLong(flowExecutionId));
          this.resumesInvoked.mark();
        } else if (dagActionType.equals(DagActionStore.FlowActionType.KILL)) {
          dagManager.handleKillFlowRequest(flowGroup, flowName, Long.parseLong(flowExecutionId));
          this.killsInvoked.mark();
        } else if (dagActionType.equals(DagActionStore.FlowActionType.LAUNCH)) {
          // If multi-active scheduler is NOT turned on we should not receive these type of events
          if (!this.isMultiActiveSchedulerEnabled) {
            this.unexpectedErrors.mark();
            throw new RuntimeException(String.format("Received LAUNCH dagAction while not in multi-active scheduler "
                + "mode for flowAction: %s", dagAction));
          }
          submitFlowToDagManagerHelper(flowGroup, flowName);
        } else {
          log.warn("Received unsupported dagAction {}. Expected to be a KILL, RESUME, or LAUNCH", dagActionType);
          this.unexpectedErrors.mark();
          return;
        }
      } else if (operation.equals("UPDATE")) {
        log.warn("Received an UPDATE action to the DagActionStore when values in this store are never supposed to be "
            + "updated. Flow group: {} name {} executionId {} were updated to action {}", flowGroup, flowName,
            flowExecutionId, dagActionType);
        this.unexpectedErrors.mark();
      } else if (operation.equals("DELETE")) {
        log.debug("Deleted flow group: {} name: {} executionId {} from DagActionStore", flowGroup, flowName, flowExecutionId);
      } else {
        log.warn("Received unsupported change type of operation {}. Expected values to be in [INSERT, UPDATE, DELETE]",
            operation);
        this.unexpectedErrors.mark();
        return;
      }
    } catch (Exception e) {
      log.warn("Ran into unexpected error processing DagActionStore changes: {}", e);
      this.unexpectedErrors.mark();
    }

    dagActionsSeenCache.put(changeIdentifier, changeIdentifier);
  }

  protected void submitFlowToDagManagerHelper(String flowGroup, String flowName) {
    // Retrieve job execution plan by recompiling the flow spec to send to the DagManager
    FlowId flowId = new FlowId().setFlowGroup(flowGroup).setFlowName(flowName);
    FlowSpec spec = null;
    try {
      URI flowUri = FlowSpec.Utils.createFlowSpecUri(flowId);
      spec = (FlowSpec) flowCatalog.getSpecs(flowUri);
      this.orchestrator.submitFlowToDagManager(spec);
    } catch (URISyntaxException e) {
      log.warn("Could not create URI object for flowId {}. Exception {}", flowId, e.getMessage());
      this.failedFlowLaunchSubmissions.mark();
      return;
    } catch (SpecNotFoundException e) {
      log.warn("Spec not found for flowId {} due to exception {}", flowId, e.getMessage());
      this.failedFlowLaunchSubmissions.mark();
      return;
    } catch (IOException e) {
      log.warn("Failed to add Job Execution Plan for flowId {} due to exception {}", flowId, e.getMessage());
      this.failedFlowLaunchSubmissions.mark();
      return;
    } catch (InterruptedException e) {
      log.warn("SpecCompiler failed to reach healthy state before compilation of flowId {}. Exception: ", flowId, e);
      this.failedFlowLaunchSubmissions.mark();
      return;
    }
    // Only mark this if the dag was successfully added
    this.flowsLaunched.mark();
  }

  @Override
  protected void createMetrics() {
    super.createMetrics();
    // Dag Action specific metrics
    this.killsInvoked = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_KILLS_INVOKED);
    this.resumesInvoked = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_RESUMES_INVOKED);
    this.flowsLaunched = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_FLOWS_LAUNCHED);
    this.failedFlowLaunchSubmissions = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_FAILED_FLOW_LAUNCHED_SUBMISSIONS);
    this.unexpectedErrors = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_UNEXPECTED_ERRORS);
    this.messageProcessedMeter = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_MESSAGE_PROCESSED);
    this.messageFilteredOutMeter = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_MESSAGES_FILTERED_OUT);
    this.produceToConsumeDelayMillis = this.getMetricContext().newContextAwareGauge(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_PRODUCE_TO_CONSUME_DELAY_MILLIS, () -> produceToConsumeDelayValue);
    this.getMetricContext().register(this.produceToConsumeDelayMillis);
  }

  @Override
  protected String getMetricsPrefix() {
    return RuntimeMetrics.DAG_ACTION_STORE_MONITOR_PREFIX + ".";
  }
}
