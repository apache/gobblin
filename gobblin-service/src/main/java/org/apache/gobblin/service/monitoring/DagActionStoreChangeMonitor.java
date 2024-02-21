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
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
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
  private ContextAwareMeter successfulLaunchSubmissions;
  private ContextAwareMeter failedLaunchSubmissions;
  private ContextAwareMeter successfulLaunchSubmissionsOnStartup;
  private ContextAwareMeter failedLaunchSubmissionsOnStartup;
  private ContextAwareMeter unexpectedErrors;
  private ContextAwareMeter messageProcessedMeter;
  private ContextAwareMeter duplicateMessagesMeter;
  private ContextAwareMeter heartbeatMessagesMeter;
  private ContextAwareMeter nullDagActionTypeMessagesMeter;
  private ContextAwareGauge produceToConsumeDelayMillis; // Reports delay from all partitions in one gauge

  private volatile Long produceToConsumeDelayValue = -1L;

  protected LaunchSubmissionMetricProxy ON_STARTUP = new NullLaunchSubmissionMetricProxy();
  protected LaunchSubmissionMetricProxy POST_STARTUP = new NullLaunchSubmissionMetricProxy();

  protected CacheLoader<String, String> cacheLoader = new CacheLoader<String, String>() {
    @Override
    public String load(String key) throws Exception {
      return key;
    }
  };

  protected LoadingCache<String, String>
      dagActionsSeenCache = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build(cacheLoader);

  @Getter
  @VisibleForTesting
  protected DagManager dagManager;
  protected Orchestrator orchestrator;
  protected boolean isMultiActiveSchedulerEnabled;
  @Getter
  @VisibleForTesting
  protected FlowCatalog flowCatalog;
  protected DagActionStore dagActionStore;

  // Note that the topic is an empty string (rather than null to avoid NPE) because this monitor relies on the consumer
  // client itself to determine all Kafka related information dynamically rather than through the config.
  public DagActionStoreChangeMonitor(String topic, Config config, DagManager dagManager, int numThreads,
      FlowCatalog flowCatalog, Orchestrator orchestrator, DagActionStore dagActionStore,
      boolean isMultiActiveSchedulerEnabled) {
    // Differentiate group id for each host
    super(topic, config.withValue(GROUP_ID_KEY,
        ConfigValueFactory.fromAnyRef(DAG_ACTION_CHANGE_MONITOR_PREFIX + UUID.randomUUID().toString())),
        numThreads);
    this.dagManager = dagManager;
    this.flowCatalog = flowCatalog;
    this.orchestrator = orchestrator;
    this.dagActionStore = dagActionStore;
    this.isMultiActiveSchedulerEnabled = isMultiActiveSchedulerEnabled;

    /*
    Metrics need to be created before initializeMonitor() below is called (or more specifically handleDagAction() is
    called on any dagAction)
     */
    buildMetricsContextAndMetrics();
  }

  @Override
  protected void assignTopicPartitions() {
    // Expects underlying consumer to handle initializing partitions and offset for the topic -
    // subscribe to all partitions from latest offset
    return;
  }

  /**
   * Load all actions from the DagActionStore to process any missed actions during service startup
   */
  protected void initializeMonitor() {
    Collection<DagActionStore.DagAction> dagActions = null;
    try {
      dagActions = dagActionStore.getDagActions();
    } catch (IOException e) {
      throw new RuntimeException(String.format("Unable to retrieve dagActions from the dagActionStore while "
          + "initializing the %s", DagActionStoreChangeMonitor.class.getCanonicalName()), e);
    }
    // TODO: make this multi-threaded to add parallelism
    for (DagActionStore.DagAction action : dagActions) {
      handleDagAction(action, true);
    }
  }

  /*
   Override this method to do the same sequence as the parent class, except create metrics. Instead, we create metrics
   earlier upon class initialization because they are used immediately as dag actions are loaded and processed from
   the DagActionStore.
  */
  @Override
  protected void startUp() {
    // Method that starts threads that processes queues
    processQueues();
    // Main thread that constantly polls messages from kafka
    consumerExecutor.execute(() -> {
      while (!shutdownRequested) {
        consume();
      }
    });
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

    produceToConsumeDelayValue = calcMillisSince(produceTimestamp);
    log.debug("Processing Dag Action message for flow group: {} name: {} executionId: {} tid: {} operation: {} lag: {}",
        flowGroup, flowName, flowExecutionId, tid, operation, produceToConsumeDelayValue);

    String changeIdentifier = tid + key;
    if (!ChangeMonitorUtils.isValidAndUniqueMessage(changeIdentifier, operation, produceTimestamp.toString(),
        dagActionsSeenCache, duplicateMessagesMeter, heartbeatMessagesMeter)) {
      return;
    }
    // check after filtering out heartbeat messages expected to have `dagActionValue == null`
    if (value.getDagAction() == null) {
      log.warn("Skipping null dag action type received for identifier {} ", changeIdentifier);
      nullDagActionTypeMessagesMeter.mark();
      return;
    }

    DagActionStore.FlowActionType dagActionType = DagActionStore.FlowActionType.valueOf(value.getDagAction().toString());

    // Used to easily log information to identify the dag action
    DagActionStore.DagAction dagAction = new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId,
        dagActionType);

    // We only expect INSERT and DELETE operations done to this table. INSERTs correspond to any type of
    // {@link DagActionStore.FlowActionType} flow requests that have to be processed. DELETEs require no action.
    try {
      if (operation.equals("INSERT")) {
        handleDagAction(dagAction, false);
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

  /**
   * For a given dagAction, calls the appropriate method in the DagManager to carry out the desired action.
   * @param isStartup true if called for dagAction loaded directly from store upon startup, false otherwise
   */
  private void handleDagAction(DagActionStore.DagAction dagAction, boolean isStartup) {
    log.info("(" + (isStartup ? "on-startup" : "post-startup") + ") DagAction change ({}) received for flow: {}",
        dagAction.getFlowActionType(), dagAction);
    if (dagAction.getFlowActionType().equals(DagActionStore.FlowActionType.RESUME)) {
      dagManager.handleResumeFlowRequest(dagAction.getFlowGroup(), dagAction.getFlowName(),
          Long.parseLong(dagAction.getFlowExecutionId()));
      this.resumesInvoked.mark();
    } else if (dagAction.getFlowActionType().equals(DagActionStore.FlowActionType.KILL)) {
      dagManager.handleKillFlowRequest(dagAction.getFlowGroup(), dagAction.getFlowName(),
          Long.parseLong(dagAction.getFlowExecutionId()));
      this.killsInvoked.mark();
    } else if (dagAction.getFlowActionType().equals(DagActionStore.FlowActionType.LAUNCH)) {
      // If multi-active scheduler is NOT turned on we should not receive these type of events
      if (!this.isMultiActiveSchedulerEnabled) {
        this.unexpectedErrors.mark();
        throw new RuntimeException(String.format("Received LAUNCH dagAction while not in multi-active scheduler "
            + "mode for flowAction: %s", dagAction));
      }
      submitFlowToDagManagerHelper(dagAction, isStartup);
    } else {
      log.warn("Received unsupported dagAction {}. Expected to be a KILL, RESUME, or LAUNCH", dagAction.getFlowActionType());
      this.unexpectedErrors.mark();
    }
  }

  /**
   * Overloaded method to provide the correct metrics proxy on or post startup
   */
  protected void submitFlowToDagManagerHelper(DagActionStore.DagAction dagAction, boolean isLoadedOnStartup) {
    submitFlowToDagManagerHelper(dagAction, isLoadedOnStartup ? ON_STARTUP : POST_STARTUP);
  }

  /**
   * Used to forward a launch flow action event from the DagActionStore. Because it may be a completely new DAG not
   * contained in the dagStore, we compile the flow to generate the dag before calling addDag(), handling any errors
   * that may result in the process.
   */
  protected void submitFlowToDagManagerHelper(DagActionStore.DagAction dagAction,
      LaunchSubmissionMetricProxy launchSubmissionMetricProxy) {
    Preconditions.checkArgument(dagAction.getFlowActionType() == DagActionStore.FlowActionType.LAUNCH);
    log.info("Forward launch flow event to DagManager for action {}", dagAction);
    // Retrieve job execution plan by recompiling the flow spec to send to the DagManager
    FlowId flowId = dagAction.getFlowId();
    FlowSpec spec = null;
    try {
      URI flowUri = FlowSpec.Utils.createFlowSpecUri(flowId);
      spec = flowCatalog.getSpecs(flowUri);
      /* Update the spec to contain the flowExecutionId from the dagAction for scheduled flows that do not already
      contain a flowExecutionId. Adhoc flowSpecs are already consistent with the dagAction so there's no effective
      change. It's crucial to adopt the consensus flowExecutionId here to prevent creating a new one during compilation.
      */
      spec.addProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, dagAction.getFlowExecutionId());
      this.orchestrator.submitFlowToDagManager(spec);
    } catch (URISyntaxException e) {
      log.warn("Could not create URI object for flowId {}. Exception {}", flowId, e.getMessage());
      launchSubmissionMetricProxy.markFailure();
      return;
    } catch (SpecNotFoundException e) {
      log.warn("Spec not found for flowId {} due to exception {}", flowId, e.getMessage());
      launchSubmissionMetricProxy.markFailure();
      return;
    } catch (IOException e) {
      log.warn("Failed to add Job Execution Plan for flowId {} due to exception {}", flowId, e.getMessage());
      launchSubmissionMetricProxy.markFailure();
      return;
    } catch (InterruptedException e) {
      log.warn("SpecCompiler failed to reach healthy state before compilation of flowId {}. Exception: ", flowId, e);
      launchSubmissionMetricProxy.markFailure();
      return;
    } finally {
      // Delete the dag action regardless of whether it was processed successfully to avoid accumulating failure cases
      try {
        this.dagActionStore.deleteDagAction(dagAction);
      } catch (IOException e) {
        log.warn("Failed to delete dag action from dagActionStore. dagAction: {}", dagAction);
      }
    }
    // Only mark this if the dag was successfully added
    launchSubmissionMetricProxy.markSuccess();
  }

  @Override
  protected void createMetrics() {
    super.createMetrics();
    // Dag Action specific metrics
    this.killsInvoked = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_KILLS_INVOKED);
    this.resumesInvoked = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_RESUMES_INVOKED);
    // TODO: rename this metrics to match the variable name after debugging launch related issues
    this.successfulLaunchSubmissions = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_SUCCESSFUL_LAUNCH_SUBMISSIONS);
    this.failedLaunchSubmissions = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_FAILED_FLOW_LAUNCHED_SUBMISSIONS);
    this.successfulLaunchSubmissionsOnStartup = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_SUCCESSFUL_LAUNCH_SUBMISSIONS_ON_STARTUP);
    this.failedLaunchSubmissionsOnStartup = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_FAILED_LAUNCH_SUBMISSIONS_ON_STARTUP);
    this.unexpectedErrors = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_UNEXPECTED_ERRORS);
    this.messageProcessedMeter = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_MESSAGE_PROCESSED);
    this.duplicateMessagesMeter = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_DUPLICATE_MESSAGES);
    this.heartbeatMessagesMeter = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_HEARTBEAT_MESSAGES);
    this.nullDagActionTypeMessagesMeter = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_NULL_DAG_ACTION_TYPE_MESSAGES);
    this.produceToConsumeDelayMillis = this.getMetricContext().newContextAwareGauge(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_PRODUCE_TO_CONSUME_DELAY_MILLIS, () -> produceToConsumeDelayValue);
    this.getMetricContext().register(this.produceToConsumeDelayMillis);

    // Setup proxy for launch submission metrics
    this.ON_STARTUP = new LaunchSubmissionMetricProxy(this.successfulLaunchSubmissionsOnStartup, this.failedLaunchSubmissionsOnStartup);
    this.POST_STARTUP = new LaunchSubmissionMetricProxy(this.successfulLaunchSubmissions, this.failedLaunchSubmissions);
  }

  @Override
  protected String getMetricsPrefix() {
    return RuntimeMetrics.DAG_ACTION_STORE_MONITOR_PREFIX + ".";
  }

  @Data
  @AllArgsConstructor
  private static class LaunchSubmissionMetricProxy {
    private ContextAwareMeter successMeter;
    private ContextAwareMeter failureMeter;

    public LaunchSubmissionMetricProxy() {}

    public void markSuccess() {
      getSuccessMeter().mark();
    }

    public void markFailure() {
      getFailureMeter().mark();
    }
  }

  private static class NullLaunchSubmissionMetricProxy extends LaunchSubmissionMetricProxy {
    @Override
    public void markSuccess() {
      // do nothing
    }

    @Override
    public void markFailure() {
      // do nothing
    }
  }
}
