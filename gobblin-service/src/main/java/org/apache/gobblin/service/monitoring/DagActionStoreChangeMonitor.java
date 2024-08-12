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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.kafka.HighLevelConsumer;
import org.apache.gobblin.runtime.metrics.RuntimeMetrics;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.service.modules.orchestration.Orchestrator;
import org.apache.gobblin.service.modules.orchestration.task.DagProcessingEngineMetrics;
import org.apache.gobblin.util.ConfigUtils;


/**
 * A DagActionStore change monitor that uses {@link DagActionStoreChangeEvent} schema to process Kafka messages received
 * from its corresponding consumer client. This monitor responds to requests to resume or delete a flow and acts as a
 * connector between the API and execution layers of GaaS.
 */
@Slf4j
public class DagActionStoreChangeMonitor extends HighLevelConsumer<String, DagActionStoreChangeEvent> {
  public static final String DAG_ACTION_CHANGE_MONITOR_PREFIX = "dagActionChangeStore";

  // Metrics
  protected ContextAwareMeter killsInvoked;
  protected ContextAwareMeter resumesInvoked;
  private ContextAwareMeter successfulLaunchSubmissions;
  private ContextAwareMeter failedLaunchSubmissions;
  private ContextAwareMeter successfulLaunchSubmissionsOnStartup;
  private ContextAwareMeter failedLaunchSubmissionsOnStartup;
  protected ContextAwareMeter unexpectedErrors;
  protected ContextAwareMeter messageProcessedMeter;
  protected ContextAwareMeter duplicateMessagesMeter;
  protected ContextAwareMeter heartbeatMessagesMeter;
  protected ContextAwareMeter nullDagActionTypeMessagesMeter;
  private ContextAwareGauge produceToConsumeDelayMillis; // Reports delay from all partitions in one gauge
  protected volatile Long produceToConsumeDelayValue = -1L;

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
  protected DagManagementStateStore dagManagementStateStore;
  protected final DagProcessingEngineMetrics dagProcEngineMetrics;
  @Getter
  private volatile boolean isActive;

  // Note that the topic is an empty string (rather than null to avoid NPE) because this monitor relies on the consumer
  // client itself to determine all Kafka related information dynamically rather than through the config.
  public DagActionStoreChangeMonitor(String topic, Config config, DagManager dagManager, int numThreads,
      FlowCatalog flowCatalog, Orchestrator orchestrator, DagManagementStateStore dagManagementStateStore,
      boolean isMultiActiveSchedulerEnabled, DagProcessingEngineMetrics dagProcEngineMetrics) {
    // Differentiate group id for each host
    super(topic, config.withValue(GROUP_ID_KEY,
        ConfigValueFactory.fromAnyRef(DAG_ACTION_CHANGE_MONITOR_PREFIX + UUID.randomUUID().toString())),
        numThreads);
    this.dagManager = dagManager;
    this.flowCatalog = flowCatalog;
    this.orchestrator = orchestrator;
    this.dagManagementStateStore = dagManagementStateStore;
    this.isMultiActiveSchedulerEnabled = isMultiActiveSchedulerEnabled;
    this.dagProcEngineMetrics = dagProcEngineMetrics;

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
    Collection<DagActionStore.DagAction> dagActions;
    try {
      dagActions = dagManagementStateStore.getDagActions();
    } catch (IOException e) {
      throw new RuntimeException(String.format("Unable to retrieve dagActions from the dagActionStore while "
          + "initializing the %s", DagActionStoreChangeMonitor.class.getCanonicalName()), e);
    }
    final ExecutorService executorService = Executors.newFixedThreadPool(ConfigUtils.getInt(this.config,ConfigurationKeys.DAG_ACTION_STORE_MONITOR_EXECUTOR_THREADS,1));

    for (DagActionStore.DagAction action : dagActions) {
      try {
        executorService.submit(()->handleDagAction(action, true));
      } catch (Exception e) {
        log.error("Unexpected error initializing from DagActionStore changes, upon {}", action, e);
        this.unexpectedErrors.mark();
      }
    }
    try {
      boolean executedSuccessfully= executorService.awaitTermination(ConfigUtils.getInt(this.config,ConfigurationKeys.DAG_ACTION_STORE_MONITOR_EXECUTOR_TIMEOUT_SECONDS,30),TimeUnit.SECONDS);
      if(!executedSuccessfully){
        log.error("Executor terminated before processing all actions during startup,consider increasing the timeOut during awaitTermination");
        this.unexpectedErrors.mark();
      }
    } catch (InterruptedException ignored) {
      log.error("Interrupted Exception in processing dag actions during startup,ignoring", ignored);
      this.unexpectedErrors.mark();
    }
  }

  /*
   Override this method to do nothing, instead we create metrics upon class initialization and start processing the
   queues and load dag actions from the DagActionStore after #setActive is called to make sure dependent services are
   initialized properly.
  */
  @Override
  protected void startUp() {}

  /*
   This method should be called once by the {@link GobblinServiceManager} only after the DagManager, FlowGraph and
   SpecCompiler are initialized and running.
   */
  public synchronized void setActive() {
    if (this.isActive) {
      return;
    }

    this.isActive = true;
    initializeMonitor();
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
  protected void processMessage(DecodeableKafkaRecord<String, DagActionStoreChangeEvent> message) {
    // This will also include the heathCheck message so that we can rely on this to monitor the health of this Monitor
    messageProcessedMeter.mark();
    String key = message.getKey();
    DagActionStoreChangeEvent value = message.getValue();

    String tid = value.getChangeEventIdentifier().getTxId();
    Long produceTimestamp = value.getChangeEventIdentifier().getProduceTimestampMillis();
    String operation = value.getChangeEventIdentifier().getOperationType().name();
    String flowGroup = value.getFlowGroup();
    String flowName = value.getFlowName();
    String flowExecutionId = value.getFlowExecutionId();
    String jobName = value.getJobName();

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

    DagActionStore.DagActionType dagActionType = DagActionStore.DagActionType.valueOf(value.getDagAction().toString());
    // Parse flowExecutionIds after filtering out HB messages to prevent exception from parsing empty strings
    long flowExecutionIdLong = Long.parseLong(flowExecutionId);

    // Used to easily log information to identify the dag action
    DagActionStore.DagAction dagAction = new DagActionStore.DagAction(flowGroup, flowName, flowExecutionIdLong, jobName,
        dagActionType);

    handleDagAction(operation, dagAction, flowGroup, flowName, flowExecutionIdLong, dagActionType);

    dagActionsSeenCache.put(changeIdentifier, changeIdentifier);
  }

  protected void handleDagAction(String operation, DagActionStore.DagAction dagAction, String flowGroup,
      String flowName, long flowExecutionId, DagActionStore.DagActionType dagActionType) {
    // We only expect INSERT and DELETE operations done to this table. INSERTs correspond to any type of
    // {@link DagActionStore.FlowActionType} flow requests that have to be processed. DELETEs require no action.
    try {
      switch (operation) {
        case "INSERT":
          handleDagAction(dagAction, false);
          this.dagProcEngineMetrics.markDagActionsObserved(dagActionType);
          break;
        case "UPDATE":
          // TODO: change this warning message and process updates if for launch or reevaluate type
          log.warn("Received an UPDATE action to the DagActionStore when values in this store are never supposed to be "
                  + "updated. Flow group: {} name {} executionId {} were updated to action {}", flowGroup, flowName,
              flowExecutionId, dagActionType);
          this.unexpectedErrors.mark();
          break;
        case "DELETE":
          log.debug("Deleted dagAction from DagActionStore: {}", dagAction);
          break;
        default:
          log.warn(
              "Received unsupported change type of operation {}. Expected values to be in [INSERT, UPDATE, DELETE]",
              operation);
          this.unexpectedErrors.mark();
          break;
      }
    } catch (Exception e) {
      log.warn("Ran into unexpected error processing DagActionStore changes: ", e);
      this.unexpectedErrors.mark();
    }
  }

  /**
   * For a given dagAction, calls the appropriate method in the DagManager to carry out the desired action.
   * @param isStartup true if called for dagAction loaded directly from store upon startup, false otherwise
   */
  protected void handleDagAction(DagActionStore.DagAction dagAction, boolean isStartup) {
    log.info("(" + (isStartup ? "on-startup" : "post-startup") + ") DagAction change ({}) received for flow: {}",
        dagAction.getDagActionType(), dagAction);
    if (dagAction.getDagActionType().equals(DagActionStore.DagActionType.RESUME)) {
      dagManager.handleResumeFlowRequest(dagAction.getFlowGroup(), dagAction.getFlowName(), dagAction.getFlowExecutionId());
      this.resumesInvoked.mark();
    } else if (dagAction.getDagActionType().equals(DagActionStore.DagActionType.KILL)) {
      dagManager.handleKillFlowRequest(dagAction.getFlowGroup(), dagAction.getFlowName(), dagAction.getFlowExecutionId());
      this.killsInvoked.mark();
    } else if (dagAction.getDagActionType().equals(DagActionStore.DagActionType.LAUNCH)) {
      // If multi-active scheduler is NOT turned on we should not receive these type of events
      if (!this.isMultiActiveSchedulerEnabled) {
        this.unexpectedErrors.mark();
        throw new RuntimeException(String.format("Received LAUNCH dagAction while not in multi-active scheduler "
            + "mode for flowAction: %s", dagAction));
      }
      submitFlowToDagManagerHelper(dagAction, isStartup);
    } else {
      log.warn("Received unsupported dagAction {}. Expected to be a KILL, RESUME, or LAUNCH", dagAction.getDagActionType());
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
    Preconditions.checkArgument(dagAction.getDagActionType() == DagActionStore.DagActionType.LAUNCH);
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
      this.orchestrator.compileAndSubmitFlowToDagManager(spec);
    } catch (URISyntaxException e) {
      log.warn("Could not create URI object for flowId {}. Exception {}", flowId, e.getMessage());
      launchSubmissionMetricProxy.markFailure();
      return;
    } catch (SpecNotFoundException e) {
      log.info("Spec not found for flowId {} due to deletion by active dagManager host due to exception {}",
          flowId, e.getMessage());
      // TODO: mark this failure if there are other valid cases of this exception
      // launchSubmissionMetricProxy.markFailure();
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
        this.dagManagementStateStore.deleteDagAction(dagAction);
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
  protected static class LaunchSubmissionMetricProxy {
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
