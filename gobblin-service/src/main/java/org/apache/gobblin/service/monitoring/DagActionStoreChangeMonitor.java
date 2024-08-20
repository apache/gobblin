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
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.jetbrains.annotations.NotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
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
import org.apache.gobblin.runtime.kafka.HighLevelConsumer;
import org.apache.gobblin.runtime.metrics.RuntimeMetrics;
import org.apache.gobblin.service.modules.orchestration.DagActionReminderScheduler;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagManagement;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.task.DagProcessingEngineMetrics;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.ExecutorsUtils;


/**
 * A DagActionStore change monitor that uses {@link DagActionStoreChangeEvent} schema to process Kafka messages received
 * from its corresponding consumer client. This monitor responds to requests to resume or delete a flow and acts as a
 * connector between the API and execution layers of GaaS.
 */
@Slf4j
public class DagActionStoreChangeMonitor extends HighLevelConsumer<String, DagActionStoreChangeEvent> {
  @VisibleForTesting @Getter private final DagManagement dagManagement;
  @VisibleForTesting @Getter private final DagActionReminderScheduler dagActionReminderScheduler;
  public static final String DAG_ACTION_CHANGE_MONITOR_PREFIX = "dagActionChangeStore";

  // Metrics
  protected ContextAwareMeter killsInvoked;
  protected ContextAwareMeter resumesInvoked;
  protected ContextAwareMeter unexpectedErrors;
  protected ContextAwareMeter messageProcessedMeter;
  protected ContextAwareMeter duplicateMessagesMeter;
  protected ContextAwareMeter heartbeatMessagesMeter;
  protected ContextAwareMeter nullDagActionTypeMessagesMeter;
  protected volatile Long produceToConsumeDelayValue = -1L;

  protected LaunchSubmissionMetricProxy ON_STARTUP = new NullLaunchSubmissionMetricProxy();
  protected LaunchSubmissionMetricProxy POST_STARTUP = new NullLaunchSubmissionMetricProxy();

  protected CacheLoader<String, String> cacheLoader = new CacheLoader<String, String>() {
    @Override
    public String load(@NotNull String key) {
      return key;
    }
  };

  protected LoadingCache<String, String>
      dagActionsSeenCache = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build(cacheLoader);

  protected DagManagementStateStore dagManagementStateStore;
  protected final DagProcessingEngineMetrics dagProcEngineMetrics;
  @Getter
  private volatile boolean isActive;

  // Note that the topic is an empty string (rather than null to avoid NPE) because this monitor relies on the consumer
  // client itself to determine all Kafka related information dynamically rather than through the config.
  public DagActionStoreChangeMonitor(Config config, int numThreads, DagManagementStateStore dagManagementStateStore,
      DagManagement dagManagement, DagActionReminderScheduler dagActionReminderScheduler, DagProcessingEngineMetrics dagProcEngineMetrics) {
    // Differentiate group id for each host
    // Pass empty string because we expect underlying client to dynamically determine the Kafka topic
    super("", config.withValue(GROUP_ID_KEY,
        ConfigValueFactory.fromAnyRef(DAG_ACTION_CHANGE_MONITOR_PREFIX + UUID.randomUUID())),
        numThreads);
    this.dagManagementStateStore = dagManagementStateStore;
    this.dagProcEngineMetrics = dagProcEngineMetrics;
    this.dagManagement = dagManagement;
    this.dagActionReminderScheduler = dagActionReminderScheduler;

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

    final ExecutorService executorService = Executors.newFixedThreadPool(ConfigUtils.getInt(this.config, ConfigurationKeys.DAG_ACTION_STORE_MONITOR_EXECUTOR_THREADS, 5), ExecutorsUtils.newThreadFactory(Optional.of(log)));

    for (DagActionStore.DagAction action : dagActions) {
      try {
        executorService.submit(() -> handleDagAction(action, true));
      } catch (Exception e) {
        log.error("Unexpected error initializing from DagActionStore changes, upon {}", action, e);
        this.unexpectedErrors.mark();
      }
    }
    try {
      boolean executedSuccessfully = executorService.awaitTermination(ConfigUtils.getInt(this.config, ConfigurationKeys.DAG_ACTION_STORE_MONITOR_EXECUTOR_TIMEOUT_SECONDS, 30), TimeUnit.SECONDS);

      if (!executedSuccessfully) {
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
   This method should be called once by the {@link GobblinServiceManager} only after the FlowGraph and
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
          /* TODO: skip deadline removal for now and let them fire
          if (dagActionType == DagActionStore.DagActionType.ENFORCE_JOB_START_DEADLINE
              || dagActionType == DagActionStore.DagActionType.ENFORCE_FLOW_FINISH_DEADLINE) {
            this.dagActionReminderScheduler.unscheduleReminderJob(dagAction, true);
            // clear any deadline reminders as well as any retry reminders
            this.dagActionReminderScheduler.unscheduleReminderJob(dagAction, false);
          }
           */
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
   * This implementation passes on the {@link DagActionStore.DagAction} to {@link DagManagement}.
   */
  protected void handleDagAction(DagActionStore.DagAction dagAction, boolean isStartup) {
    log.info("(" + (isStartup ? "on-startup" : "post-startup") + ") DagAction change ({}) received for flow: {}",
        dagAction.getDagActionType(), dagAction);
    LaunchSubmissionMetricProxy launchSubmissionMetricProxy = isStartup ? ON_STARTUP : POST_STARTUP;
    try {
      switch (dagAction.getDagActionType()) {
        case ENFORCE_FLOW_FINISH_DEADLINE:
        case ENFORCE_JOB_START_DEADLINE:
        case KILL :
        case LAUNCH :
        case REEVALUATE :
        case RESUME:
          dagManagement.addDagAction(new DagActionStore.LeaseParams(dagAction));
          break;
        default:
          log.warn("Received unsupported dagAction {}. Expected to be a RESUME, KILL, REEVALUATE or LAUNCH", dagAction.getDagActionType());
          this.unexpectedErrors.mark();
      }
    } catch (IOException e) {
      log.warn("Failed to addDagAction for flowId {} due to exception {}", dagAction.getFlowId(), e.getMessage());
      launchSubmissionMetricProxy.markFailure();
    }
  }

  @Override
  protected void createMetrics() {
    super.createMetrics();
    // Dag Action specific metrics
    this.killsInvoked = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_KILLS_INVOKED);
    this.resumesInvoked = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_RESUMES_INVOKED);
    // TODO: rename this metrics to match the variable name after debugging launch related issues
    ContextAwareMeter successfulLaunchSubmissions = this.getMetricContext()
        .contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_SUCCESSFUL_LAUNCH_SUBMISSIONS);
    ContextAwareMeter failedLaunchSubmissions = this.getMetricContext()
        .contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_FAILED_FLOW_LAUNCHED_SUBMISSIONS);
    ContextAwareMeter successfulLaunchSubmissionsOnStartup = this.getMetricContext()
        .contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_SUCCESSFUL_LAUNCH_SUBMISSIONS_ON_STARTUP);
    ContextAwareMeter failedLaunchSubmissionsOnStartup = this.getMetricContext()
        .contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_FAILED_LAUNCH_SUBMISSIONS_ON_STARTUP);
    this.unexpectedErrors = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_UNEXPECTED_ERRORS);
    this.messageProcessedMeter = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_MESSAGE_PROCESSED);
    this.duplicateMessagesMeter = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_DUPLICATE_MESSAGES);
    this.heartbeatMessagesMeter = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_HEARTBEAT_MESSAGES);
    this.nullDagActionTypeMessagesMeter = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_NULL_DAG_ACTION_TYPE_MESSAGES);
    // Reports delay from all partitions in one gauge
    ContextAwareGauge<Long> produceToConsumeDelayMillis = this.getMetricContext()
        .newContextAwareGauge(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_PRODUCE_TO_CONSUME_DELAY_MILLIS,
            () -> produceToConsumeDelayValue);
    this.getMetricContext().register(produceToConsumeDelayMillis);

    // Setup proxy for launch submission metrics
    this.ON_STARTUP = new LaunchSubmissionMetricProxy(successfulLaunchSubmissionsOnStartup,
        failedLaunchSubmissionsOnStartup);
    this.POST_STARTUP = new LaunchSubmissionMetricProxy(successfulLaunchSubmissions, failedLaunchSubmissions);
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
