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

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;
import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.runtime.kafka.HighLevelConsumer;
import org.apache.gobblin.runtime.metrics.RuntimeMetrics;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagManagement;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.Orchestrator;
import org.apache.gobblin.service.modules.orchestration.task.DagProcessingEngineMetrics;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.ExecutorsUtils;


/**
 * A DagActionStore change monitor that uses {@link DagActionStoreChangeEvent} schema to process Kafka messages received
 * from its corresponding consumer client. This monitor responds to requests to resume or delete a flow and acts as a
 * connector between the API and execution layers of GaaS.
 */
@Slf4j
public abstract class DagActionStoreChangeMonitor extends HighLevelConsumer<String, DagActionStoreChangeEvent> {
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

  protected CacheLoader<String, String> cacheLoader = new CacheLoader<String, String>() {
    @Override
    public String load(@NotNull String key) {
      return key;
    }
  };

  protected LoadingCache<String, String>
      dagActionsSeenCache = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build(cacheLoader);

  protected Orchestrator orchestrator;
  @Getter
  @VisibleForTesting
  protected FlowCatalog flowCatalog;
  protected DagManagementStateStore dagManagementStateStore;
  protected final DagProcessingEngineMetrics dagProcEngineMetrics;
  @Getter
  private volatile boolean isActive;

  // Note that the topic is an empty string (rather than null to avoid NPE) because this monitor relies on the consumer
  // client itself to determine all Kafka related information dynamically rather than through the config.
  public DagActionStoreChangeMonitor(String topic, Config config, int numThreads, FlowCatalog flowCatalog,
      Orchestrator orchestrator, DagManagementStateStore dagManagementStateStore, DagProcessingEngineMetrics dagProcEngineMetrics) {
    // Differentiate group id for each host
    super(topic, config.withValue(GROUP_ID_KEY,
        ConfigValueFactory.fromAnyRef(DAG_ACTION_CHANGE_MONITOR_PREFIX + UUID.randomUUID())),
        numThreads);
    this.flowCatalog = flowCatalog;
    this.orchestrator = orchestrator;
    this.dagManagementStateStore = dagManagementStateStore;
    this.dagProcEngineMetrics = dagProcEngineMetrics;

    /*
    Metrics need to be created before initializeMonitor() below is called (or more specifically handleDagAction() is
    called on any dagAction)
     */
    buildMetricsContextAndMetrics();
  }

  @Override
  protected void assignTopicPartitions() {
    // This implementation expects underlying consumer (HighLevelConsumer::GobblinKafkaConsumerClient) to handle
    // initializing partitions and offset for the topic. It should subscribe to all partitions from latest offset
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
    executorService.shutdown();

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

  protected abstract void handleDagAction(String operation, DagActionStore.DagAction dagAction, String flowGroup,
      String flowName, long flowExecutionId, DagActionStore.DagActionType dagActionType);

  /**
   * This implementation passes on the {@link DagActionStore.DagAction} to {@link DagManagement}.
   */
  protected abstract void handleDagAction(DagActionStore.DagAction dagAction, boolean isStartup);

  @Override
  protected void createMetrics() {
    super.createMetrics();
    // Dag Action specific metrics
    this.killsInvoked = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_KILLS_INVOKED);
    this.resumesInvoked = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_RESUMES_INVOKED);
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
  }

  @Override
  protected String getMetricsPrefix() {
    return RuntimeMetrics.DAG_ACTION_STORE_MONITOR_PREFIX + ".";
  }
}
