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
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;
import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.kafka.HighLevelConsumer;
import org.apache.gobblin.runtime.metrics.RuntimeMetrics;
import org.apache.gobblin.service.modules.orchestration.DagManager;


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
  private ContextAwareMeter unexpectedErrors;
  private ContextAwareMeter messageProcessedMeter;

  protected CacheLoader<String, String> cacheLoader = new CacheLoader<String, String>() {
    @Override
    public String load(String key) throws Exception {
      return key;
    }
  };

  protected LoadingCache<String, String>
      dagActionsSeenCache = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build(cacheLoader);

  @Inject
  protected DagActionStore dagActionStore;

  @Inject
  protected DagManager dagManager;

  // Note that the topic is an empty string (rather than null to avoid NPE) because this monitor relies on the consumer
  // client itself to determine all Kafka related information dynamically rather than through the config.
  public DagActionStoreChangeMonitor(String topic, Config config, int numThreads) {
    // Differentiate group id for each host
    super(topic, config.withValue(GROUP_ID_KEY,
        ConfigValueFactory.fromAnyRef(DAG_ACTION_CHANGE_MONITOR_PREFIX + UUID.randomUUID().toString())),
        numThreads);
  }

  @Override
  protected void assignTopicPartitions() {
    // Expects underlying consumer to handle initializing partitions and offset for the topic -
    // subscribe to all partitions from latest offset
    return;
  }

  @Override
  /*
  This class is multi-threaded and this message will be called by multiple threads, however any given message will be
  partitioned and processed by only one thread (and corresponding queue).
   */
  protected void processMessage(DecodeableKafkaRecord message) {
    // This will also include the heathCheck message so that we can rely on this to monitor the health of this Monitor
    messageProcessedMeter.mark();
    String key = (String) message.getKey();
    DagActionStoreChangeEvent value = (DagActionStoreChangeEvent) message.getValue();

    Long timestamp = value.getChangeEventIdentifier().getTimestamp();
    String operation = value.getChangeEventIdentifier().getOperationType().name();
    String flowGroup = value.getFlowGroup();
    String flowName = value.getFlowName();
    String flowExecutionId = value.getFlowExecutionId();

    log.debug("Processing Dag Action message for flow group: {} name: {} executionId: {} timestamp {} operation {}",
        flowGroup, flowName, flowExecutionId, timestamp, operation);

    String changeIdentifier = timestamp + key;
    if (!ChangeMonitorUtils.shouldProcessMessage(changeIdentifier, dagActionsSeenCache, operation,
        timestamp.toString())) {
      return;
    }

    // Retrieve the Dag Action taken from MySQL table unless operation is DELETE
    DagActionStore.DagActionValue dagAction = null;
    if (!operation.equals("DELETE")) {
      try {
        dagAction = dagActionStore.getDagAction(flowGroup, flowName, flowExecutionId).getDagActionValue();
      } catch (IOException e) {
        log.warn("Encountered IOException trying to retrieve dagAction for flow group: {} name: {} executionId: {}. " + "Exception: {}", flowGroup, flowName, flowExecutionId, e);
        this.unexpectedErrors.mark();
      } catch (SpecNotFoundException e) {
        log.warn("DagAction not found for flow group: {} name: {} executionId: {} Exception: {}", flowGroup, flowName,
            flowExecutionId, e);
        this.unexpectedErrors.mark();
      } catch (SQLException throwables) {
        log.warn("Encountered SQLException trying to retrieve dagAction for flow group: {} name: {} executionId: {}. " + "Exception: {}", flowGroup, flowName, flowExecutionId, throwables);
        throwables.printStackTrace();
      }
    }

    // We only expert INSERT and DELETE operations done to this table. INSERTs correspond to resume or delete flow
    // requests that have to be processed. DELETEs require no action.
    try {
      if (operation.equals("INSERT")) {
        if (dagAction.equals(DagActionStore.DagActionValue.RESUME)) {
          dagManager.handleResumeFlowRequest(flowGroup, flowName,Long.parseLong(flowExecutionId));
          this.resumesInvoked.mark();
        } else if (dagAction.equals(DagActionStore.DagActionValue.KILL)) {
          dagManager.handleKillFlowRequest(flowGroup, flowName, Long.parseLong(flowExecutionId));
          this.killsInvoked.mark();
        } else {
          log.warn("Received unsupported dagAction {}. Expected to be a KILL or RESUME", dagAction);
          this.unexpectedErrors.mark();
          return;
        }
      } else if (operation.equals("UPDATE")) {
        log.warn("Received an UPDATE action to the DagActionStore when values in this store are never supposed to be "
            + "updated. Flow group: {} name {} executionId {} were updated to action {}", flowGroup, flowName,
            flowExecutionId, dagAction);
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

  @Override
  protected void createMetrics() {
    super.createMetrics();
    this.killsInvoked = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_KILLS_INVOKED);
    this.resumesInvoked = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_RESUMES_INVOKED);
    this.unexpectedErrors = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_UNEXPECTED_ERRORS);
    this.messageProcessedMeter = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_MESSAGE_PROCESSED);
  }

}
