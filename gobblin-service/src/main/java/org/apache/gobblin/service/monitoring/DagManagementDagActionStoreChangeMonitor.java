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

import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.service.modules.orchestration.DagActionReminderScheduler;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagManagement;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.Orchestrator;
import org.apache.gobblin.service.modules.orchestration.task.DagProcessingEngineMetrics;


/**
 A DagActionStore change monitor that uses {@link DagActionStoreChangeEvent} schema to process Kafka messages received
 * from its corresponding consumer client. This monitor responds to requests to resume or delete a flow and acts as a
 * connector between the API and execution layers of GaaS.
 */
@Slf4j
public class DagManagementDagActionStoreChangeMonitor extends DagActionStoreChangeMonitor {
  private final DagManagement dagManagement;
  @VisibleForTesting @Getter
  private final DagActionReminderScheduler dagActionReminderScheduler;

  // Note that the topic is an empty string (rather than null to avoid NPE) because this monitor relies on the consumer
  // client itself to determine all Kafka related information dynamically rather than through the config.
  public DagManagementDagActionStoreChangeMonitor(Config config, int numThreads, DagManagementStateStore dagManagementStateStore,
      DagManagement dagManagement, DagActionReminderScheduler dagActionReminderScheduler, DagProcessingEngineMetrics dagProcEngineMetrics) {
    // DagManager is only needed in the `handleDagAction` method of its parent class and not needed in this class,
    // so we are passing a null value for DagManager to its parent class.
    super("", config, numThreads, null, null, dagManagementStateStore, dagProcEngineMetrics);
    this.dagManagement = dagManagement;
    this.dagActionReminderScheduler = dagActionReminderScheduler;
  }

  @Override
  protected void handleDagAction(String operation, DagActionStore.DagAction dagAction, String flowGroup, String flowName,
      long flowExecutionId, DagActionStore.DagActionType dagActionType) {
    // We only expect INSERT and DELETE operations done to this table. INSERTs correspond to any type of
    // {@link DagActionStore.FlowActionType} flow requests that have to be processed.
    try {
      switch (operation) {
        case "INSERT":
          handleDagAction(dagAction, false);
          this.dagProcEngineMetrics.markDagActionsObserved(dagActionType);
          break;
        case "UPDATE":
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
   * This implementation passes on the {@link DagActionStore.DagAction} to {@link DagManagement} instead of finding a
   * {@link org.apache.gobblin.runtime.api.FlowSpec} and passing the spec to {@link Orchestrator}.
   */
  @Override
  protected void handleDagAction(DagActionStore.DagAction dagAction, boolean isStartup) {
    log.info("(" + (isStartup ? "on-startup" : "post-startup") + ") DagAction change ({}) received for flow: {}",
        dagAction.getDagActionType(), dagAction);
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
    }
  }
}
