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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.jetbrains.annotations.NotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.typesafe.config.Config;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.service.modules.orchestration.DagActionReminderScheduler;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagManagement;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.Orchestrator;


/**
 * A {@link DagActionStoreChangeMonitor} that should be used {@link org.apache.gobblin.service.ServiceConfigKeys#DAG_PROCESSING_ENGINE_ENABLED}
 * is set.
 */
@Slf4j
public class DagManagementDagActionStoreChangeMonitor extends DagActionStoreChangeMonitor {
  private final DagManagement dagManagement;
  @VisibleForTesting @Getter
  private final DagActionReminderScheduler dagActionReminderScheduler;
  protected final LoadingCache<DagActionStore.DagAction, Boolean> deleteDeadlineDagActionCache;

  // Note that the topic is an empty string (rather than null to avoid NPE) because this monitor relies on the consumer
  // client itself to determine all Kafka related information dynamically rather than through the config.
  public DagManagementDagActionStoreChangeMonitor(Config config, int numThreads,
      FlowCatalog flowCatalog, Orchestrator orchestrator, DagManagementStateStore dagManagementStateStore,
      boolean isMultiActiveSchedulerEnabled, DagManagement dagManagement, DagActionReminderScheduler dagActionReminderScheduler) {
    // DagManager is only needed in the `handleDagAction` method of its parent class and not needed in this class,
    // so we are passing a null value for DagManager to its parent class.
    super("", config, null, numThreads, flowCatalog, orchestrator, dagManagementStateStore, isMultiActiveSchedulerEnabled);
    this.dagManagement = dagManagement;
    this.dagActionReminderScheduler = dagActionReminderScheduler;
    CacheLoader<DagActionStore.DagAction, Boolean> deleteDeadlineDagActionCacheLoader = new CacheLoader<DagActionStore.DagAction, Boolean>() {
      @Override
      public Boolean load(DagActionStore.@NotNull DagAction key) throws Exception {
        return false;
      }
    };
    this.deleteDeadlineDagActionCache = CacheBuilder.newBuilder()
        .expireAfterWrite(5, TimeUnit.MINUTES).build(deleteDeadlineDagActionCacheLoader);
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
          break;
        case "UPDATE":
          log.warn("Received an UPDATE action to the DagActionStore when values in this store are never supposed to be "
                  + "updated. Flow group: {} name {} executionId {} were updated to action {}", flowGroup, flowName,
              flowExecutionId, dagActionType);
          this.unexpectedErrors.mark();
          break;
        case "DELETE":
          log.debug("Deleted dagAction from DagActionStore: {}", dagAction);
          if (dagActionType == DagActionStore.DagActionType.ENFORCE_JOB_START_DEADLINE
              || dagActionType == DagActionStore.DagActionType.ENFORCE_FLOW_FINISH_DEADLINE) {
            if (!this.dagActionReminderScheduler.unscheduleReminderJob(dagAction, true)) { //todo delete
              log.warn("Trigger not found for {}. Possibly an out-of-order event received.", dagAction);
              this.deleteDeadlineDagActionCache.put(dagAction, true);
            }
            // clear any deadline reminders as well as any retry reminders
            this.dagActionReminderScheduler.unscheduleReminderJob(dagAction, false);
          }
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
    LaunchSubmissionMetricProxy launchSubmissionMetricProxy = isStartup ? ON_STARTUP : POST_STARTUP;
    try {
      switch (dagAction.getDagActionType()) {
        case ENFORCE_FLOW_FINISH_DEADLINE:
        case ENFORCE_JOB_START_DEADLINE:
          if (this.deleteDeadlineDagActionCache.get(dagAction)) {
            log.info("Ignoring {} because the delete equivalent of the same received already.", dagAction);
            break;
          }
        case KILL :
        case LAUNCH :
        case REEVALUATE :
        case RESUME:
          dagManagement.addDagAction(dagAction);
          break;
        default:
          log.warn("Received unsupported dagAction {}. Expected to be a RESUME, KILL, REEVALUATE or LAUNCH", dagAction.getDagActionType());
          this.unexpectedErrors.mark();
      }
    } catch (IOException e) {
      log.warn("Failed to addDagAction for flowId {} due to exception {}", dagAction.getFlowId(), e.getMessage());
      launchSubmissionMetricProxy.markFailure();
    } catch (ExecutionException e) {
      log.error(e.getMessage());
    }
  }
}
