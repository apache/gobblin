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

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.runtime.metrics.RuntimeMetrics;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.service.modules.orchestration.DagManagement;
import org.apache.gobblin.service.modules.orchestration.Orchestrator;


/**
 * A {@link DagActionStoreChangeMonitor} that should be used {@link org.apache.gobblin.service.ServiceConfigKeys#DAG_PROCESSING_ENGINE_ENABLED}
 * is set.
 */
@Slf4j
public class DagManagementDagActionStoreChangeMonitor extends DagActionStoreChangeMonitor {
  private final DagManagement dagManagement;
  protected ContextAwareMeter unexpectedLaunchEventErrors;

  // Note that the topic is an empty string (rather than null to avoid NPE) because this monitor relies on the consumer
  // client itself to determine all Kafka related information dynamically rather than through the config.
  public DagManagementDagActionStoreChangeMonitor(Config config, int numThreads,
      FlowCatalog flowCatalog, Orchestrator orchestrator, DagActionStore dagActionStore,
      boolean isMultiActiveSchedulerEnabled, DagManagement dagManagement) {
    // DagManager is only needed in the `handleDagAction` method of its parent class and not needed in this class,
    // so we are passing a null value for DagManager to its parent class.
    super("", config, null, numThreads, flowCatalog, orchestrator, dagActionStore, isMultiActiveSchedulerEnabled);
    this.dagManagement = dagManagement;
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
      // todo - add actions for other other type of dag actions
      if (dagAction.getDagActionType().equals(DagActionStore.DagActionType.LAUNCH)) {
        // If multi-active scheduler is NOT turned on we should not receive these type of events
        if (!this.isMultiActiveSchedulerEnabled) {
          this.unexpectedLaunchEventErrors.mark();
          throw new RuntimeException(String.format("Received LAUNCH dagAction while not in multi-active scheduler "
              + "mode for flowAction: %s", dagAction));
        }
        dagManagement.addDagAction(dagAction);
      } else {
        log.warn("Received unsupported dagAction {}. Expected to be a KILL, RESUME, or LAUNCH", dagAction.getDagActionType());
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
    this.unexpectedLaunchEventErrors = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_DAG_ACTION_STORE_MONITOR_UNEXPECTED_ERRORS);
  }
}
