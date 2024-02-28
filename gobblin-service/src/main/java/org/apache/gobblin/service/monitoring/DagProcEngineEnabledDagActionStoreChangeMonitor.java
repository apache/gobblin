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

import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.service.modules.orchestration.DagManagement;
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.service.modules.orchestration.Orchestrator;


/**
 * A DagActionStore change monitor that uses {@link DagActionStoreChangeEvent} schema to process Kafka messages received
 * from its corresponding consumer client. This monitor responds to requests to resume or delete a flow and acts as a
 * connector between the API and execution layers of GaaS.
 */
@Slf4j
public class DagProcEngineEnabledDagActionStoreChangeMonitor extends DagActionStoreChangeMonitor {
  private final DagManagement dagManagement;

  // Note that the topic is an empty string (rather than null to avoid NPE) because this monitor relies on the consumer
  // client itself to determine all Kafka related information dynamically rather than through the config.
  public DagProcEngineEnabledDagActionStoreChangeMonitor(String topic, Config config, DagManager dagManager, int numThreads,
      FlowCatalog flowCatalog, Orchestrator orchestrator, DagActionStore dagActionStore,
      boolean isMultiActiveSchedulerEnabled, DagManagement dagManagement) {
    // Differentiate group id for each host
    super(topic, config, dagManager, numThreads, flowCatalog, orchestrator, dagActionStore, isMultiActiveSchedulerEnabled);
    this.dagManagement = dagManagement;
  }

  @Override
  protected void handleDagAction(DagActionStore.DagAction dagAction, boolean isStartup) {
    log.info("(" + (isStartup ? "on-startup" : "post-startup") + ") DagAction change ({}) received for flow: {}",
        dagAction.getFlowActionType(), dagAction);
    LaunchSubmissionMetricProxy launchSubmissionMetricProxy = isStartup ? ON_STARTUP : POST_STARTUP;
    try {
      // todo - add actions for other other type of dag actions
      if (dagAction.getFlowActionType().equals(DagActionStore.FlowActionType.LAUNCH)) {
        // If multi-active scheduler is NOT turned on we should not receive these type of events
        if (!this.isMultiActiveSchedulerEnabled) {
          this.unexpectedErrors.mark();
          throw new RuntimeException(String.format("Received LAUNCH dagAction while not in multi-active scheduler "
              + "mode for flowAction: %s", dagAction));
        }
        dagManagement.addDagAction(dagAction);
      } else {
        log.warn("Received unsupported dagAction {}. Expected to be a KILL, RESUME, or LAUNCH", dagAction.getFlowActionType());
        this.unexpectedErrors.mark();
      }
    } catch (IOException e) {
      log.warn("Failed to add Job Execution Plan for flowId {} due to exception {}", dagAction.getFlowId(), e.getMessage());
      launchSubmissionMetricProxy.markFailure();
    }
  }
}
