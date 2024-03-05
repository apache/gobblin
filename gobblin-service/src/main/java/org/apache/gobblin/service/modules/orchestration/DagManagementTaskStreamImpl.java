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

package org.apache.gobblin.service.modules.orchestration;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.proc.DagProc;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.service.modules.orchestration.task.LaunchDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.util.ConfigUtils;


/**
 * NewDagManager has these functionalities :
 * a) manages {@link Dag}s through {@link DagManagementStateStore}.
 * b) load {@link Dag}s on service-start / set-active.
 * c) accept adhoc new dag launch requests from Orchestrator.
 */
@Slf4j
@Singleton
@Data
public class DagManagementTaskStreamImpl implements DagManagement, DagTaskStream {
  public static final String DAG_MANAGER_PREFIX = "gobblin.service.dagManager.";
  private static final int INITIAL_HOUSEKEEPING_THREAD_DELAY = 2;

  private final Config config;
  @Inject private FlowCatalog flowCatalog;
  private final boolean dagProcessingEngineEnabled;
  private Map<URI, TopologySpec> topologySpecMap = new HashMap<>();
  @Getter private final EventSubmitter eventSubmitter;
  @Getter private static final DagManagerMetrics dagManagerMetrics = new DagManagerMetrics();
  private volatile boolean isActive = false;

  @Inject(optional=true)
  protected Optional<DagActionStore> dagActionStore;
  @Inject
  @Getter DagManagementStateStore dagManagementStateStore;
  @Getter DagProcArbitrationHandler dagProcArbitrationHandler;
  private static final int MAX_HOUSEKEEPING_THREAD_DELAY = 180;
  private final BlockingQueue<DagActionStore.DagAction> dagActionQueue = new LinkedBlockingQueue<>();
  // TODO: when dagTaskStream class is made separate create another queue for reminderDagActions and process them 2X more often as new dagActions, for now put onto same queue

  @Inject
  public DagManagementTaskStreamImpl(Config config, Optional<DagActionStore> dagActionStore, DagManagementStateStore dagManagementStateStore,
      DagProcArbitrationHandler dagProcArbitrationHandler) {
    this.config = config;
    this.dagActionStore = dagActionStore;
    this.dagProcessingEngineEnabled = ConfigUtils.getBoolean(config, ServiceConfigKeys.DAG_PROCESSING_ENGINE_ENABLED, false);
    this.dagManagementStateStore = dagManagementStateStore;
    this.dagProcArbitrationHandler = dagProcArbitrationHandler;
    MetricContext metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(ConfigFactory.empty()), getClass());
    this.eventSubmitter = new EventSubmitter.Builder(metricContext, "org.apache.gobblin.service").build();
  }

  public void setActive(boolean active) {
    if (this.isActive == active) {
      log.info("DagManager already {}, skipping further actions.", (!active) ? "inactive" : "active");
    }
    this.isActive = active;
    try {
      if (this.isActive) {
        log.info("Activating NewDagManager.");
        //Initializing state store for persisting Dags.
        this.dagManagementStateStore.start();
        dagManagerMetrics.activate();
        loadDagFromDagStateStore();
      } else { //Mark the DagManager inactive.
        log.info("Inactivating the DagManager. Shutting down all DagManager threads");
        dagManagerMetrics.cleanup();
      }
    } catch (IOException e) {
        log.error("Exception encountered when activating the new DagManager", e);
        throw new RuntimeException(e);
    }
  }

  private void loadDagFromDagStateStore() throws IOException {
    List<Dag<JobExecutionPlan>> dags = this.dagManagementStateStore.getDags();
    log.info("Loading " + dags.size() + " dags from dag state store");
    for (Dag<JobExecutionPlan> dag : dags) {
      addDag(dag, false, false);
    }
  }

  @Override
  public synchronized void addDag(FlowSpec flowSpec, Dag<JobExecutionPlan> dag, boolean persist, boolean setStatus)
      throws IOException {
    addDag(dag, persist, setStatus);
    // Only the active newDagManager should delete the flowSpec
    if (isActive) {
      deleteSpecFromCatalogIfAdhoc(flowSpec);
    }
  }

  private void addDag(Dag<JobExecutionPlan> dag, boolean persist, boolean setStatus) throws IOException {
    // TODO: Used to track missing dag issue, remove later as needed
    log.info("Add dag (persist: {}, setStatus: {}): {}", persist, setStatus, dag);
    if (!isActive) {
      log.warn("Skipping add dag because this instance of DagManager is not active for dag: {}", dag);
      return;
    }

    DagManager.DagId dagId = DagManagerUtils.generateDagId(dag);
    String dagIdString = dagId.toString();
    if (persist) {
      // Persist the dag
      this.dagManagementStateStore.checkpointDag(dag);
      // After persisting the dag, its status will be tracked by active dagManagers so the action should be deleted
      // to avoid duplicate executions upon leadership change
      if (this.dagActionStore.isPresent()) {
        this.dagActionStore.get().deleteDagAction(dagId.toDagAction(DagActionStore.FlowActionType.LAUNCH));
      }
    }

    DagActionStore.DagAction dagAction = new DagActionStore.DagAction(
        dagId.getFlowGroup(), dagId.getFlowName(), dagId.getFlowExecutionId(), DagActionStore.FlowActionType.LAUNCH);

    if (this.dagManagementStateStore.containsDag(dagId)) {
      log.warn("Already tracking a dag with dagId {}, skipping.", dagIdString);
      return;
    }

    this.dagManagementStateStore.checkpointDag(dag);
    if (!this.dagActionQueue.offer(dagAction)) {
      throw new RuntimeException("Could not add dag action " + dagAction + " to the queue");
    }

    if (setStatus) {
      DagManagerUtils.submitPendingExecStatus(dag, this.eventSubmitter);
    }
  }

  public void addDagAction(DagActionStore.DagAction dagAction) {
    this.dagActionQueue.add(dagAction);
  }

  private void deleteSpecFromCatalogIfAdhoc(FlowSpec flowSpec) {
    if (!flowSpec.isScheduled()) {
      this.flowCatalog.remove(flowSpec.getUri(), new Properties(), false);
    }
  }

  @Override
  public boolean hasNext() {
    return !this.dagActionQueue.isEmpty();
  }

  @Override
  public DagTask<DagProc> next() {
    while (true) {
      try {
        DagActionStore.DagAction dagAction = this.dagActionQueue.take();  //`take` blocks till element is not available
        // TODO: determine handle reminder events later
        /* Use current time for event timestamp as this assumes that most hosts' clocks will be synchronized enough for
        the resulting actions to be considered the same event when arbitration is done. Also note that we should always
        skip flowExecutionId replacement because the flowExecutionIds have been determined at prior stage and should
        no longer be altered.
         */
        MultiActiveLeaseArbiter.LeaseAttemptStatus leaseAttemptStatus =
            this.dagProcArbitrationHandler.tryAcquireLease(dagAction, System.currentTimeMillis(), false, true);
        if (leaseAttemptStatus instanceof MultiActiveLeaseArbiter.LeasedToAnotherStatus) {
          // TODO: see if can re-add to queue automatically in DagProcArbitrationHandler, update event time when deciding what the value is
          log.info("Multi-active execution - Setting reminder to revisit dagAction: {}", dagAction);
          this.dagProcArbitrationHandler.scheduleReminderForEvent((MultiActiveLeaseArbiter.LeasedToAnotherStatus) leaseAttemptStatus, ((MultiActiveLeaseArbiter.LeasedToAnotherStatus) leaseAttemptStatus).getEventTimeMillis());
        } else if (leaseAttemptStatus instanceof MultiActiveLeaseArbiter.LeaseObtainedStatus) {
          log.info("Multi-active execution - Obtained lease for dagAction: {}", dagAction);
          return createDagTask(dagAction, (MultiActiveLeaseArbiter.LeaseObtainedStatus) leaseAttemptStatus);
        } else if (leaseAttemptStatus instanceof MultiActiveLeaseArbiter.NoLongerLeasingStatus) {
          log.info("Multi-active execution - Skipping already completed dagAction: {}", dagAction);
        }
      } catch (Throwable t) {
        //TODO: need to handle exceptions gracefully
        log.error("Error getting DagAction from the queue / creating DagTask", t);
      }
    }
  }

  private DagTask createDagTask(DagActionStore.DagAction dagAction, MultiActiveLeaseArbiter.LeaseObtainedStatus leaseObtainedStatus) {
    DagActionStore.FlowActionType flowActionType = dagAction.getFlowActionType();

    switch (flowActionType) {
      case LAUNCH:
        return new LaunchDagTask(dagAction, leaseObtainedStatus);
      default:
        throw new UnsupportedOperationException("Not yet implemented");
    }
  }
}
