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
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.linkedin.r2.util.NamedThreadFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
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
public class NewDagManager implements DagManagement {
  public static final String DAG_MANAGER_PREFIX = "gobblin.service.dagManager.";
  private static final int INITIAL_HOUSEKEEPING_THREAD_DELAY = 2;

  private final Config config;
  @Inject private FlowCatalog flowCatalog;
  private final boolean dagProcessingEngineEnabled;
  private Map<URI, TopologySpec> topologySpecMap = new HashMap<>();
  @Getter private final EventSubmitter eventSubmitter;
  @Getter private static final DagManagerMetrics dagManagerMetrics = new DagManagerMetrics();
  private ScheduledExecutorService houseKeepingThreadPool;
  private volatile boolean isActive = false;

  @Inject(optional=true)
  protected Optional<DagActionStore> dagActionStore;
  @Inject
  @Getter DagManagementStateStore dagManagementStateStore;
  private static final int MAX_HOUSEKEEPING_THREAD_DELAY = 180;
  private final BlockingQueue<DagActionStore.DagAction> dagActionQueue = new LinkedBlockingQueue<>();

  @Inject
  public NewDagManager(Config config, Optional<DagActionStore> dagActionStore, DagManagementStateStore dagManagementStateStore) {
    this.config = config;
    this.dagActionStore = dagActionStore;
    this.dagProcessingEngineEnabled = ConfigUtils.getBoolean(config, ConfigurationKeys.DAG_PROCESSING_ENGINE_ENABLED, false);
    this.dagManagementStateStore = dagManagementStateStore;
    MetricContext metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(ConfigFactory.empty()), getClass());
    this.eventSubmitter = new EventSubmitter.Builder(metricContext, "org.apache.gobblin.service").build();
  }

  public void setActive(boolean active) throws IOException {
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
        this.houseKeepingThreadPool = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("LoadDagsThread"));
        for (int delay = INITIAL_HOUSEKEEPING_THREAD_DELAY; delay < MAX_HOUSEKEEPING_THREAD_DELAY; delay *= 2) {
          this.houseKeepingThreadPool.schedule(() -> {
            try {
              loadDagFromDagStateStore();
            } catch (Exception e) {
              log.error("failed to sync dag state store due to ", e);
            }
          }, delay, TimeUnit.MINUTES);
        }
      } else { //Mark the DagManager inactive.
        log.info("Inactivating the DagManager. Shutting down all DagManager threads");
        dagManagerMetrics.cleanup();
        this.houseKeepingThreadPool.shutdown();
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
    try {
      DagActionStore.DagAction dagAction = this.dagActionQueue.take();  //`take` blocks till element is not available
      // todo reconsider the use of MultiActiveLeaseArbiter
      //MultiActiveLeaseArbiter.LeaseAttemptStatus leaseAttemptStatus = new MultiActiveLeaseArbiter.LeaseObtainedStatus(dagAction);
      // todo - uncomment after flow trigger handler provides such an api
      //Properties jobProps = getJobProperties(dagAction);
      //flowTriggerHandler.getLeaseOnDagAction(jobProps, dagAction, System.currentTimeMillis());
      //if (leaseAttemptStatus instanceof MultiActiveLeaseArbiter.LeaseObtainedStatus) {
      // can it return null? is this iterator allowed to return null?
      return createDagTask(dagAction, new MultiActiveLeaseArbiter.LeaseObtainedStatus(dagAction, System.currentTimeMillis()));
      //}
    } catch (Throwable t) {
      //TODO: need to handle exceptions gracefully
      log.error("Error getting DagAction from the queue / creating DagTask", t);
    }
    return null;
  }

  private DagTask createDagTask(DagActionStore.DagAction dagAction, MultiActiveLeaseArbiter.LeaseAttemptStatus leaseObtainedStatus) {
    DagActionStore.FlowActionType flowActionType = dagAction.getFlowActionType();

    switch (flowActionType) {
      case LAUNCH:
        return new LaunchDagTask(dagAction, leaseObtainedStatus);
      default:
        throw new UnsupportedOperationException("Not yet implemented");
    }
  }
}
