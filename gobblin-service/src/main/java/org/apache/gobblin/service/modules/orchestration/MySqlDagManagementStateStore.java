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
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.google.inject.Inject;
import com.typesafe.config.Config;

import javax.inject.Singleton;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.flowgraph.DagNodeId;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.JobStatus;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 * An implementation of {@link DagManagementStateStore} to provide information about dags, dag nodes and their job states.
 * This store maintains and utilizes in-memory references about dags and their job states and is used
 * to determine what the current status of the {@link Dag} and/or {@link Dag.DagNode} is and what actions needs to be
 * taken next likewise mark it as: complete, failed, sla breached or simply clean up after completion.
 * This also encapsulates mysql based tables, i) dagStateStore, ii) failedDagStore, iii) userQuotaManager.
 * They are used here to provide complete access to dag related information at one place.
 */
@Slf4j
@Singleton
public class MySqlDagManagementStateStore implements DagManagementStateStore {
  // todo - these two stores should merge
  private DagStateStoreWithDagNodes dagStateStore;
  private DagStateStoreWithDagNodes failedDagStateStore;
  private final JobStatusRetriever jobStatusRetriever;
  private boolean dagStoresInitialized = false;
  private final UserQuotaManager quotaManager;
  Map<URI, TopologySpec> topologySpecMap;
  private final Config config;
  public static final String FAILED_DAG_STATESTORE_PREFIX = "failedDagStateStore";
  public static final String DAG_STATESTORE_CLASS_KEY = "dagStateStoreClass";
  FlowCatalog flowCatalog;
  @Getter
  private final DagManagerMetrics dagManagerMetrics = new DagManagerMetrics();
  private final DagActionStore dagActionStore;

  @Inject
  public MySqlDagManagementStateStore(Config config, FlowCatalog flowCatalog, UserQuotaManager userQuotaManager,
      JobStatusRetriever jobStatusRetriever, DagActionStore dagActionStore) {
    this.quotaManager = userQuotaManager;
    this.config = config;
    this.flowCatalog = flowCatalog;
    this.jobStatusRetriever = jobStatusRetriever;
    this.dagManagerMetrics.activate();
    this.dagActionStore = dagActionStore;
   }

  // It should be called after topology spec map is set
  private synchronized void start() {
    if (!dagStoresInitialized) {
      this.dagStateStore = createDagStateStore(config, topologySpecMap);
      this.failedDagStateStore = createDagStateStore(ConfigUtils.getConfigOrEmpty(config, FAILED_DAG_STATESTORE_PREFIX).withFallback(config),
          topologySpecMap);
      // This implementation does not need to update quota usage when the service restarts or when its leadership status
      // changes because quota usage are persisted in mysql table. For the same reason, there is no need to call getDags also.
      // Also, calling getDags during startUp may fail, because the topologies that are required to deserialize dags may
      // not have been added to the topology catalog yet.
      // initQuota(getDags());
      dagStoresInitialized = true;
    }
  }

  @Override
  public FlowSpec getFlowSpec(URI uri) throws SpecNotFoundException {
    return this.flowCatalog.getSpecs(uri);
  }

  @Override
  public void removeFlowSpec(URI uri, Properties headers, boolean triggerListener) {
    this.flowCatalog.remove(uri, headers, triggerListener);
  }

  public synchronized void setTopologySpecMap(Map<URI, TopologySpec> topologySpecMap) {
    this.topologySpecMap = topologySpecMap;
    start();
  }

  private DagStateStoreWithDagNodes createDagStateStore(Config config, Map<URI, TopologySpec> topologySpecMap) {
    try {
      Class<?> dagStateStoreClass = Class.forName(ConfigUtils.getString(config, DAG_STATESTORE_CLASS_KEY, MysqlDagStateStoreWithDagNodes.class.getName()));
      return (DagStateStoreWithDagNodes) GobblinConstructorUtils.invokeLongestConstructor(dagStateStoreClass, config, topologySpecMap);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void addDag(Dag<JobExecutionPlan> dag) throws IOException {
    this.dagStateStore.writeCheckpoint(dag);
  }

  @Override
  public void markDagFailed(Dag.DagId dagId) throws IOException {
    Dag<JobExecutionPlan> dag = this.dagStateStore.getDag(dagId);
    this.failedDagStateStore.writeCheckpoint(dag);
    this.dagStateStore.cleanUp(dagId);
    // todo - updated failedDagStateStore iff cleanup returned 1
    // or merge dagStateStore and failedDagStateStore and change the flag that marks a dag `failed`
    log.info("Marked dag failed {}", dagId);
  }

  @Override
  public void deleteDag(Dag.DagId dagId) throws IOException {
    if (this.dagStateStore.cleanUp(dagId)) {
      log.info("Deleted dag {}", dagId);
    } else {
      log.info("Dag deletion was tried but did not happen {}", dagId);
    }
  }

  @Override
  public void deleteFailedDag(Dag.DagId dagId) throws IOException {
    this.failedDagStateStore.cleanUp(dagId);
    log.info("Deleted failed dag {}", dagId);
  }

  @Override
  public Optional<Dag<JobExecutionPlan>> getFailedDag(Dag.DagId dagId) throws IOException {
    return Optional.of(this.failedDagStateStore.getDag(dagId));
  }

  @Override
  public synchronized void updateDagNode(Dag.DagNode<JobExecutionPlan> dagNode)
      throws IOException {
    this.dagStateStore.updateDagNode(dagNode);
  }

  @Override
  public Optional<Dag<JobExecutionPlan>> getDag(Dag.DagId dagId) throws IOException {
    return Optional.ofNullable(this.dagStateStore.getDag(dagId));
  }

  @Override
  public Pair<Optional<Dag.DagNode<JobExecutionPlan>>, Optional<JobStatus>> getDagNodeWithJobStatus(DagNodeId dagNodeId)
      throws IOException {
    Optional<Dag.DagNode<JobExecutionPlan>> dagNode = this.dagStateStore.getDagNode(dagNodeId);
    if (dagNode.isPresent()) {
      return ImmutablePair.of(dagNode, getJobStatus(dagNodeId));
    } else {
      // no point of searching for status if the node itself is absent.
      return ImmutablePair.of(Optional.empty(), Optional.empty());
    }
  }

  @Override
  public Set<Dag.DagNode<JobExecutionPlan>> getDagNodes(Dag.DagId dagId) throws IOException {
    return this.dagStateStore.getDagNodes(dagId);}

  @Override
  public void tryAcquireQuota(Collection<Dag.DagNode<JobExecutionPlan>> dagNodes) throws IOException {
    this.quotaManager.checkQuota(dagNodes);
  }

  @Override
  public boolean releaseQuota(Dag.DagNode<JobExecutionPlan> dagNode) throws IOException {
    return this.quotaManager.releaseQuota(dagNode);
  }

  @Override
  public Optional<JobStatus> getJobStatus(DagNodeId dagNodeId) {
    Iterator<JobStatus> jobStatusIterator = this.jobStatusRetriever.getJobStatusesForFlowExecution(dagNodeId.getFlowName(),
        dagNodeId.getFlowGroup(), dagNodeId.getFlowExecutionId(), dagNodeId.getJobName(), dagNodeId.getJobGroup());

    if (jobStatusIterator.hasNext()) {
      // there must exist exactly one job status for a dag node id, because fields of dag node id makes the primary key
      // of the job status table
      return Optional.of(jobStatusIterator.next());
    } else {
      return java.util.Optional.empty();
    }
  }

  @Override
  public boolean existsJobDagAction(String flowGroup, String flowName, long flowExecutionId, String jobName,
      DagActionStore.DagActionType dagActionType) throws IOException {
    return this.dagActionStore.exists(flowGroup, flowName, flowExecutionId, jobName, dagActionType);
  }

  @Override
  public boolean existsFlowDagAction(String flowGroup, String flowName, long flowExecutionId,
      DagActionStore.DagActionType dagActionType) throws IOException, SQLException {
    return this.dagActionStore.exists(flowGroup, flowName, flowExecutionId, dagActionType);
  }

  @Override
  public void addJobDagAction(String flowGroup, String flowName, long flowExecutionId, String jobName,
      DagActionStore.DagActionType dagActionType) throws IOException {
    log.info("Adding Dag Action for flowGroup {}, flowName {}, flowExecutionId {}, jobName {}, dagActionType {}",
        flowGroup, flowName, flowExecutionId, jobName, dagActionType);
    this.dagActionStore.addJobDagAction(flowGroup, flowName, flowExecutionId, jobName, dagActionType);
  }

  @Override
  public boolean deleteDagAction(DagActionStore.DagAction dagAction) throws IOException {
    log.info("Deleting Dag Action {}", dagAction);
    return this.dagActionStore.deleteDagAction(dagAction);
  }

  @Override
  public Collection<DagActionStore.DagAction> getDagActions() throws IOException {
    return this.dagActionStore.getDagActions();
  }
}