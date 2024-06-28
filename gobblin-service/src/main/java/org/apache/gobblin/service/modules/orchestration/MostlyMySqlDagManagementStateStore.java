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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
public class MostlyMySqlDagManagementStateStore implements DagManagementStateStore {
  private final Map<DagNodeId, Dag.DagNode<JobExecutionPlan>> dagNodes = new ConcurrentHashMap<>();
  // dagToJobs holds a map of dagId to running jobs of that dag
  private final Map<DagManager.DagId, Set<Dag.DagNode<JobExecutionPlan>>> dagToJobs = new ConcurrentHashMap<>();
  private DagStateStore dagStateStore;
  private DagStateStore failedDagStateStore;
  private JobStatusRetriever jobStatusRetriever;
  private boolean dagStoresInitialized = false;
  private final UserQuotaManager quotaManager;
  Map<URI, TopologySpec> topologySpecMap;
  private final Config config;
  public static final String FAILED_DAG_STATESTORE_PREFIX = "failedDagStateStore";
  public static final String DAG_STATESTORE_CLASS_KEY = DagManager.DAG_MANAGER_PREFIX + "dagStateStoreClass";
  FlowCatalog flowCatalog;
  @Getter
  private final DagManagerMetrics dagManagerMetrics = new DagManagerMetrics();
  private final DagActionStore dagActionStore;

  @Inject
  public MostlyMySqlDagManagementStateStore(Config config, FlowCatalog flowCatalog, UserQuotaManager userQuotaManager,
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

  public synchronized void setTopologySpecMap(Map<URI, TopologySpec> topologySpecMap) throws IOException {
    this.topologySpecMap = topologySpecMap;
    start();
  }

  private DagStateStore createDagStateStore(Config config, Map<URI, TopologySpec> topologySpecMap) {
    try {
      Class<?> dagStateStoreClass = Class.forName(ConfigUtils.getString(config, DAG_STATESTORE_CLASS_KEY, MysqlDagStateStore.class.getName()));
      return (DagStateStore) GobblinConstructorUtils.invokeLongestConstructor(dagStateStoreClass, config, topologySpecMap);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void checkpointDag(Dag<JobExecutionPlan> dag) throws IOException {
    this.dagStateStore.writeCheckpoint(dag);
  }

  @Override
  public void markDagFailed(Dag<JobExecutionPlan> dag) throws IOException {
    this.dagStateStore.cleanUp(dag);
    // todo - updated failedDagStateStore iff cleanup returned 1
    this.failedDagStateStore.writeCheckpoint(dag);
  }

  @Override
  public void deleteDag(Dag<JobExecutionPlan> dag) throws IOException {
    this.dagStateStore.cleanUp(dag);
  }

  @Override
  public void deleteFailedDag(Dag<JobExecutionPlan> dag) throws IOException {
    this.failedDagStateStore.cleanUp(dag);
  }

  @Override
  public void deleteDag(DagManager.DagId dagId) throws IOException {
    this.dagStateStore.cleanUp(dagId.toString());
  }

  @Override
  public void deleteFailedDag(DagManager.DagId dagId) throws IOException {
    this.failedDagStateStore.cleanUp(dagId.toString());
  }

  @Override
  public List<Dag<JobExecutionPlan>> getDags() throws IOException {
    return this.dagStateStore.getDags();
  }

  @Override
  public Optional<Dag<JobExecutionPlan>> getFailedDag(DagManager.DagId dagId) throws IOException {
    return Optional.of(this.failedDagStateStore.getDag(dagId.toString()));
  }

  @Override
  public Set<String> getFailedDagIds() throws IOException {
    return this.failedDagStateStore.getDagIds();
  }

  @Override
  // todo - updating different maps here and in addDagNodeState can result in inconsistency between the maps
  public synchronized void deleteDagNodeState(DagManager.DagId dagId, Dag.DagNode<JobExecutionPlan> dagNode) {
    this.dagNodes.remove(dagNode.getValue().getId());
    if (this.dagToJobs.containsKey(dagId)) {
      this.dagToJobs.get(dagId).remove(dagNode);
      if (this.dagToJobs.get(dagId).isEmpty()) {
        this.dagToJobs.remove(dagId);
      }
    }
  }

  // todo - updating different mapps here and in deleteDagNodeState can result in inconsistency between the maps
  @Override
  public synchronized void addDagNodeState(Dag.DagNode<JobExecutionPlan> dagNode, DagManager.DagId dagId)
      throws IOException {
    if (!containsDag(dagId)) {
      throw new RuntimeException("Dag " + dagId + " not found");
    }
    this.dagNodes.put(dagNode.getValue().getId(), dagNode);
    if (!this.dagToJobs.containsKey(dagId)) {
      this.dagToJobs.put(dagId, new HashSet<>());
    }
    this.dagToJobs.get(dagId).add(dagNode);
  }

  @Override
  public Optional<Dag<JobExecutionPlan>> getDag(DagManager.DagId dagId) throws IOException {
    return Optional.ofNullable(this.dagStateStore.getDag(dagId.toString()));
  }

  @Override
  public boolean containsDag(DagManager.DagId dagId) throws IOException {
    return this.dagStateStore.existsDag(dagId);
  }

  @Override
  public Pair<Optional<Dag.DagNode<JobExecutionPlan>>, Optional<JobStatus>> getDagNodeWithJobStatus(DagNodeId dagNodeId) {
    if (this.dagNodes.containsKey(dagNodeId)) {
      return ImmutablePair.of(Optional.of(this.dagNodes.get(dagNodeId)), getJobStatus(dagNodeId));
    } else {
      // no point of searching for status if the node itself is absent.
      return ImmutablePair.of(Optional.empty(), Optional.empty());
    }
  }

  @Override
  public Set<Dag.DagNode<JobExecutionPlan>> getDagNodes(DagManager.DagId dagId) {
    Set<Dag.DagNode<JobExecutionPlan>> dagNodes = this.dagToJobs.get(dagId);
    if (dagNodes != null) {
      return dagNodes;
    } else {
      return new HashSet<>();
    }
  }

  public void initQuota(Collection<Dag<JobExecutionPlan>> dags) {
    // This implementation does not need to update quota usage when the service restarts or when its leadership status changes
    // because quota usage are persisted in mysql table
  }

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

  /* todo - this method works because when the jobs finish they are deleted from the DMSS -> if no more job is found, means
   no more running jobs.
   But DMSS still has dags and which still contains dag nodes. We need to revisit this method's logic when we change
   DMSS to a fully mysql backed implementation. then we may want to consider this approach
   return getDagNodes(dagId).stream()
       .anyMatch(node -> !FlowStatusGenerator.FINISHED_STATUSES.contains(node.getValue().getExecutionStatus().name()));
  */
  @Override
  public boolean hasRunningJobs(DagManager.DagId dagId) {
    return !getDagNodes(dagId).isEmpty();
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
    this.dagActionStore.addJobDagAction(flowGroup, flowName, flowExecutionId, jobName, dagActionType);
  }

  @Override
  public boolean deleteDagAction(DagActionStore.DagAction dagAction) throws IOException {
    return this.dagActionStore.deleteDagAction(dagAction);
  }

  @Override
  public Collection<DagActionStore.DagAction> getDagActions() throws IOException {
    return this.dagActionStore.getDagActions();
  }
}