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
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.typesafe.config.Config;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.flowgraph.DagNodeId;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
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
public class MostlyMySqlDagManagementStateStore implements DagManagementStateStore {
  private final Map<Dag.DagNode<JobExecutionPlan>, Dag<JobExecutionPlan>> jobToDag = new ConcurrentHashMap<>();
  private final Map<DagNodeId, Dag.DagNode<JobExecutionPlan>> dagNodes = new ConcurrentHashMap<>();
  // dagToJobs holds a map of dagId to running jobs of that dag
  private final Map<DagManager.DagId, LinkedList<Dag.DagNode<JobExecutionPlan>>> dagToJobs = new ConcurrentHashMap<>();
  private final Map<DagManager.DagId, Long> dagToDeadline = new ConcurrentHashMap<>();
  private DagStateStore dagStateStore;
  private DagStateStore failedDagStateStore;
  private boolean dagStoresInitialized = false;
  private final UserQuotaManager quotaManager;
  @Setter Map<URI, TopologySpec> topologySpecMap;
  private final Config config;
  private static final String FAILED_DAG_STATESTORE_PREFIX = "failedDagStateStore";
  public static final String DAG_STATESTORE_CLASS_KEY = DagManager.DAG_MANAGER_PREFIX + "dagStateStoreClass";

  @Inject
  public MostlyMySqlDagManagementStateStore(Config config) throws IOException {
    this.quotaManager = new MysqlUserQuotaManager(config);
    this.config = config;
   }

  @Override
  public void start() throws IOException {
    if (!dagStoresInitialized) {
      this.dagStateStore = createDagStateStore(config, topologySpecMap);
      this.failedDagStateStore = createDagStateStore(ConfigUtils.getConfigOrEmpty(config, FAILED_DAG_STATESTORE_PREFIX).withFallback(config),
          topologySpecMap);
      initQuota(getDags());
      dagStoresInitialized = true;
    }
  }

  DagStateStore createDagStateStore(Config config, Map<URI, TopologySpec> topologySpecMap) {
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
  // todo - updating different mapps here and in addDagNodeState can result in inconsistency between the maps
  public synchronized void deleteDagNodeState(DagManager.DagId dagId, Dag.DagNode<JobExecutionPlan> dagNode) {
    this.jobToDag.remove(dagNode);
    this.dagNodes.remove(dagNode.getValue().getId());
    this.dagToDeadline.remove(dagId);
    this.dagToJobs.get(dagId).remove(dagNode);
    if (this.dagToJobs.get(dagId).isEmpty()) {
      this.dagToJobs.remove(dagId);
    }
  }

  // todo - updating different mapps here and in deleteDagNodeState can result in inconsistency between the maps
  @Override
  public synchronized void addDagNodeState(Dag.DagNode<JobExecutionPlan> dagNode, DagManager.DagId dagId)
      throws IOException {
    Optional<Dag<JobExecutionPlan>> dag = getDag(dagId);
    if (!dag.isPresent()) {
      throw new RuntimeException("Dag " + dagId + " not found");
    }
    this.jobToDag.put(dagNode, dag.get());
    this.dagNodes.put(dagNode.getValue().getId(), dagNode);
    if (!this.dagToJobs.containsKey(dagId)) {
      this.dagToJobs.put(dagId, Lists.newLinkedList());
    }
    this.dagToJobs.get(dagId).add(dagNode);
  }

  @Override
  public Optional<Dag<JobExecutionPlan>> getDag(DagManager.DagId dagId) throws IOException {
    return Optional.of(this.dagStateStore.getDag(dagId.toString()));
  }

  @Override
  public boolean containsDag(DagManager.DagId dagId) throws IOException {
    return this.dagStateStore.existsDag(dagId);
  }

  @Override
  public Optional<Dag.DagNode<JobExecutionPlan>> getDagNode(DagNodeId dagNodeId) {
    return Optional.of(this.dagNodes.get(dagNodeId));
  }


  @Override
  public Optional<Dag<JobExecutionPlan>> getParentDag(Dag.DagNode<JobExecutionPlan> dagNode) {
    return Optional.of(this.jobToDag.get(dagNode));
  }

  @Override
  public List<Dag.DagNode<JobExecutionPlan>> getDagNodes(DagManager.DagId dagId) {
    List<Dag.DagNode<JobExecutionPlan>> dagNodes = this.dagToJobs.get(dagId);
    if (dagNodes != null) {
      return dagNodes;
    } else {
      return Lists.newLinkedList();
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
}