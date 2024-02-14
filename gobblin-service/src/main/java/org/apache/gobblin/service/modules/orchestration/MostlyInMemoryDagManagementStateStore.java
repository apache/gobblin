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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.modules.flowgraph.Dag;
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
public class MostlyInMemoryDagManagementStateStore implements DagManagementStateStore {
  @Getter private final Map<Dag.DagNode<JobExecutionPlan>, Dag<JobExecutionPlan>> jobToDag = new HashMap<>();
  private final Map<String, Dag<JobExecutionPlan>> dags = new HashMap<>();
  private final Map<String, Dag.DagNode<JobExecutionPlan>> dagNodes = new HashMap<>();
  // dagToJobs holds a map of dagId to running jobs of that dag
  final Map<String, LinkedList<Dag.DagNode<JobExecutionPlan>>> dagToJobs = new HashMap<>();
  final Map<String, Long> dagToDeadline = new HashMap<>();
  private final Set<String> dagIdstoClean = new HashSet<>();

  private final DagStateStore dagStateStore;
  private final DagStateStore failedDagStateStore;
  private final UserQuotaManager quotaManager;
  private static final String FAILED_DAG_STATESTORE_PREFIX = "failedDagStateStore";
  public static final String DAG_STATESTORE_CLASS_KEY = DagManager.DAG_MANAGER_PREFIX + "dagStateStoreClass";

  public MostlyInMemoryDagManagementStateStore(Config config, Map<URI, TopologySpec> topologySpecMap) throws IOException {
    this.dagStateStore = createDagStateStore(config, topologySpecMap);
    this.failedDagStateStore = createDagStateStore(
        ConfigUtils.getConfigOrEmpty(config, FAILED_DAG_STATESTORE_PREFIX).withFallback(config), topologySpecMap);
    this.quotaManager = new MysqlUserQuotaManager(config);
    this.quotaManager.init(getDags());
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
  public void writeCheckpoint(Dag<JobExecutionPlan> dag) throws IOException {
    this.dagStateStore.writeCheckpoint(dag);
  }

  @Override
  public void writeFailedDagCheckpoint(Dag<JobExecutionPlan> dag) throws IOException {
    this.failedDagStateStore.writeCheckpoint(dag);
  }

  @Override
  public void cleanUp(Dag<JobExecutionPlan> dag) throws IOException {
    this.dagStateStore.cleanUp(dag);
  }

  @Override
  public void cleanUpFailedDag(Dag<JobExecutionPlan> dag) throws IOException {
    this.failedDagStateStore.cleanUp(dag);
  }

  @Override
  public void cleanUp(String dagId) throws IOException {
    this.dagStateStore.cleanUp(dagId);
  }

  @Override
  public void cleanUpFailedDag(String dagId) throws IOException {
    this.failedDagStateStore.cleanUp(dagId);
  }

  @Override
  public List<Dag<JobExecutionPlan>> getDags() throws IOException {
    return this.dagStateStore.getDags();
  }

  @Override
  public Dag<JobExecutionPlan> getFailedDag(String dagId) throws IOException {
    return this.failedDagStateStore.getDag(dagId);
  }

  @Override
  public Set<String> getDagIds() throws IOException {
    return this.dagStateStore.getDagIds();
  }

  @Override
  public Set<String> getFailedDagIds() throws IOException {
    return this.failedDagStateStore.getDagIds();
  }

  @Override
  public synchronized void deleteDagNodeState(String dagId, Dag.DagNode<JobExecutionPlan> dagNode) {
    this.jobToDag.remove(dagNode);
    this.dagNodes.remove(dagNode.getValue().getId());
    this.dagToDeadline.remove(dagId);
    this.dagToJobs.get(dagId).remove(dagNode);
    if (this.dagToJobs.get(dagId).isEmpty()) {
      this.dagToJobs.remove(dagId);
    }
  }

  @Override
  public synchronized void addDagNodeState(String dagId, Dag.DagNode<JobExecutionPlan> dagNode) {
    Dag<JobExecutionPlan> dag = this.dags.get(dagId);
    this.jobToDag.put(dagNode, dag);
    this.dagNodes.put(dagNode.getValue().getId(), dagNode);
    if (!this.dagToJobs.containsKey(dagId)) {
      this.dagToJobs.put(dagId, Lists.newLinkedList());
    }
    this.dagToJobs.get(dagId).add(dagNode);
  }

  @Override
  public Dag<JobExecutionPlan> getDag(String dagId) {
    return this.dags.get(dagId);
  }

  @Override
  public void addDag(String dagId, Dag<JobExecutionPlan> dag) {
    this.dags.put(dagId, dag);
  }

  @Override
  public boolean containsDag(String dagId) {
    return this.dags.containsKey(dagId);
  }

  @Override
  public Dag.DagNode<JobExecutionPlan> getDagNode(String dagNodeId) {
    return this.dagNodes.get(dagNodeId);
  }


  @Override
  public Dag<JobExecutionPlan> getParentDag(Dag.DagNode<JobExecutionPlan> dagNode) {
    return this.jobToDag.get(dagNode);
  }

  @Override
  public LinkedList<Dag.DagNode<JobExecutionPlan>> getDagNodes(String dagId) {
    if (this.dagToJobs.containsKey(dagId)) {
      return this.dagToJobs.get(dagId);
    } else {
      return Lists.newLinkedList();
    }
  }

  public List<Dag.DagNode<JobExecutionPlan>> getAllDagNodes() {
    List<Dag.DagNode<JobExecutionPlan>> allJobs = new ArrayList<>();
    for (Collection<Dag.DagNode<JobExecutionPlan>> collection : this.dagToJobs.values()) {
      allJobs.addAll(collection);
    }
    return allJobs;
  }

  @Override
  public boolean addCleanUpDag(String dagId) {
    return this.dagIdstoClean.add(dagId);
  }

  public void initQuotaManageer(Collection<Dag<JobExecutionPlan>> dags) {
    // This implementation does not need to update quota usage when the service restarts or when its leadership status changes
  }

  @Override
  public void checkQuota(Collection<Dag.DagNode<JobExecutionPlan>> dagNodes) throws IOException {
    this.quotaManager.checkQuota(dagNodes);
  }

  @Override
  public boolean releaseQuota(Dag.DagNode<JobExecutionPlan> dagNode) throws IOException {
    return this.quotaManager.releaseQuota(dagNode);
  }
}