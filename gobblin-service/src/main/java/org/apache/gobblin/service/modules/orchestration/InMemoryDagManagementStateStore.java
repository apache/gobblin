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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;


/**
 * An implementation of {@link DagManagementStateStore} to provide information about dags, dag nodes and their job states.
 * This store maintains and utilizes in-memory references about dags and their job states and is used
 * to determine what the current status of the {@link Dag} and/or {@link Dag.DagNode} is and what actions needs to be
 * taken next likewise mark it as: complete, failed, sla breached or simply clean up after completion.
 * Going forward, each of these in-memory references will be read/write from MySQL store.
 * Thus, the {@link DagManager} would then be stateless and operate independently.
 */
@Alpha
@Slf4j
public class InMemoryDagManagementStateStore implements DagManagementStateStore {
  @Getter
  private final Map<Dag.DagNode<JobExecutionPlan>, Dag<JobExecutionPlan>> jobToDag = new HashMap<>();
  private final Map<String, Dag<JobExecutionPlan>> dags = new HashMap<>();
  private final Map<String, Dag<JobExecutionPlan>> dagIdToDags = new HashMap<>();
  private final Map<String, Dag.DagNode<JobExecutionPlan>> dagNodes = new HashMap<>();
  private final Set<String> failedDagIds = new HashSet<>();
  private final Map<String, Dag<JobExecutionPlan>> dagIdToResumingDags = new HashMap<>();
  // dagToJobs holds a map of dagId to running jobs of that dag
  final Map<String, LinkedList<Dag.DagNode<JobExecutionPlan>>> dagToJobs = new HashMap<>();
  final Map<String, Long> dagToSLA = new HashMap<>();
  final Map<String, Long> dagToDeadline = new HashMap<>();
  private final Set<String> dagIdstoClean = new HashSet<>();
  private DagActionStore dagActionStore;

  @Override
  public synchronized void deleteJobState(String dagId, Dag.DagNode<JobExecutionPlan> dagNode) {
    this.jobToDag.remove(dagNode);
    this.dagToJobs.get(dagId).remove(dagNode);
    this.dagToDeadline.remove(dagId);
    this.dagToSLA.remove(dagId);
  }

  @Override
  public synchronized void addJobState(String dagId, Dag.DagNode<JobExecutionPlan> dagNode) {
    Dag<JobExecutionPlan> dag = this.dagIdToDags.get(dagId);
    this.jobToDag.put(dagNode, dag);
    this.dagNodes.put(dagNode.getId().toString(), dagNode);
    if (this.dagToJobs.containsKey(dagId)) {
      this.dagToJobs.get(dagId).add(dagNode);
    } else {
      LinkedList<Dag.DagNode<JobExecutionPlan>> dagNodeList = Lists.newLinkedList();
      dagNodeList.add(dagNode);
      this.dagToJobs.put(dagId, dagNodeList);
    }
  }

  @Override
  public synchronized void removeDagActionFromStore(DagActionStore.DagAction dagAction) throws IOException {
    this.dagActionStore.deleteDagAction(dagAction);
  }

  @Override
  public Map<String, Long> getDagToSLA() {
    //todo - implement
    // also need to add/remove
    return null;
  }

  @Override
  public Dag<JobExecutionPlan> getDag(String dagId) {
    return this.dags.get(dagId);
  }

  @Override
  public Dag<JobExecutionPlan> addDag(String dagId, Dag<JobExecutionPlan> dag) {
    return this.dags.put(dagId, dag);
  }

  @Override
  public boolean containsDag(String dagId) {
    return this.dagIdToDags.containsKey(dagId);
  }

  @Override
  public Dag.DagNode<JobExecutionPlan> getDagNode(String dagNodeId) {
    return this.dagNodes.get(dagNodeId);
  }

  @Override
  public Dag<JobExecutionPlan> getDagForJob(Dag.DagNode<JobExecutionPlan> dagNode) {
    return this.jobToDag.get(dagNode);
  }

  @Override
  public LinkedList<Dag.DagNode<JobExecutionPlan>> getJobs(String dagId) throws IOException {
    if(this.dagToJobs.containsKey(dagId)) {
      return this.dagToJobs.get(dagId);
    }
    throw new IOException("Dag Id: " + dagId +  "is not present");
  }

  public List<Dag.DagNode<JobExecutionPlan>> getAllJobs() throws IOException {
    List<Dag.DagNode<JobExecutionPlan>> allJobs = new ArrayList<>();
    for (Collection<Dag.DagNode<JobExecutionPlan>> collection : this.dagToJobs.values()) {
      allJobs.addAll(collection);
    }
    return allJobs;
  }

  @Override
  public boolean addFailedDag(String dagId) {
    return this.failedDagIds.add(dagId);
  }

  @Override
  public boolean existsFailedDag(String dagId) {
    return this.failedDagIds.contains(dagId);
  }

  @Override
  public boolean addCleanUpDag(String dagId) {
    return this.dagIdstoClean.add(dagId);
  }

  @Override
  public boolean checkCleanUpDag(String dagId) {
    return this.dagIdstoClean.contains(dagId);
  }
}
