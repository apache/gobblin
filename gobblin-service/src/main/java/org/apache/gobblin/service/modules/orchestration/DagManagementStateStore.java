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
import java.util.List;
import java.util.Map;

import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;


/**
 * An interface to provide abstractions for managing {@link Dag} and {@link org.apache.gobblin.service.modules.flowgraph.Dag.DagNode} states
 * and allows add/delete and other functions.
 */
public interface DagManagementStateStore {
  Dag<JobExecutionPlan> getDag(String dagId);
  Dag<JobExecutionPlan> addDag(String dagId, Dag<JobExecutionPlan> dag);
  boolean containsDag(String dagId);
  Dag.DagNode<JobExecutionPlan> getDagNode(String dagNodeId);
  Dag<JobExecutionPlan> getDagForJob(Dag.DagNode<JobExecutionPlan> dagNode);
  List<Dag.DagNode<JobExecutionPlan>> getJobs(String dagId) throws IOException;
  List<Dag.DagNode<JobExecutionPlan>> getAllJobs() throws IOException;
  boolean addFailedDag(String dagId);
  boolean existsFailedDag(String dagId);
  boolean addCleanUpDag(String dagId);
  boolean checkCleanUpDag(String dagId);
  void addJobState(String dagId, Dag.DagNode<JobExecutionPlan> dagNode);
  void deleteJobState(String dagId, Dag.DagNode<JobExecutionPlan> dagNode);
  void removeDagActionFromStore(DagActionStore.DagAction dagAction) throws IOException;
  Map<String, Long> getDagToSLA();
  }
