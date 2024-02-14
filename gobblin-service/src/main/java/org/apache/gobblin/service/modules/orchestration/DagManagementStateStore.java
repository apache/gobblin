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
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.gobblin.exception.QuotaExceededException;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;


/**
 * An interface to provide abstractions for managing {@link Dag} and {@link org.apache.gobblin.service.modules.flowgraph.Dag.DagNode} states
 * and allows add/delete and other functions.
 */
public interface DagManagementStateStore {
  void addDag(Dag<JobExecutionPlan> dag);
  /**
   * Persist the {@link Dag} to the backing store.
   * This is not an actual checkpoint but more like a Write-ahead log, where uncommitted job will be persisted
   * and be picked up again when leader transition happens.
   * @param dag The dag submitted to {@link DagManager}
   */
  void writeCheckpoint(Dag<JobExecutionPlan> dag) throws IOException;
  boolean containsDag(String dagId);
  Dag<JobExecutionPlan> getDag(DagManager.DagId dagId) throws IOException;
  /**
   * Return a list of all dag IDs contained in the dag state store.
   */
  Set<String> getDagIds() throws IOException;
  /**
   * Delete the {@link Dag} from the backing store, typically upon completion of execution.
   * @param dag The dag completed/cancelled execution on {@link org.apache.gobblin.runtime.api.SpecExecutor}.
   */
  default void cleanUp(Dag<JobExecutionPlan> dag) throws IOException {
    cleanUp(DagManagerUtils.generateDagId(dag).toString());
  }
  /**
   * Delete the {@link Dag} from the backing store, typically upon completion of execution.
   * @param dagId The ID of the dag to clean up.
   */
  void cleanUp(String dagId) throws IOException;
  void writeFailedDagCheckpoint(Dag<JobExecutionPlan> dag) throws IOException;
  Set<String> getFailedDagIds() throws IOException;
  Dag<JobExecutionPlan> getFailedDag(String dagId) throws IOException;
  default void cleanUpFailedDag(Dag<JobExecutionPlan> dag) throws IOException {
    cleanUpFailedDag(DagManagerUtils.generateDagId(dag).toString());
  }
  void cleanUpFailedDag(String dagId) throws IOException;
  void addDagNodeState(String dagId, Dag.DagNode<JobExecutionPlan> dagNode);
  Dag.DagNode<JobExecutionPlan> getDagNode(String dagNodeId);
  List<Dag.DagNode<JobExecutionPlan>> getDagNodes(String dagId) throws IOException;
  Dag<JobExecutionPlan> getParentDag(Dag.DagNode<JobExecutionPlan> dagNode);
  void deleteDagNodeState(String dagId, Dag.DagNode<JobExecutionPlan> dagNode);

  /**
   * Load all currently running {@link Dag}s from the underlying store. Typically, invoked when a new {@link DagManager}
   * takes over or on restart of service.
   * @return a {@link List} of currently running {@link Dag}s.
   */
  List<Dag<JobExecutionPlan>> getDags() throws IOException;

  /**
   * Initialize with the provided set of dags.
   */
  void initQuota(Collection<Dag<JobExecutionPlan>> dags) throws IOException;

  /**
   * Checks if the dagNode exceeds the statically configured user quota for the proxy user, requester user and flowGroup.
   * It also increases the quota usage for proxy user, requester and the flowGroup of the given DagNode by one.
   * @throws QuotaExceededException if the quota is exceeded
   */
  void checkQuota(Collection<Dag.DagNode<JobExecutionPlan>> dagNode) throws IOException;

  /**
   * Decrement the quota by one for the proxy user and requesters corresponding to the provided {@link Dag.DagNode}.
   * It is usually used with `deleteDagNodeState`, but can also be used independently sometimes.
   * Returns true if successfully reduces the quota usage
   */
  boolean releaseQuota(Dag.DagNode<JobExecutionPlan> dagNode) throws IOException;
}
