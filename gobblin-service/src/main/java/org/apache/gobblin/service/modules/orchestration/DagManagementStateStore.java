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
import org.apache.gobblin.service.modules.flowgraph.DagNodeId;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;


/**
 * An interface to provide abstractions for managing {@link Dag} and {@link org.apache.gobblin.service.modules.flowgraph.Dag.DagNode} states
 * and allows add/delete and other functions.
 */
public interface DagManagementStateStore {
  /**
   * Persist the {@link Dag} to the backing store.
   * It is named this because it is usually called to checkpoint any changes in {@link Dag} or in its {@link Dag.DagNode}s.
   * @param dag The dag submitted to {@link DagManager}
   */
  void writeCheckpoint(Dag<JobExecutionPlan> dag) throws IOException;
  boolean containsDag(DagManager.DagId dagId)
      throws IOException;
  Dag<JobExecutionPlan> getDag(DagManager.DagId dagId) throws IOException;

  /**
   * Delete the {@link Dag} from the backing store, typically upon completion of execution.
   * @param dag The dag completed/cancelled execution on {@link org.apache.gobblin.runtime.api.SpecExecutor}.
   */
  default void cleanUp(Dag<JobExecutionPlan> dag) throws IOException {
    cleanUp(DagManagerUtils.generateDagId(dag));
  }

  /**
   * Delete the {@link Dag} from the backing store, typically upon completion of execution.
   * @param dagId The ID of the dag to clean up.
   */
  void cleanUp(DagManager.DagId dagId) throws IOException;
  void writeFailedDagCheckpoint(Dag<JobExecutionPlan> dag) throws IOException;

  /**
   * Return a list of all dag IDs contained in the dag state store.
   */
  Set<String> getFailedDagIds() throws IOException;

  Dag<JobExecutionPlan> getFailedDag(DagManager.DagId dagId) throws IOException;

  default void cleanUpFailedDag(Dag<JobExecutionPlan> dag) throws IOException {
    cleanUpFailedDag(DagManagerUtils.generateDagId(dag));
  }

  void cleanUpFailedDag(DagManager.DagId dagId) throws IOException;
  void addDagNodeState(DagManager.DagId dagId, Dag.DagNode<JobExecutionPlan> dagNode)
      throws IOException;
  Dag.DagNode<JobExecutionPlan> getDagNode(DagNodeId dagNodeId);
  List<Dag.DagNode<JobExecutionPlan>> getDagNodes(DagManager.DagId dagId) throws IOException;
  Dag<JobExecutionPlan> getParentDag(Dag.DagNode<JobExecutionPlan> dagNode);
  void deleteDagNodeState(DagManager.DagId dagId, Dag.DagNode<JobExecutionPlan> dagNode);

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
  void tryAcquireQuota(Collection<Dag.DagNode<JobExecutionPlan>> dagNode) throws IOException;

  /**
   * Decrement the quota by one for the proxy user and requesters corresponding to the provided {@link Dag.DagNode}.
   * It is usually used with `deleteDagNodeState`, but can also be used independently sometimes.
   * Returns true if successfully reduces the quota usage
   */
  boolean releaseQuota(Dag.DagNode<JobExecutionPlan> dagNode) throws IOException;
}
