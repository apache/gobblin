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

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.exception.QuotaExceededException;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.flowgraph.DagNodeId;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;


/**
 * An interface to provide abstractions for managing {@link Dag} and {@link org.apache.gobblin.service.modules.flowgraph.Dag.DagNode} states
 * and allows add/delete and other functions.
 */
@Alpha
public interface DagManagementStateStore {
  /**
   * Checkpoints any changes in {@link Dag} or in its {@link Dag.DagNode}s.
   * e.g. on adding a failed dag in store to retry later, on submitting a dag node to spec producer because that changes
   * dag node's state, on resuming a dag, on receiving a new dag from orchestrator.
   * Opposite of this is {@link DagManagementStateStore#deleteDag} that removes the Dag from the store.
   * @param dag The dag to checkpoint
   */
  void checkpointDag(Dag<JobExecutionPlan> dag) throws IOException;

  /**
   * Returns true if the dag is present in the store.
   * @param dagId DagId of the dag
   */
  boolean containsDag(DagManager.DagId dagId) throws IOException;

  /**
   * Returns a dag if present, null otherwise.
   * @param dagId DagId of the dag
   */
  Dag<JobExecutionPlan> getDag(DagManager.DagId dagId) throws IOException;

  /**
   * Delete the {@link Dag} from the backing store, typically called upon completion of execution.
   * @param dag The dag completed/cancelled execution on {@link org.apache.gobblin.runtime.api.SpecExecutor}.
   */
  default void deleteDag(Dag<JobExecutionPlan> dag) throws IOException {
    deleteDag(DagManagerUtils.generateDagId(dag));
  }

  /**
   * Delete the {@link Dag} from the backing store, typically upon completion of execution.
   * @param dagId The ID of the dag to clean up.
   */
  void deleteDag(DagManager.DagId dagId) throws IOException;

  /**
   * This marks the dag as a failed one.
   * Failed dags are queried using {@link DagManagementStateStore#getFailedDagIds()} later to be retried.
   * @param dag failing dag
   * @throws IOException
   */
  void markDagFailed(Dag<JobExecutionPlan> dag) throws IOException;

  /**
   * Return a list of all failed dags' IDs contained in the dag state store.
   */
  Set<String> getFailedDagIds() throws IOException;

  /**
   * Returns the failed dag.
   * If the dag is not found or is not marked as failed through {@link DagManagementStateStore#markDagFailed(Dag)}, it returns null.
   * @param dagId dag id of the failed dag
   */
  Dag<JobExecutionPlan> getFailedDag(DagManager.DagId dagId) throws IOException;

  /**
   * Deletes the failed dag. No-op if dag is not found or is not marked as failed.
   * @param dag
   * @throws IOException
   */
  default void deleteFailedDag(Dag<JobExecutionPlan> dag) throws IOException {
    deleteFailedDag(DagManagerUtils.generateDagId(dag));
  }

  void deleteFailedDag(DagManager.DagId dagId) throws IOException;

  /**
   * Adds state of a {@link org.apache.gobblin.service.modules.flowgraph.Dag.DagNode} to the store.
   * Note that a DagNode is a part of a Dag and may already be present in the store thorugh
   * {@link DagManagementStateStore#checkpointDag}. This call is just an additional identifier which may be used
   * for DagNode level operations. In the future, it may be merged with checkpointDag.
   * @param dagNode dag node to be added
   * @param dagId dag id of the dag this dag node belongs to
   * @throws IOException
   */
  void addDagNodeState(Dag.DagNode<JobExecutionPlan> dagNode, DagManager.DagId dagId)
      throws IOException;

  /**
   * Returns the requested {@link  org.apache.gobblin.service.modules.flowgraph.Dag.DagNode}
   * @param dagNodeId of the dag ndoe
   */
  Dag.DagNode<JobExecutionPlan> getDagNode(DagNodeId dagNodeId);

  /**
   * Returns a list of {@link org.apache.gobblin.service.modules.flowgraph.Dag.DagNode} for a {@link Dag}
   * @param dagId DagId of the dag for which all DagNodes are requested
   */
  List<Dag.DagNode<JobExecutionPlan>> getDagNodes(DagManager.DagId dagId) throws IOException;

  /**
   * Returns the {@link Dag} the provided {@link org.apache.gobblin.service.modules.flowgraph.Dag.DagNode} belongs to,
   * or null if the dag node is not found.
   */
  Dag<JobExecutionPlan> getParentDag(Dag.DagNode<JobExecutionPlan> dagNode);

  /**
   * Deletes the dag node state that was added through {@link DagManagementStateStore#addDagNodeState(Dag.DagNode, DagManager.DagId)}
   * No-op if the dag node is not found in the store.
   * @param dagNode dag node to be deleted
   * @param dagId dag id of the dag this dag node belongs to
   */
  void deleteDagNodeState(DagManager.DagId dagId, Dag.DagNode<JobExecutionPlan> dagNode);

  /**
   * Loads all currently running {@link Dag}s from the underlying store. Typically, invoked when a new {@link DagManager}
   * takes over or on restart of service.
   * @return a {@link List} of currently running {@link Dag}s.
   */
  List<Dag<JobExecutionPlan>> getDags() throws IOException;

  /**
   * Initialize the dag quotas with the provided set of dags.
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
