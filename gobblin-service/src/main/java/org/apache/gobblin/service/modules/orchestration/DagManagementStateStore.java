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
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.exception.QuotaExceededException;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.flowgraph.DagNodeId;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.JobStatus;


/**
 * An interface to provide abstractions for managing {@link Dag} and {@link org.apache.gobblin.service.modules.flowgraph.Dag.DagNode} states
 * and allows add/delete and other functions.
 */
@Alpha
public interface DagManagementStateStore {
  /**
   * Returns a {@link FlowSpec} for the given URI.
   * @throws SpecNotFoundException if the spec is not found
   */
  FlowSpec getFlowSpec(URI uri) throws SpecNotFoundException;

  /**
   * Removes a {@link FlowSpec} with the given URI and pass the deletion to listeners if `triggerListener` is true
   * No-op if the flow spec was not present in the store.
   */
  void removeFlowSpec(URI uri, Properties headers, boolean triggerListener);

  /**
   * Checkpoints any changes in {@link Dag} or in its {@link Dag.DagNode}s.
   * e.g. on adding a failed dag in store to retry later, on submitting a dag node to spec producer because that changes
   * dag node's state, on resuming a dag, on receiving a new dag from orchestrator.
   * Calling on a previously checkpointed Dag updates it.
   * Opposite of this is {@link DagManagementStateStore#deleteDag} that removes the Dag from the store.
   * @param dag The dag to checkpoint
   */
  void checkpointDag(Dag<JobExecutionPlan> dag) throws IOException;

  /**
   @return whether `dagId` is currently known due to {@link DagManagementStateStore#checkpointDag} but not yet
   {@link DagManagementStateStore#deleteDag}
   */
  boolean containsDag(DagManager.DagId dagId) throws IOException;

  /**
   @return the {@link Dag}, if present
   */
  Optional<Dag<JobExecutionPlan>> getDag(DagManager.DagId dagId) throws IOException;

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
   * Failed dags are queried using {@link DagManagementStateStore#getFailedDag(DagManager.DagId)} ()} later to be retried.
   * @param dag failing dag
   */
  void markDagFailed(Dag<JobExecutionPlan> dag) throws IOException;

  /**
   * Return a list of all failed dags' IDs contained in the dag state store.
   */
  Set<String> getFailedDagIds() throws IOException;

  /**
   * Returns the failed dag.
   * If the dag is not found because it was never marked as failed through {@link DagManagementStateStore#markDagFailed(Dag)},
   * it returns Optional.absent.
   * @param dagId dag id of the failed dag
   */
  Optional<Dag<JobExecutionPlan>> getFailedDag(DagManager.DagId dagId) throws IOException;

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
   * Note that a DagNode is a part of a Dag and must already be present in the store through
   * {@link DagManagementStateStore#checkpointDag}. This call is just an additional identifier which may be used
   * for DagNode level operations. In the future, it may be merged with checkpointDag.
   * @param dagNode dag node to be added
   * @param dagId dag id of the dag this dag node belongs to
   */
  void addDagNodeState(Dag.DagNode<JobExecutionPlan> dagNode, DagManager.DagId dagId) throws IOException;

  /**
   * Returns the requested {@link  org.apache.gobblin.service.modules.flowgraph.Dag.DagNode} and its {@link JobStatus},
   * or Optional.absent if it is not found.
   * @param dagNodeId of the dag node
   */
  Optional<Pair<Dag.DagNode<JobExecutionPlan>, JobStatus>> getDagNodeWithJobStatus(DagNodeId dagNodeId);

  /**
   * Returns a list of {@link org.apache.gobblin.service.modules.flowgraph.Dag.DagNode} for a {@link Dag}.
   * Returned list will be empty if the dag is not found in the store.
   * @param dagId DagId of the dag for which all DagNodes are requested
   */
  List<Dag.DagNode<JobExecutionPlan>> getDagNodes(DagManager.DagId dagId) throws IOException;

  /**
   * Returns the {@link Dag} the provided {@link org.apache.gobblin.service.modules.flowgraph.Dag.DagNode} belongs to
   * or Optional.absent if it is not found.
   */
  Optional<Dag<JobExecutionPlan>> getParentDag(Dag.DagNode<JobExecutionPlan> dagNode);

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

  /**
   * Returns a {@link DagManagerMetrics} that monitors dags execution.
   */
  DagManagerMetrics getDagManagerMetrics();

  /**
   * @return {@link JobStatus} or {@link Optional#empty} if not present in the Job-Status Store
   */
  Optional<JobStatus> getJobStatus(DagNodeId dagNodeId);

  /**
   * Returns true if the {@link Dag} identified by the given {@link org.apache.gobblin.service.modules.orchestration.DagManager.DagId}
   * has any running job, false otherwise.
   */
  public boolean hasRunningJobs(DagManager.DagId dagId);
}