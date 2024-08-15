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
// todo - this should merge with DagStateStoreWithDagNodes interface and `addDagNodeState` and `addDagNode` should merge and rename to
  // `updateDagNode`
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
   * Adds a {@link Dag} in the store.
   * Opposite of this is {@link DagManagementStateStore#deleteDag} that removes the Dag from the store.
   * @param dag The dag to checkpoint
   */
  void addDag(Dag<JobExecutionPlan> dag) throws IOException;

  /**
   @return the {@link Dag}, if present
   */
  Optional<Dag<JobExecutionPlan>> getDag(Dag.DagId dagId) throws IOException;

  /**
   * Delete the {@link Dag} from the backing store, typically upon completion of execution.
   * @param dagId The ID of the dag to clean up.
   */
  void deleteDag(Dag.DagId dagId) throws IOException;

  /**
   * This marks the dag as a failed one.
   * Failed dags are queried using {@link DagManagementStateStore#getFailedDag(Dag.DagId)} ()} later to be retried.
   * @param dagId failing dag's dagId
   */
  void markDagFailed(Dag.DagId dagId) throws IOException;

  /**
   * Returns the failed dag.
   * If the dag is not found because it was never marked as failed through
   * {@link DagManagementStateStore#markDagFailed(Dag.DagId)},
   * it returns Optional.absent.
   * @param dagId dag id of the failed dag
   */
  Optional<Dag<JobExecutionPlan>> getFailedDag(Dag.DagId dagId) throws IOException;

  void deleteFailedDag(Dag.DagId dagId) throws IOException;

  /**
   * Adds state of a {@link org.apache.gobblin.service.modules.flowgraph.Dag.DagNode} to the store.
   * Note that a DagNode is a part of a Dag and must already be present in the store through
   * {@link DagManagementStateStore#addDag}. This call is just an additional identifier which may be used
   * for DagNode level operations. In the future, it may be merged with checkpointDag.
   * @param dagNode dag node to be added
   * @param dagId dag id of the dag this dag node belongs to
   */
  void updateDagNode(Dag.DagNode<JobExecutionPlan> dagNode) throws IOException;

  /**
   * Returns the requested {@link  org.apache.gobblin.service.modules.flowgraph.Dag.DagNode} and its {@link JobStatus}.
   * Both params are returned as optional and are empty if not present in the store.
   * JobStatus can be non-empty only if DagNode is non-empty.
   * @param dagNodeId of the dag node
   */
  Pair<Optional<Dag.DagNode<JobExecutionPlan>>, Optional<JobStatus>> getDagNodeWithJobStatus(DagNodeId dagNodeId)
      throws IOException;

  /**
   * Returns a list of {@link org.apache.gobblin.service.modules.flowgraph.Dag.DagNode} for a {@link Dag}.
   * Returned list will be empty if the dag is not found in the store.
   * @param dagId DagId of the dag for which all DagNodes are requested
   */
  Set<Dag.DagNode<JobExecutionPlan>> getDagNodes(Dag.DagId dagId) throws IOException;

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
   * Check if an action exists in dagAction store by flow group, flow name, flow execution id, and job name.
   * @param flowGroup flow group for the dag action
   * @param flowName flow name for the dag action
   * @param flowExecutionId flow execution for the dag action
   * @param jobName job name for the dag action
   * @param dagActionType the value of the dag action
   * @throws IOException
   */
  boolean existsJobDagAction(String flowGroup, String flowName, long flowExecutionId, String jobName,
      DagActionStore.DagActionType dagActionType) throws IOException;

  /**
   * Check if an action exists in dagAction store by flow group, flow name, and flow execution id, it assumes jobName is
   * empty ("").
   * @param flowGroup flow group for the dag action
   * @param flowName flow name for the dag action
   * @param flowExecutionId flow execution for the dag action
   * @param dagActionType the value of the dag action
   * @throws IOException
   */
  boolean existsFlowDagAction(String flowGroup, String flowName, long flowExecutionId,
      DagActionStore.DagActionType dagActionType) throws IOException, SQLException;

  /** Persist the {@link DagActionStore.DagAction} in {@link DagActionStore} for durability */
  default void addDagAction(DagActionStore.DagAction dagAction) throws IOException {
    addJobDagAction(
        dagAction.getFlowGroup(),
        dagAction.getFlowName(),
        dagAction.getFlowExecutionId(),
        dagAction.getJobName(),
        dagAction.getDagActionType());
  }

  /**
   * Persist the dag action in {@link DagActionStore} for durability
   * @param flowGroup flow group for the dag action
   * @param flowName flow name for the dag action
   * @param flowExecutionId flow execution for the dag action
   * @param jobName job name for the dag action
   * @param dagActionType the value of the dag action
   * @throws IOException
   */
  void addJobDagAction(String flowGroup, String flowName, long flowExecutionId, String jobName,
      DagActionStore.DagActionType dagActionType) throws IOException;

  /**
   * Persist the dag action in {@link DagActionStore} for durability. This method assumes an empty jobName.
   * @param flowGroup flow group for the dag action
   * @param flowName flow name for the dag action
   * @param flowExecutionId flow execution for the dag action
   * @param dagActionType the value of the dag action
   * @throws IOException
   */
  default void addFlowDagAction(String flowGroup, String flowName, long flowExecutionId,
      DagActionStore.DagActionType dagActionType) throws IOException {
    addDagAction(DagActionStore.DagAction.forFlow(flowGroup, flowName, flowExecutionId, dagActionType));
  }

  /**
   * delete the dag action from {@link DagActionStore}
   * @param dagAction containing all information needed to identify dag and specific action value
   * @throws IOException
   * @return true if we successfully delete one record, return false if the record does not exist
   */
  boolean deleteDagAction(DagActionStore.DagAction dagAction) throws IOException;

  /***
   * Get all {@link DagActionStore.DagAction}s from the {@link DagActionStore}.
   * @throws IOException Exception in retrieving {@link DagActionStore.DagAction}s.
   */
  Collection<DagActionStore.DagAction> getDagActions() throws IOException;
}