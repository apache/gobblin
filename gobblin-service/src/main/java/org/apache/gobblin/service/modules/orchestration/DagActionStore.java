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
import java.sql.SQLException;
import java.util.Collection;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.flowgraph.DagNodeId;


/**
 * GaaS store for pending {@link DagAction}s on a flow or job.
 * See javadoc for {@link DagAction}
 */
public interface DagActionStore {
  String NO_JOB_NAME_DEFAULT = "";
  enum DagActionType {
    ENFORCE_JOB_START_DEADLINE, // Enforce job start deadline
    ENFORCE_FLOW_FINISH_DEADLINE, // Enforce flow finish deadline
    KILL, // Kill invoked through API call
    LAUNCH, // Launch new flow execution invoked adhoc or through scheduled trigger
    REEVALUATE, // Re-evaluate what needs to be done upon receipt of a final job status
    RESUME, // Resume flow invoked through API call
  }

  /**
   * A DagAction uniquely identifies a particular flow (or job level) execution and the action to be performed on it,
   * denoted by the `dagActionType` field.
   * These are created and stored either by a REST client request or generated within GaaS. The Flow Management layer
   * retrieves and executes {@link DagAction}s to progress a flow's execution or enforce execution deadlines.
   *
   * Flow group, name, and executionId are sufficient to define a flow level action (used with
   * {@link DagActionStore#NO_JOB_NAME_DEFAULT}). When `jobName` is provided, it can be used to identify the specific
   * job on which the action is to be performed. The schema of this class matches exactly that of the
   * {@link DagActionStore}.
   *
   */
  @Data
  @RequiredArgsConstructor
  class DagAction {
    final String flowGroup;
    final String flowName;
    final long flowExecutionId;
    final String jobName;
    final DagActionType dagActionType;

    public static DagAction forFlow(String flowGroup, String flowName, long flowExecutionId, DagActionType dagActionType) {
      return new DagAction(flowGroup, flowName, flowExecutionId, NO_JOB_NAME_DEFAULT, dagActionType);
    }

    public FlowId getFlowId() {
      return new FlowId().setFlowGroup(this.flowGroup).setFlowName(this.flowName);
    }

    /**
     *   Replace flow execution id with agreed upon event time to easily track the flow
     */
    public DagAction updateFlowExecutionId(long eventTimeMillis) {
      return new DagAction(this.getFlowGroup(), this.getFlowName(), eventTimeMillis, this.getJobName(), this.getDagActionType());
    }

    /**
     * Creates and returns a {@link DagNodeId} for this DagAction.
     */
    public DagNodeId getDagNodeId() {
      return new DagNodeId(this.flowGroup, this.flowName, this.flowExecutionId, this.flowGroup, this.jobName);
    }

    /**
     * Creates and returns a {@link Dag.DagId} for this DagAction.
     */
    public Dag.DagId getDagId() {
      return new Dag.DagId(this.flowGroup, this.flowName, this.flowExecutionId);
    }
  }

  /**
   * This object is used locally (in-memory) by the {@link MultiActiveLeaseArbiter} to identify a particular
   * {@link DagAction} along with the time it was requested, denoted by the `eventTimeMillis` field. It also tracks
   * whether it has been previously passed to the {@link MultiActiveLeaseArbiter} to attempt ownership over the flow
   * event, indicated by the 'isReminder' field (true when it has been previously attempted).
   */
  @Data
  @RequiredArgsConstructor
  class LeaseParams {
    final DagAction dagAction;
    final boolean isReminder;
    final long eventTimeMillis;

    /**
     * Creates a lease object for a dagAction and eventTimeMillis representing an original event (isReminder is False)
     */
    public LeaseParams(DagAction dagAction, long eventTimeMillis) {
      this(dagAction, false, eventTimeMillis);
    }

    public LeaseParams(DagAction dagAction) {
      this(dagAction, System.currentTimeMillis());
    }

    /**
     * Replace flow execution id in dagAction with agreed upon event time to easily track the flow
     */
    public DagAction updateDagActionFlowExecutionId(long flowExecutionId) {
      return this.dagAction.updateFlowExecutionId(flowExecutionId);
    }
  }



  /**
   * Check if an action exists in dagAction store by flow group, flow name, flow execution id, and job name.
   * @param flowGroup flow group for the dag action
   * @param flowName flow name for the dag action
   * @param flowExecutionId flow execution for the dag action
   * @param jobName job name for the dag action
   * @param dagActionType the value of the dag action
   * @throws IOException
   */
  boolean exists(String flowGroup, String flowName, long flowExecutionId, String jobName, DagActionType dagActionType) throws IOException;

  /**
   * Check if an action exists in dagAction store by flow group, flow name, and flow execution id, it assumes jobName is
   * empty ("").
   * @param flowGroup flow group for the dag action
   * @param flowName flow name for the dag action
   * @param flowExecutionId flow execution for the dag action
   * @param dagActionType the value of the dag action
   * @throws IOException
   */
  boolean exists(String flowGroup, String flowName, long flowExecutionId, DagActionType dagActionType) throws IOException, SQLException;

  /** Persist the {@link DagAction} in {@link DagActionStore} for durability */
  default void addDagAction(DagAction dagAction) throws IOException {
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
  void addJobDagAction(String flowGroup, String flowName, long flowExecutionId, String jobName, DagActionType dagActionType) throws IOException;

  /**
   * Persist the dag action in {@link DagActionStore} for durability. This method assumes an empty jobName.
   * @param flowGroup flow group for the dag action
   * @param flowName flow name for the dag action
   * @param flowExecutionId flow execution for the dag action
   * @param dagActionType the value of the dag action
   * @throws IOException
   */
  default void addFlowDagAction(String flowGroup, String flowName, long flowExecutionId, DagActionType dagActionType) throws IOException {
    addDagAction(DagAction.forFlow(flowGroup, flowName, flowExecutionId, dagActionType));
  }

  /**
   * delete the dag action from {@link DagActionStore}
   * @param dagAction containing all information needed to identify dag and specific action value
   * @throws IOException
   * @return true if we successfully delete one record, return false if the record does not exist
   */
  boolean deleteDagAction(DagAction dagAction) throws IOException;

  /***
   * Get all {@link DagAction}s from the {@link DagActionStore}.
   * @throws IOException Exception in retrieving {@link DagAction}s.
   */
  Collection<DagAction> getDagActions() throws IOException;
}
