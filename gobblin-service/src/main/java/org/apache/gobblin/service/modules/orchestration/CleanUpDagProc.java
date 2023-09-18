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
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.exception.MaybeRetryableException;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.FlowStatusGenerator;
import org.apache.gobblin.service.monitoring.JobStatus;


/**
 * An implementation of {@link DagProc} that is responsible for clean up {@link Dag} that has been completed
 * or has reached an end state likewise: FAILED, COMPLETE or CANCELED
 *
 */
@Slf4j
@Alpha
public class CleanUpDagProc extends DagProc {

  private MultiActiveLeaseArbiter multiActiveLeaseArbiter;

  private DagManagementStateStore dagManagementStateStore;

  private MetricContext metricContext;

  private Optional<EventSubmitter> eventSubmitter;

  private DagManagerMetrics dagManagerMetrics;

  private DagStateStore dagStateStore;

  private DagStateStore failedDagStateStore;

  private DagTaskStream dagTaskStream;

  private static final long DAG_FLOW_STATUS_TOLERANCE_TIME_MILLIS = TimeUnit.MINUTES.toMillis(5);

  //TODO: instantiate an object of this class

  @Override
  protected Object initialize() throws MaybeRetryableException, IOException {
    String dagIdToClean = ""; //TODO: implement this dagID
    if(!this.dagManagementStateStore.hasRunningJobs(dagIdToClean)) {
      Dag<JobExecutionPlan> dag = this.dagManagementStateStore.getDagIdToDags().get(dagIdToClean);
      return dag;
    }
    return null;
  }

  @Override
  protected Object act(Object state) throws ExecutionException, InterruptedException, IOException {
    Dag<JobExecutionPlan> dag = (Dag<JobExecutionPlan>) state;
    DagManager.DagId dagId = DagManagerUtils.generateDagId(dag);
    LinkedList<Dag.DagNode<JobExecutionPlan>> dagNodeList = this.dagManagementStateStore.getDagToJobs().get(dagId);
    while (!dagNodeList.isEmpty()) {
      Dag.DagNode<JobExecutionPlan> dagNode = dagNodeList.poll();
      this.dagManagementStateStore.deleteJobState(dagId.toString(), dagNode);
    }
    if (dag.getFlowEvent() == null) {
      // If the dag flow event is not set, then it is successful
      dag.setFlowEvent(TimingEvent.FlowTimings.FLOW_SUCCEEDED);
    } else {
      addFailedDag(dagId.toString(), dag);
    }
    JobStatus flowStatus = dagTaskStream.pollFlowStatus(dag);
    if (flowStatus != null && FlowStatusGenerator.FINISHED_STATUSES.contains(flowStatus.getEventName())) {
      FlowId flowId = DagManagerUtils.getFlowId(dag);

      switch (dag.getFlowEvent()) {
        case TimingEvent.FlowTimings.FLOW_SUCCEEDED:
          this.dagManagerMetrics.emitFlowSuccessMetrics(flowId);
          this.dagManagerMetrics.conditionallyMarkFlowAsState(flowId, DagManager.FlowState.SUCCESSFUL);
          break;
        case TimingEvent.FlowTimings.FLOW_FAILED:
          this.dagManagerMetrics.emitFlowFailedMetrics(flowId);
          this.dagManagerMetrics.conditionallyMarkFlowAsState(flowId, DagManager.FlowState.FAILED);
          break;
        case TimingEvent.FlowTimings.FLOW_CANCELLED:
          this.dagManagerMetrics.emitFlowSlaExceededMetrics(flowId);
          this.dagManagerMetrics.conditionallyMarkFlowAsState(flowId, DagManager.FlowState.FAILED);
          break;
        default:
          log.warn("Unexpected flow event {} for dag {}", dag.getFlowEvent(), dagId);
      }
      log.info("Dag {} has finished with status {}; Cleaning up dag from the state store.", dagId, dag.getFlowEvent());
      cleanUpDag(dagId.toString());
    }
    return null;
  }

  @Override
  protected void sendNotification(Object result) throws MaybeRetryableException {
    long cleanUpProcessingTime = System.currentTimeMillis();
    Dag<JobExecutionPlan> dag = (Dag<JobExecutionPlan>) result;
    String dagId = DagManagerUtils.generateDagId(dag).toString();
    DagManagerUtils.emitFlowEvent(this.eventSubmitter, this.dagManagementStateStore.getDagIdToDags().get(dagId), dag.getFlowEvent());
    dag.setEventEmittedTimeMillis(cleanUpProcessingTime);
  }

  /**
   * Add a dag to failed dag state store
   */
  private synchronized void addFailedDag(String dagId, Dag<JobExecutionPlan> dag) {
    try {
      log.info("Adding dag " + dagId + " to failed dag state store");
      this.failedDagStateStore.writeCheckpoint(this.dagManagementStateStore.getDagIdToDags().get(dagId));
    } catch (IOException e) {
      log.error("Failed to add dag " + dagId + " to failed dag state store", e);
    }
    this.dagManagementStateStore.getFailedDagIds().add(dagId);
  }

  /**
   * Note that removal of a {@link Dag} entry in {@link #dags} needs to be happen after {@link #cleanUp()}
   * since the real {@link Dag} object is required for {@link #cleanUp()},
   * and cleaning of all relevant states need to be atomic
   * @param dagId
   */
  private synchronized void cleanUpDag(String dagId) {
    log.info("Cleaning up dagId {}", dagId);
    // clears flow event after cancelled job to allow resume event status to be set
    this.dagManagementStateStore.getDagIdToDags().get(dagId).setFlowEvent(null);
    try {
      this.dagStateStore.cleanUp(this.dagManagementStateStore.getDagIdToDags().get(dagId));
    } catch (IOException ioe) {
      log.error(String.format("Failed to clean %s from backStore due to:", dagId), ioe);
    }
    this.dagManagementStateStore.getDagIdToDags().remove(dagId);
    this.dagManagementStateStore.getDagToJobs().remove(dagId);
  }
}
