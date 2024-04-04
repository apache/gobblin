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

package org.apache.gobblin.service.modules.orchestration.proc;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.flowgraph.DagNodeId;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagManagerUtils;
import org.apache.gobblin.service.modules.orchestration.task.ReevaluateDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.FlowStatusGenerator;
import org.apache.gobblin.service.monitoring.JobStatus;


/**
 * suggest:
 * A {@link DagProc} to launch any subsequent (dependent) job(s) once all pre-requisite job(s) in the Dag have succeeded.
 * When there are no more jobs to run and no more running, it cleans up the Dag.
 * (In future), if there are multiple new jobs to be launched, separate launch dag actions are created for each of them.
 */
@Slf4j
public class ReevaluateDagProc extends DagProc<Pair<Optional<Dag.DagNode<JobExecutionPlan>>, Optional<JobStatus>>> {

  public ReevaluateDagProc(ReevaluateDagTask reEvaluateDagTask) {
    super(reEvaluateDagTask);
  }

  @Override
  protected Pair<Optional<Dag.DagNode<JobExecutionPlan>>, Optional<JobStatus>> initialize(DagManagementStateStore dagManagementStateStore)
      throws IOException {
    Pair<Optional<Dag.DagNode<JobExecutionPlan>>, Optional<JobStatus>> dagNodeWithJobStatus =
        dagManagementStateStore.getDagNodeWithJobStatus(this.dagNodeId);

    if (!dagNodeWithJobStatus.getLeft().isPresent() || !dagNodeWithJobStatus.getRight().isPresent()) {
      // this is possible when MALA malfunctions and a duplicated reevaluate dag proc is launched for a dag node that is
      // already "reevaluated" and cleaned up.
      return ImmutablePair.of(Optional.empty(), Optional.empty());
    }

    ExecutionStatus executionStatus = ExecutionStatus.valueOf(dagNodeWithJobStatus.getRight().get().getEventName());
    if (!FlowStatusGenerator.FINISHED_STATUSES.contains(executionStatus.name())) {
      log.warn("Job status for dagNode {} is {}. Re-evaluate dag action should have been created only for finished status - {}",
          dagNodeId, executionStatus, FlowStatusGenerator.FINISHED_STATUSES);
      // this may happen if adding job status in the store failed after adding a ReevaluateDagAction in KafkaJobStatusMonitor
      throw new RuntimeException(String.format("Job status %s is not final for job %s", executionStatus, getDagId()));
    }

    setStatus(dagManagementStateStore, dagNodeWithJobStatus.getLeft().get(), executionStatus);
    return dagNodeWithJobStatus;
  }

  @Override
  protected void act(DagManagementStateStore dagManagementStateStore, Pair<Optional<Dag.DagNode<JobExecutionPlan>>, Optional<JobStatus>> dagNodeWithJobStatus)
      throws IOException {
    if (!dagNodeWithJobStatus.getLeft().isPresent()) {
      // one of the reason this could arise is when the MALA leasing doesn't work cleanly and another DagProc::process
      // has cleaned up the Dag, yet did not complete the lease before this current one acquired its own
      log.error("DagNode or its job status not found for a Reevaluate DagAction with dag node id {}", this.dagNodeId);
      // todo - add metrics to count such occurrences
      return;
    }

    Dag.DagNode<JobExecutionPlan> dagNode = dagNodeWithJobStatus.getLeft().get();
    JobStatus jobStatus = dagNodeWithJobStatus.getRight().get();
    ExecutionStatus executionStatus = dagNode.getValue().getExecutionStatus();
    Dag<JobExecutionPlan> dag = dagManagementStateStore.getDag(getDagId()).get();
    onJobFinish(dagManagementStateStore, dagNode, executionStatus, dag);

    if (jobStatus.isShouldRetry()) {
      log.info("Retrying job: {}, current attempts: {}, max attempts: {}",
          DagManagerUtils.getFullyQualifiedJobName(dagNode), jobStatus.getCurrentAttempts(), jobStatus.getMaxAttempts());
      // todo - be careful when unsetting this, it is possible that this is set to FAILED because some other job in the
      // dag failed and is also not retryable. in that case if this job's retry passes, overall status of the dag can be
      // set to PASS, which would be incorrect.
      dag.setFlowEvent(null);
      DagProcUtils.submitJobToExecutor(dagManagementStateStore, dagNode, getDagId());
    } else if (!dagManagementStateStore.hasRunningJobs(getDagId())) {
      if (dag.getFlowEvent() == null) {
        // If the dag flow event is not set and there are no more jobs running, then it is successful
        // also note that `onJobFinish` method does whatever is required to do after job finish, determining a Dag's
        // status is not possible on individual job's finish status
        dag.setFlowEvent(TimingEvent.FlowTimings.FLOW_SUCCEEDED);
      }
      String flowEvent = dag.getFlowEvent();
      DagManagerUtils.emitFlowEvent(eventSubmitter, dag, flowEvent);
      if (flowEvent.equals(TimingEvent.FlowTimings.FLOW_SUCCEEDED)) {
        // todo - verify if work from PR#3641 is required
        dagManagementStateStore.deleteDag(getDagId());
      } else {
        dagManagementStateStore.markDagFailed(dag);
      }
    }
  }

  /**
   * Sets status of a dag node inside the given Dag.
   * todo - DMSS should support this functionality like an atomic get-and-set operation.
   */
  private void setStatus(DagManagementStateStore dagManagementStateStore,
      Dag.DagNode<JobExecutionPlan> dagNode, ExecutionStatus executionStatus) throws IOException {
    Dag<JobExecutionPlan> dag = dagManagementStateStore.getDag(getDagId()).get();
    DagNodeId dagNodeId = dagNode.getValue().getId();
    for (Dag.DagNode<JobExecutionPlan> node : dag.getNodes()) {
      if (node.getValue().getId().equals(dagNodeId)) {
        node.getValue().setExecutionStatus(executionStatus);
        dagManagementStateStore.checkpointDag(dag);
        return;
      }
    }
    log.error("DagNode with id {} not found in Dag {}", dagNodeId, getDagId());
  }

  /**
   * Method that defines the actions to be performed when a job finishes either successfully or with failure.
   * This method updates the state of the dag and performs clean up actions as necessary.
   */
  private void onJobFinish(DagManagementStateStore dagManagementStateStore, Dag.DagNode<JobExecutionPlan> dagNode,
      ExecutionStatus executionStatus, Dag<JobExecutionPlan> dag) throws IOException {
    String jobName = DagManagerUtils.getFullyQualifiedJobName(dagNode);
    log.info("Job {} of Dag {} has finished with status {}", jobName, getDagId(), executionStatus.name());
    // Only decrement counters and quota for jobs that actually ran on the executor, not from a GaaS side failure/skip event
    if (dagManagementStateStore.releaseQuota(dagNode)) {
      dagManagementStateStore.getDagManagerMetrics().decrementRunningJobMetrics(dagNode);
    }

    switch (executionStatus) {
      case FAILED:
        dag.setMessage("Flow failed because job " + jobName + " failed");
        dag.setFlowEvent(TimingEvent.FlowTimings.FLOW_FAILED);
        dagManagementStateStore.getDagManagerMetrics().incrementExecutorFailed(dagNode);
        break;
      case CANCELLED:
        dag.setFlowEvent(TimingEvent.FlowTimings.FLOW_CANCELLED);
        break;
      case COMPLETE:
        dagManagementStateStore.getDagManagerMetrics().incrementExecutorSuccess(dagNode);
        submitNextNodes(dagManagementStateStore, dag);
        break;
      default:
        log.warn("It should not reach here. Job status {} is unexpected.", executionStatus);
    }

    //Checkpoint the dag state, it should have an updated value of dag fields
    dagManagementStateStore.checkpointDag(dag);
    dagManagementStateStore.deleteDagNodeState(getDagId(), dagNode);
  }

  /**
   * Submit next set of Dag nodes in the Dag identified by the provided dagId
   */
  private void submitNextNodes(DagManagementStateStore dagManagementStateStore, Dag<JobExecutionPlan> dag) {
    Set<Dag.DagNode<JobExecutionPlan>> nextNodes = DagManagerUtils.getNext(dag);

    if (nextNodes.size() > 1) {
      handleMultipleJobs(nextNodes);
    }

    if (!nextNodes.isEmpty()) {
      Dag.DagNode<JobExecutionPlan> nextNode = nextNodes.stream().findFirst().get();
      DagProcUtils.submitJobToExecutor(dagManagementStateStore, nextNode, getDagId());
      log.info("Submitted job {} for dagId {}", DagManagerUtils.getJobName(nextNode), getDagId());
    }
  }

  private void handleMultipleJobs(Set<Dag.DagNode<JobExecutionPlan>> nextNodes) {
    throw new UnsupportedOperationException("More than one start job is not allowed");
  }
}
