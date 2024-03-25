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

import com.codahale.metrics.Timer;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.DagActionStore;
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
 * An implementation for {@link DagProc} that launches a new job if there exists a job whose pre-requisite jobs are
 * completed successfully. If there are no more jobs to run and no job is running for the Dag, it cleans up the Dag.
 * (In future), if there are multiple new jobs to be launched, separate launch dag actions are created for each of them.
 */
@Slf4j
public class ReevaluateDagProc extends DagProc<Optional<Dag.DagNode<JobExecutionPlan>>, Void> {
  private final Timer jobStatusPolledTimer;
  private final DagNodeId dagNodeId;
  private Optional<JobStatus> jobStatus;

  public ReevaluateDagProc(ReevaluateDagTask reEvaluateDagTask) {
    super(reEvaluateDagTask);
    this.jobStatusPolledTimer = metricContext.timer(ServiceMetricNames.JOB_STATUS_POLLED_TIMER);
    this.dagNodeId = getDagNodeId();
  }

  @Override
  protected Optional<Dag.DagNode<JobExecutionPlan>> initialize(DagManagementStateStore dagManagementStateStore)
      throws IOException {
    Optional<Dag.DagNode<JobExecutionPlan>> dagNode = dagManagementStateStore.getDagNode(this.dagNodeId);
    if (!dagNode.isPresent()) {
      log.error("DagNode not found for a Reevaluate DagAction with dag node id {}", this.dagNodeId);
      return Optional.empty();
    }

    this.jobStatus = dagManagementStateStore.getJobStatus(getDagNodeId().getFlowGroup(), getDagNodeId().getFlowName(),
        getDagNodeId().getFlowExecutionId(), getDagNodeId().getJobGroup(), getDagNodeId().getJobName());
    if (!this.jobStatus.isPresent()) {
      log.error("JobStatus not found for the dag node {}", getDagNodeId());
      return Optional.empty();
    }

    ExecutionStatus executionStatus = ExecutionStatus.valueOf(jobStatus.get().getEventName());
    if (!FlowStatusGenerator.FINISHED_STATUSES.contains(executionStatus.name())) {
      log.warn("Job status for dagNode {} is {}. Re-evaluate dag action should have been created only for finished status - {}",
          dagNodeId, executionStatus, FlowStatusGenerator.FINISHED_STATUSES);
      return Optional.empty();
    }

    setStatus(dagManagementStateStore, dagNode.get(), executionStatus);
    return dagNode;
  }

  @Override
  protected Void act(DagManagementStateStore dagManagementStateStore, Optional<Dag.DagNode<JobExecutionPlan>> dagNode)
      throws IOException {
    if (!dagNode.isPresent()) {
      // for several reasons it may receive an empty dag node, refer initialize method
      return null;
    }

    ExecutionStatus executionStatus = dagNode.get().getValue().getExecutionStatus();
    onJobFinish(dagManagementStateStore, dagNode.get(), executionStatus);
    dagManagementStateStore.deleteDagNodeState(getDagId(), dagNode.get());

    Dag<JobExecutionPlan> dag = dagManagementStateStore.getDag(getDagId()).get();

    if (this.jobStatus.get().isShouldRetry()) {
      log.info("Retrying job: {}, current attempts: {}, max attempts: {}",
          DagManagerUtils.getFullyQualifiedJobName(dagNode.get()),
          jobStatus.get().getCurrentAttempts(), jobStatus.get().getMaxAttempts());
      dag.setFlowEvent(null);
      DagProcUtils.submitJobToExecutor(dagManagementStateStore, dagNode.get(), getDagId());
    }

    if (!DagProcUtils.hasRunningJobs(getDagId(), dagManagementStateStore)) {
      if (dag.getFlowEvent() == null) {
        // If the dag flow event is not set, then it is successful
        dag.setFlowEvent(TimingEvent.FlowTimings.FLOW_SUCCEEDED);
        // send an event before cleaning up dag
        DagManagerUtils.emitFlowEvent(eventSubmitter, dag, dag.getFlowEvent());
        // todo - verify if work from PR#3641 is required
        dagManagementStateStore.deleteDag(getDagId());
      } else {
        DagManagerUtils.emitFlowEvent(eventSubmitter, dag, dag.getFlowEvent());
        dagManagementStateStore.markDagFailed(dag);
      }
    }

    return null;
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
  private void onJobFinish(DagManagementStateStore dagManagementStateStore,
      Dag.DagNode<JobExecutionPlan> dagNode, ExecutionStatus executionStatus)
      throws IOException {
    String jobName = DagManagerUtils.getFullyQualifiedJobName(dagNode);
    log.info("Job {} of Dag {} has finished with status {}", jobName, getDagId(), executionStatus.name());
    // Only decrement counters and quota for jobs that actually ran on the executor, not from a GaaS side failure/skip event
    if (dagManagementStateStore.releaseQuota(dagNode)) {
      dagManagementStateStore.getDagManagerMetrics().decrementRunningJobMetrics(dagNode);
    }

    Dag<JobExecutionPlan> dag = dagManagementStateStore.getDag(getDagId()).get();

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
        submitNext(dagManagementStateStore);
        break;
      default:
        log.warn("It should not reach here. Job status {} is unexpected.", executionStatus);
    }

    //Checkpoint the dag state, it should have an updated value of dag fields
    dagManagementStateStore.checkpointDag(dag);
  }

  /**
   * Submit next set of Dag nodes in the Dag identified by the provided dagId
   */
  void submitNext(DagManagementStateStore dagManagementStateStore) throws IOException {
    // get the most up-to-date dag from the store before finding the next dag nodes to run
    Dag<JobExecutionPlan> dag = dagManagementStateStore.getDag(getDagId()).get();
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

  private DagNodeId findDagNodeForDagAction(DagActionStore.DagAction dagAction) {
    return new DagNodeId(dagAction.getFlowGroup(), dagAction.getFlowName(), Long.parseLong(dagAction.getFlowExecutionId()),
        dagAction.getFlowGroup(), dagAction.getJobName());
  }

  private void handleMultipleJobs(Set<Dag.DagNode<JobExecutionPlan>> nextNodes) {
    throw new UnsupportedOperationException("More than one start job is not allowed");
  }
}
