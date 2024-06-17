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
    return dagManagementStateStore.getDagNodeWithJobStatus(this.dagNodeId);
  }

  @Override
  protected void act(DagManagementStateStore dagManagementStateStore, Pair<Optional<Dag.DagNode<JobExecutionPlan>>,
      Optional<JobStatus>> dagNodeWithJobStatus) throws IOException {
    if (!dagNodeWithJobStatus.getLeft().isPresent()) {
      // one of the reason this could arise is when the MALA leasing doesn't work cleanly and another DagProc::process
      // has cleaned up the Dag, yet did not complete the lease before this current one acquired its own
      log.error("DagNode or its job status not found for a Reevaluate DagAction with dag node id {}", this.dagNodeId);
      // todo - add metrics to count such occurrences
      return;
    }

    Dag.DagNode<JobExecutionPlan> dagNode = dagNodeWithJobStatus.getLeft().get();

    if (!dagNodeWithJobStatus.getRight().isPresent()) {
      // Usually reevaluate dag action is created by JobStatusMonitor when a finished job status is available,
      // but when reevaluate/resume/launch dag proc found multiple parallel jobs to run next, it creates reevaluate
      // dag actions for each of those parallel job and in this scenario there is no job status available.
      // If the job status is not present, this job was never launched, submit it now.
      DagProcUtils.submitJobToExecutor(dagManagementStateStore, dagNode, getDagId());
      return;
    }

    Dag<JobExecutionPlan> dag = dagManagementStateStore.getDag(getDagId()).get();
    JobStatus jobStatus = dagNodeWithJobStatus.getRight().get();
    ExecutionStatus executionStatus = ExecutionStatus.valueOf(jobStatus.getEventName());
    // pass dag, so that dag is updated too, updated information will be required in onJobFinish in finding next jobs to submit
    setStatus(dagManagementStateStore, dag, getDagNodeId(), executionStatus);

    if (!FlowStatusGenerator.FINISHED_STATUSES.contains(executionStatus.name())) {
      log.warn("Job status for dagNode {} is {}. Re-evaluate dag action should have been created only for finished status - {}",
          dagNodeId, executionStatus, FlowStatusGenerator.FINISHED_STATUSES);
      // this may happen if adding job status in the store failed after adding a ReevaluateDagAction in KafkaJobStatusMonitor
      throw new RuntimeException(String.format("Job status for dagNode %s is %s. Re-evaluate dag action are created for"
              + " new jobs with no job status when there are multiple of them to run next; or when a job finishes with status - %s",
          dagNodeId, executionStatus, FlowStatusGenerator.FINISHED_STATUSES));
    }

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

      DagProcUtils.removeFlowFinishDeadlineDagAction(dagManagementStateStore, getDagId());
    }
  }

  /**
   * Sets status of a dag node inside the given Dag.
   * todo - DMSS should support this functionality like an atomic get-and-set operation.
   */
  private void setStatus(DagManagementStateStore dagManagementStateStore,
      Dag<JobExecutionPlan> dag, DagNodeId dagNodeId, ExecutionStatus executionStatus) throws IOException {
    for (Dag.DagNode<JobExecutionPlan> node : dag.getNodes()) {
      if (node.getValue().getId().equals(dagNodeId)) {
        node.getValue().setExecutionStatus(executionStatus);
        dagManagementStateStore.addDagNodeState(node, getDagId());
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
        DagProcUtils.submitNextNodes(dagManagementStateStore, dag, getDagId());
        break;
      default:
        log.warn("It should not reach here. Job status {} is unexpected.", executionStatus);
    }

    // Checkpoint the dag state, it should have an updated value of dag fields
    dagManagementStateStore.checkpointDag(dag);
    dagManagementStateStore.deleteDagNodeState(getDagId(), dagNode);
  }
}
