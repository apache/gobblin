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
import java.util.List;
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
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.service.modules.orchestration.DagManagerUtils;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.FlowStatusGenerator;
import org.apache.gobblin.service.monitoring.JobStatus;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;


/**
 * An implementation for {@link DagProc} that launches a new job.
 */
@Slf4j
public class ReEvaluateDagProc extends LaunchDagProc {
  private final JobStatusRetriever jobStatusRetriever;
  private final Timer jobStatusPolledTimer;

  public ReEvaluateDagProc(DagTask reEvaluateDagTask, JobStatusRetriever jobStatusRetriever) {
    // ReEvaluateDagProc would not require flowCompilationValidationHelper so setting it null
    super(reEvaluateDagTask, null);
    this.jobStatusRetriever = jobStatusRetriever;
    this.jobStatusPolledTimer = metricContext.timer(ServiceMetricNames.JOB_STATUS_POLLED_TIMER);
  }

  @Override
  protected Optional<Dag<JobExecutionPlan>> initialize(DagManagementStateStore dagManagementStateStore)
      throws IOException {
    return dagManagementStateStore.getDag(getDagId());
  }

  @Override
  protected Optional<Dag<JobExecutionPlan>> act(DagManagementStateStore dagManagementStateStore, Optional<Dag<JobExecutionPlan>> dag)
      throws IOException {

    Dag.DagNode<JobExecutionPlan> dagNode = dagManagementStateStore.getDagNode(findDagNodeForDagAction(dagTask.getDagAction())).get();
    DagNodeId dagNodeId = new DagNodeId(dagTask.getDagAction().getFlowGroup(), dagTask.getDagAction().getFlowName(),
        Long.parseLong(dagTask.getDagAction().getFlowExecutionId()), dagTask.getDagAction().getFlowGroup(), "jobName");
    JobStatus jobStatus = DagManagerUtils.pollJobStatus(dagNode, this.jobStatusRetriever, this.jobStatusPolledTimer).get();
    ExecutionStatus executionStatus = ExecutionStatus.valueOf(jobStatus.getEventName());
    JobExecutionPlan jobExecutionPlan = DagManagerUtils.getJobExecutionPlan(dagNode);

    switch (executionStatus) {
      case COMPLETE:
      case FAILED:
      case CANCELLED:
        jobExecutionPlan.setExecutionStatus(executionStatus);
        onJobFinish(dagManagementStateStore, dagNode, executionStatus);
        dagManagementStateStore.deleteDagNodeState(getDagId(), dagNode);  // delete after submitting next jobs! otherwise may not find next jobs to run
        break;
      default:
        log.warn("Job status for dagNode " + dagNodeId + " is " + executionStatus + ". Expected Statuses are " + FlowStatusGenerator.FINISHED_STATUSES);
        break;
    }

    if (jobStatus.isShouldRetry()) {
      log.info("Retrying job: {}, current attempts: {}, max attempts: {}", DagManagerUtils.getFullyQualifiedJobName(dagNode),
          jobStatus.getCurrentAttempts(), jobStatus.getMaxAttempts());
      dag.get().setFlowEvent(null);
      DagProcUtils.submitJobToExecutor(dagManagementStateStore, dagNode);
    }

    if (!hasRunningJobs(getDagId(), dagManagementStateStore)) {
      // Collect all the dagIds that are finished
      dagManagementStateStore.deleteDag(getDagId());
      if (dag.get().getFlowEvent() == null) {
        // If the dag flow event is not set, then it is successful
        dag.get().setFlowEvent(TimingEvent.FlowTimings.FLOW_SUCCEEDED);
      } else {
        dagManagementStateStore.markDagFailed(dag.get());
      }
      // send an event before cleaning up dag
      DagManagerUtils.emitFlowEvent(eventSubmitter, dag.get(), dag.get().getFlowEvent());
      //dag.get().setEventEmittedTimeMillis(cleanUpProcessingTime);  // how to handle the case when event is not emitted and dag is cleaned
    }

    return dag;
  }

  public static boolean hasRunningJobs(DagManager.DagId dagId, DagManagementStateStore dagManagementStateStore)
      throws IOException {
    List<Dag.DagNode<JobExecutionPlan>> dagNodes = dagManagementStateStore.getDagNodes(dagId);
    return dagNodes != null && !dagNodes.isEmpty();
  }

  /**
   * Method that defines the actions to be performed when a job finishes either successfully or with failure.
   * This method updates the state of the dag and performs clean up actions as necessary.
   */
  private void onJobFinish(DagManagementStateStore dagManagementStateStore,
      Dag.DagNode<JobExecutionPlan> dagNode, ExecutionStatus executionStatus)
      throws IOException {
    //todo - do addJobState(dagId, dagNode); for all jobs,
    Dag<JobExecutionPlan> dag = dagManagementStateStore.getParentDag(dagNode).get();
    String dagId = DagManagerUtils.generateDagId(dag).toString();
    String jobName = DagManagerUtils.getFullyQualifiedJobName(dagNode);
    log.info("Job {} of Dag {} has finished with status {}", jobName, dagId, executionStatus.name());
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
        submitNext(dagManagementStateStore, dag);
        break;
      default:
        log.warn("It should not reach here. Job status is unexpected.");
    }
  }


  /**
   * Submit next set of Dag nodes in the Dag identified by the provided dagId
   */
  synchronized void submitNext(
      DagManagementStateStore dagManagementStateStore, Dag<JobExecutionPlan> dag) throws IOException {
    Set<Dag.DagNode<JobExecutionPlan>> nextNodes = DagManagerUtils.getNext(dag);

    if (nextNodes.size() > 1) {
      handleMultipleJobs(nextNodes);
    }

    if (nextNodes.isEmpty()) {
      dagManagementStateStore.deleteDagNodeState(getDagId(), dag.getNodes().get(0));
      return;
    } else {
      Dag.DagNode<JobExecutionPlan> dagNode = nextNodes.stream().findFirst().get();
      DagProcUtils.submitJobToExecutor(dagManagementStateStore, dagNode);
      dagManagementStateStore.addDagNodeState(dagNode, getDagId());
      log.info("Submitted job {} for dagId {}", DagManagerUtils.getJobName(dagNode), getDagId());
    }

    //Checkpoint the dag state, it should have an updated value of dag nodes
    dagManagementStateStore.checkpointDag(dag);
  }

  private DagNodeId findDagNodeForDagAction(DagActionStore.DagAction dagAction) {
    return new DagNodeId(dagAction.getFlowGroup(), dagAction.getFlowName(), Long.parseLong(dagAction.getFlowExecutionId()), dagAction.getFlowGroup(), "jobName");
  }

  private void handleMultipleJobs(Set<Dag.DagNode<JobExecutionPlan>> nextNodes) {
     throw new UnsupportedOperationException("More than one start job is not allowed");
  }
}
