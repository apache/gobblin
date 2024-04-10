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
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagManagerUtils;
import org.apache.gobblin.service.modules.orchestration.TimingEventUtils;
import org.apache.gobblin.service.modules.orchestration.task.ResumeDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;

import static org.apache.gobblin.service.ExecutionStatus.CANCELLED;
import static org.apache.gobblin.service.ExecutionStatus.FAILED;
import static org.apache.gobblin.service.ExecutionStatus.PENDING_RESUME;


/**
 * An implementation for {@link DagProc} that resumes a dag and submits the job that failed/killed previously.
 */
@Slf4j
public class ResumeDagProc extends DagProc<Optional<Dag<JobExecutionPlan>>> {

  public ResumeDagProc(ResumeDagTask resumeDagTask) {
    super(resumeDagTask);
  }

  @Override
  protected Optional<Dag<JobExecutionPlan>> initialize(DagManagementStateStore dagManagementStateStore)
      throws IOException {
   return dagManagementStateStore.getFailedDag(getDagId());
  }

  @Override
  protected void act(DagManagementStateStore dagManagementStateStore, Optional<Dag<JobExecutionPlan>> dag)
      throws IOException {
    log.info("Request to resume dag {}", getDagId());

    if (!dag.isPresent()) {
      // todo - add a metric here
      log.error("Dag " + dagId + " was not found in dag state store");
      return;
    }

    long flowResumeTime = System.currentTimeMillis();

    // Set the flow and its failed or cancelled nodes to PENDING_RESUME so that the flow will be resumed from the point before it failed
    DagManagerUtils.emitFlowEvent(eventSubmitter, dag.get(), TimingEvent.FlowTimings.FLOW_PENDING_RESUME);

    for (Dag.DagNode<JobExecutionPlan> node : dag.get().getNodes()) {
      ExecutionStatus executionStatus = node.getValue().getExecutionStatus();
      if (executionStatus.equals(FAILED) || executionStatus.equals(CANCELLED)) {
        node.getValue().setExecutionStatus(PENDING_RESUME);
        // reset currentAttempts because we do not want to count previous execution's attempts in deciding whether to retry a job
        node.getValue().setCurrentAttempts(0);
        DagManagerUtils.incrementJobGeneration(node);
        Map<String, String> jobMetadata = TimingEventUtils.getJobMetadata(Maps.newHashMap(), node.getValue());
        eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.JOB_PENDING_RESUME).stop(jobMetadata);
      }
      // Set flowStartTime so that flow SLA will be based on current time instead of original flow
      node.getValue().setFlowStartTime(flowResumeTime);
    }

    dagManagementStateStore.checkpointDag(dag.get());
    dagManagementStateStore.deleteFailedDag(dag.get());
    resumeDag(dagManagementStateStore, dag.get());
  }

  private void resumeDag(DagManagementStateStore dagManagementStateStore, Dag<JobExecutionPlan> dag) {
    Set<Dag.DagNode<JobExecutionPlan>> nextNodes = DagManagerUtils.getNext(dag);

    if (nextNodes.size() > 1) {
      handleMultipleJobs(nextNodes);
    }

    //Submit jobs from the dag ready for execution.
    for (Dag.DagNode<JobExecutionPlan> dagNode : nextNodes) {
      DagProcUtils.submitJobToExecutor(dagManagementStateStore, dagNode, getDagId());
      log.info("Submitted job {} for dagId {}", DagManagerUtils.getJobName(dagNode), getDagId());
    }
  }

  private void handleMultipleJobs(Set<Dag.DagNode<JobExecutionPlan>> nextNodes) {
    throw new UnsupportedOperationException("More than one start job is not allowed");
  }
}
