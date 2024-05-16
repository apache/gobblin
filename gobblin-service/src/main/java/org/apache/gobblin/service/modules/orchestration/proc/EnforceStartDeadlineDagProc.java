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
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagManagerUtils;
import org.apache.gobblin.service.modules.orchestration.task.EnforceStartDeadlineDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;

import static org.apache.gobblin.service.ExecutionStatus.ORCHESTRATED;
import static org.apache.gobblin.service.ExecutionStatus.valueOf;


/**
 * An implementation for {@link DagProc} that marks the {@link Dag} as failed and cancel the job if it does not start in
 * {@link org.apache.gobblin.service.modules.orchestration.DagManager#JOB_START_SLA_TIME} time.
 */
@Slf4j
public class EnforceStartDeadlineDagProc extends DagProc<Optional<Dag<JobExecutionPlan>>> {

  public EnforceStartDeadlineDagProc(EnforceStartDeadlineDagTask enforceStartDeadlineDagTask) {
    super(enforceStartDeadlineDagTask);
  }

  @Override
  protected Optional<Dag<JobExecutionPlan>> initialize(DagManagementStateStore dagManagementStateStore)
      throws IOException {
   return dagManagementStateStore.getDag(getDagId());
  }

  @Override
  protected void act(DagManagementStateStore dagManagementStateStore, Optional<Dag<JobExecutionPlan>> dag)
      throws IOException {
    log.info("Request to enforce deadlines for dag {}", getDagId());

    if (!dag.isPresent()) {
      // todo - add a metric here
      log.error("Did not find Dag with id {}, it might be already cancelled/finished and thus cleaned up from the store.",
          getDagId());
      return;
    }

    enforceStartDeadline(dagManagementStateStore, dag);
  }

  private void enforceStartDeadline(DagManagementStateStore dagManagementStateStore, Optional<Dag<JobExecutionPlan>> dag)
      throws IOException {
    Pair<Optional<Dag.DagNode<JobExecutionPlan>>, Optional<org.apache.gobblin.service.monitoring.JobStatus>>
        dagNodeToCheckDeadline = dagManagementStateStore.getDagNodeWithJobStatus(getDagNodeId());
    if (!dagNodeToCheckDeadline.getLeft().isPresent()) {
      // this should never happen; a job for which DEADLINE_ENFORCEMENT dag action is created must have a dag node in store
      return;
    }

    Dag.DagNode<JobExecutionPlan> dagNode = dagNodeToCheckDeadline.getLeft().get();
    long timeOutForJobStart = DagManagerUtils.getJobStartSla(dagNode, 1000000L);//, this.defaultJobStartSlaTimeMillis);
    Optional<org.apache.gobblin.service.monitoring.JobStatus> jobStatus = dagNodeToCheckDeadline.getRight();
    if (!jobStatus.isPresent()) {
      return;
    }

    ExecutionStatus executionStatus = valueOf(jobStatus.get().getEventName());
    long jobOrchestratedTime = jobStatus.get().getOrchestratedTime();
    // note that second condition should be true because that's how the triggered dag action reached here
    if (executionStatus == ORCHESTRATED && System.currentTimeMillis() > jobOrchestratedTime + timeOutForJobStart) {
      log.info("Job {} of flow {} exceeded the job start SLA of {} ms. Killing the job now...",
          DagManagerUtils.getJobName(dagNode), DagManagerUtils.getFullyQualifiedDagName(dag.get()), timeOutForJobStart);
      dagManagementStateStore.getDagManagerMetrics().incrementCountsStartSlaExceeded(dagNode);
      DagProcUtils.cancelDagNode(dagNode, dagManagementStateStore);
      dag.get().setFlowEvent(TimingEvent.FlowTimings.FLOW_START_DEADLINE_EXCEEDED);
      dag.get().setMessage("Flow killed because no update received for " + timeOutForJobStart + " ms after orchestration");
      dagManagementStateStore.checkpointDag(dag.get());
    }
  }
}
