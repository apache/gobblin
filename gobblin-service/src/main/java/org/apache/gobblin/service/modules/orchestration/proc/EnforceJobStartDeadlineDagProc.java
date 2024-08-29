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

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagUtils;
import org.apache.gobblin.service.modules.orchestration.DagProcessingEngine;
import org.apache.gobblin.service.modules.orchestration.task.DagProcessingEngineMetrics;
import org.apache.gobblin.service.modules.orchestration.task.EnforceJobStartDeadlineDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;

import static org.apache.gobblin.service.ExecutionStatus.ORCHESTRATED;
import static org.apache.gobblin.service.ExecutionStatus.valueOf;


/**
 * An implementation for {@link DagProc} that marks the {@link Dag} as failed and cancel the job if it does not start in
 * {@link org.apache.gobblin.service.ServiceConfigKeys#JOB_START_SLA_TIME} time.
 */
@Slf4j
public class EnforceJobStartDeadlineDagProc extends DeadlineEnforcementDagProc {

  public EnforceJobStartDeadlineDagProc(EnforceJobStartDeadlineDagTask enforceJobStartDeadlineDagTask, Config config) {
    super(enforceJobStartDeadlineDagTask, config);
  }

  protected void enforceDeadline(DagManagementStateStore dagManagementStateStore, Dag<JobExecutionPlan> dag,
      DagProcessingEngineMetrics dagProcEngineMetrics) throws IOException {
    Pair<Optional<Dag.DagNode<JobExecutionPlan>>, Optional<org.apache.gobblin.service.monitoring.JobStatus>>
        dagNodeToCheckDeadline = dagManagementStateStore.getDagNodeWithJobStatus(getDagNodeId());
    if (!dagNodeToCheckDeadline.getLeft().isPresent()) {
      // this should never happen; a job for which DEADLINE_ENFORCEMENT dag action is created must have a dag node in store
      dagProcEngineMetrics.markDagActionsAct(getDagActionType(), false);
      log.error("Dag node {} not found for EnforceJobStartDeadlineDagProc", getDagNodeId());
      return;
    }

    Dag.DagNode<JobExecutionPlan> dagNode = dagNodeToCheckDeadline.getLeft().get();
    long timeOutForJobStart = DagUtils.getJobStartDeadline(dagNode, DagProcessingEngine.getDefaultJobStartDeadlineTimeMillis());
    Optional<org.apache.gobblin.service.monitoring.JobStatus> jobStatus = dagNodeToCheckDeadline.getRight();
    if (!jobStatus.isPresent()) {
      dagProcEngineMetrics.markDagActionsAct(getDagActionType(), false);
      log.error("Some job status should be present for dag node {} that this EnforceJobStartDeadlineDagProc belongs.", getDagNodeId());
      return;
    }

    ExecutionStatus executionStatus = valueOf(jobStatus.get().getEventName());
    long jobOrchestratedTime = jobStatus.get().getOrchestratedTime();
    // note that second condition should be true because the triggered dag action has waited enough before reaching here
    if (executionStatus == ORCHESTRATED && System.currentTimeMillis() > jobOrchestratedTime + timeOutForJobStart) {
      log.info("Job exceeded the job start deadline. Killing it now. Job - {}, jobOrchestratedTime - {}, timeOutForJobStart - {}",
          DagUtils.getJobName(dagNode), jobOrchestratedTime, timeOutForJobStart);
      dagManagementStateStore.getDagManagerMetrics().incrementCountsStartSlaExceeded(dagNode);
      DagProcUtils.cancelDagNode(dagNode, dagManagementStateStore);
      dag.setFlowEvent(TimingEvent.FlowTimings.FLOW_START_DEADLINE_EXCEEDED);
      dag.setMessage("Flow killed because no update received for " + timeOutForJobStart + " ms after orchestration");
    }
    dagProcEngineMetrics.markDagActionsAct(getDagActionType(), true);
  }
}
