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

import com.google.common.collect.Maps;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagUtils;
import org.apache.gobblin.service.modules.orchestration.TimingEventUtils;
import org.apache.gobblin.service.modules.orchestration.task.DagProcessingEngineMetrics;
import org.apache.gobblin.service.modules.orchestration.task.ResumeDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;

import static org.apache.gobblin.service.ExecutionStatus.CANCELLED;
import static org.apache.gobblin.service.ExecutionStatus.FAILED;
import static org.apache.gobblin.service.ExecutionStatus.PENDING_RESUME;


/**
 * An implementation for {@link DagProc} that resumes a dag and submits the job that previously failed or was killed.
 */
@Slf4j
public class ResumeDagProc extends DagProc<Optional<Dag<JobExecutionPlan>>> {

  public ResumeDagProc(ResumeDagTask resumeDagTask, Config config) {
    super(resumeDagTask, config);
  }

  @Override
  protected Optional<Dag<JobExecutionPlan>> initialize(DagManagementStateStore dagManagementStateStore)
      throws IOException {
      return dagManagementStateStore.getFailedDag(getDagId());
  }

  @Override
  protected void act(DagManagementStateStore dagManagementStateStore, Optional<Dag<JobExecutionPlan>> failedDag,
      DagProcessingEngineMetrics dagProcEngineMetrics) throws IOException {
    log.info("Request to resume dag {}", getDagId());

    if (!failedDag.isPresent()) {
      dagProcEngineMetrics.markDagActionsAct(getDagActionType(), false);
      log.error("Dag " + dagId + " was not found in dag state store");
      return;
    }

    long flowResumeTime = System.currentTimeMillis();

    // Set the flow and its failed or cancelled nodes to PENDING_RESUME so that the flow will be resumed from the point before it failed
    DagProcUtils.setAndEmitFlowEvent(eventSubmitter, failedDag.get(), TimingEvent.FlowTimings.FLOW_PENDING_RESUME);

    for (Dag.DagNode<JobExecutionPlan> node : failedDag.get().getNodes()) {
      ExecutionStatus executionStatus = node.getValue().getExecutionStatus();
      if (executionStatus.equals(FAILED) || executionStatus.equals(CANCELLED)) {
        node.getValue().setExecutionStatus(PENDING_RESUME);
        // reset currentAttempts because we do not want to count previous execution's attempts in deciding whether to retry a job
        node.getValue().setCurrentAttempts(0);
        DagUtils.incrementJobGeneration(node);
        Map<String, String> jobMetadata = TimingEventUtils.getJobMetadata(Maps.newHashMap(), node.getValue());
        eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.JOB_PENDING_RESUME).stop(jobMetadata);
      }
      // Set flowStartTime so that flow start deadline and flow completion deadline will be based on current time instead of original flow
      node.getValue().setFlowStartTime(flowResumeTime);
    }

    // these two statements effectively move the dag from failed dag store to (running) dag store.
    // to prevent loss in the unlikely event of failure between the two, we add first.
    dagManagementStateStore.addDag(failedDag.get());

    // if it fails here, it will check point the failed dag in the (running) dag store again, which is idempotent
    dagManagementStateStore.deleteFailedDag(getDagId());

    DagProcUtils.submitNextNodes(dagManagementStateStore, failedDag.get(), getDagId());

    DagProcUtils.sendEnforceFlowFinishDeadlineDagAction(dagManagementStateStore, getDagTask().getDagAction());
    dagProcEngineMetrics.markDagActionsAct(getDagActionType(), true);
  }
}
