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

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.task.DagProcessingEngineMetrics;
import org.apache.gobblin.service.modules.orchestration.task.KillDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;


/**
 * An implementation for {@link DagProc} that kills all the nodes of a dag.
 * If the dag action has job name set, then it kills only that particular job/dagNode.
 */
@Slf4j
public class KillDagProc extends DagProc<Optional<Dag<JobExecutionPlan>>> {
  private final boolean shouldKillSpecificJob;

  public KillDagProc(KillDagTask killDagTask, Config config) {
    super(killDagTask, config);
    this.shouldKillSpecificJob = !getDagNodeId().getJobName().equals(DagActionStore.NO_JOB_NAME_DEFAULT);
  }

  @Override
  protected Optional<Dag<JobExecutionPlan>> initialize(DagManagementStateStore dagManagementStateStore)
      throws IOException {
      return dagManagementStateStore.getDag(getDagId());
  }

  @Override
  protected void act(DagManagementStateStore dagManagementStateStore, Optional<Dag<JobExecutionPlan>> dag,
      DagProcessingEngineMetrics dagProcEngineMetrics) throws IOException {
    log.info("Request to kill dag {} (node: {})", getDagId(), shouldKillSpecificJob ? getDagNodeId() : "<<all>>");

    if (!dag.isPresent()) {
      dagProcEngineMetrics.markDagActionsAct(getDagActionType(), false);
      log.error("Did not find Dag with id {}, it might be already cancelled/finished and thus cleaned up from the store.", getDagId());
      return;
    }

    dag.get().setMessage("Flow killed by request");
    DagProcUtils.setAndEmitFlowEvent(DagProc.eventSubmitter, dag.get(), TimingEvent.FlowTimings.FLOW_CANCELLED);

    if (this.shouldKillSpecificJob) {
      Optional<Dag.DagNode<JobExecutionPlan>> dagNodeToCancel = dagManagementStateStore.getDagNodeWithJobStatus(this.dagNodeId).getLeft();
      if (dagNodeToCancel.isPresent()) {
        DagProcUtils.cancelDagNode(dagNodeToCancel.get());
      } else {
        dagProcEngineMetrics.markDagActionsAct(getDagActionType(), false);
        log.error("Did not find Dag node with id {}, it might be already cancelled/finished and thus cleaned up from the store.", getDagNodeId());
      }
    } else {
      DagProcUtils.cancelDag(dag.get());
    }
    dagProcEngineMetrics.markDagActionsAct(getDagActionType(), true);
  }
}
