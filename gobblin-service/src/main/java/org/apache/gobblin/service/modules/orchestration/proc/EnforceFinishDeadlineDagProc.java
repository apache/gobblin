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

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagManagerUtils;
import org.apache.gobblin.service.modules.orchestration.task.EnforceFinishDeadlineDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;


/**
 * An implementation for {@link DagProc} that kills all the jobs if the dag does not finish in
 * {@link org.apache.gobblin.configuration.ConfigurationKeys#GOBBLIN_FLOW_SLA_TIME} time.
 */
@Slf4j
public class EnforceFinishDeadlineDagProc extends DagProc<Optional<Dag<JobExecutionPlan>>> {

  public EnforceFinishDeadlineDagProc(EnforceFinishDeadlineDagTask enforceFinishDeadlineDagTask) {
    super(enforceFinishDeadlineDagTask);
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

    enforceFinishDeadline(dagManagementStateStore, dag);
  }

  private void enforceFinishDeadline(DagManagementStateStore dagManagementStateStore, Optional<Dag<JobExecutionPlan>> dag)
      throws IOException {
    Dag.DagNode<JobExecutionPlan> dagNode = dag.get().getNodes().get(0);
    long flowSla = DagManagerUtils.getFlowSLA(dagNode);
    long flowStartTime = DagManagerUtils.getFlowStartTime(dagNode);

    if (System.currentTimeMillis() > flowStartTime + flowSla) {
      log.info("Dag {} exceeded the SLA of {} ms. Killing it now...", getDagId(), flowSla);
      //dagManagementStateStore.getDagManagerMetrics().incrementExecutorSlaExceeded(node);
      List<Dag.DagNode<JobExecutionPlan>> dagNodesToCancel = dag.get().getNodes();
      log.info("Found {} DagNodes to cancel (DagId {}).", dagNodesToCancel.size(), getDagId());

      for (Dag.DagNode<JobExecutionPlan> dagNodeToCancel : dagNodesToCancel) {
        DagProcUtils.cancelDagNode(dagNodeToCancel, dagManagementStateStore);
      }

      dag.get().setFlowEvent(TimingEvent.FlowTimings.FLOW_RUN_DEADLINE_EXCEEDED);
      dag.get().setMessage("Flow killed due to exceeding SLA of " + flowSla + " ms");
      dagManagementStateStore.checkpointDag(dag.get());
    }
  }
}
