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

package org.apache.gobblin.service.modules.orchestration;

import java.io.IOException;
import java.util.List;

import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;


/**
 * A {@link DagTask} responsible to handle kill tasks.
 */
@WorkInProgress
public class KillDagTask extends DagTask {
  protected DagManager.DagId killDagId;
  public KillDagTask(DagManager.DagId killDagId) {
    this.killDagId = killDagId;
  }
  /**
   * initializes the job properties associated with a {@link DagTask}
   * @param dagNodes
   * @param triggerTimeStamp
   */
  @Override
  void initialize(Object dagNodes, long triggerTimeStamp) {
    List<Dag.DagNode<JobExecutionPlan>> dagNodesToKill = (List<Dag.DagNode<JobExecutionPlan>>) dagNodes;
    this.flowAction = new DagActionStore.DagAction(killDagId.getFlowGroup(), killDagId.getFlowName(),
        killDagId.getFlowExecutionId(), DagActionStore.FlowActionType.KILL);
    for (Dag.DagNode<JobExecutionPlan> dagNodeToKill : dagNodesToKill) {
      //this overrides to the last value in the list of dagNode avoiding multi-hop flows
      //TODO: handle to take in a list of job props for multi-hop flows
      this.jobProps = dagNodeToKill.getValue().getJobSpec().getConfigAsProperties();
    }
    this.triggerTimeStamp = triggerTimeStamp;
  }

  @Override
  void conclude() {
  }

  @Override
  KillDagProc host(DagTaskVisitor visitor) throws IOException {
    return (KillDagProc) visitor.meet(this);
  }
}
