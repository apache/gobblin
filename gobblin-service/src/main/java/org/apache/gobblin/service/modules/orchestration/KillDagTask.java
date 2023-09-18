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

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;


/**
 * A {@link DagTask} responsible to handle kill tasks.
 */
@Alpha
public class KillDagTask extends DagTask {
  protected DagManager.DagId killDagId;
  public KillDagTask(DagManager.DagId killDagId) {
    this.killDagId = killDagId;
  }

  /**
   * initializes the job properties associated with a {@link DagTask}
   * @param dag
   * @param triggerTimeStamp
   */
  @Override
  void initialize(Object dag, long triggerTimeStamp) {
    Dag<JobExecutionPlan> killDag = (Dag<JobExecutionPlan>) dag;
    this.flowAction = new DagActionStore.DagAction(killDagId.getFlowGroup(), killDagId.getFlowName(),
        killDagId.getFlowExecutionId(), DagActionStore.FlowActionType.KILL);
    this.jobProps = killDag.getStartNodes().get(0).getValue().getJobSpec().getConfigAsProperties();
    this.triggerTimeStamp = triggerTimeStamp;
  }

  @Override
  KillDagProc host(DagTaskVisitor visitor) throws IOException {
    return (KillDagProc) visitor.meet(this);
  }
}
