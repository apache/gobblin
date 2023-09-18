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
 * A {@link DagTask} responsible to handle resume tasks.
 */
@Alpha
public class ResumeDagTask extends DagTask {
  protected DagManager.DagId resumeDagId;

  public ResumeDagTask(DagManager.DagId resumeDagId) {
    this.resumeDagId = resumeDagId;
  }
  /**
   * initializes the job properties associated with a {@link DagTask}
   * @param dag
   * @param triggerTimeStamp
   */
  @Override
  void initialize(Object dag, long triggerTimeStamp) {
    Dag<JobExecutionPlan> resumeDag = (Dag<JobExecutionPlan>) dag;
    this.flowAction = new DagActionStore.DagAction(resumeDagId.getFlowGroup(), resumeDagId.getFlowName(),
        resumeDagId.getFlowExecutionId(), DagActionStore.FlowActionType.KILL);
    this.jobProps = resumeDag.getStartNodes().get(0).getValue().getJobSpec().getConfigAsProperties();
    this.triggerTimeStamp = triggerTimeStamp;
  }

  @Override
  ResumeDagProc host(DagTaskVisitor visitor) throws IOException, InstantiationException, IllegalAccessException {
    return (ResumeDagProc) visitor.meet(this);  }
}
