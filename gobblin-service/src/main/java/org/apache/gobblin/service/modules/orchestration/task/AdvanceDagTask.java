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

package org.apache.gobblin.service.modules.orchestration.task;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.service.modules.orchestration.DagManagerUtils;
import org.apache.gobblin.service.modules.orchestration.DagProcessingEngine;
import org.apache.gobblin.service.modules.orchestration.DagTaskVisitor;
import org.apache.gobblin.service.modules.orchestration.proc.DagProc;


/**
 * An implementation of {@link DagTask} that is responsible for advancing the dag to the next node based
 * on its current flow and job status. It is added to the {@link org.apache.gobblin.service.modules.orchestration.DagTaskStream}
 * by the {@link org.apache.gobblin.service.monitoring.KafkaJobStatusMonitor} after it consumes the appropriate
 * {@link org.apache.gobblin.metrics.GobblinTrackingEvent} for the {@link org.apache.gobblin.service.modules.flowgraph.Dag}
 */

@Alpha
@Slf4j
public class AdvanceDagTask extends DagTask {
  //todo - move it to super
  @Getter DagManager.DagId advanceDagId;

//  @Getter
//  private final Dag.DagNode<JobExecutionPlan> dagNode;

  public AdvanceDagTask(DagActionStore.DagAction dagAction, MultiActiveLeaseArbiter.LeaseAttemptStatus leaseObtainedStatus) {
    super(dagAction, leaseObtainedStatus);
    this.advanceDagId = DagManagerUtils.generateDagId(dagAction.getFlowGroup(), dagAction.getFlowName(), dagAction.getFlowExecutionId());
  }

  @Override
  public DagProc host(DagTaskVisitor<DagProc> visitor, DagProcessingEngine dagProcessingEngine) {
    return visitor.meet(this, dagProcessingEngine);
  }
}
