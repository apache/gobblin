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

import java.io.IOException;

import lombok.Getter;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.service.modules.orchestration.DagManagerUtils;
import org.apache.gobblin.service.modules.orchestration.DagProcessingEngine;
import org.apache.gobblin.service.modules.orchestration.DagTaskVisitor;
import org.apache.gobblin.service.modules.orchestration.proc.DagProc;


/**
 * Defines an individual task in a Dag.
 * Upon completion of the {@link DagProc#process(DagManagementStateStore)} it will mark the lease
 * acquired by {@link org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter} as complete
 */

@Alpha
public abstract class DagTask {
  @Getter public DagActionStore.DagAction dagAction;
  private MultiActiveLeaseArbiter.LeaseAttemptStatus leaseObtainedStatus;
  @Getter DagManager.DagId dagId;

  public DagTask(DagActionStore.DagAction dagAction, MultiActiveLeaseArbiter.LeaseAttemptStatus leaseObtainedStatus) {
    this.dagAction = dagAction;
    this.leaseObtainedStatus = leaseObtainedStatus;
    this.dagId = DagManagerUtils.generateDagId(dagAction.getFlowGroup(), dagAction.getFlowName(), dagAction.getFlowExecutionId());
  }

  public abstract DagProc host(DagTaskVisitor<DagProc> visitor, DagProcessingEngine dagProcessingEngine);

  /**
   * Currently, conclusion of {@link DagTask} marks and records a successful release of lease.
   * It is invoked after {@link DagProc#process(DagManagementStateStore)} is completed successfully.
   * @param multiActiveLeaseArbiter
   * @throws IOException
   */
  public void conclude(MultiActiveLeaseArbiter multiActiveLeaseArbiter) throws IOException {
    // todo
    //multiActiveLeaseArbiter.recordLeaseSuccess(leaseObtainedStatus);
  }

}
