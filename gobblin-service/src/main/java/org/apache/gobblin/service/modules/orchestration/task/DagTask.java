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
import java.sql.SQLException;

import lombok.Getter;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.service.modules.orchestration.DagManagerUtils;
import org.apache.gobblin.service.modules.orchestration.DagTaskVisitor;
import org.apache.gobblin.service.modules.orchestration.proc.DagProc;


/**
 * Defines an individual task on a Dag.
 * Upon successful completion of the corresponding {@link DagProc#process(DagManagementStateStore)},
 * {@link DagTask#conclude()} must be called.
 */

@Alpha
public abstract class DagTask {
  @Getter public final DagActionStore.DagAction dagAction;
  private final MultiActiveLeaseArbiter.LeaseObtainedStatus leaseObtainedStatus;
  private final DagActionStore dagActionStore;
  @Getter protected final DagManager.DagId dagId;

  public DagTask(DagActionStore.DagAction dagAction, MultiActiveLeaseArbiter.LeaseObtainedStatus leaseObtainedStatus,
      DagActionStore dagActionStore) {
    this.dagAction = dagAction;
    this.leaseObtainedStatus = leaseObtainedStatus;
    this.dagActionStore = dagActionStore;
    this.dagId = DagManagerUtils.generateDagId(dagAction.getFlowGroup(), dagAction.getFlowName(), dagAction.getFlowExecutionId());
  }

  public abstract <T> T host(DagTaskVisitor<T> visitor);

  /**
   * Any cleanup work, including removing the dagAction from the dagActionStore and completing the lease acquired to
   * work on this task, is done in this method.
   * Returns true if concluding dag task finished successfully otherwise false.
   */
  // todo call it from the right place
  public boolean conclude() {
    try {
      // If dagAction is not present in store then it has been deleted by another participant, so we can skip deletion
      if (this.dagActionStore.exists(this.dagAction)) {
        this.dagActionStore.deleteDagAction(this.dagAction);
      }
      return this.leaseObtainedStatus.completeLease();
    } catch (IOException | SQLException e) {
      // TODO: Decide appropriate exception to throw and add to the commit method's signature
      throw new RuntimeException(e);
    }
  }
}
