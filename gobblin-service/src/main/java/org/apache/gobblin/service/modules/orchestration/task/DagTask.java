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
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagTaskVisitor;
import org.apache.gobblin.service.modules.orchestration.LeaseAttemptStatus;
import org.apache.gobblin.service.modules.orchestration.proc.DagProc;


/**
 * Defines an individual task on a Dag.
 * Upon successful completion of the corresponding {@link DagProc#process(DagManagementStateStore)},
 * {@link DagTask#conclude()} must be called.
 */

@Alpha
@Slf4j
public abstract class DagTask {
  @Getter public final DagActionStore.DagAction dagAction;
  protected final DagManagementStateStore dagManagementStateStore;
  private final LeaseAttemptStatus.LeaseObtainedStatus leaseObtainedStatus;
  private final DagProcessingEngineMetrics dagProcEngineMetrics;

  public DagTask(DagActionStore.DagAction dagAction, LeaseAttemptStatus.LeaseObtainedStatus leaseObtainedStatus,
      DagManagementStateStore dagManagementStateStore, DagProcessingEngineMetrics dagProcEngineMetrics) {
    this.dagAction = dagAction;
    this.leaseObtainedStatus = leaseObtainedStatus;
    this.dagManagementStateStore = dagManagementStateStore;
    this.dagProcEngineMetrics = dagProcEngineMetrics;
  }

  public abstract <T> T host(DagTaskVisitor<T> visitor);

  /**
   * Any cleanup work, including removing the dagAction from the dagActionStore and completing the lease acquired to
   * work on this task, is done in this method.
   * Returns true if concluding dag task finished successfully otherwise false.
   */
  public boolean conclude() {
    try {
      this.dagManagementStateStore.deleteDagAction(this.dagAction);
      boolean completedLease = this.leaseObtainedStatus.completeLease();
      this.dagProcEngineMetrics.markDagActionsConclude(this.dagAction.getDagActionType(), completedLease);
      return completedLease;
    } catch (Exception e) {
      try {
        this.dagProcEngineMetrics.markDagActionsConclude(this.dagAction.getDagActionType(), false);
      } catch(Exception ex){
        log.error("Exception encountered in emitting metrics for dag node ID: {}, dag action type: {}. Stacktrace: ", dagAction.getDagNodeId(), dagAction.getDagActionType(), ex);
      }
      // TODO: Decide appropriate exception to throw and add to the commit method's signature
      log.error("Exception encountered in processing this DagTask, for dag node ID: {}, dag action type: {}. Stacktrace: ", dagAction.getDagNodeId(), dagAction.getDagActionType(), e);
      return false;
    }
  }
}
