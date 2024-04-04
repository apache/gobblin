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
  private final LeaseAttemptStatus.LeaseObtainedStatus leaseObtainedStatus;
  private final DagActionStore dagActionStore;

  public DagTask(DagActionStore.DagAction dagAction, LeaseAttemptStatus.LeaseObtainedStatus leaseObtainedStatus,
      DagActionStore dagActionStore) {
    this.dagAction = dagAction;
    this.leaseObtainedStatus = leaseObtainedStatus;
    this.dagActionStore = dagActionStore;
  }

  public abstract <T> T host(DagTaskVisitor<T> visitor);

  /**
   * Any cleanup work, including removing the dagAction from the dagActionStore and completing the lease acquired to
   * work on this task, is done in this method.
   * Returns true if concluding dag task finished successfully otherwise false.
   */
  public final boolean conclude() {
    try {
      this.dagActionStore.deleteDagAction(this.dagAction);
      return this.leaseObtainedStatus.completeLease();
    } catch (IOException e) {
      // TODO: Decide appropriate exception to throw and add to the commit method's signature
      throw new RuntimeException(e);
    }
  }
}
