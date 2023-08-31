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
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.inject.Singleton;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.service.modules.orchestration.task.KillDagTask;


/**
 * Holds a stream of {@link DagTask}s that {@link DagProcessingEngine} would pull from for processing.
 * It provides an implementation for {@link DagManagement} that defines the rules for a flow and job.
 * Implements {@link Iterator} to provide {@link DagTask}s as soon as it's available to {@link DagProcessingEngine}
 */

@Alpha
@Slf4j
@Singleton
// change to iterable
public class DagTaskStream implements Iterator<DagTask>{
  private final BlockingQueue<DagActionStore.DagAction> dagActionQueue = new LinkedBlockingQueue<>();

  @Override
  public boolean hasNext() {
    return !this.dagActionQueue.isEmpty();
  }

  @Override
  public DagTask next() {
    try {
      DagActionStore.DagAction dagAction = this.dagActionQueue.take();  //`take` blocks till element is not available
      // todo reconsider the use of MultiActiveLeaseArbiter
      //MultiActiveLeaseArbiter.LeaseAttemptStatus leaseAttemptStatus = new MultiActiveLeaseArbiter.LeaseObtainedStatus(dagAction);
      // todo - uncomment after flow trigger handler provides such an api
      //Properties jobProps = getJobProperties(dagAction);
      //flowTriggerHandler.getLeaseOnDagAction(jobProps, dagAction, System.currentTimeMillis());
      //if (leaseAttemptStatus instanceof MultiActiveLeaseArbiter.LeaseObtainedStatus) {
        // can it return null? is this iterator allowed to return null?
        return createDagTask(dagAction, new MultiActiveLeaseArbiter.LeaseObtainedStatus(dagAction, System.currentTimeMillis()));
      //}
    } catch (Exception e) {
      //TODO: need to handle exceptions gracefully
      log.error("Error getting DagAction from the queue / creating DagTask", e);
    }
    return null;
  }

  private DagTask createDagTask(DagActionStore.DagAction dagAction, MultiActiveLeaseArbiter.LeaseAttemptStatus leaseObtainedStatus) {
    DagActionStore.FlowActionType flowActionType = dagAction.getFlowActionType();

    switch (flowActionType) {
      case KILL:
        return new KillDagTask(dagAction, leaseObtainedStatus);
      case RESUME:
      case LAUNCH:
      case ADVANCE:
      default:
       throw new UnsupportedOperationException("Yet to provide implementation.");
    }
  }

  protected void complete(DagTask dagTask) throws IOException {
    //dagTask.conclude(this.flowTriggerHandler.getMultiActiveLeaseArbiter());
  }

  public void addDagAction(DagActionStore.DagAction dagAction) {
    this.dagActionQueue.add(dagAction);
  }
}
