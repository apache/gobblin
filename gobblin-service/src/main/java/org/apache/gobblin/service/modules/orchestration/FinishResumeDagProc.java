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
import java.util.Map;
import java.util.concurrent.ExecutionException;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.exception.MaybeRetryableException;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.JobStatus;

import static org.apache.gobblin.service.ExecutionStatus.CANCELLED;
import static org.apache.gobblin.service.ExecutionStatus.FAILED;
import static org.apache.gobblin.service.ExecutionStatus.PENDING_RESUME;


/**
 * An implementation of {@link DagProc} that will process {@link DagTask} will PENDING_RESUME job status.
 * This can be handled either via {@link AdvanceDagProc} or have a separate procedure to handle PENDING_RESUME events.
 * Currently, I have this boilerplate code, but can decide if it makes have a separate procedure for completion of
 * PENDING_RESUME events.
 */

@Alpha
@Slf4j
public class FinishResumeDagProc extends DagProc {
  private DagManagementStateStore dagManagementStateStore = new DagManagementStateStore();

  private DagTaskStream dagTaskStream;
  private DagStateStore dagStateStore;
  private DagStateStore failedDagStateStore;

  @Override
  protected Object initialize() {
    return null;
  }

  @Override
  protected Object act(Object state) throws ExecutionException, InterruptedException, IOException {
    for (Map.Entry<String, Dag<JobExecutionPlan>> dag : this.dagManagementStateStore.getDagIdToResumingDags().entrySet()) {
      JobStatus flowStatus = this.dagTaskStream.pollFlowStatus(dag.getValue());
      if (flowStatus == null || !flowStatus.getEventName().equals(PENDING_RESUME.name())) {
        continue;
      }

      boolean dagReady = true;
      for (Dag.DagNode<JobExecutionPlan> node : dag.getValue().getNodes()) {
        JobStatus jobStatus = this.dagTaskStream.pollJobStatus(node);
        if (jobStatus == null || jobStatus.getEventName().equals(FAILED.name()) || jobStatus.getEventName().equals(CANCELLED.name())) {
          dagReady = false;
          break;
        }
      }

      if (dagReady) {
        this.dagStateStore.writeCheckpoint(dag.getValue());
        this.failedDagStateStore.cleanUp(dag.getValue());
        this.dagManagementStateStore.removeDagActionFromStore(DagManagerUtils.generateDagId(dag.getValue()), DagActionStore.FlowActionType.RESUME);
        this.dagManagementStateStore.getFailedDagIds().remove(dag.getKey());
        this.dagManagementStateStore.getDagIdToResumingDags().remove(dag.getKey());
      }
    }
    return null;
  }

  @Override
  protected void sendNotification(Object result) throws MaybeRetryableException {

  }
}
