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
package org.apache.gobblin.service.modules.orchestration.proc;

import java.io.IOException;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.exception.MaybeRetryableException;
import org.apache.gobblin.service.modules.orchestration.task.AdvanceDagTask;


/**
 * An implementation of {@link DagProc} dealing which advancing to the next node in the {@link Dag}.
 * This Dag Procedure will deal with pending Job statuses such as: PENDING, PENDING_RESUME, PENDING_RETRY
 * as well jobs that have reached an end state with statuses such as: COMPLETED, FAILED and CANCELLED.
 * Primarily, it will be responsible for polling the flow and job statuses and advancing to the next node in the dag.
 *
 */
@Slf4j
@Alpha
public class AdvanceDagProc extends DagProc {

  private AdvanceDagTask advanceDagTask;

  public AdvanceDagProc(AdvanceDagTask advanceDagTask) {
    this.advanceDagTask = advanceDagTask;
  }

  @Override
  protected Object initialize(DagManagementStateStore dagManagementStateStore) throws MaybeRetryableException, IOException {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  protected Object act(Object state, DagManagementStateStore dagManagementStateStore) throws MaybeRetryableException, Exception {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  protected void sendNotification(Object result, EventSubmitter eventSubmitter) throws MaybeRetryableException, IOException {
    throw new UnsupportedOperationException("Not supported");
  }
}
