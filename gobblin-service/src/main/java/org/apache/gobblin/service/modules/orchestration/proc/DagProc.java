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

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.service.modules.flowgraph.DagNodeId;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;


/**
 * Responsible for performing the actual work for a given {@link DagTask} by first initializing its state, performing
 * actions based on the type of {@link DagTask}. Submitting events in time is important (PR#3641), hence initialize and
 * act methods submit events as they happen.
 */
@Alpha
@Slf4j
@RequiredArgsConstructor
public abstract class DagProc<S> {
  protected final DagTask dagTask;
  @Getter protected final DagManager.DagId dagId;
  @Getter protected final DagNodeId dagNodeId;
  protected static final MetricContext metricContext = Instrumented.getMetricContext(new State(), DagProc.class);
  protected static final EventSubmitter eventSubmitter = new EventSubmitter.Builder(
      metricContext, "org.apache.gobblin.service").build();

  public DagProc(DagTask dagTask) {
    this.dagTask = dagTask;
    this.dagId = this.dagTask.getDagId();
    this.dagNodeId = this.dagTask.getDagNodeId();
  }

  public final void process(DagManagementStateStore dagManagementStateStore) throws IOException {
    S state = initialize(dagManagementStateStore);   // todo - retry
    act(dagManagementStateStore, state);   // todo - retry
    commit(dagManagementStateStore);   // todo - retry
    log.info("{} successfully concluded actions for dagId : {}", getClass().getSimpleName(), this.dagId);
  }

  protected abstract S initialize(DagManagementStateStore dagManagementStateStore) throws IOException;

  protected abstract void act(DagManagementStateStore dagManagementStateStore, S state) throws IOException;

  // todo - commit the modified dags to the persistent store, maybe not required for InMem dagManagementStateStore
  protected void commit(DagManagementStateStore dagManagementStateStore) {

  }
}
