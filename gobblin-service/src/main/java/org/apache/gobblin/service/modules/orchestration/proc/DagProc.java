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

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.service.modules.flowgraph.DagNodeId;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.service.modules.orchestration.DagManagerUtils;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;


/**
 * Responsible for performing the actual work for a given {@link DagTask} by first initializing its state, performing
 * actions based on the type of {@link DagTask}. Submitting events in time is found to be important in
 * <a href="https://github.com/apache/gobblin/pull/3641">PR#3641</a>, hence initialize and act methods submit events as
 * they happen.
 */
@Alpha
@Data
@Slf4j
public abstract class DagProc<T> {
  protected final DagTask dagTask;
  @Getter protected final DagManager.DagId dagId;
  @Getter protected final DagNodeId dagNodeId;
  protected static final MetricContext metricContext = Instrumented.getMetricContext(new State(), DagProc.class);
  protected static final EventSubmitter eventSubmitter = new EventSubmitter.Builder(
      metricContext, "org.apache.gobblin.service").build();

  public DagProc(DagTask dagTask) {
    this.dagTask = dagTask;
    this.dagId = DagManagerUtils.generateDagId(this.dagTask.getDagAction().getFlowGroup(),
        this.dagTask.getDagAction().getFlowName(), this.dagTask.getDagAction().getFlowExecutionId());
    this.dagNodeId = this.dagTask.getDagAction().getDagNodeId();
  }

  public final void process(DagManagementStateStore dagManagementStateStore) throws IOException {
    T state = initialize(dagManagementStateStore);   // todo - retry
    act(dagManagementStateStore, state);   // todo - retry
    log.info("{} concluded processing for dagId : {}", getClass().getSimpleName(), this.dagId);
  }

  protected abstract T initialize(DagManagementStateStore dagManagementStateStore) throws IOException;

  protected abstract void act(DagManagementStateStore dagManagementStateStore, T state) throws IOException;
}
