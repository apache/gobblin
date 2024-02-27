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

import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Responsible for performing the actual work for a given {@link DagTask} by first initializing its state, performing
 * actions based on the type of {@link DagTask} and finally submitting an event to the executor.
 */
@Alpha
@Slf4j
public abstract class DagProc<S, T> {
  protected final EventSubmitter eventSubmitter;
  protected final MetricContext metricContext;

  public DagProc() {
    this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(ConfigFactory.empty()), getClass());
    this.eventSubmitter = new EventSubmitter.Builder(metricContext, "org.apache.gobblin.service").build();
  }

  public final void process(DagManagementStateStore dagManagementStateStore) throws IOException {
    S state = initialize(dagManagementStateStore);   // todo - retry
    T result = act(dagManagementStateStore, state);   // todo - retry
    commit(dagManagementStateStore, result);   // todo - retry
  }

  protected abstract S initialize(DagManagementStateStore dagManagementStateStore) throws IOException;

  protected abstract T act(DagManagementStateStore dagManagementStateStore, S state) throws IOException;

  // todo - commit the modified dags to the persistent store, maybe not required for InMem dagManagementStateStore
  protected abstract void commit(DagManagementStateStore dagManagementStateStore, T result);
}
