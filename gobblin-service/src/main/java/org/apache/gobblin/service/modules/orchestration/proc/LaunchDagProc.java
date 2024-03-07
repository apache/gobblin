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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.task.LaunchDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;


/**
 * An implementation for {@link LaunchDagTask}
 */
@Slf4j
@Alpha
public class LaunchDagProc extends DagProc<Optional<Dag<JobExecutionPlan>>, Optional<Dag<JobExecutionPlan>>> {
  private final LaunchDagTask launchDagTask;
  private final AtomicLong orchestrationDelayCounter;

  public LaunchDagProc(LaunchDagTask launchDagTask) {
    this.launchDagTask = launchDagTask;
    this.orchestrationDelayCounter = new AtomicLong(0);
    ContextAwareGauge<Long> orchestrationDelayMetric = this.metricContext.newContextAwareGauge
        (ServiceMetricNames.FLOW_ORCHESTRATION_DELAY, orchestrationDelayCounter::get);
    this.metricContext.register(orchestrationDelayMetric);
  }

  @Override
  protected Optional<Dag<JobExecutionPlan>> initialize(DagManagementStateStore dagManagementStateStore)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected Optional<Dag<JobExecutionPlan>> act(DagManagementStateStore dagManagementStateStore, Optional<Dag<JobExecutionPlan>> dag)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected void sendNotification(Optional<Dag<JobExecutionPlan>> result, EventSubmitter eventSubmitter)
      throws IOException {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  protected void commit(DagManagementStateStore dagManagementStateStore, Optional<Dag<JobExecutionPlan>> dag) {
    throw new UnsupportedOperationException("Not yet implemented");
  }
}
