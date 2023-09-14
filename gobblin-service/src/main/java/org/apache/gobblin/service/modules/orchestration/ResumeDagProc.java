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

import com.google.api.client.util.Lists;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metastore.MysqlDagStateStoreFactory;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.runtime.api.MysqlMultiActiveLeaseArbiter;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.exception.MaybeRetryableException;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.util.ConfigUtils;

import static org.apache.gobblin.service.ExecutionStatus.CANCELLED;
import static org.apache.gobblin.service.ExecutionStatus.FAILED;
import static org.apache.gobblin.service.ExecutionStatus.PENDING_RESUME;


/**
 * An implementation of {@link DagProc} for resuming {@link DagTask}.
 */
@WorkInProgress
@Slf4j
public final class ResumeDagProc extends DagProc {
  private DagManager.DagId resumeDagId;
  private DagManagementStateStore dagManagementStateStore;
  private MetricContext metricContext;
  private Optional<EventSubmitter> eventSubmitter;
  private DagStateStore failedDagStateStore;
  private MultiActiveLeaseArbiter multiActiveLeaseArbiter;

  public ResumeDagProc(DagManager.DagId resumeDagId) throws InstantiationException, IllegalAccessException, IOException {
    this.resumeDagId = resumeDagId;
    //TODO: add this to dagproc factory instead
    this.dagManagementStateStore = new DagManagementStateStore();
    this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(ConfigFactory.empty()), getClass());
    this.multiActiveLeaseArbiter = new MysqlMultiActiveLeaseArbiter(ConfigBuilder.create().build());
    this.eventSubmitter = Optional.of(new EventSubmitter.Builder(this.metricContext, "org.apache.gobblin.service").build());
    this.failedDagStateStore = (DagStateStore) (MysqlDagStateStoreFactory.class.newInstance()).createStateStore(ConfigBuilder.create()
        .build(), State.class);
  }


  @Override
  public void process(MultiActiveLeaseArbiter.LeaseAttemptStatus leaseStatus) {
    try {
      Object state = this.initialize();
      Object result = this.act(state);
      this.sendNotification(result);
      this.multiActiveLeaseArbiter.recordLeaseSuccess((MultiActiveLeaseArbiter.LeaseObtainedStatus) leaseStatus);
      log.info("Successfully processed Pending Resume Dag Request");
    } catch (Exception | MaybeRetryableException ex) {
      log.info("Need to handle the exception here");
      return;
    }
  }
  @Override
  protected Object initialize() {
    return new Dag<JobExecutionPlan>(Lists.newArrayList());
  }

  @Override
  protected Object act(Object state) throws IOException {
    String dagIdToResume= resumeDagId.toString();
    Dag<JobExecutionPlan> dag = this.failedDagStateStore.getDag(dagIdToResume);
    if (!this.dagManagementStateStore.getFailedDagIds().contains(dagIdToResume)) {
      log.warn("No dag found with dagId " + dagIdToResume + ", so cannot resume flow");
      this.dagManagementStateStore.removeDagActionFromStore(resumeDagId, DagActionStore.FlowActionType.RESUME);
      return dag;
    }

    if (dag == null) {
      log.error("Dag " + dagIdToResume + " was found in memory but not found in failed dag state store");
      this.dagManagementStateStore.removeDagActionFromStore(resumeDagId, DagActionStore.FlowActionType.RESUME);
      return dag;
    }
    this.dagManagementStateStore.getResumingDags().put(dagIdToResume, dag);
    return dag;
  }

  @Override
  protected void sendNotification(Object result) throws MaybeRetryableException {
    Dag<JobExecutionPlan> dag = (Dag<JobExecutionPlan>) result;
    DagManagerUtils.emitFlowEvent(this.eventSubmitter, dag, TimingEvent.FlowTimings.FLOW_PENDING_RESUME);
    long flowResumeTime = System.currentTimeMillis();

    // Set the flow and it's failed or cancelled nodes to PENDING_RESUME so that the flow will be resumed from the point before it failed
    for (Dag.DagNode<JobExecutionPlan> node : dag.getNodes()) {
      ExecutionStatus executionStatus = node.getValue().getExecutionStatus();
      if (executionStatus.equals(FAILED) || executionStatus.equals(CANCELLED)) {
        node.getValue().setExecutionStatus(PENDING_RESUME);
        // reset currentAttempts because we do not want to count previous execution's attempts in deciding whether to retry a job
        node.getValue().setCurrentAttempts(0);
        DagManagerUtils.incrementJobGeneration(node);
        Map<String, String> jobMetadata = TimingEventUtils.getJobMetadata(Maps.newHashMap(), node.getValue());
        this.eventSubmitter.get().getTimingEvent(TimingEvent.LauncherTimings.JOB_PENDING_RESUME).stop(jobMetadata);
      }

      // Set flowStartTime so that flow SLA will be based on current time instead of original flow
      node.getValue().setFlowStartTime(flowResumeTime);
    }
  }
}
