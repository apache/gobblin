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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
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
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.exception.MaybeRetryableException;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;

import static org.apache.gobblin.service.ExecutionStatus.CANCELLED;
import static org.apache.gobblin.service.ExecutionStatus.FAILED;
import static org.apache.gobblin.service.ExecutionStatus.PENDING_RESUME;


/**
 * An implementation of {@link DagProc} for resuming {@link DagTask}.
 */

@Alpha
@Slf4j
public final class ResumeDagProc extends DagProc {
  private DagManager.DagId resumeDagId;
  private DagManagementStateStore dagManagementStateStore;
  private MetricContext metricContext;
  private Optional<EventSubmitter> eventSubmitter;
  private DagStateStore dagStateStore;
  private DagStateStore failedDagStateStore;
  private MultiActiveLeaseArbiter multiActiveLeaseArbiter;
  private UserQuotaManager quotaManager;
  private DagManagerMetrics dagManagerMetrics;

  public ResumeDagProc(DagManager.DagId resumeDagId) throws InstantiationException, IllegalAccessException, IOException {
    this.resumeDagId = resumeDagId;
    //TODO: add this to dagproc factory instead
    this.dagManagementStateStore = new DagManagementStateStore();
    this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(ConfigFactory.empty()), getClass());
    this.multiActiveLeaseArbiter = new MysqlMultiActiveLeaseArbiter(ConfigBuilder.create().build());
    this.eventSubmitter = Optional.of(new EventSubmitter.Builder(this.metricContext, "org.apache.gobblin.service").build());
    this.dagStateStore = (DagStateStore) (MysqlDagStateStoreFactory.class.newInstance()).createStateStore(ConfigBuilder.create()
        .build(), State.class);
    this.failedDagStateStore = (DagStateStore) (MysqlDagStateStoreFactory.class.newInstance()).createStateStore(ConfigBuilder.create()
        .build(), State.class);
    this.quotaManager = GobblinConstructorUtils.invokeConstructor(UserQuotaManager.class,
        ConfigUtils.getString(ConfigBuilder.create().build(), ServiceConfigKeys.QUOTA_MANAGER_CLASS, ServiceConfigKeys.DEFAULT_QUOTA_MANAGER),
        ConfigBuilder.create().build());
    this.dagManagerMetrics = new DagManagerMetrics();
  }

  @Override
  protected Object initialize() throws IOException {
    String dagIdToResume= resumeDagId.toString();
    Dag<JobExecutionPlan> dag = this.failedDagStateStore.getDag(dagIdToResume);
    return dag;
  }

  @Override
  protected Object act(Object state) throws IOException {
    String dagIdToResume= resumeDagId.toString();
    Dag<JobExecutionPlan> dag = (Dag<JobExecutionPlan>) state;
    if (!this.dagManagementStateStore.getFailedDagIds().contains(dagIdToResume)) {
      log.warn("No dag found with dagId " + dagIdToResume + ", so cannot resume flow");
      this.dagManagementStateStore.removeDagActionFromStore(resumeDagId, DagActionStore.FlowActionType.RESUME);
      return dag;
    }

    if (dag == null) {
      log.error("Dag " + dagIdToResume + " was found in memory but not found in failed dag state store");
      this.dagManagementStateStore.removeDagActionFromStore(resumeDagId, DagActionStore.FlowActionType.RESUME);
      return null;
    }
    this.dagManagementStateStore.getDagIdToResumingDags().put(dagIdToResume, dag);
    return dag;
  }

  @Override
  protected void sendNotification(Object result) throws MaybeRetryableException {
    Dag<JobExecutionPlan> dag = (Dag<JobExecutionPlan>) result;
    Preconditions.checkArgument(dag != null, "No such dag found, so cannot send notification");
    DagManagerUtils.emitFlowEvent(this.eventSubmitter, dag, TimingEvent.FlowTimings.FLOW_PENDING_RESUME);
    //TODO: set this value passed from the caller instead of setting it fresh here
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
