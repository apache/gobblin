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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import com.google.api.client.util.Lists;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
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
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.runtime.api.MysqlMultiActiveLeaseArbiter;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.dag_action_store.MysqlDagActionStore;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.exception.MaybeRetryableException;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.utils.FlowCompilationValidationHelper;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;

import static org.apache.gobblin.service.ExecutionStatus.PENDING;


/**
 * An implmentation of {@link DagProc} for launching {@link DagTask}.
 */
@Slf4j
@WorkInProgress
public final class LaunchDagProc extends DagProc {
  private String flowGroup;
  private String flowName;
  private FlowCatalog flowCatalog;
  private FlowCompilationValidationHelper flowCompilationValidationHelper;
  private Optional<DagActionStore> dagActionStore;
  private Optional<EventSubmitter> eventSubmitter;
  private MultiActiveLeaseArbiter multiActiveLeaseArbiter;
  private DagStateStore dagStateStore;
  private DagManagementStateStore dagManagementStateStore;
  private DagManagerMetrics dagManagerMetrics;
  private UserQuotaManager quotaManager;
  private MetricContext metricContext;

  /**
   * The instantiation of {@link LaunchDagProc} would be refactored and handled by {@link DagProcFactory}.
   * Currently, this is providing basic means for instantiating and providing attributes required for processing
   * the launch of a Dag.
   * @param flowGroup
   * @param flowName
   * @throws IOException
   * @throws InstantiationException
   * @throws IllegalAccessException
   */

  public LaunchDagProc(String flowGroup, String flowName) throws IOException, InstantiationException, IllegalAccessException {
    this.flowGroup = flowGroup;
    this.flowName = flowName;
    this.flowCatalog = new FlowCatalog(ConfigBuilder.create().build());
//    this.flowCompilationValidationHelper = new FlowCompilationValidationHelper()
    this.quotaManager = GobblinConstructorUtils.invokeConstructor(UserQuotaManager.class,
        ConfigUtils.getString(ConfigBuilder.create().build(), ServiceConfigKeys.QUOTA_MANAGER_CLASS, ServiceConfigKeys.DEFAULT_QUOTA_MANAGER),
        ConfigBuilder.create().build());
    this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(ConfigFactory.empty()), getClass());
    this.eventSubmitter = Optional.of(new EventSubmitter.Builder(this.metricContext, "org.apache.gobblin.service").build());
    this.multiActiveLeaseArbiter = new MysqlMultiActiveLeaseArbiter(ConfigBuilder.create().build());
    this.dagActionStore = Optional.of(new MysqlDagActionStore(ConfigBuilder.create().build()));
    this.dagStateStore = (DagStateStore) (MysqlDagStateStoreFactory.class.newInstance()).createStateStore(ConfigBuilder.create()
        .build(), State.class);
    this.dagManagementStateStore = new DagManagementStateStore();
    this.dagManagerMetrics = new DagManagerMetrics();
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
    return null;
  }

  /**
   * Post initialization of the Dag with the current state, it will identify the {@link Dag.DagNode}s to be launched.
   * The return type is kept as {@link Object} since we might want to refactor
   * or add more responsibility as part of the actions taken. Hence, after completing all possible scenarios,
   * it will make sense to update the method signature with its appropriate type.
   * @param state
   * @return
   * @throws IOException
   */
  @Override
  protected Object act(Object state) throws IOException {
    Dag<JobExecutionPlan> dag = (Dag<JobExecutionPlan>) state;
    this.dagStateStore.writeCheckpoint(dag);
    return new Dag<JobExecutionPlan>(Lists.newArrayList());
  }

  /**
   * This will send an event marking the Job status to be PENDING, which wil be picked up by {@link AdvanceDagProc}
   * for further progressing the job state.
   * @param result
   * @throws MaybeRetryableException
   */
  @Override
  protected void sendNotification(Object result) throws MaybeRetryableException {
    Dag<JobExecutionPlan> dag = (Dag<JobExecutionPlan>) result;
    Preconditions.checkArgument(!dag.isEmpty(), "Dag is empty without any nodes. Cannot be launched!");
    if (this.eventSubmitter.isPresent()) {
      for (Dag.DagNode<JobExecutionPlan> dagNode : dag.getNodes()) {
        JobExecutionPlan jobExecutionPlan = DagManagerUtils.getJobExecutionPlan(dagNode);
        Map<String, String> jobMetadata = TimingEventUtils.getJobMetadata(Maps.newHashMap(), jobExecutionPlan);
        this.eventSubmitter.get().getTimingEvent(TimingEvent.LauncherTimings.JOB_PENDING).stop(jobMetadata);
        jobExecutionPlan.setExecutionStatus(PENDING);
      }
    }
  }
}
