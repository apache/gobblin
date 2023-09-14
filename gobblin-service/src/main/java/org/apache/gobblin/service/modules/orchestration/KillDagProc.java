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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.google.api.client.util.Lists;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.runtime.api.MysqlMultiActiveLeaseArbiter;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.exception.MaybeRetryableException;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.util.ConfigUtils;

import static org.apache.gobblin.service.ExecutionStatus.CANCELLED;


/**
 * An implementation of {@link DagProc} for killing {@link DagTask}.
 */
@Slf4j
@WorkInProgress
public final class KillDagProc extends DagProc {

  private DagManager.DagId killDagId;
  private DagManagementStateStore dagManagementStateStore;
  private MetricContext metricContext;
  private Optional<EventSubmitter> eventSubmitter;
  private MultiActiveLeaseArbiter multiActiveLeaseArbiter;


  public KillDagProc(DagManager.DagId killDagId) throws IOException {
    this.killDagId = killDagId;
    //TODO: add this to dagproc factory
    this.dagManagementStateStore = new DagManagementStateStore();
    //TODO: add this to dagproc factory
    this.multiActiveLeaseArbiter = new MysqlMultiActiveLeaseArbiter(ConfigBuilder.create().build());
    this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(ConfigFactory.empty()), getClass());
    this.eventSubmitter = Optional.of(new EventSubmitter.Builder(this.metricContext, "org.apache.gobblin.service").build());

  }

  @Override
  public void process(MultiActiveLeaseArbiter.LeaseAttemptStatus leaseStatus) {
    try {
      Object state = this.initialize();
      Object result = this.act(state);
      this.sendNotification(result);
      this.multiActiveLeaseArbiter.recordLeaseSuccess((MultiActiveLeaseArbiter.LeaseObtainedStatus) leaseStatus);
      log.info("Successfully processed Kill Dag Request");
    } catch (Exception | MaybeRetryableException ex) {
      log.info("Need to handle the exception here");
    }
  }
  @Override
  protected List<Dag.DagNode<JobExecutionPlan>> initialize() {
    Map<String, LinkedList<Dag.DagNode<JobExecutionPlan>>> dagToJobs = this.dagManagementStateStore.getDagToJobs();
    if(dagToJobs.containsKey(killDagId.toString())) {
      return dagToJobs.get(killDagId.toString());
    }
    return Lists.newArrayList();
  }

  /**
   * Post initialization of the Dag with the current state, it will identify the {@link Dag.DagNode}s to be killed
   * and cancel the job on the executor. The return type is kept as {@link Object} since we might want to refactor
   * or add more responsibility as part of the actions taken. Hence, after completing all possible scenarios,
   * it will make sense to update the method signature with its appropriate type.
   * @param state
   * @return
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws IOException
   */
  @Override
  protected Object act(Object state) throws InterruptedException, ExecutionException, IOException {
    List<Dag.DagNode<JobExecutionPlan>> dagNodesToCancel = (List<Dag.DagNode<JobExecutionPlan>>)state;
    //TODO: add a preconditions check for empty lists
    Preconditions.checkArgument(!dagNodesToCancel.isEmpty(), "Dag doesn't contain any DagNodes to be cancelled");
    String dagToCancel = killDagId.toString();

    log.info("Found {} DagNodes to cancel.", dagNodesToCancel.size());
    for (Dag.DagNode<JobExecutionPlan> dagNodeToCancel : dagNodesToCancel) {
      killDagNode(dagNodeToCancel);
    }
    this.dagManagementStateStore.getDags().get(dagToCancel).setFlowEvent(TimingEvent.FlowTimings.FLOW_CANCELLED);
    this.dagManagementStateStore.getDags().get(dagToCancel).setMessage("Flow killed by request");
    this.dagManagementStateStore.removeDagActionFromStore(killDagId, DagActionStore.FlowActionType.KILL);
    return this.dagManagementStateStore.getDags().get(dagToCancel);

  }

  @Override
  protected void sendNotification(Object result) throws MaybeRetryableException {
    Dag<JobExecutionPlan> dag = (Dag<JobExecutionPlan>) result;
    for(Dag.DagNode<JobExecutionPlan> dagNodeToCancel : dag.getNodes()) {
      sendCancellationEvent(dagNodeToCancel.getValue());
    }
  }

  protected static void killDagNode(Dag.DagNode<JobExecutionPlan> dagNodeToCancel) throws ExecutionException, InterruptedException {
    Properties props = new Properties();
    if (dagNodeToCancel.getValue().getJobFuture().isPresent()) {
      Future future = dagNodeToCancel.getValue().getJobFuture().get();
      String serializedFuture = DagManagerUtils.getSpecProducer(dagNodeToCancel).serializeAddSpecResponse(future);
      props.put(ConfigurationKeys.SPEC_PRODUCER_SERIALIZED_FUTURE, serializedFuture);
    }
    if (dagNodeToCancel.getValue().getJobSpec().getConfig().hasPath(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)) {
      props.setProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY,
          dagNodeToCancel.getValue().getJobSpec().getConfig().getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY));
    }
    DagManagerUtils.getSpecProducer(dagNodeToCancel).cancelJob(dagNodeToCancel.getValue().getJobSpec().getUri(), props);
  }

  private void sendCancellationEvent(JobExecutionPlan jobExecutionPlan) {
    if (this.eventSubmitter.isPresent()) {
      Map<String, String> jobMetadata = TimingEventUtils.getJobMetadata(Maps.newHashMap(), jobExecutionPlan);
      this.eventSubmitter.get().getTimingEvent(TimingEvent.LauncherTimings.JOB_CANCEL).stop(jobMetadata);
      jobExecutionPlan.setExecutionStatus(CANCELLED);
    }
  }
}

