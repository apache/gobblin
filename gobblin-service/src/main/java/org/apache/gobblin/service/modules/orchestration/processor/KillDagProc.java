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

package org.apache.gobblin.service.modules.orchestration.processor;

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

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagManagerUtils;
import org.apache.gobblin.service.modules.orchestration.TimingEventUtils;
import org.apache.gobblin.service.modules.orchestration.exception.MaybeRetryableException;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.service.modules.orchestration.task.KillDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;

import static org.apache.gobblin.service.ExecutionStatus.CANCELLED;


/**
 * An implementation of {@link DagProc} for killing {@link DagTask}.
 */
@Slf4j
@Alpha
public final class KillDagProc extends DagProc {

  private KillDagTask killDagTask;
  private DagManagementStateStore dagManagementStateStore;
  private MetricContext metricContext;
  private Optional<EventSubmitter> eventSubmitter;


  public KillDagProc(KillDagTask killDagTask, DagManagementStateStore dagManagementStateStore, MetricContext metricContext, Optional<EventSubmitter> eventSubmitter) throws IOException {
    this.killDagTask = killDagTask;
    this.dagManagementStateStore = dagManagementStateStore;
    this.metricContext = metricContext;
    this.eventSubmitter = eventSubmitter;
  }

  public KillDagProc(KillDagTask killDagTask) {
    this.killDagTask = killDagTask;
  }

  @Override
  protected List<Dag.DagNode<JobExecutionPlan>> initialize(DagManagementStateStore dagManagementStateStore) {
    Map<String, LinkedList<Dag.DagNode<JobExecutionPlan>>> dagToJobs = dagManagementStateStore.getDagToJobs();
    if(dagToJobs.containsKey(this.killDagTask.getKillDagId().toString())) {
      return dagToJobs.get(this.killDagTask.getKillDagId().toString());
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
  protected Object act(Object state) throws Exception {
    List<Dag.DagNode<JobExecutionPlan>> dagNodesToCancel = (List<Dag.DagNode<JobExecutionPlan>>)state;
    Preconditions.checkArgument(!dagNodesToCancel.isEmpty(), "Dag doesn't contain any DagNodes to be cancelled");
    String dagToCancel = this.killDagTask.getKillDagId().toString();

    log.info("Found {} DagNodes to cancel.", dagNodesToCancel.size());
    for (Dag.DagNode<JobExecutionPlan> dagNodeToCancel : dagNodesToCancel) {
      killDagNode(dagNodeToCancel);
    }
    this.dagManagementStateStore.getDagIdToDags().get(dagToCancel).setFlowEvent(TimingEvent.FlowTimings.FLOW_CANCELLED);
    this.dagManagementStateStore.getDagIdToDags().get(dagToCancel).setMessage("Flow killed by request");
    this.dagManagementStateStore.removeDagActionFromStore(this.killDagTask.getKillDagId(), DagActionStore.FlowActionType.KILL);
    return this.dagManagementStateStore.getDagIdToDags().get(dagToCancel);

  }

  @Override
  protected void sendNotification(Object result) throws MaybeRetryableException {
    Dag<JobExecutionPlan> dag = (Dag<JobExecutionPlan>) result;
    for(Dag.DagNode<JobExecutionPlan> dagNodeToCancel : dag.getNodes()) {
      sendCancellationEvent(dagNodeToCancel.getValue());
    }
  }

  public static void killDagNode(Dag.DagNode<JobExecutionPlan> dagNodeToCancel) throws ExecutionException, InterruptedException {
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

