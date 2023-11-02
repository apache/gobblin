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
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
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
public final class KillDagProc extends DagProc<List<Dag.DagNode<JobExecutionPlan>>, Dag<JobExecutionPlan>> {

  private KillDagTask killDagTask;

  public KillDagProc(KillDagTask killDagTask) {
    this.killDagTask = killDagTask;
  }

  @Override
  protected List<Dag.DagNode<JobExecutionPlan>> initialize(DagManagementStateStore dagManagementStateStore) throws IOException {
    String dagToCancel = this.killDagTask.getKillDagId().toString();
    return dagManagementStateStore.getJobs(dagToCancel);
  }

  /**
   * Post initialization of the Dag with the current state, it will identify the {@link Dag.DagNode}s to be killed
   * and cancel the job on the executor. The return type is kept as {@link Object} since we might want to refactor
   * or add more responsibility as part of the actions taken. Hence, after completing all possible scenarios,
   * it will make sense to update the method signature with its appropriate type.
   * @param dagNodesToCancel
   * @return
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws IOException
   */
  @Override
  protected Dag<JobExecutionPlan> act(List<Dag.DagNode<JobExecutionPlan>> dagNodesToCancel, DagManagementStateStore dagManagementStateStore) throws Exception {
    String dagToCancel = this.killDagTask.getKillDagId().toString();

    log.info("Found {} DagNodes to cancel.", dagNodesToCancel.size());
    for (Dag.DagNode<JobExecutionPlan> dagNodeToCancel : dagNodesToCancel) {
      killDagNode(dagNodeToCancel);
    }
    dagManagementStateStore.getDag(dagToCancel).setFlowEvent(TimingEvent.FlowTimings.FLOW_CANCELLED);
    dagManagementStateStore.getDag(dagToCancel).setMessage("Flow killed by request");
    dagManagementStateStore.removeDagActionFromStore(this.killDagTask.getKillDagId(), DagActionStore.FlowActionType.KILL);
    return dagManagementStateStore.getDag(dagToCancel);

  }

  @Override
  protected void sendNotification(Dag<JobExecutionPlan> dag, EventSubmitter eventSubmitter) throws MaybeRetryableException {
    for(Dag.DagNode<JobExecutionPlan> dagNodeToCancel : dag.getNodes()) {
      sendCancellationEvent(dagNodeToCancel.getValue(), eventSubmitter);
    }
  }

  /**
   * Responsible for killing/canceling the job for a {@link org.apache.gobblin.service.modules.flowgraph.Dag.DagNode}
   * by invoking the {@link org.apache.gobblin.runtime.api.SpecProducer#cancelJob(URI, Properties)}
   * @param dagNodeToCancel
   * @throws ExecutionException
   * @throws InterruptedException
   */
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

  private void sendCancellationEvent(JobExecutionPlan jobExecutionPlan, EventSubmitter eventSubmitter) {
      Map<String, String> jobMetadata = TimingEventUtils.getJobMetadata(Maps.newHashMap(), jobExecutionPlan);
      eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.JOB_CANCEL).stop(jobMetadata);
      jobExecutionPlan.setExecutionStatus(CANCELLED);
  }
}

