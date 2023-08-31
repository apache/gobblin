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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.google.common.collect.Maps;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagManagerUtils;
import org.apache.gobblin.service.modules.orchestration.DagProcessingEngine;
import org.apache.gobblin.service.modules.orchestration.TimingEventUtils;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.service.modules.orchestration.task.KillDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;

import static org.apache.gobblin.service.ExecutionStatus.CANCELLED;

/**
 * An implementation of {@link DagProc} for killing {@link DagTask}.
 */
@Slf4j
@Alpha
public final class KillDagProc extends DagProc<KillDagProc.CancelEntity, Dag<JobExecutionPlan>> {

  // should dag task be a part of dag proc?
  private final KillDagTask killDagTask;

  public KillDagProc(KillDagTask killDagTask, DagProcessingEngine dagProcessingEngine) {
    super(dagProcessingEngine);
    this.killDagTask = killDagTask;
  }

  protected CancelEntity initialize(DagManagementStateStore dagManagementStateStore) throws IOException {
    Dag<JobExecutionPlan> dagToCancel = dagManagementStateStore.getDag(this.killDagTask.getDagId().toString());
    List<Dag.DagNode<JobExecutionPlan>> dagNodesToCancel = new ArrayList<>(dagManagementStateStore.getJobs(this.killDagTask.getDagId().toString()));
    return new CancelEntity(dagToCancel, dagNodesToCancel);
  }

  @Override
  public Dag<JobExecutionPlan> act(DagManagementStateStore dagManagementStateStore, CancelEntity cancelEntity) throws IOException {
    if (cancelEntity.dagToCancel == null || cancelEntity.dagNodesToCancel.isEmpty()) {
      log.warn("No dag with id " + this.killDagTask.getDagId() + " found to kill");
      return null;
    }
    for (Dag.DagNode<JobExecutionPlan> dagNodeToCancel : cancelEntity.dagNodesToCancel) {
      cancelDagNode(cancelEntity.dagToCancel, dagNodeToCancel, dagManagementStateStore);
    }
    return cancelEntity.dagToCancel;
  }

  private void cancelDagNode(Dag<JobExecutionPlan> dagToCancel, Dag.DagNode<JobExecutionPlan> dagNodeToCancel, DagManagementStateStore dagManagementStateStore) throws IOException {
    Properties props = new Properties();
    try {
      if (dagNodeToCancel.getValue().getJobFuture().isPresent()) {
        Future future = dagNodeToCancel.getValue().getJobFuture().get();
        String serializedFuture = DagManagerUtils.getSpecProducer(dagNodeToCancel).serializeAddSpecResponse(future);
        props.put(ConfigurationKeys.SPEC_PRODUCER_SERIALIZED_FUTURE, serializedFuture);
        sendCancellationEvent(dagNodeToCancel.getValue());
      }
      if (dagNodeToCancel.getValue().getJobSpec().getConfig().hasPath(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)) {
        props.setProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY,
            dagNodeToCancel.getValue().getJobSpec().getConfig().getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY));
      }
      DagManagerUtils.getSpecProducer(dagNodeToCancel).cancelJob(dagNodeToCancel.getValue().getJobSpec().getUri(), props);
      dagToCancel.setFlowEvent(TimingEvent.FlowTimings.FLOW_CANCELLED);
      dagToCancel.setMessage("Flow killed by request");
      dagManagementStateStore.removeDagActionFromStore(this.killDagTask.getDagAction());
    } catch (ExecutionException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  private void sendCancellationEvent(JobExecutionPlan jobExecutionPlan) {
    Map<String, String> jobMetadata = TimingEventUtils.getJobMetadata(Maps.newHashMap(), jobExecutionPlan);
    this.eventSubmitter.get().getTimingEvent(TimingEvent.LauncherTimings.JOB_CANCEL).stop(jobMetadata);
    jobExecutionPlan.setExecutionStatus(CANCELLED);
  }

  @AllArgsConstructor
  static class CancelEntity {
    private Dag<JobExecutionPlan> dagToCancel;
    private List<Dag.DagNode<JobExecutionPlan>> dagNodesToCancel;
  }
}

