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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.directory.api.util.Strings;

import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.flowgraph.DagNodeId;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagManagerUtils;
import org.apache.gobblin.service.modules.orchestration.TimingEventUtils;
import org.apache.gobblin.service.modules.orchestration.task.KillDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;

import static org.apache.gobblin.service.ExecutionStatus.CANCELLED;


/**
 * An implementation for {@link DagProc} that kills all the nodes of a dag.
 * If the dag action has job name set, then it kills only that particular job/dagNode.
 */
@Slf4j
public class KillDagProc extends DagProc<Optional<Dag<JobExecutionPlan>>> {
  private final Optional<DagNodeId> dagNodeId;

  public KillDagProc(KillDagTask killDagTask) {
    super(killDagTask);
    this.dagNodeId = Strings.isEmpty(this.dagTask.getDagAction().getJobName())
        ? Optional.empty()
        : Optional.of(new DagNodeId(this.dagTask.getDagAction().getFlowGroup(), this.dagTask.getDagAction().getFlowName(),
        Long.parseLong(this.dagTask.getDagAction().getFlowExecutionId()),
        this.dagTask.getDagAction().getFlowGroup(), this.dagTask.getDagAction().getJobName()));
  }

  @Override
  protected Optional<Dag<JobExecutionPlan>> initialize(DagManagementStateStore dagManagementStateStore)
      throws IOException {
   return dagManagementStateStore.getDag(getDagId());
  }

  @Override
  protected void act(DagManagementStateStore dagManagementStateStore, Optional<Dag<JobExecutionPlan>> dag)
      throws IOException {
    if (!dag.isPresent()) {
      log.error("Did not find Dag with id {}, it might be already cancelled/finished and thus cleaned up from the store.", getDagId());
      return;
    }

    dag.get().setFlowEvent(TimingEvent.FlowTimings.FLOW_CANCELLED);
    dag.get().setMessage("Flow killed by request");

    dagManagementStateStore.checkpointDag(dag.get());

    if (this.dagNodeId.isPresent()) {
      cancelDagNode(dagManagementStateStore.getDagNodeWithJobStatus(this.dagNodeId.get()).getLeft().get());
      return;
    }

    List<Dag.DagNode<JobExecutionPlan>> dagNodesToCancel = dag.get().getNodes();
    log.info("Found {} DagNodes to cancel (DagId {}).", dagNodesToCancel.size(), getDagId());

    for (Dag.DagNode<JobExecutionPlan> dagNodeToCancel : dagNodesToCancel) {
      cancelDagNode(dagNodeToCancel);
      // todo - why was it not being cleaned up in DagManager?
      dagManagementStateStore.deleteDagNodeState(getDagId(), dagNodeToCancel);
    }
  }

  private void cancelDagNode(Dag.DagNode<JobExecutionPlan> dagNodeToCancel) throws IOException {
    Properties props = new Properties();
    if (dagNodeToCancel.getValue().getJobSpec().getConfig().hasPath(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)) {
      props.setProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY,
          dagNodeToCancel.getValue().getJobSpec().getConfig().getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY));
    }

    try {
      if (dagNodeToCancel.getValue().getJobFuture().isPresent()) {
        Future future = dagNodeToCancel.getValue().getJobFuture().get();
        String serializedFuture = DagManagerUtils.getSpecProducer(dagNodeToCancel).serializeAddSpecResponse(future);
        props.put(ConfigurationKeys.SPEC_PRODUCER_SERIALIZED_FUTURE, serializedFuture);
        sendCancellationEvent(dagNodeToCancel.getValue());
      } else {
        log.warn("No Job future when canceling DAG node (hence, not sending cancellation event) - {}",
            dagNodeToCancel.getValue().getJobSpec().getUri());
      }
      DagManagerUtils.getSpecProducer(dagNodeToCancel).cancelJob(dagNodeToCancel.getValue().getJobSpec().getUri(), props).get();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private void sendCancellationEvent(JobExecutionPlan jobExecutionPlan) {
    Map<String, String> jobMetadata = TimingEventUtils.getJobMetadata(Maps.newHashMap(), jobExecutionPlan);
    eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.JOB_CANCEL).stop(jobMetadata);
    jobExecutionPlan.setExecutionStatus(CANCELLED);
  }
}
