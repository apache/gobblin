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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.google.inject.Provider;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.troubleshooter.IssueSeverity;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.ForceKillHandler;
import org.apache.gobblin.service.modules.orchestration.task.DagProcessingEngineMetrics;
import org.apache.gobblin.service.modules.orchestration.task.KillDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;


/**
 * An implementation for {@link DagProc} that kills all the nodes of a dag.
 * If the dag action has job name set, then it kills only that particular job/dagNode.
 */
@Slf4j
public class KillDagProc extends DagProc<Optional<Dag<JobExecutionPlan>>> {
  private final boolean shouldKillSpecificJob;
  private final Provider<ForceKillHandler> forceKillHandler;
  private final Provider<JobStatusRetriever> jobStatusRetriever;

  public KillDagProc(KillDagTask killDagTask, Config config) {
    this(killDagTask, config, null, null);
  }

  public KillDagProc(KillDagTask killDagTask, Config config, Provider<ForceKillHandler> forceKillHandler,
      Provider<JobStatusRetriever> jobStatusRetriever) {
    super(killDagTask, config);
    this.shouldKillSpecificJob = !getDagNodeId().getJobName().equals(DagActionStore.NO_JOB_NAME_DEFAULT);
    this.forceKillHandler = forceKillHandler;
    this.jobStatusRetriever = forceKillHandler == null ? jobStatusRetriever
        : Objects.requireNonNull(jobStatusRetriever, "jobStatusRetriever");
  }

  @Override
  protected Optional<Dag<JobExecutionPlan>> initialize(DagManagementStateStore dagManagementStateStore)
      throws IOException {
      return dagManagementStateStore.getDag(getDagId());
  }

  @Override
  protected void act(DagManagementStateStore dagManagementStateStore, Optional<Dag<JobExecutionPlan>> dag,
      DagProcessingEngineMetrics dagProcEngineMetrics) throws IOException {
    log.info("Request to kill dag {} (node: {})", getDagId(), shouldKillSpecificJob ? getDagNodeId() : "<<all>>");

    if (!dag.isPresent()) {
      if (this.forceKillHandler != null) {
        forceKillFromJobStatus(dagManagementStateStore, dagProcEngineMetrics);
        return;
      }
      dagProcEngineMetrics.markDagActionsAct(getDagActionType(), false);
      log.error("Did not find Dag with id {}, it might be already cancelled/finished and thus cleaned up from the store.", getDagId());
      OrchestratorIssueEmitter.emitFlowIssue(eventSubmitter, getDagId(), IssueSeverity.WARN,
          "DAG not found for kill request. It might be already cancelled/finished: " + getDagId());
      return;
    }

    dag.get().setMessage("Flow killed by request");
    DagProcUtils.setAndEmitFlowEvent(eventSubmitter, dag.get(), TimingEvent.FlowTimings.FLOW_CANCELLED);

    if (this.shouldKillSpecificJob) {
      Optional<Dag.DagNode<JobExecutionPlan>> dagNodeToCancel = dagManagementStateStore.getDagNodeWithJobStatus(this.dagNodeId).getLeft();
      if (dagNodeToCancel.isPresent()) {
        DagProcUtils.cancelDagNode(dagNodeToCancel.get(), dagManagementStateStore);
      } else {
        dagProcEngineMetrics.markDagActionsAct(getDagActionType(), false);
        log.error("Did not find Dag node with id {}, it might be already cancelled/finished and thus cleaned up from the store.", getDagNodeId());
        OrchestratorIssueEmitter.emitJobIssue(eventSubmitter, getDagId(), getDagNodeId().getJobName(),
            IssueSeverity.WARN, "DagNode not found for kill request. It might be already cancelled/finished: " + getDagNodeId());
      }
    } else {
      DagProcUtils.cancelDag(dag.get(), dagManagementStateStore);
    }
    dagProcEngineMetrics.markDagActionsAct(getDagActionType(), true);
  }

  private void forceKillFromJobStatus(DagManagementStateStore store, DagProcessingEngineMetrics metrics)
      throws IOException {
    List<State> retainedStates = this.jobStatusRetriever.get().getJobStatusStatesForFlowExecution(
        getDagId().getFlowName(), getDagId().getFlowGroup(), getDagId().getFlowExecutionId());
    List<State> scopedStates = new ArrayList<>();
    boolean hasJob = false;
    for (State state : retainedStates) {
      if (!matchesFlow(state)) {
        log.warn("Ignoring out-of-scope job status during force kill of {}", getDagId());
        continue;
      }
      boolean flowSummary = isFlowSummary(state);
      if (flowSummary || !this.shouldKillSpecificJob
          || (getDagNodeId().getJobGroup().equals(state.getProp(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD))
              && getDagNodeId().getJobName().equals(state.getProp(TimingEvent.FlowEventConstants.JOB_NAME_FIELD)))) {
        scopedStates.add(new State(state.getProperties()));
        hasJob |= !flowSummary;
      }
    }

    Optional<Dag<JobExecutionPlan>> activeDag = store.getDag(getDagId());
    if (activeDag.isPresent()) {
      act(store, activeDag, metrics);
      return;
    }
    if (!hasJob) {
      reportForceKillFailure(metrics, "No retained job status found for force kill");
      return;
    }

    ForceKillHandler.Result result = Objects.requireNonNull(this.forceKillHandler.get().forceKill(
        this.dagTask.getDagAction(), Collections.unmodifiableList(scopedStates),
        () -> store.getDag(getDagId()).isPresent()), "forceKill result");
    if (result == ForceKillHandler.Result.ACTIVE_DAG_PRESENT) {
      activeDag = store.getDag(getDagId());
      if (activeDag.isPresent()) {
        act(store, activeDag, metrics);
      } else {
        reportForceKillFailure(metrics, "Active DAG changed during force kill; retained stopping was abandoned");
      }
    } else if (result == ForceKillHandler.Result.ACKNOWLEDGED) {
      metrics.markDagActionsAct(getDagActionType(), true);
      log.info("Force-kill requests acknowledged for {}; process termination is not confirmed", getDagId());
    } else {
      reportForceKillFailure(metrics, "Force kill did not acknowledge all retained executions");
    }
  }

  private boolean matchesFlow(State state) {
    return getDagId().getFlowGroup().equals(state.getProp(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD))
        && getDagId().getFlowName().equals(state.getProp(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD))
        && Long.toString(getDagId().getFlowExecutionId())
            .equals(state.getProp(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD));
  }

  private static boolean isFlowSummary(State state) {
    return JobStatusRetriever.NA_KEY.equals(state.getProp(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD))
        && JobStatusRetriever.NA_KEY.equals(state.getProp(TimingEvent.FlowEventConstants.JOB_NAME_FIELD));
  }

  private void reportForceKillFailure(DagProcessingEngineMetrics metrics, String message) {
    metrics.markDagActionsAct(getDagActionType(), false);
    log.warn("{}: {} (node: {})", message, getDagId(), getDagNodeId());
    OrchestratorIssueEmitter.emitFlowIssue(eventSubmitter, getDagId(), IssueSeverity.WARN,
        message + ": " + getDagNodeId());
  }
}
