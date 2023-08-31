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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.service.modules.orchestration.DagManagerUtils;
import org.apache.gobblin.service.modules.orchestration.NewDagManager;
import org.apache.gobblin.service.modules.orchestration.TimingEventUtils;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.service.modules.orchestration.task.LaunchDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;


/**
 * An implementation of {@link DagProc} for launching {@link DagTask}.
 */
@Slf4j
@Alpha
public class LaunchDagProc extends DagProc<Optional<Dag<JobExecutionPlan>>, Optional<Dag<JobExecutionPlan>>> {
  private final LaunchDagTask launchDagTask;
  NewDagManager newDagManager;
  private final AtomicLong orchestrationDelay;

  public LaunchDagProc(LaunchDagTask launchDagTask, NewDagManager newDagManager) {
    this.launchDagTask = launchDagTask;
    this.newDagManager = newDagManager;
    this.orchestrationDelay = new AtomicLong(0);
    ContextAwareGauge<Long> orchestrationDelayMetric = metricContext.newContextAwareGauge(ServiceMetricNames.FLOW_ORCHESTRATION_DELAY,
        orchestrationDelay::get);
    metricContext.register(orchestrationDelayMetric);
  }

  @Override
  protected Optional<Dag<JobExecutionPlan>> initialize(DagManagementStateStore dagManagementStateStore) throws IOException {
    Dag<JobExecutionPlan> dag = dagManagementStateStore.getDag(this.launchDagTask.getDagId().toString());
    return Optional.of(dag);
  }

  @Override
  protected Optional<Dag<JobExecutionPlan>> act(DagManagementStateStore dagManagementStateStore, Optional<Dag<JobExecutionPlan>> dag) throws IOException {
    if (!dag.isPresent()) {
      log.warn("No dag with id " + this.launchDagTask.getDagId() + " found to launch");
      return Optional.empty();
    }
    initializeDag(dag.get(), dagManagementStateStore);
    return dag;
  }

  @Override
  protected void commit(DagManagementStateStore dagManagementStateStore, Optional<Dag<JobExecutionPlan>> dag) {
    if (dag.isPresent()) {
      FlowId flowId = DagManagerUtils.getFlowId(dag.get());
      NewDagManager.getDagManagerMetrics().registerFlowMetric(flowId, dag.get());
      DagManagerUtils.submitPendingExecStatus(dag.get(), this.eventSubmitter);
      // Set flow status to running
      DagManagerUtils.emitFlowEvent(this.eventSubmitter, dag.get(), TimingEvent.FlowTimings.FLOW_RUNNING);
      NewDagManager.getDagManagerMetrics().conditionallyMarkFlowAsState(flowId, DagManager.FlowState.RUNNING);
    }
  }

  protected void initializeDag(Dag<JobExecutionPlan> dag, DagManagementStateStore dagManagementStateStore)
      throws IOException {
    //Add Dag to the map of running dags
    String dagId = DagManagerUtils.generateDagId(dag).toString();
    log.info("Initializing Dag {}", DagManagerUtils.getFullyQualifiedDagName(dag));

    //A flag to indicate if the flow is already running.
    boolean isDagRunning = false;
    //Are there any jobs already in the running state? This check is for Dags already running
    //before a leadership change occurs.
    for (Dag.DagNode<JobExecutionPlan> dagNode : dag.getNodes()) {
      if (DagManagerUtils.getExecutionStatus(dagNode) == ExecutionStatus.RUNNING) {
        dagManagementStateStore.addDagNodeState(dagId, dagNode);
        //Update the running jobs counter.
        NewDagManager.getDagManagerMetrics().incrementRunningJobMetrics(dagNode);
        isDagRunning = true;
      }
    }

    log.debug("Dag {} submitting jobs ready for execution.", DagManagerUtils.getFullyQualifiedDagName(dag));
    //Determine the next set of jobs to run and submit them for execution
    Map<String, Set<Dag.DagNode<JobExecutionPlan>>> nextSubmitted = submitNext(dagManagementStateStore, dagId);
    for (Dag.DagNode<JobExecutionPlan> dagNode : nextSubmitted.get(dagId)) {
      dagManagementStateStore.addDagNodeState(dagId, dagNode);
    }

    // Report the orchestration delay the first time the Dag is initialized. Orchestration delay is defined as
    // the time difference between the instant when a flow first transitions to the running state and the instant
    // when the flow is submitted to Gobblin service.
    if (!isDagRunning) {
      this.orchestrationDelay.set(System.currentTimeMillis() - DagManagerUtils.getFlowExecId(dag));
    }

    log.info("Dag {} Initialization complete.", DagManagerUtils.getFullyQualifiedDagName(dag));
  }

  /**
   * Submit next set of Dag nodes in the Dag identified by the provided dagId
   * @param dagId The dagId that should be processed.
   * @return
   * @throws IOException
   */
  // todo - convert to return set only
  synchronized Map<String, Set<Dag.DagNode<JobExecutionPlan>>> submitNext(DagManagementStateStore dagManagementStateStore,
      String dagId) throws IOException {
    Dag<JobExecutionPlan> dag = dagManagementStateStore.getDag(dagId);
    Set<Dag.DagNode<JobExecutionPlan>> nextNodes = DagManagerUtils.getNext(dag);
    List<String> nextJobNames = new ArrayList<>();

    //Submit jobs from the dag ready for execution.
    for (Dag.DagNode<JobExecutionPlan> dagNode : nextNodes) {
      submitJob(dagManagementStateStore, dagNode);
      nextJobNames.add(DagManagerUtils.getJobName(dagNode));
    }

    log.info("Submitting next nodes for dagId {}, where next jobs to be submitted are {}", dagId, nextJobNames);
    //Checkpoint the dag state
    newDagManager.getDagStateStore().writeCheckpoint(dag);

    Map<String, Set<Dag.DagNode<JobExecutionPlan>>> dagIdToNextJobs = Maps.newHashMap();
    dagIdToNextJobs.put(dagId, nextNodes);
    return dagIdToNextJobs;
  }

  /**
   * Submits a {@link JobSpec} to a {@link SpecExecutor}.
   */
  private void submitJob(DagManagementStateStore dagManagementStateStore, Dag.DagNode<JobExecutionPlan> dagNode) {
    DagManagerUtils.incrementJobAttempt(dagNode);
    JobExecutionPlan jobExecutionPlan = DagManagerUtils.getJobExecutionPlan(dagNode);
    jobExecutionPlan.setExecutionStatus(ExecutionStatus.RUNNING);
    JobSpec jobSpec = DagManagerUtils.getJobSpec(dagNode);
    Map<String, String> jobMetadata = TimingEventUtils.getJobMetadata(Maps.newHashMap(), jobExecutionPlan);

    String specExecutorUri = DagManagerUtils.getSpecExecutorUri(dagNode);

    // Run this spec on selected executor
    SpecProducer<Spec> producer;
    try {
      this.newDagManager.getUserQuotaManager().checkQuota(Collections.singleton(dagNode));
      producer = DagManagerUtils.getSpecProducer(dagNode);
      TimingEvent jobOrchestrationTimer = this.eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.JOB_ORCHESTRATED);

      // Increment job count before submitting the job onto the spec producer, in case that throws an exception.
      // By this point the quota is allocated, so it's imperative to increment as missing would introduce the potential to decrement below zero upon quota release.
      // Quota release is guaranteed, despite failure, because exception handling within would mark the job FAILED.
      // When the ensuing kafka message spurs DagManager processing, the quota is released and the counts decremented
      // Ensure that we do not double increment for flows that are retried
      if (dagNode.getValue().getCurrentAttempts() == 1) {
        NewDagManager.getDagManagerMetrics().incrementRunningJobMetrics(dagNode);
      }
      // Submit the job to the SpecProducer, which in turn performs the actual job submission to the SpecExecutor instance.
      // The SpecProducer implementations submit the job to the underlying executor and return when the submission is complete,
      // either successfully or unsuccessfully. To catch any exceptions in the job submission, the DagManagerThread
      // blocks (by calling Future#get()) until the submission is completed.
      Future<?> addSpecFuture = producer.addSpec(jobSpec);
      dagNode.getValue().setJobFuture(com.google.common.base.Optional.of(addSpecFuture));
      //Persist the dag
      newDagManager.getDagStateStore().writeCheckpoint(dagManagementStateStore.getDag(DagManagerUtils.generateDagId(dagNode).toString()));

      addSpecFuture.get();

      jobMetadata.put(TimingEvent.METADATA_MESSAGE, producer.getExecutionLink(addSpecFuture, specExecutorUri));
      // Add serialized job properties as part of the orchestrated job event metadata
      jobMetadata.put(JobExecutionPlan.JOB_PROPS_KEY, dagNode.getValue().toString());
      jobOrchestrationTimer.stop(jobMetadata);
      log.info("Orchestrated job: {} on Executor: {}", DagManagerUtils.getFullyQualifiedJobName(dagNode), specExecutorUri);
      NewDagManager.getDagManagerMetrics().incrementJobsSentToExecutor(dagNode);
    } catch (Exception e) {
      TimingEvent jobFailedTimer = this.eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.JOB_FAILED);
      String message = "Cannot submit job " + DagManagerUtils.getFullyQualifiedJobName(dagNode) + " on executor " + specExecutorUri;
      log.error(message, e);
      jobMetadata.put(TimingEvent.METADATA_MESSAGE, message + " due to " + e.getMessage());
      if (jobFailedTimer != null) {
        jobFailedTimer.stop(jobMetadata);
      }
    }
  }
}
