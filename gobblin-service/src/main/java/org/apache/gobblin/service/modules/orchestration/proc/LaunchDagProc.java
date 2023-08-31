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
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagManagementTaskStreamImpl;
import org.apache.gobblin.service.modules.orchestration.DagManagerUtils;
import org.apache.gobblin.service.modules.orchestration.TimingEventUtils;
import org.apache.gobblin.service.modules.orchestration.task.LaunchDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.utils.FlowCompilationValidationHelper;


/**
 * An implementation for {@link DagProc} that launches a new job.
 */
@Slf4j
@Alpha
public class LaunchDagProc extends DagProc<Optional<Dag<JobExecutionPlan>>, Optional<Dag<JobExecutionPlan>>> {
  private final LaunchDagTask launchDagTask;
  private final FlowCompilationValidationHelper flowCompilationValidationHelper;
  private static final AtomicLong orchestrationDelayCounter = new AtomicLong(0);
  static {
    metricContext.register(
        metricContext.newContextAwareGauge(ServiceMetricNames.FLOW_ORCHESTRATION_DELAY, orchestrationDelayCounter::get));
  }

  public LaunchDagProc(LaunchDagTask launchDagTask, FlowCompilationValidationHelper flowCompilationValidationHelper) {
    this.launchDagTask = launchDagTask;
    this.flowCompilationValidationHelper = flowCompilationValidationHelper;
  }

  @Override
  protected Optional<Dag<JobExecutionPlan>> initialize(DagManagementStateStore dagManagementStateStore)
      throws IOException {
    try {
      DagActionStore.DagAction dagAction = this.launchDagTask.getDagAction();
      URI flowUri = FlowSpec.Utils.createFlowSpecUri(dagAction.getFlowId());
      FlowSpec flowSpec = dagManagementStateStore.getFlowSpec(flowUri);
      flowSpec.addProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, dagAction.getFlowExecutionId());
      return this.flowCompilationValidationHelper.createExecutionPlanIfValid(flowSpec).toJavaUtil();
    } catch (URISyntaxException | SpecNotFoundException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Optional<Dag<JobExecutionPlan>> act(DagManagementStateStore dagManagementStateStore, Optional<Dag<JobExecutionPlan>> dag)
      throws IOException {
    if (!dag.isPresent()) {
      log.warn("Dag with id " + this.launchDagTask.getDagId() + " could not be compiled.");
      // todo - add metrics
      return Optional.empty();
    }
    submitNextNodes(dagManagementStateStore, dag.get());
    log.info("Launch dagProc concluded actions for dagId : {}", this.launchDagTask.getDagId());
    return dag;
  }

  /**
   * Submit next set of Dag nodes in the provided Dag.
   */
   private void submitNextNodes(DagManagementStateStore dagManagementStateStore,
       Dag<JobExecutionPlan> dag) throws IOException {
     Set<Dag.DagNode<JobExecutionPlan>> nextNodes = DagManagerUtils.getNext(dag);

     //Submit jobs from the dag ready for execution.
     for (Dag.DagNode<JobExecutionPlan> dagNode : nextNodes) {
       submitJobToExecutor(dagManagementStateStore, dagNode);
       dagManagementStateStore.addDagNodeState(dagNode, this.launchDagTask.getDagId());
       log.info("Submitted job {} for dagId {}", DagManagerUtils.getJobName(dagNode), this.launchDagTask.getDagId());
     }

     //Checkpoint the dag state, it should have an updated value of dag nodes
     dagManagementStateStore.checkpointDag(dag);
   }

  /**
   * Submits a {@link JobSpec} to a {@link SpecExecutor}.
   */
  private void submitJobToExecutor(DagManagementStateStore dagManagementStateStore, Dag.DagNode<JobExecutionPlan> dagNode) {
    DagManagerUtils.incrementJobAttempt(dagNode);
    JobExecutionPlan jobExecutionPlan = DagManagerUtils.getJobExecutionPlan(dagNode);
    JobSpec jobSpec = DagManagerUtils.getJobSpec(dagNode);
    Map<String, String> jobMetadata = TimingEventUtils.getJobMetadata(Maps.newHashMap(), jobExecutionPlan);

    String specExecutorUri = DagManagerUtils.getSpecExecutorUri(dagNode);

    // Run this spec on selected executor
    SpecProducer<Spec> producer;
    try {
      producer = DagManagerUtils.getSpecProducer(dagNode);
      TimingEvent jobOrchestrationTimer = eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.JOB_ORCHESTRATED);

      // Increment job count before submitting the job onto the spec producer, in case that throws an exception.
      // By this point the quota is allocated, so it's imperative to increment as missing would introduce the potential to decrement below zero upon quota release.
      // Quota release is guaranteed, despite failure, because exception handling within would mark the job FAILED.
      // When the ensuing kafka message spurs DagManager processing, the quota is released and the counts decremented
      // Ensure that we do not double increment for flows that are retried
      if (DagManagerUtils.getJobExecutionPlan(dagNode).getCurrentAttempts() == 1) {
        DagManagementTaskStreamImpl.getDagManagerMetrics().incrementRunningJobMetrics(dagNode);
      }
      // Submit the job to the SpecProducer, which in turn performs the actual job submission to the SpecExecutor instance.
      // The SpecProducer implementations submit the job to the underlying executor and return when the submission is complete,
      // either successfully or unsuccessfully. To catch any exceptions in the job submission, the DagManagerThread
      // blocks (by calling Future#get()) until the submission is completed.
      dagManagementStateStore.tryAcquireQuota(Collections.singleton(dagNode));
      Future<?> addSpecFuture = producer.addSpec(jobSpec);
      dagNode.getValue().setJobFuture(com.google.common.base.Optional.of(addSpecFuture));
      addSpecFuture.get();
      jobExecutionPlan.setExecutionStatus(ExecutionStatus.ORCHESTRATED);
      jobMetadata.put(TimingEvent.METADATA_MESSAGE, producer.getExecutionLink(addSpecFuture, specExecutorUri));
      // Add serialized job properties as part of the orchestrated job event metadata
      jobMetadata.put(JobExecutionPlan.JOB_PROPS_KEY, dagNode.getValue().toString());
      jobOrchestrationTimer.stop(jobMetadata);
      log.info("Orchestrated job: {} on Executor: {}", DagManagerUtils.getFullyQualifiedJobName(dagNode), specExecutorUri);
      DagManagementTaskStreamImpl.getDagManagerMetrics().incrementJobsSentToExecutor(dagNode);
    } catch (Exception e) {
      TimingEvent jobFailedTimer = eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.JOB_FAILED);
      String message = "Cannot submit job " + DagManagerUtils.getFullyQualifiedJobName(dagNode) + " on executor " + specExecutorUri;
      log.error(message, e);
      jobMetadata.put(TimingEvent.METADATA_MESSAGE, message + " due to " + e.getMessage());
      if (jobFailedTimer != null) {
        jobFailedTimer.stop(jobMetadata);
      }
      try {
        // when there is no exception, quota will be released in job status monitor or re-evaluate dag proc
        dagManagementStateStore.releaseQuota(dagNode);
      } catch (IOException ex) {
        log.error("Could not release quota while handling e", ex);
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void sendNotification(Optional<Dag<JobExecutionPlan>> result, EventSubmitter eventSubmitter) {
    throw new UnsupportedOperationException("Not yet implemented");
  }
}
