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
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagProcessingEngine;
import org.apache.gobblin.service.modules.orchestration.DagUtils;
import org.apache.gobblin.service.modules.orchestration.TimingEventUtils;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.spec.SerializationConstants;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PropertiesUtils;

import static org.apache.gobblin.service.ExecutionStatus.*;


/**
 * A class to group together all the utility methods for {@link DagProc} derived class implementations.
 * Also refer {@link DagUtils} that contains utility methods for operations with {@link Dag}
 */
@Slf4j
public class DagProcUtils {

  /**
   * If there is a single job to run next, it runs it. If there are multiple jobs to run, it creates a
   * {@link DagActionStore.DagActionType#REEVALUATE} dag action for
   * each of them and those jobs will be launched in respective {@link ReevaluateDagProc}.
   */
  public static void submitNextNodes(DagManagementStateStore dagManagementStateStore, Dag<JobExecutionPlan> dag,
      Dag.DagId dagId) throws IOException {
    Set<Dag.DagNode<JobExecutionPlan>> nextNodes = DagUtils.getNext(dag);

    if (nextNodes.size() == 1) {
      Dag.DagNode<JobExecutionPlan> dagNode = nextNodes.iterator().next();
      DagProcUtils.submitJobToExecutor(dagManagementStateStore, dagNode, dagId);
    } else {
      for (Dag.DagNode<JobExecutionPlan> dagNode : nextNodes) {
        JobExecutionPlan jobExecutionPlan = dagNode.getValue();
        dagManagementStateStore.addJobDagAction(jobExecutionPlan.getFlowGroup(), jobExecutionPlan.getFlowName(),
            jobExecutionPlan.getFlowExecutionId(), jobExecutionPlan.getJobName(), DagActionStore.DagActionType.REEVALUATE);
      }
    }
  }

  /**
   * - submits a {@link JobSpec} to a {@link SpecExecutor}
   * - emits a {@link TimingEvent.LauncherTimings#JOB_ORCHESTRATED} {@link GobblinTrackingEvent}
   * that measures the time needed to submit the job to {@link SpecExecutor}
   * - increment running jobs counter for the {@link Dag}, the proxy user that submitted the job and the {@link SpecExecutor} job was sent to
   * - add updated dag node state to dagManagementStateStore
   */
  public static void submitJobToExecutor(DagManagementStateStore dagManagementStateStore, Dag.DagNode<JobExecutionPlan> dagNode,
      Dag.DagId dagId) {
    DagUtils.incrementJobAttempt(dagNode);
    JobExecutionPlan jobExecutionPlan = DagUtils.getJobExecutionPlan(dagNode);
    JobSpec jobSpec = DagUtils.getJobSpec(dagNode);
    Map<String, String> jobMetadata = TimingEventUtils.getJobMetadata(Maps.newHashMap(), jobExecutionPlan);

    String specExecutorUri = DagUtils.getSpecExecutorUri(dagNode);

    // Run this spec on selected executor
    SpecProducer<Spec> producer;
    try {
      producer = DagUtils.getSpecProducer(dagNode);
      // todo - submits an event with some other name, because it is not really orchestration happening here
      TimingEvent jobOrchestrationTimer = DagProc.eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.JOB_ORCHESTRATED);

      // Increment job count before submitting the job onto the spec producer, in case that throws an exception.
      // By this point the quota is allocated, so it's imperative to increment as missing would introduce the potential to decrement below zero upon quota release.
      // Quota release is guaranteed, despite failure, because exception handling within would mark the job FAILED.
      // When the ensuing kafka message spurs DagManager processing, the quota is released and the counts decremented
      // Ensure that we do not double increment for flows that are retried
      if (DagUtils.getJobExecutionPlan(dagNode).getCurrentAttempts() == 1) {
        dagManagementStateStore.getDagManagerMetrics().incrementRunningJobMetrics(dagNode);
      }
      // Submit the job to the SpecProducer, which in turn performs the actual job submission to the SpecExecutor instance.
      // The SpecProducer implementations submit the job to the underlying executor and return when the submission is complete,
      // either successfully or unsuccessfully. To catch any exceptions in the job submission, the DagManagerThread
      // blocks (by calling Future#get()) until the submission is completed.
      dagManagementStateStore.tryAcquireQuota(Collections.singleton(dagNode));

      Future<?> addSpecFuture = producer.addSpec(jobSpec);
      // todo - we should add future.get() instead of the complete future into the JobExecutionPlan
      dagNode.getValue().setJobFuture(Optional.of(addSpecFuture));
      addSpecFuture.get();
      jobExecutionPlan.setExecutionStatus(ExecutionStatus.ORCHESTRATED);
      jobMetadata.put(TimingEvent.METADATA_MESSAGE, producer.getExecutionLink(addSpecFuture, specExecutorUri));
      // Add serialized job properties as part of the orchestrated job event metadata
      jobMetadata.put(JobExecutionPlan.JOB_PROPS_KEY, PropertiesUtils.serialize(jobSpec.getConfigAsProperties()));
      jobMetadata.put(SerializationConstants.FLOW_START_TIME_KEY, String.valueOf(dagNode.getValue().getFlowStartTime()));
      jobOrchestrationTimer.stop(jobMetadata);
      log.info("Orchestrated job: {} on Executor: {}", DagUtils.getFullyQualifiedJobName(dagNode), specExecutorUri);
      dagManagementStateStore.getDagManagerMetrics().incrementJobsSentToExecutor(dagNode);
      dagManagementStateStore.updateDagNode(dagNode);
      sendEnforceJobStartDeadlineDagAction(dagManagementStateStore, dagNode);
    } catch (Exception e) {
      TimingEvent jobFailedTimer = DagProc.eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.JOB_FAILED);
      String message = "Cannot submit job " + DagUtils.getFullyQualifiedJobName(dagNode) + " on executor " + specExecutorUri;
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

    log.info("Submitted job {} for dagId {}", DagUtils.getJobName(dagNode), dagId);
  }

  public static void cancelDagNode(Dag.DagNode<JobExecutionPlan> dagNodeToCancel, DagManagementStateStore dagManagementStateStore) throws IOException {
    Properties cancelJobArgs = new Properties();
    String serializedFuture = null;

    if (dagNodeToCancel.getValue().getJobSpec().getConfig().hasPath(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)) {
      cancelJobArgs.setProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY,
          dagNodeToCancel.getValue().getJobSpec().getConfig().getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY));
    }

    try {
      if (dagNodeToCancel.getValue().getJobFuture().isPresent()) {
        Future<?> future = dagNodeToCancel.getValue().getJobFuture().get();
        serializedFuture = DagUtils.getSpecProducer(dagNodeToCancel).serializeAddSpecResponse(future);
        cancelJobArgs.put(ConfigurationKeys.SPEC_PRODUCER_SERIALIZED_FUTURE, serializedFuture);
      } else {
        log.warn("No Job future when canceling DAG node - {}", dagNodeToCancel.getValue().getId());
      }
      DagUtils.getSpecProducer(dagNodeToCancel).cancelJob(dagNodeToCancel.getValue().getJobSpec().getUri(), cancelJobArgs).get();
      sendJobCancellationEvent(dagNodeToCancel);
      log.info("Cancelled dag node {}, spec_producer_future {}", dagNodeToCancel.getValue().getId(), serializedFuture);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public static void cancelDag(Dag<JobExecutionPlan> dag, DagManagementStateStore dagManagementStateStore) throws IOException {
    List<Dag.DagNode<JobExecutionPlan>> dagNodesToCancel = dag.getNodes();
    log.info("Found {} DagNodes to cancel (DagId {}).", dagNodesToCancel.size(), DagUtils.generateDagId(dag));

    for (Dag.DagNode<JobExecutionPlan> dagNodeToCancel : dagNodesToCancel) {
      DagProcUtils.cancelDagNode(dagNodeToCancel, dagManagementStateStore);
    }
  }

  private static void sendJobCancellationEvent(Dag.DagNode<JobExecutionPlan> dagNodeToCancel) {
    JobExecutionPlan jobExecutionPlan = dagNodeToCancel.getValue();
    Map<String, String> jobMetadata = TimingEventUtils.getJobMetadata(Maps.newHashMap(), jobExecutionPlan);
    DagProc.eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.JOB_CANCEL).stop(jobMetadata);
  }

  /**
   * Sets {@link Dag#flowEvent} and emits a {@link GobblinTrackingEvent} of the provided
   * flow event type.
   */
  public static void setAndEmitFlowEvent(EventSubmitter eventSubmitter, Dag<JobExecutionPlan> dag, String flowEvent) {
    if (!dag.isEmpty()) {
      // Every dag node will contain the same flow metadata
      Config config = DagUtils.getDagJobConfig(dag);
      Map<String, String> flowMetadata = TimingEventUtils.getFlowMetadata(config);
      dag.setFlowEvent(flowEvent);

      if (dag.getMessage() != null) {
        flowMetadata.put(TimingEvent.METADATA_MESSAGE, dag.getMessage());
      }

      eventSubmitter.getTimingEvent(flowEvent).stop(flowMetadata);
    }
  }

  private static void sendEnforceJobStartDeadlineDagAction(DagManagementStateStore dagManagementStateStore, Dag.DagNode<JobExecutionPlan> dagNode)
      throws IOException {
    DagActionStore.DagAction dagAction = new DagActionStore.DagAction(dagNode.getValue().getFlowGroup(),
        dagNode.getValue().getFlowName(), dagNode.getValue().getFlowExecutionId(), dagNode.getValue().getJobName(),
        DagActionStore.DagActionType.ENFORCE_JOB_START_DEADLINE);
    try {
      dagManagementStateStore.addDagAction(dagAction);
    } catch (IOException e) {
      if (e.getCause() != null && e.getCause() instanceof SQLIntegrityConstraintViolationException) {
        // delete old dag action and have a new deadline dag proc with the new deadline time
        dagManagementStateStore.deleteDagAction(dagAction);
        log.warn("Duplicate ENFORCE_JOB_START_DEADLINE Dag Action is being created. Ignoring... " + e.getMessage());
        dagManagementStateStore.addDagAction(dagAction);
      }
    }
  }

  public static void sendEnforceFlowFinishDeadlineDagAction(DagManagementStateStore dagManagementStateStore, DagActionStore.DagAction launchDagAction)
      throws IOException {
    DagActionStore.DagAction dagAction = DagActionStore.DagAction.forFlow(launchDagAction.getFlowGroup(), launchDagAction.getFlowName(),
        launchDagAction.getFlowExecutionId(), DagActionStore.DagActionType.ENFORCE_FLOW_FINISH_DEADLINE);
    try {
      dagManagementStateStore.addDagAction(dagAction);
    } catch (IOException e) {
      if (e.getCause() != null && e.getCause() instanceof SQLIntegrityConstraintViolationException) {
        dagManagementStateStore.deleteDagAction(dagAction);
        log.warn("Duplicate ENFORCE_FLOW_FINISH_DEADLINE Dag Action is being created. Ignoring... " + e.getMessage());
        dagManagementStateStore.addDagAction(dagAction);
      }
    }
  }

  public static long getDefaultJobStartDeadline(Config config) {
    TimeUnit jobStartTimeUnit = TimeUnit.valueOf(ConfigUtils.getString(
        config, ConfigurationKeys.GOBBLIN_JOB_START_DEADLINE_TIME_UNIT, ConfigurationKeys.FALLBACK_GOBBLIN_JOB_START_DEADLINE_TIME_UNIT));
    return jobStartTimeUnit.toMillis(ConfigUtils.getLong(config, ConfigurationKeys.GOBBLIN_JOB_START_DEADLINE_TIME,
        ConfigurationKeys.FALLBACK_GOBBLIN_JOB_START_DEADLINE_TIME));
  }

  public static boolean isJobLevelStatus(String jobName) {
    return !jobName.equals(JobStatusRetriever.NA_KEY);
  }

  public static void removeEnforceJobStartDeadlineDagAction(DagManagementStateStore dagManagementStateStore, String flowGroup,
      String flowName, long flowExecutionId, String jobName) {
    DagActionStore.DagAction enforceJobStartDeadlineDagAction = new DagActionStore.DagAction(flowGroup, flowName,
        flowExecutionId, jobName, DagActionStore.DagActionType.ENFORCE_JOB_START_DEADLINE);
    log.info("Deleting dag action {}", enforceJobStartDeadlineDagAction);
    // todo - add metrics

    try {
      dagManagementStateStore.deleteDagAction(enforceJobStartDeadlineDagAction);
    } catch (IOException e) {
      log.warn("Failed to delete dag action {}", enforceJobStartDeadlineDagAction);
    }
  }

  public static void removeFlowFinishDeadlineDagAction(DagManagementStateStore dagManagementStateStore, Dag.DagId dagId) {
    DagActionStore.DagAction enforceFlowFinishDeadlineDagAction = DagActionStore.DagAction.forFlow(dagId.getFlowGroup(),
        dagId.getFlowName(), dagId.getFlowExecutionId(),
        DagActionStore.DagActionType.ENFORCE_FLOW_FINISH_DEADLINE);
    log.info("Deleting dag action {}", enforceFlowFinishDeadlineDagAction);
    // todo - add metrics

    try {
      dagManagementStateStore.deleteDagAction(enforceFlowFinishDeadlineDagAction);
    } catch (IOException e) {
      log.warn("Failed to delete dag action {}", enforceFlowFinishDeadlineDagAction);
    }
  }

  /**
   * Returns true if all dag nodes are finished, and it is not possible to run any new dag node.
   * If failure option is {@link org.apache.gobblin.service.modules.orchestration.DagProcessingEngine.FailureOption#FINISH_RUNNING},
   * no new jobs should be orchestrated, so even if some job can run, dag should be considered finished.
   */
  public static boolean isDagFinished(Dag<JobExecutionPlan> dag) {
    /*
    The algo for this method is that it adds all the dag nodes into a set `canRun` that signifies all the nodes that can
    run in this dag. This also includes all the jobs that are completed. It scans all the nodes and if the node is
    completed it adds it to the `completed` set; if the node is failed/cancelled it removes all its dependant nodes from
    `canRun` set. In the end if there are more nodes that "canRun" than "completed", dag is not finished.
    For FINISH_RUNNING failure option, there is an additional condition that all the remaining `canRun` jobs should already
    be running/orchestrated/pending_retry/pending_resume. Basically they should already be out of PENDING state, in order
    for dag to be considered "NOT FINISHED".
     */
    List<Dag.DagNode<JobExecutionPlan>> nodes = dag.getNodes();
    Set<Dag.DagNode<JobExecutionPlan>> canRun = new HashSet<>(nodes);
    Set<Dag.DagNode<JobExecutionPlan>> completed = new HashSet<>();
    boolean anyFailure = false;

    for (Dag.DagNode<JobExecutionPlan> node : nodes) {
      if (!canRun.contains(node)) {
        continue;
      }
      ExecutionStatus status = node.getValue().getExecutionStatus();
      if (status == ExecutionStatus.FAILED || status == ExecutionStatus.CANCELLED) {
        anyFailure = true;
        removeDescendantsFromCanRun(node, dag, canRun);
        completed.add(node);
      } else if (status == ExecutionStatus.COMPLETE) {
        completed.add(node);
      } else if (status == ExecutionStatus.PENDING) {
        // Remove PENDING node if its parents are not in canRun, this means remove the pending nodes also from canRun set
        // if its parents cannot run
        if (!areAllParentsInCanRun(node, canRun)) {
          canRun.remove(node);
        }
      } else if (!(status == COMPILED || status == PENDING_RESUME || status == PENDING_RETRY || status == ORCHESTRATED ||
                  status == RUNNING)) {
        throw new RuntimeException("Unexpected status " + status + " for dag node " + node);
      }
    }

    assert canRun.size() >= completed.size();

    DagProcessingEngine.FailureOption failureOption = DagUtils.getFailureOption(dag);

    if (!anyFailure || failureOption == DagProcessingEngine.FailureOption.FINISH_ALL_POSSIBLE) {
      // In the end, check if there are more nodes in canRun than completed
      return canRun.size() == completed.size();
    } else if (failureOption == DagProcessingEngine.FailureOption.FINISH_RUNNING) {
      // if all the remaining jobs are pending/compiled (basically not started yet) return true
      canRun.removeAll(completed);
      return canRun.stream().allMatch(node -> (node.getValue().getExecutionStatus() == PENDING || node.getValue().getExecutionStatus() == COMPILED));
    } else {
      throw new RuntimeException("Unexpected failure option " + failureOption);
    }
  }

  private static void removeDescendantsFromCanRun(Dag.DagNode<JobExecutionPlan> node, Dag<JobExecutionPlan> dag,
      Set<Dag.DagNode<JobExecutionPlan>> canRun) {
    for (Dag.DagNode<JobExecutionPlan> child : dag.getChildren(node)) {
      canRun.remove(child);
      removeDescendantsFromCanRun(child, dag, canRun); // Recursively remove all descendants
    }
  }

  private static boolean areAllParentsInCanRun(Dag.DagNode<JobExecutionPlan> node,
      Set<Dag.DagNode<JobExecutionPlan>> canRun) {
    return node.getParentNodes() == null || canRun.containsAll(node.getParentNodes());
  }

  public static String calcFlowStatus(Dag<JobExecutionPlan> dag) {
    Set<ExecutionStatus> jobsStatuses = dag.getNodes().stream().map(node -> node.getValue().getExecutionStatus())
        .collect(Collectors.toSet());

    if (jobsStatuses.contains(FAILED)) {
      return TimingEvent.FlowTimings.FLOW_FAILED;
    } else if (jobsStatuses.contains(CANCELLED)) {
      return TimingEvent.FlowTimings.FLOW_CANCELLED;
    } else if (jobsStatuses.contains(PENDING_RESUME)) {
      return TimingEvent.FlowTimings.FLOW_PENDING_RESUME;
    } else if (jobsStatuses.stream().allMatch(jobStatus -> jobStatus == COMPLETE)) {
      return TimingEvent.FlowTimings.FLOW_SUCCEEDED;
    } else {
      return TimingEvent.FlowTimings.FLOW_RUNNING;
    }
  }
}
