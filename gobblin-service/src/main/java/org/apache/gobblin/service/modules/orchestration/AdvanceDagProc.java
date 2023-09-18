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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.runtime.api.MysqlMultiActiveLeaseArbiter;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.exception.MaybeRetryableException;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.JobStatus;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;

import static org.apache.gobblin.service.ExecutionStatus.CANCELLED;
import static org.apache.gobblin.service.ExecutionStatus.FAILED;
import static org.apache.gobblin.service.ExecutionStatus.RUNNING;
import static org.apache.gobblin.service.ExecutionStatus.valueOf;


/**
 * An implementation of {@link DagProc} dealing which advancing to the next node in the {@link Dag}.
 * This Dag Procedure will deal with pending Job statuses such as: PENDING, PENDING_RESUME, PENDING_RETRY
 * as well jobs that have reached an end state with statuses such as: COMPLETED, FAILED and CANCELLED.
 * Primarily, it will be responsible for polling the flow and job statuses and advancing to the next node in the dag.
 *
 */
@Slf4j
@Alpha
public class AdvanceDagProc extends DagProc {
  private DagManager.DagId advanceDagId;

  private Optional<DagActionStore> dagActionStore;
  private com.google.common.base.Optional<EventSubmitter> eventSubmitter;
  private DagStateStore dagStateStore;
  private DagStateStore failedDagStateStore;
  private MetricContext metricContext;
  private DagManagementStateStore dagManagementStateStore;
  private DagManagerMetrics dagManagerMetrics;
  private MultiActiveLeaseArbiter multiActiveLeaseArbiter;

  private final AtomicLong orchestrationDelay = new AtomicLong(0);

  private DagTaskStream dagTaskStream;
  private UserQuotaManager quotaManager = GobblinConstructorUtils.invokeConstructor(UserQuotaManager.class,
      ConfigUtils.getString(ConfigBuilder.create().build(), ServiceConfigKeys.QUOTA_MANAGER_CLASS, ServiceConfigKeys.DEFAULT_QUOTA_MANAGER),
      ConfigBuilder.create().build());

  public AdvanceDagProc() throws IOException {
    //TODO: add this to dagproc factory instead
    this.dagManagementStateStore = new DagManagementStateStore();
    this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(ConfigFactory.empty()), getClass());
    this.multiActiveLeaseArbiter = new MysqlMultiActiveLeaseArbiter(ConfigBuilder.create().build());
    this.eventSubmitter = Optional.of(new EventSubmitter.Builder(this.metricContext, "org.apache.gobblin.service").build());
    this.dagTaskStream = new DagTaskStream();
    dagManagerMetrics = new DagManagerMetrics();
  }
  @Override
  protected Object initialize() {
    String dagIdToAdvance = advanceDagId.toString();
    Dag<JobExecutionPlan> dag = this.dagManagementStateStore.getDagIdToDags().get(dagIdToAdvance);
    return dag;
  }

  @Override
  protected Object act(Object state) throws ExecutionException, InterruptedException, IOException {
    Dag<JobExecutionPlan> dag = (Dag<JobExecutionPlan>) state;
    Map<String, Set<Dag.DagNode<JobExecutionPlan>>> nextSubmitted = Maps.newHashMap();
    List<Dag.DagNode<JobExecutionPlan>> nodesToCleanUp = Lists.newArrayList();

    for(Dag.DagNode<JobExecutionPlan> dagNode : dag.getNodes()) {
      try {
        JobStatus jobStatus = dagTaskStream.pollJobStatus(dagNode);
        ExecutionStatus status = valueOf(jobStatus.getEventName());
        JobExecutionPlan jobExecutionPlan = dagNode.getValue();
        jobExecutionPlan.setExecutionStatus(status);
        switch (status) {
          case COMPLETE:
          case FAILED:
          case CANCELLED:
            nextSubmitted.putAll(onJobFinish(dagNode));
            nodesToCleanUp.add(dagNode);
            break;
          case PENDING:
            Dag<JobExecutionPlan> dagPendingToExecute = this.dagManagementStateStore.getJobToDag().get(dagNode);
            executePendingDag(dagPendingToExecute);
            break;
          case PENDING_RESUME:
            Dag<JobExecutionPlan> dagPendingResumeToExecute = this.dagManagementStateStore.getJobToDag().get(dagNode);
            String dagIdPendingResume = DagManagerUtils.generateDagId(dagPendingResumeToExecute).toString();
            if(this.dagManagementStateStore.getDagIdToResumingDags().containsKey(dagIdPendingResume)) {

              if (jobStatus != null && (jobStatus.getEventName().equals(FAILED.name()) || jobStatus.getEventName().equals(CANCELLED.name()))) {
                updatePendingResumeDagState(dagPendingResumeToExecute);
              }
            }
            executePendingDag(dagPendingResumeToExecute);
            break;
          case PENDING_RETRY:
            attemptJobRetry(jobStatus, dagNode);
            break;
          default:
            jobExecutionPlan.setExecutionStatus(RUNNING);
            break;
        }
      } catch (Exception e) {
        // Error occurred while processing dag, continue processing other dags assigned to this thread
        log.error(String.format("Exception caught in DagManager while processing dag %s due to ",
            DagManagerUtils.getFullyQualifiedDagName(dagNode)), e);
      }
    }
    for (Map.Entry<String, Set<Dag.DagNode<JobExecutionPlan>>> entry: nextSubmitted.entrySet()) {
      String dagId = entry.getKey();
      Set<Dag.DagNode<JobExecutionPlan>> dagNodes = entry.getValue();
      for (Dag.DagNode<JobExecutionPlan> dagNode: dagNodes) {
        this.dagManagementStateStore.addJobState(dagId, dagNode);
      }
    }

    for (Dag.DagNode<JobExecutionPlan> dagNode: nodesToCleanUp) {
      String dagId = DagManagerUtils.generateDagId(dagNode).toString();
      this.dagManagementStateStore.deleteJobState(dagId, dagNode);
    }
    return null;
  }

  @Override
  protected void sendNotification(Object result) throws MaybeRetryableException {
    Dag<JobExecutionPlan> dag = (Dag<JobExecutionPlan>) result;
    String dagFlowEvent = dag.getFlowEvent() == null ? TimingEvent.FlowTimings.FLOW_SUCCEEDED : dag.getFlowEvent();
    DagManagerUtils.emitFlowEvent(this.eventSubmitter, dag, dagFlowEvent);
  }

  /**
   * Method that defines the actions to be performed when a job finishes either successfully or with failure.
   * This method updates the state of the dag and performs clean up actions as necessary.
   */
  private Map<String, Set<Dag.DagNode<JobExecutionPlan>>> onJobFinish(Dag.DagNode<JobExecutionPlan> dagNode) throws IOException {
    Dag<JobExecutionPlan> dag = this.dagManagementStateStore.getJobToDag().get(dagNode);
    String dagId = DagManagerUtils.generateDagId(dag).toString();
    String jobName = DagManagerUtils.getFullyQualifiedJobName(dagNode);
    ExecutionStatus jobStatus = DagManagerUtils.getExecutionStatus(dagNode);
    log.info("Job {} of Dag {} has finished with status {}", jobName, dagId, jobStatus.name());
    // Only decrement counters and quota for jobs that actually ran on the executor, not from a GaaS side failure/skip event
    if (quotaManager.releaseQuota(dagNode)) {
      dagManagerMetrics.decrementRunningJobMetrics(dagNode);
    }

    switch (jobStatus) {
      case FAILED:
        dag.setMessage("Flow failed because job " + jobName + " failed");
        dag.setFlowEvent(TimingEvent.FlowTimings.FLOW_FAILED);
        dagManagerMetrics.incrementExecutorFailed(dagNode);
        return Maps.newHashMap();
      case CANCELLED:
        dag.setFlowEvent(TimingEvent.FlowTimings.FLOW_CANCELLED);
        return Maps.newHashMap();
      case COMPLETE:
        dagManagerMetrics.incrementExecutorSuccess(dagNode);
        return submitNext(dagId);
      default:
        log.warn("It should not reach here. Job status is unexpected.");
        return Maps.newHashMap();
    }
  }


  /**
   * Submit next set of Dag nodes in the Dag identified by the provided dagId
   * @param dagId The dagId that should be processed.
   * @return
   * @throws IOException
   */
  synchronized Map<String, Set<Dag.DagNode<JobExecutionPlan>>> submitNext(String dagId) throws IOException {
    Dag<JobExecutionPlan> dag = this.dagManagementStateStore.getDagIdToDags().get(dagId);
    Set<Dag.DagNode<JobExecutionPlan>> nextNodes = DagManagerUtils.getNext(dag);
    List<String> nextJobNames = new ArrayList<>();

    //Submit jobs from the dag ready for execution.
    for (Dag.DagNode<JobExecutionPlan> dagNode : nextNodes) {
      submitJob(dagNode);
      nextJobNames.add(DagManagerUtils.getJobName(dagNode));
    }
    log.info("Submitting next nodes for dagId {}, where next jobs to be submitted are {}", dagId, nextJobNames);
    //Checkpoint the dag state
    this.dagStateStore.writeCheckpoint(dag);

    Map<String, Set<Dag.DagNode<JobExecutionPlan>>> dagIdToNextJobs = Maps.newHashMap();
    dagIdToNextJobs.put(dagId, nextNodes);
    return dagIdToNextJobs;
  }
  /**
   * Submits a {@link JobSpec} to a {@link org.apache.gobblin.runtime.api.SpecExecutor}.
   */
  private void submitJob(Dag.DagNode<JobExecutionPlan> dagNode) {
    DagManagerUtils.incrementJobAttempt(dagNode);
    JobExecutionPlan jobExecutionPlan = DagManagerUtils.getJobExecutionPlan(dagNode);
    jobExecutionPlan.setExecutionStatus(RUNNING);
    JobSpec jobSpec = DagManagerUtils.getJobSpec(dagNode);
    Map<String, String> jobMetadata = TimingEventUtils.getJobMetadata(Maps.newHashMap(), jobExecutionPlan);

    String specExecutorUri = DagManagerUtils.getSpecExecutorUri(dagNode);

    // Run this spec on selected executor
    SpecProducer<Spec> producer;
    try {
      quotaManager.checkQuota(Collections.singleton(dagNode));

      producer = DagManagerUtils.getSpecProducer(dagNode);
      TimingEvent jobOrchestrationTimer = this.eventSubmitter.isPresent() ? this.eventSubmitter.get().
          getTimingEvent(TimingEvent.LauncherTimings.JOB_ORCHESTRATED) : null;

      // Increment job count before submitting the job onto the spec producer, in case that throws an exception.
      // By this point the quota is allocated, so it's imperative to increment as missing would introduce the potential to decrement below zero upon quota release.
      // Quota release is guaranteed, despite failure, because exception handling within would mark the job FAILED.
      // When the ensuing kafka message spurs DagManager processing, the quota is released and the counts decremented
      // Ensure that we do not double increment for flows that are retried
      if (dagNode.getValue().getCurrentAttempts() == 1) {
        dagManagerMetrics.incrementRunningJobMetrics(dagNode);
      }
      // Submit the job to the SpecProducer, which in turn performs the actual job submission to the SpecExecutor instance.
      // The SpecProducer implementations submit the job to the underlying executor and return when the submission is complete,
      // either successfully or unsuccessfully. To catch any exceptions in the job submission, the DagManagerThread
      // blocks (by calling Future#get()) until the submission is completed.
      Future<?> addSpecFuture = producer.addSpec(jobSpec);
      dagNode.getValue().setJobFuture(Optional.of(addSpecFuture));
      //Persist the dag
      this.dagStateStore.writeCheckpoint(this.dagManagementStateStore.getDagIdToDags().get(DagManagerUtils.generateDagId(dagNode).toString()));

      addSpecFuture.get();

      jobMetadata.put(TimingEvent.METADATA_MESSAGE, producer.getExecutionLink(addSpecFuture, specExecutorUri));
      // Add serialized job properties as part of the orchestrated job event metadata
      jobMetadata.put(JobExecutionPlan.JOB_PROPS_KEY, dagNode.getValue().toString());
      if (jobOrchestrationTimer != null) {
        jobOrchestrationTimer.stop(jobMetadata);
      }
      log.info("Orchestrated job: {} on Executor: {}", DagManagerUtils.getFullyQualifiedJobName(dagNode), specExecutorUri);
      this.dagManagerMetrics.incrementJobsSentToExecutor(dagNode);
    } catch (Exception e) {
      TimingEvent jobFailedTimer = this.eventSubmitter.isPresent() ? this.eventSubmitter.get().
          getTimingEvent(TimingEvent.LauncherTimings.JOB_FAILED) : null;
      String message = "Cannot submit job " + DagManagerUtils.getFullyQualifiedJobName(dagNode) + " on executor " + specExecutorUri;
      log.error(message, e);
      jobMetadata.put(TimingEvent.METADATA_MESSAGE, message + " due to " + e.getMessage());
      if (jobFailedTimer != null) {
        jobFailedTimer.stop(jobMetadata);
      }
    }
  }

  private void executePendingDag(Dag<JobExecutionPlan> dag) throws IOException {
    //Add Dag to the map of running dags
    String dagId = DagManagerUtils.generateDagId(dag).toString();
    log.info("Initializing Dag {}", DagManagerUtils.getFullyQualifiedDagName(dag));
    if (this.dagManagementStateStore.getDagIdToDags().containsKey(dagId)) {
      log.warn("Already tracking a dag with dagId {}, skipping.", dagId);
      return;
    }

    this.dagManagementStateStore.getDagIdToDags().put(dagId, dag);
    log.debug("Dag {} - determining if any jobs are already running.", DagManagerUtils.getFullyQualifiedDagName(dag));

    //A flag to indicate if the flow is already running.
    boolean isDagRunning = false;
    //Are there any jobs already in the running state? This check is for Dags already running
    //before a leadership change occurs.
    for (Dag.DagNode<JobExecutionPlan> dagNode : dag.getNodes()) {
      if (DagManagerUtils.getExecutionStatus(dagNode) == RUNNING) {
        this.dagManagementStateStore.addJobState(dagId, dagNode);
        //Update the running jobs counter.
        dagManagerMetrics.incrementRunningJobMetrics(dagNode);
        isDagRunning = true;
      }
    }

    FlowId flowId = DagManagerUtils.getFlowId(dag);
    this.dagManagerMetrics.registerFlowMetric(flowId, dag);

    log.debug("Dag {} submitting jobs ready for execution.", DagManagerUtils.getFullyQualifiedDagName(dag));
    //Determine the next set of jobs to run and submit them for execution
    Map<String, Set<Dag.DagNode<JobExecutionPlan>>> nextSubmitted = submitNext(dagId);
    for (Dag.DagNode<JobExecutionPlan> dagNode: nextSubmitted.get(dagId)) {
      this.dagManagementStateStore.addJobState(dagId, dagNode);
    }

    // Set flow status to running
    dag.setFlowEvent(TimingEvent.FlowTimings.FLOW_RUNNING);
    dagManagerMetrics.conditionallyMarkFlowAsState(flowId, DagManager.FlowState.RUNNING);

    // Report the orchestration delay the first time the Dag is initialized. Orchestration delay is defined as
    // the time difference between the instant when a flow first transitions to the running state and the instant
    // when the flow is submitted to Gobblin service.
    if (!isDagRunning) {
      //TODO: need to set orchestration delay
      this.orchestrationDelay.set(System.currentTimeMillis() - DagManagerUtils.getFlowExecId(dag));
    }

    log.info("Dag {} Initialization complete.", DagManagerUtils.getFullyQualifiedDagName(dag));
  }

  private void updatePendingResumeDagState(Dag<JobExecutionPlan> dag) throws IOException {
      this.dagStateStore.writeCheckpoint(dag);
      this.failedDagStateStore.cleanUp(dag);
      this.dagManagementStateStore.removeDagActionFromStore(DagManagerUtils.generateDagId(dag), DagActionStore.FlowActionType.RESUME);
      this.dagManagementStateStore.getFailedDagIds().remove(dag);
      this.dagManagementStateStore.getDagIdToResumingDags().remove(dag);
  }

  private void attemptJobRetry(JobStatus jobStatus, Dag.DagNode<JobExecutionPlan> dagNode) {
    try {
      if (jobStatus != null && jobStatus.isShouldRetry()) {
        log.info("Retrying job: {}, current attempts: {}, max attempts: {}", DagManagerUtils.getFullyQualifiedJobName(dagNode),
            jobStatus.getCurrentAttempts(), jobStatus.getMaxAttempts());
        this.dagManagementStateStore.getJobToDag().get(dagNode).setFlowEvent(null);
        submitJob(dagNode);
      }
    } catch (Exception ex) {
      // Error occurred while processing dag, continue processing other dags assigned to this thread
      log.error(String.format("Exception caught in DagManager while processing dag %s due to ",
          DagManagerUtils.getFullyQualifiedDagName(dagNode)), ex);
    }
  }
}
