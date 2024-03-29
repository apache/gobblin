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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Future;

import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.service.modules.orchestration.DagManagerUtils;
import org.apache.gobblin.service.modules.orchestration.TimingEventUtils;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;


/**
 * A class to group together all the utility methods for {@link DagProc} derived class implementations.
 */
@Slf4j
public class DagProcUtils {

  /**
   * - submits a {@link JobSpec} to a {@link SpecExecutor}
   * - emits a {@link TimingEvent.LauncherTimings#JOB_ORCHESTRATED} {@link org.apache.gobblin.metrics.GobblinTrackingEvent}
   * that measures the time needed to submit the job to {@link SpecExecutor}
   * - increment running jobs counter for the {@link Dag}, the proxy user that submitted the job and the {@link SpecExecutor} job was sent to
   * - add updated dag node state to dagManagementStateStore
   */
  public static void submitJobToExecutor(DagManagementStateStore dagManagementStateStore, Dag.DagNode<JobExecutionPlan> dagNode,
      DagManager.DagId dagId) {
    DagManagerUtils.incrementJobAttempt(dagNode);
    JobExecutionPlan jobExecutionPlan = DagManagerUtils.getJobExecutionPlan(dagNode);
    JobSpec jobSpec = DagManagerUtils.getJobSpec(dagNode);
    Map<String, String> jobMetadata = TimingEventUtils.getJobMetadata(Maps.newHashMap(), jobExecutionPlan);

    String specExecutorUri = DagManagerUtils.getSpecExecutorUri(dagNode);

    // Run this spec on selected executor
    SpecProducer<Spec> producer;
    try {
      producer = DagManagerUtils.getSpecProducer(dagNode);
      // todo - submits an event with some other name, because it is not really orchestration happening here
      TimingEvent jobOrchestrationTimer = DagProc.eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.JOB_ORCHESTRATED);

      // Increment job count before submitting the job onto the spec producer, in case that throws an exception.
      // By this point the quota is allocated, so it's imperative to increment as missing would introduce the potential to decrement below zero upon quota release.
      // Quota release is guaranteed, despite failure, because exception handling within would mark the job FAILED.
      // When the ensuing kafka message spurs DagManager processing, the quota is released and the counts decremented
      // Ensure that we do not double increment for flows that are retried
      if (DagManagerUtils.getJobExecutionPlan(dagNode).getCurrentAttempts() == 1) {
        dagManagementStateStore.getDagManagerMetrics().incrementRunningJobMetrics(dagNode);
      }
      // Submit the job to the SpecProducer, which in turn performs the actual job submission to the SpecExecutor instance.
      // The SpecProducer implementations submit the job to the underlying executor and return when the submission is complete,
      // either successfully or unsuccessfully. To catch any exceptions in the job submission, the DagManagerThread
      // blocks (by calling Future#get()) until the submission is completed.
      dagManagementStateStore.tryAcquireQuota(Collections.singleton(dagNode));
      Future<?> addSpecFuture = producer.addSpec(jobSpec);
      // todo - we should add future.get() instead of the complete future into the JobExecutionPlan
      dagNode.getValue().setJobFuture(com.google.common.base.Optional.of(addSpecFuture));
      addSpecFuture.get();
      jobExecutionPlan.setExecutionStatus(ExecutionStatus.ORCHESTRATED);
      jobMetadata.put(TimingEvent.METADATA_MESSAGE, producer.getExecutionLink(addSpecFuture, specExecutorUri));
      // Add serialized job properties as part of the orchestrated job event metadata
      jobMetadata.put(JobExecutionPlan.JOB_PROPS_KEY, dagNode.getValue().toString());
      jobOrchestrationTimer.stop(jobMetadata);
      log.info("Orchestrated job: {} on Executor: {}", DagManagerUtils.getFullyQualifiedJobName(dagNode), specExecutorUri);
      dagManagementStateStore.getDagManagerMetrics().incrementJobsSentToExecutor(dagNode);
      dagManagementStateStore.addDagNodeState(dagNode, dagId);
    } catch (Exception e) {
      TimingEvent jobFailedTimer = DagProc.eventSubmitter.getTimingEvent(TimingEvent.LauncherTimings.JOB_FAILED);
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
}
