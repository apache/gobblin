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

package org.apache.gobblin.service;

import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.annotations.Context;
import com.linkedin.restli.server.annotations.Finder;
import com.linkedin.restli.server.annotations.QueryParam;
import java.util.Collections;
import java.util.Iterator;

import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.resources.ComplexKeyResourceTemplate;

import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.service.monitoring.FlowStatusGenerator;


/**
 * Resource for handling flow status requests
 */
@RestLiCollection(name = "flowstatuses", namespace = "org.apache.gobblin.service", keyName = "id")
public class FlowStatusResource extends ComplexKeyResourceTemplate<FlowStatusId, EmptyRecord, FlowStatus> {
  private static final Logger LOG = LoggerFactory.getLogger(FlowStatusResource.class);
  public static final String FLOW_STATUS_GENERATOR_INJECT_NAME = "FlowStatusGenerator";
  public static final String MESSAGE_SEPARATOR = ", ";

  @Inject @javax.inject.Inject @javax.inject.Named(FLOW_STATUS_GENERATOR_INJECT_NAME)
  FlowStatusGenerator _flowStatusGenerator;

  public FlowStatusResource() {}

  /**
   * Retrieve the FlowStatus with the given key
   * @param key flow status id key containing group name and flow name
   * @return {@link FlowStatus} with flow status for the latest execution of the flow
   */
  @Override
  public FlowStatus get(ComplexResourceKey<FlowStatusId, EmptyRecord> key) {
    String flowGroup = key.getKey().getFlowGroup();
    String flowName = key.getKey().getFlowName();
    long flowExecutionId = key.getKey().getFlowExecutionId();

    LOG.info("Get called with flowGroup " + flowGroup + " flowName " + flowName + " flowExecutionId " + flowExecutionId);

    org.apache.gobblin.service.monitoring.FlowStatus flowStatus =
        _flowStatusGenerator.getFlowStatus(flowName, flowGroup, flowExecutionId);

    // this returns null to raise a 404 error if flowStatus is null
    return convertFlowStatus(flowStatus);
  }

  @Finder("latestFlowStatus")
  public List<FlowStatus> getLatestFlowStatus(@Context PagingContext context,
      @QueryParam("flowId") FlowId flowId) {
    LOG.info("getLatestFlowStatus called with flowGroup " + flowId.getFlowGroup() + " flowName " + flowId.getFlowName());

    org.apache.gobblin.service.monitoring.FlowStatus latestFlowStatus =
        _flowStatusGenerator.getLatestFlowStatus(flowId.getFlowName(), flowId.getFlowGroup());

    if (latestFlowStatus != null) {
      return Collections.singletonList(convertFlowStatus(latestFlowStatus));
    }

    // will return 404 status code
    return null;
  }

  /**
   * Forms a {@link org.apache.gobblin.service.FlowStatus} from a {@link org.apache.gobblin.service.monitoring.FlowStatus}
   * @param monitoringFlowStatus
   * @return a {@link org.apache.gobblin.service.FlowStatus} converted from a {@link org.apache.gobblin.service.monitoring.FlowStatus}
   */
  private FlowStatus convertFlowStatus(org.apache.gobblin.service.monitoring.FlowStatus monitoringFlowStatus) {
    if (monitoringFlowStatus == null) {
      return null;
    }

    Iterator<org.apache.gobblin.service.monitoring.JobStatus> jobStatusIter = monitoringFlowStatus.getJobStatusIterator();
    JobStatusArray jobStatusArray = new JobStatusArray();
    FlowId flowId = new FlowId().setFlowName(monitoringFlowStatus.getFlowName())
        .setFlowGroup(monitoringFlowStatus.getFlowGroup());
    long flowStartTime = Long.MAX_VALUE;
    long flowEndTime = -1L;
    // flow execution status is complete unless job status indicates it is running or failed
    ExecutionStatus flowExecutionStatus = ExecutionStatus.COMPLETE;
    StringBuffer flowMessagesStringBuffer = new StringBuffer();

    while (jobStatusIter.hasNext()) {
      org.apache.gobblin.service.monitoring.JobStatus queriedJobStatus = jobStatusIter.next();
      JobStatus jobStatus = new JobStatus();

      jobStatus.setFlowId(flowId)
          .setJobId(new JobId().setJobName(queriedJobStatus.getJobName())
              .setJobGroup(queriedJobStatus.getJobGroup()))
          .setExecutionStatistics(new JobStatistics()
              .setExecutionStartTime(queriedJobStatus.getStartTime())
              .setExecutionEndTime(queriedJobStatus.getEndTime())
              .setProcessedCount(queriedJobStatus.getProcessedCount()))
          .setExecutionStatus(timingEventToStatus(queriedJobStatus.getEventName()))
          .setMessage(queriedJobStatus.getMessage())
          .setJobState(new JobState().setLowWatermark(queriedJobStatus.getLowWatermark()).
              setHighWatermark(queriedJobStatus.getHighWatermark()));

      jobStatusArray.add(jobStatus);

      if (queriedJobStatus.getStartTime() < flowStartTime){
        flowStartTime = queriedJobStatus.getStartTime();
      }

      // TODO: end time should be left as -1 if not all jobs have started for the flow
      // need to have flow job count to determine this
      if (queriedJobStatus.getEndTime() > flowEndTime){
        flowEndTime = queriedJobStatus.getEndTime();
      }

      if (!queriedJobStatus.getMessage().isEmpty()) {
        flowMessagesStringBuffer.append(queriedJobStatus.getMessage());
        flowMessagesStringBuffer.append(MESSAGE_SEPARATOR);
      }

      flowExecutionStatus = updatedFlowExecutionStatus(jobStatus.getExecutionStatus(), flowExecutionStatus);
    }

    String flowMessages = flowMessagesStringBuffer.length() > 0 ?
        flowMessagesStringBuffer.substring(0, flowMessagesStringBuffer.length() -
            MESSAGE_SEPARATOR.length()) : StringUtils.EMPTY;

    return new FlowStatus()
        .setId(new FlowStatusId().setFlowGroup(flowId.getFlowGroup()).setFlowName(flowId.getFlowName())
            .setFlowExecutionId(monitoringFlowStatus.getFlowExecutionId()))
        .setExecutionStatistics(new FlowStatistics().setExecutionStartTime(flowStartTime)
            .setExecutionEndTime(flowEndTime))
        .setMessage(flowMessages)
        .setExecutionStatus(flowExecutionStatus)
        .setJobStatuses(jobStatusArray);
  }

  /**
   * Maps a timing event name to a flow/job ExecutionStatus
   * @param timingEvent timing event name
   * @return status string
   */
  private ExecutionStatus timingEventToStatus(String timingEvent) {
    ExecutionStatus status;

    switch (timingEvent) {
      case TimingEvent.LauncherTimings.JOB_FAILED:
      case TimingEvent.LauncherTimings.JOB_CANCEL:
        status = ExecutionStatus.FAILED;
        break;
      case TimingEvent.LauncherTimings.JOB_COMPLETE:
        status = ExecutionStatus.COMPLETE;
        break;
      default:
        status = ExecutionStatus.RUNNING;
    }

    return status;
  }

  /**
   * Determines the new flow status based on the current flow status and new job status
   * @param jobExecutionStatus job status
   * @param currentFlowExecutionStatus current flow status
   * @return updated flow status
   */
  private ExecutionStatus updatedFlowExecutionStatus(ExecutionStatus jobExecutionStatus,
      ExecutionStatus currentFlowExecutionStatus) {

    // if any job failed or flow has failed then return failed status
    if (currentFlowExecutionStatus == ExecutionStatus.FAILED ||
        jobExecutionStatus == ExecutionStatus.FAILED) {
      return ExecutionStatus.FAILED;
    }

    // if job still running then flow is still running
    if (jobExecutionStatus == ExecutionStatus.RUNNING) {
      return ExecutionStatus.RUNNING;
    }

    return currentFlowExecutionStatus;
  }
}

