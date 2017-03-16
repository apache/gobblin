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

package gobblin.service;

import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.resources.ComplexKeyResourceTemplate;

import gobblin.metrics.event.TimingEvent;
import gobblin.service.monitoring.FlowStatusGenerator;


@RestLiCollection(name = "flowstatuses", namespace = "gobblin.service")
/**
 * Rest.li resource for handling flow status reqowstatusuests
 */
public class FlowStatusResource extends ComplexKeyResourceTemplate<FlowStatusId, EmptyRecord, FlowStatus> {
  private static final Logger LOG = LoggerFactory.getLogger(FlowStatusResource.class);
  public static final String FLOW_STATUS_GENERATOR_INJECT_NAME = "FlowStatusGenerator";

  public static final String STATUS_RUNNING = "Running";
  public static final String STATUS_FAILED = "Failed";
  public static final String STATUS_COMPLETE = "Complete";
  public static final String MESSAGE_SEPARATOR = ", ";

  @Inject @javax.inject.Inject @javax.inject.Named(FLOW_STATUS_GENERATOR_INJECT_NAME)
  FlowStatusGenerator _flowStatusGenerator;

  public FlowStatusResource() {}

  /**
   * Retrieve the latest {@link FlowStatus} with the given key
   * @param key flow status id key containing group name and flow name
   * @return {@link FlowStatus} with flow status for the latest execution of the flow
   */
  @Override
  public FlowStatus get(ComplexResourceKey<FlowStatusId, EmptyRecord> key) {
    String flowGroup = key.getKey().getFlowGroup();
    String flowName = key.getKey().getFlowName();

    LOG.info("Get called with flowGroup " + flowGroup + " flowName " + flowName);

    gobblin.service.monitoring.FlowStatus latestFlowStatus =
        _flowStatusGenerator.getLatestFlowStatus(flowName, flowGroup);

    if (latestFlowStatus.getFlowExecutionId() != -1) {
      Iterator<gobblin.service.monitoring.JobStatus> jobStatusIter = latestFlowStatus.getJobStatusIterator();
      JobStatusArray jobStatusArray = new JobStatusArray();
      long flowStartTime = Long.MAX_VALUE;
      long flowEndTime = -1L;
      // flow execution status is complete unless job status indicates it is running or failed
      String flowExecutionStatus = STATUS_COMPLETE;
      StringBuffer flowMessagesStringBuffer = new StringBuffer();

      while (jobStatusIter.hasNext()) {
        gobblin.service.monitoring.JobStatus queriedJobStatus = jobStatusIter.next();
        JobStatus jobStatus = new JobStatus();

        jobStatus.setFlowGroup(flowGroup)
            .setFlowName(flowName)
            .setJobName(queriedJobStatus.getJobName())
            .setJobGroup(queriedJobStatus.getJobGroup())
            .setExecutionStartTime(queriedJobStatus.getStartTime())
            .setExecutionEndTime(queriedJobStatus.getEndTime())
            .setExecutionStatus(timingEventToStatus(queriedJobStatus.getEventName()))
            .setMessage(queriedJobStatus.getMessage())
            .setProcessedCount(queriedJobStatus.getProcessedCount())
            .setWatermark(queriedJobStatus.getWatermark());

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

      return new FlowStatus().setFlowGroup(flowGroup)
          .setFlowName(flowName)
          .setExecutionStartTime(flowStartTime)
          .setExecutionEndTime(flowEndTime)
          .setMessage(flowMessages)
          .setExecutionStatus(flowExecutionStatus)
          .setJobStatuses(jobStatusArray);
    }

    // will return 404 status code
    return null;
  }

  /**
   * Maps a timing event name to a flow/job status string
   * @param timingEvent timing event name
   * @return status string
   */
  private String timingEventToStatus(String timingEvent) {
    String status;

    switch (timingEvent) {
      case TimingEvent.LauncherTimings.JOB_FAILED:
      case TimingEvent.LauncherTimings.JOB_CANCEL:
        status = STATUS_FAILED;
        break;
      case TimingEvent.LauncherTimings.JOB_COMPLETE:
        status = STATUS_COMPLETE;
        break;
      default:
        status = STATUS_RUNNING;
    }

    return status;
  }

  /**
   * Determines the new flow status based on the current flow status and new job status
   * @param jobExecutionStatus job status
   * @param currentFlowExecutionStatus current flow status
   * @return updated flow status
   */
  private String updatedFlowExecutionStatus(String jobExecutionStatus, String currentFlowExecutionStatus) {

    // if any job failed or flow has failed then return failed status
    if (currentFlowExecutionStatus.equals(STATUS_FAILED) || jobExecutionStatus.equals(STATUS_FAILED)) {
      return STATUS_FAILED;
    }

    // if job still running then flow is still running
    if (jobExecutionStatus.equals(STATUS_RUNNING)) {
      return STATUS_RUNNING;
    }

    return currentFlowExecutionStatus;
  }
}

