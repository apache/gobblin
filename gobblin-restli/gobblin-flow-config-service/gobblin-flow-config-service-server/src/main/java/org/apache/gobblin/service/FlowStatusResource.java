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

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.annotations.Context;
import com.linkedin.restli.server.annotations.Finder;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.resources.ComplexKeyResourceTemplate;

import org.apache.gobblin.service.monitoring.FlowStatusGenerator;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;


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
      @QueryParam("flowId") FlowId flowId, @Optional @QueryParam("count") Integer count) {
    if (count == null) {
      count = 1;
    }
    LOG.info("getLatestFlowStatus called with flowGroup " + flowId.getFlowGroup() + " flowName " + flowId.getFlowName() + " count " + count);

    List<org.apache.gobblin.service.monitoring.FlowStatus> flowStatuses =
        _flowStatusGenerator.getLatestFlowStatus(flowId.getFlowName(), flowId.getFlowGroup(), count);

    if (flowStatuses != null) {
      return flowStatuses.stream().map(this::convertFlowStatus).collect(Collectors.toList());
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

    long flowEndTime = 0L;
    ExecutionStatus flowExecutionStatus = ExecutionStatus.$UNKNOWN;

    StringBuffer flowMessagesStringBuffer = new StringBuffer();

    while (jobStatusIter.hasNext()) {
      org.apache.gobblin.service.monitoring.JobStatus queriedJobStatus = jobStatusIter.next();

      // Check if this is the flow status instead of a single job status
      if (isFlowStatus(queriedJobStatus)) {
        flowEndTime = queriedJobStatus.getEndTime();
        flowExecutionStatus = ExecutionStatus.valueOf(queriedJobStatus.getEventName());
        continue;
      }

      JobStatus jobStatus = new JobStatus();

      jobStatus.setFlowId(flowId)
          .setJobId(new JobId().setJobName(queriedJobStatus.getJobName())
              .setJobGroup(queriedJobStatus.getJobGroup()))
          .setExecutionStatistics(new JobStatistics()
              .setExecutionStartTime(queriedJobStatus.getStartTime())
              .setExecutionEndTime(queriedJobStatus.getEndTime())
              .setProcessedCount(queriedJobStatus.getProcessedCount()))
          .setExecutionStatus(ExecutionStatus.valueOf(queriedJobStatus.getEventName()))
          .setMessage(queriedJobStatus.getMessage())
          .setJobState(new JobState().setLowWatermark(queriedJobStatus.getLowWatermark()).
              setHighWatermark(queriedJobStatus.getHighWatermark()));

      jobStatusArray.add(jobStatus);

      if (!queriedJobStatus.getMessage().isEmpty()) {
        flowMessagesStringBuffer.append(queriedJobStatus.getMessage());
        flowMessagesStringBuffer.append(MESSAGE_SEPARATOR);
      }
    }

    String flowMessages = flowMessagesStringBuffer.length() > 0 ?
        flowMessagesStringBuffer.substring(0, flowMessagesStringBuffer.length() -
            MESSAGE_SEPARATOR.length()) : StringUtils.EMPTY;

    return new FlowStatus()
        .setId(new FlowStatusId().setFlowGroup(flowId.getFlowGroup()).setFlowName(flowId.getFlowName())
            .setFlowExecutionId(monitoringFlowStatus.getFlowExecutionId()))
        .setExecutionStatistics(new FlowStatistics().setExecutionStartTime(getFlowStartTime(monitoringFlowStatus))
            .setExecutionEndTime(flowEndTime))
        .setMessage(flowMessages)
        .setExecutionStatus(flowExecutionStatus)
        .setJobStatuses(jobStatusArray);
  }

  /**
   * Check if a {@link org.apache.gobblin.service.monitoring.JobStatus} is the special job status that represents the
   * entire flow's status
   */
  private static boolean isFlowStatus(org.apache.gobblin.service.monitoring.JobStatus jobStatus) {
    return jobStatus.getJobName().equals(JobStatusRetriever.NA_KEY) && jobStatus.getJobGroup().equals(JobStatusRetriever.NA_KEY);
  }

  /**
   * Return the flow start time given a {@link org.apache.gobblin.service.monitoring.FlowStatus}. Flow execution ID is
   * assumed to be the flow start time.
   */
  private static long getFlowStartTime(org.apache.gobblin.service.monitoring.FlowStatus flowStatus) {
    return flowStatus.getFlowExecutionId();
  }
}

