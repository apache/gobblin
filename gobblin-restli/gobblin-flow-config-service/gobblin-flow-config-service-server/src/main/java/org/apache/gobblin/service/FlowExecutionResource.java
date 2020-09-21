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

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.linkedin.data.template.SetMode;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.UpdateResponse;
import com.linkedin.restli.server.annotations.Context;
import com.linkedin.restli.server.annotations.Finder;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.resources.ComplexKeyResourceTemplate;

import org.apache.gobblin.service.monitoring.FlowStatusGenerator;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;


/**
 * Resource for handling flow execution requests
 */
@RestLiCollection(name = "flowexecutions", namespace = "org.apache.gobblin.service", keyName = "id")
public class FlowExecutionResource extends ComplexKeyResourceTemplate<FlowStatusId, EmptyRecord, FlowExecution> {
  private static final Logger LOG = LoggerFactory.getLogger(FlowExecutionResource.class);
  public static final String FLOW_STATUS_GENERATOR_INJECT_NAME = "FlowStatusGenerator";
  public static final String MESSAGE_SEPARATOR = ", ";

  @Inject @javax.inject.Inject @javax.inject.Named(FLOW_STATUS_GENERATOR_INJECT_NAME)
  FlowStatusGenerator _flowStatusGenerator;

  public FlowExecutionResource() {}

  /**
   * Retrieve the FlowExecution with the given key
   * @param key {@link FlowStatusId} of flow to get
   * @return corresponding {@link FlowExecution}
   */
  @Override
  public FlowExecution get(ComplexResourceKey<FlowStatusId, EmptyRecord> key) {
    FlowExecution flowExecution = convertFlowStatus(getFlowStatusFromGenerator(key, this._flowStatusGenerator));
    if (flowExecution == null) {
      throw new RestLiServiceException(HttpStatus.S_404_NOT_FOUND, "No flow execution found for flowStatusId " + key.getKey()
      + ". The flowStatusId may be incorrect, or the flow execution may have been cleaned up.");
    }
    return flowExecution;
  }

  @Finder("latestFlowExecution")
  public List<FlowExecution> getLatestFlowExecution(@Context PagingContext context, @QueryParam("flowId") FlowId flowId,
      @Optional @QueryParam("count") Integer count, @Optional @QueryParam("tag") String tag, @Optional @QueryParam("executionStatus") String executionStatus) {
    List<org.apache.gobblin.service.monitoring.FlowStatus> flowStatuses = getLatestFlowStatusesFromGenerator(flowId, count, tag, executionStatus, this._flowStatusGenerator);

    if (flowStatuses != null) {
      return flowStatuses.stream().map(FlowExecutionResource::convertFlowStatus).collect(Collectors.toList());
    }

    throw new RestLiServiceException(HttpStatus.S_404_NOT_FOUND, "No flow execution found for flowId " + flowId
        + ". The flowId may be incorrect, or the flow execution may have been cleaned up.");
  }

  /**
   * Kill the FlowExecution with the given key
   * @param key {@link FlowStatusId} of flow to kill
   * @return {@link UpdateResponse}
   */
  @Override
  public UpdateResponse delete(ComplexResourceKey<FlowStatusId, EmptyRecord> key) {
    String flowGroup = key.getKey().getFlowGroup();
    String flowName = key.getKey().getFlowName();
    Long flowExecutionId = key.getKey().getFlowExecutionId();
    _flowStatusGenerator.killFlow(flowGroup, flowName, flowExecutionId);
    return new UpdateResponse(HttpStatus.S_200_OK);
  }

  public static org.apache.gobblin.service.monitoring.FlowStatus getFlowStatusFromGenerator(ComplexResourceKey<FlowStatusId, EmptyRecord> key,
      FlowStatusGenerator flowStatusGenerator) {
    String flowGroup = key.getKey().getFlowGroup();
    String flowName = key.getKey().getFlowName();
    long flowExecutionId = key.getKey().getFlowExecutionId();

    LOG.info("Get called with flowGroup " + flowGroup + " flowName " + flowName + " flowExecutionId " + flowExecutionId);

    return flowStatusGenerator.getFlowStatus(flowName, flowGroup, flowExecutionId, null);
  }

  public static List<org.apache.gobblin.service.monitoring.FlowStatus> getLatestFlowStatusesFromGenerator(FlowId flowId,
      Integer count, String tag, String executionStatus, FlowStatusGenerator flowStatusGenerator) {
    if (count == null) {
      count = 1;
    }
    LOG.info("get latest called with flowGroup " + flowId.getFlowGroup() + " flowName " + flowId.getFlowName() + " count " + count);

    return flowStatusGenerator.getLatestFlowStatus(flowId.getFlowName(), flowId.getFlowGroup(), count, tag, executionStatus);
  }

  /**
   * Forms a {@link FlowExecution} from a {@link org.apache.gobblin.service.monitoring.FlowStatus}
   * @param monitoringFlowStatus
   * @return a {@link FlowExecution} converted from a {@link org.apache.gobblin.service.monitoring.FlowStatus}
   */
  public static FlowExecution convertFlowStatus(org.apache.gobblin.service.monitoring.FlowStatus monitoringFlowStatus) {
    if (monitoringFlowStatus == null) {
      return null;
    }

    Iterator<org.apache.gobblin.service.monitoring.JobStatus> jobStatusIter = monitoringFlowStatus.getJobStatusIterator();
    JobStatusArray jobStatusArray = new JobStatusArray();
    FlowId flowId = new FlowId().setFlowName(monitoringFlowStatus.getFlowName())
        .setFlowGroup(monitoringFlowStatus.getFlowGroup());

    long flowEndTime = 0L;
    ExecutionStatus flowExecutionStatus = ExecutionStatus.$UNKNOWN;

    String flowMessage = "";

    while (jobStatusIter.hasNext()) {
      org.apache.gobblin.service.monitoring.JobStatus queriedJobStatus = jobStatusIter.next();

      // Check if this is the flow status instead of a single job status
      if (JobStatusRetriever.isFlowStatus(queriedJobStatus)) {
        flowEndTime = queriedJobStatus.getEndTime();
        flowExecutionStatus = ExecutionStatus.valueOf(queriedJobStatus.getEventName());
        if (queriedJobStatus.getMessage() != null) {
          flowMessage = queriedJobStatus.getMessage();
        }
        continue;
      }

      JobStatus jobStatus = new JobStatus();

      jobStatus.setFlowId(flowId)
          .setJobId(new JobId().setJobName(queriedJobStatus.getJobName())
              .setJobGroup(queriedJobStatus.getJobGroup()))
          .setJobTag(queriedJobStatus.getJobTag(), SetMode.IGNORE_NULL)
          .setExecutionStatistics(new JobStatistics()
              .setExecutionStartTime(queriedJobStatus.getStartTime())
              .setExecutionEndTime(queriedJobStatus.getEndTime())
              .setProcessedCount(queriedJobStatus.getProcessedCount()))
          .setExecutionStatus(ExecutionStatus.valueOf(queriedJobStatus.getEventName()))
          .setMessage(queriedJobStatus.getMessage())
          .setJobState(new JobState().setLowWatermark(queriedJobStatus.getLowWatermark()).
              setHighWatermark(queriedJobStatus.getHighWatermark()));

      if (!Strings.isNullOrEmpty(queriedJobStatus.getMetrics())) {
        jobStatus.setMetrics(queriedJobStatus.getMetrics());
      }

      jobStatusArray.add(jobStatus);
    }

    jobStatusArray.sort(Comparator.comparing((JobStatus js) -> js.getExecutionStatistics().getExecutionStartTime()));

    return new FlowExecution()
        .setId(new FlowStatusId().setFlowGroup(flowId.getFlowGroup()).setFlowName(flowId.getFlowName())
            .setFlowExecutionId(monitoringFlowStatus.getFlowExecutionId()))
        .setExecutionStatistics(new FlowStatistics().setExecutionStartTime(getFlowStartTime(monitoringFlowStatus))
            .setExecutionEndTime(flowEndTime))
        .setMessage(flowMessage)
        .setExecutionStatus(flowExecutionStatus)
        .setJobStatuses(jobStatusArray);
  }

  /**
   * Return the flow start time given a {@link org.apache.gobblin.service.monitoring.FlowStatus}. Flow execution ID is
   * assumed to be the flow start time.
   */
  private static long getFlowStartTime(org.apache.gobblin.service.monitoring.FlowStatus flowStatus) {
    return flowStatus.getFlowExecutionId();
  }
}

