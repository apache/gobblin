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

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ObjectUtils;

import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.PathKeys;
import com.linkedin.restli.server.ResourceLevel;
import com.linkedin.restli.server.UpdateResponse;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.Context;
import com.linkedin.restli.server.annotations.Finder;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.PathKeysParam;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.resources.ComplexKeyResourceTemplate;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.service.monitoring.FlowStatus;
import org.apache.gobblin.service.monitoring.FlowStatusGenerator;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;


/**
 * Resource for handling flow execution requests
 */
@Slf4j
@RestLiCollection(name = "flowexecutions", namespace = "org.apache.gobblin.service", keyName = "id")
public class FlowExecutionResource extends ComplexKeyResourceTemplate<FlowStatusId, EmptyRecord, FlowExecution> {

  @Inject
  FlowExecutionResourceHandlerInterface flowExecutionResourceHandler;

  public FlowExecutionResource() {}

  /**
   * Retrieve the FlowExecution with the given key
   * @param key {@link FlowStatusId} of flow to get
   * @return corresponding {@link FlowExecution}
   */
  @Override
  public FlowExecution get(ComplexResourceKey<FlowStatusId, EmptyRecord> key) {
    return this.flowExecutionResourceHandler.get(key);
  }

  /**
   * Retrieve the most recent matching FlowExecution(s) of the identified FlowId
   * @param includeIssues include job issues in the response. Otherwise empty array of issues will be returned.
   */
  @Finder("latestFlowExecution")
  public List<FlowExecution> getLatestFlowExecution(@Context PagingContext context, @QueryParam("flowId") FlowId flowId,
      @Optional @QueryParam("count") Integer count, @Optional @QueryParam("tag") String tag, @Optional @QueryParam("executionStatus") String executionStatus,
      @Optional("false") @QueryParam("includeIssues") Boolean includeIssues) {
    return this.flowExecutionResourceHandler.getLatestFlowExecution(context, flowId, count, tag, executionStatus, includeIssues);
  }

  /**
   * Retrieve the most recent matching FlowExecution(s) for each flow in the identified flowGroup
   * @param countPerFlow (maximum) number of FlowExecutions for each flow in flowGroup   *
   * @param includeIssues include job issues in the response. Otherwise empty array of issues will be returned.
   * @return
   */
  @Finder("latestFlowGroupExecutions")
  public List<FlowExecution> getLatestFlowGroupExecutions(@Context PagingContext context, @QueryParam("flowGroup") String flowGroup,
      @Optional @QueryParam("countPerFlow") Integer countPerFlow, @Optional @QueryParam("tag") String tag,
      @Optional("false") @QueryParam("includeIssues") Boolean includeIssues) {
    return this.flowExecutionResourceHandler.getLatestFlowGroupExecutions(context, flowGroup, countPerFlow, tag, includeIssues);
  }

  /**
   * Resume a failed {@link FlowExecution} from the point before failure.
   * @param pathKeys key of {@link FlowExecution} specified in path
   */
  @Action(name="resume",resourceLevel= ResourceLevel.ENTITY)
  public void resume(@PathKeysParam PathKeys pathKeys) {
    this.flowExecutionResourceHandler.resume(pathKeys.get("id"));
  }

  /**
   * Kill the FlowExecution with the given key
   * @param key {@link FlowStatusId} of flow to kill
   * @return {@link UpdateResponse}
   */
  @Override
  public UpdateResponse delete(ComplexResourceKey<FlowStatusId, EmptyRecord> key) {
    return this.flowExecutionResourceHandler.delete(key);
  }

  public static List<FlowStatus> getLatestFlowStatusesFromGenerator(FlowId flowId,
      Integer count, String tag, String executionStatus, FlowStatusGenerator flowStatusGenerator) {
    if (count == null) {
      count = 1;
    }
    log.info("get latest called with flowGroup " + flowId.getFlowGroup() + " flowName " + flowId.getFlowName() + " count " + count);

    return flowStatusGenerator.getLatestFlowStatus(flowId.getFlowName(), flowId.getFlowGroup(), count, tag, executionStatus);
  }

  public static FlowStatus getFlowStatusFromGenerator(ComplexResourceKey<FlowStatusId, EmptyRecord> key,
      FlowStatusGenerator flowStatusGenerator) {
    String flowGroup = key.getKey().getFlowGroup();
    String flowName = key.getKey().getFlowName();
    long flowExecutionId = key.getKey().getFlowExecutionId();

    log.info("Get called with flowGroup " + flowGroup + " flowName " + flowName + " flowExecutionId " + flowExecutionId);

    return flowStatusGenerator.getFlowStatus(flowName, flowGroup, flowExecutionId, null);
  }

  /**
   * Forms a {@link FlowExecution} from a {@link FlowStatus}
   * @param monitoringFlowStatus
   * @return a {@link FlowExecution} converted from a {@link FlowStatus}
   */
  public static FlowExecution convertFlowStatus(FlowStatus monitoringFlowStatus,
      boolean includeIssues) {
    if (monitoringFlowStatus == null) {
      return null;
    }

    Iterator<org.apache.gobblin.service.monitoring.JobStatus> jobStatusIter = monitoringFlowStatus.getJobStatusIterator();
    JobStatusArray jobStatusArray = new JobStatusArray();
    FlowId flowId = new FlowId().setFlowName(monitoringFlowStatus.getFlowName())
        .setFlowGroup(monitoringFlowStatus.getFlowGroup());

    long flowEndTime = 0L;
    long maxJobEndTime = Long.MIN_VALUE;
    String flowMessage = "";

    while (jobStatusIter.hasNext()) {
      org.apache.gobblin.service.monitoring.JobStatus queriedJobStatus = jobStatusIter.next();

      // Check if this is the flow status instead of a single job status
      if (JobStatusRetriever.isFlowStatus(queriedJobStatus)) {
        flowEndTime = queriedJobStatus.getEndTime();
        if (queriedJobStatus.getMessage() != null) {
          flowMessage = queriedJobStatus.getMessage();
        }
        continue;
      }

      maxJobEndTime = Math.max(maxJobEndTime, queriedJobStatus.getEndTime());

      org.apache.gobblin.service.JobStatus jobStatus = new org.apache.gobblin.service.JobStatus();

      Long timeLeft = estimateCopyTimeLeft(queriedJobStatus.getLastProgressEventTime(), queriedJobStatus.getStartTime(),
          queriedJobStatus.getProgressPercentage());

      jobStatus.setFlowId(flowId)
          .setJobId(new JobId()
              .setJobName(queriedJobStatus.getJobName())
              .setJobGroup(queriedJobStatus.getJobGroup()))
          .setJobTag(queriedJobStatus.getJobTag(), SetMode.IGNORE_NULL)
          .setExecutionStatistics(new JobStatistics()
              .setExecutionStartTime(queriedJobStatus.getStartTime())
              .setExecutionEndTime(queriedJobStatus.getEndTime())
              .setProcessedCount(queriedJobStatus.getProcessedCount())
              .setJobProgress(queriedJobStatus.getProgressPercentage())
              .setEstimatedSecondsToCompletion(timeLeft))
          .setExecutionStatus(ExecutionStatus.valueOf(queriedJobStatus.getEventName()))
          .setMessage(queriedJobStatus.getMessage())
          .setJobState(new JobState()
              .setLowWatermark(queriedJobStatus.getLowWatermark()).
                  setHighWatermark(queriedJobStatus.getHighWatermark()));

      if (includeIssues) {
        jobStatus.setIssues(new IssueArray(queriedJobStatus.getIssues().get().stream()
            .map(FlowExecutionResource::convertIssueToRestApiObject)
            .collect(Collectors.toList())));
      } else {
        jobStatus.setIssues(new IssueArray());
      }

      if (!Strings.isNullOrEmpty(queriedJobStatus.getMetrics())) {
        jobStatus.setMetrics(queriedJobStatus.getMetrics());
      }

      jobStatusArray.add(jobStatus);
    }

    // If DagManager is not enabled, we have to determine flow end time by individual job's end times.
    flowEndTime = flowEndTime == 0L ? maxJobEndTime : flowEndTime;

    jobStatusArray.sort(Comparator.comparing((org.apache.gobblin.service.JobStatus js) -> js.getExecutionStatistics().getExecutionStartTime()));

    return new FlowExecution()
        .setId(new FlowStatusId().setFlowGroup(flowId.getFlowGroup()).setFlowName(flowId.getFlowName())
            .setFlowExecutionId(monitoringFlowStatus.getFlowExecutionId()))
        .setExecutionStatistics(new FlowStatistics().setExecutionStartTime(getFlowStartTime(monitoringFlowStatus))
            .setExecutionEndTime(flowEndTime))
        .setMessage(flowMessage)
        .setExecutionStatus(monitoringFlowStatus.getFlowExecutionStatus())
        .setJobStatuses(jobStatusArray);
  }

  private static org.apache.gobblin.service.Issue convertIssueToRestApiObject(
      org.apache.gobblin.runtime.troubleshooter.Issue issues) {
    org.apache.gobblin.service.Issue converted = new org.apache.gobblin.service.Issue();

    converted.setCode(issues.getCode())
        .setSummary(ObjectUtils.firstNonNull(issues.getSummary(), ""))
        .setDetails(ObjectUtils.firstNonNull(issues.getDetails(), ""))
        .setSeverity(IssueSeverity.valueOf(issues.getSeverity().name()))
        .setTime(issues.getTime().toInstant().toEpochMilli());

    if (issues.getProperties() != null) {
      converted.setProperties(new StringMap(issues.getProperties()));
    } else {
      converted.setProperties(new StringMap());
    }

    return converted;
  }

  /**
   * Return the flow start time given a {@link FlowStatus}. Flow execution ID is
   * assumed to be the flow start time.
   */
  private static long getFlowStartTime(FlowStatus flowStatus) {
    return flowStatus.getFlowExecutionId();
  }

  /**
   * Estimate the time left to complete the copy based on the following formula -
   *  timeLeft = (100/completionPercentage - 1) * timeElapsed
   * @param currentTime as an epoch
   * @param startTime as an epoch
   * @param completionPercentage of the job
   * @return time left in seconds
   */
  public static long estimateCopyTimeLeft(Long currentTime, Long startTime, int completionPercentage) {
    if (completionPercentage == 0) {
      return 0;
    }

    Instant current = Instant.ofEpochMilli(currentTime);
    Instant start = Instant.ofEpochMilli(startTime);
    long timeElapsed = Duration.between(start, current).getSeconds();
    return (long) (timeElapsed * (100.0 / (double) completionPercentage - 1));
  }
}

