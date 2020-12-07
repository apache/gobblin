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

package org.apache.gobblin.service.monitoring;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import lombok.Builder;

import org.apache.gobblin.annotation.Alpha;


/**
 * Generator for {@link FlowStatus}, which relies on a {@link JobStatusRetriever}.
 */
@Alpha
@Builder
public class FlowStatusGenerator {
  public static final List<String> FINISHED_STATUSES = Lists.newArrayList("FAILED", "COMPLETE", "CANCELLED");
  public static final int MAX_LOOKBACK = 100;

  private final JobStatusRetriever jobStatusRetriever;

  /**
   * Get the flow statuses of last <code>count</code> (or fewer) executions
   * @param flowName
   * @param flowGroup
   * @param count
   * @param tag
   * @return the latest <code>count</code>{@link FlowStatus}es. null is returned if there is no flow execution found.
   * If tag is not null, the job status list only contains jobs matching the tag.
   */
  public List<FlowStatus> getLatestFlowStatus(String flowName, String flowGroup, int count, String tag) {
    List<Long> flowExecutionIds = jobStatusRetriever.getLatestExecutionIdsForFlow(flowName, flowGroup, count);

    if (flowExecutionIds == null || flowExecutionIds.isEmpty()) {
      return null;
    }
    List<FlowStatus> flowStatuses =
        flowExecutionIds.stream().map(flowExecutionId -> getFlowStatus(flowName, flowGroup, flowExecutionId, tag))
            .collect(Collectors.toList());

    return flowStatuses;
  }

  /**
   * Get the flow statuses of last <code>count</code> (or fewer) executions
   * @param flowName
   * @param flowGroup
   * @param count
   * @param tag
   * @param executionStatus
   * @return the latest <code>count</code>{@link FlowStatus}es. null is returned if there is no flow execution found.
   * If tag is not null, the job status list only contains jobs matching the tag.
   * If executionStatus is not null, the latest <code>count</code> flow statuses with that status are returned (as long
   * as they are within the last {@link #MAX_LOOKBACK} executions for this flow).
   */
  public List<FlowStatus> getLatestFlowStatus(String flowName, String flowGroup, int count, String tag, String executionStatus) {
    if (executionStatus == null) {
      return getLatestFlowStatus(flowName, flowGroup, count, tag);
    } else {
      List<FlowStatus> flowStatuses = getLatestFlowStatus(flowName, flowGroup, MAX_LOOKBACK, tag);
      if (flowStatuses == null) {
        return null;
      }
      List<FlowStatus> matchingFlowStatuses = new ArrayList<>();

      for (FlowStatus flowStatus : flowStatuses) {
        if (matchingFlowStatuses.size() == count) {
          return matchingFlowStatuses;
        }

        String executionStatusFromFlow = getExecutionStatus(flowStatus);
        if (executionStatusFromFlow.equals(executionStatus)) {
          matchingFlowStatuses.add(getFlowStatus(flowName, flowGroup, flowStatus.getFlowExecutionId(), tag));
        }
      }

      return matchingFlowStatuses;
    }
  }

  /**
   * Return the executionStatus of the given {@link FlowStatus}. Note that the {@link FlowStatus#jobStatusIterator}
   * will have it's cursor moved forward by this.
   */
  private String getExecutionStatus(FlowStatus flowStatus) {
    List<JobStatus> jobStatuses = Lists.newArrayList(flowStatus.getJobStatusIterator());
    jobStatuses = jobStatuses.stream().filter(JobStatusRetriever::isFlowStatus).collect(Collectors.toList());
    return jobStatuses.isEmpty() ? "" : jobStatuses.get(0).getEventName();
  }

  /**
   * Get the flow status for a specific execution.
   * @param flowName
   * @param flowGroup
   * @param flowExecutionId
   * @param tag String to filter the returned job statuses
   * @return the flow status, null is returned if the flow status does not exist. If tag is not null, the job status
   * list only contains jobs matching the tag.
   */
  public FlowStatus getFlowStatus(String flowName, String flowGroup, long flowExecutionId, String tag) {
    FlowStatus flowStatus = null;
    Iterator<JobStatus> jobStatusIterator =
        jobStatusRetriever.getJobStatusesForFlowExecution(flowName, flowGroup, flowExecutionId);

    if (tag != null) {
      jobStatusIterator = Iterators.filter(jobStatusIterator, js -> JobStatusRetriever.isFlowStatus(js) ||
          (js.getJobTag() != null && js.getJobTag().equals(tag)));
    }

    if (jobStatusIterator.hasNext()) {
      flowStatus = new FlowStatus(flowName, flowGroup, flowExecutionId, jobStatusIterator);
    }

    return flowStatus;
  }

  /**
   * Return true if another instance of a flow is running. A flow is determined to be in the RUNNING state, if any of the
   * jobs in the flow are in the RUNNING state.
   * @param flowName
   * @param flowGroup
   * @return true, if any jobs of the flow are RUNNING.
   */
  public boolean isFlowRunning(String flowName, String flowGroup) {
    List<FlowStatus> flowStatusList = getLatestFlowStatus(flowName, flowGroup, 1, null);
    if (flowStatusList == null || flowStatusList.isEmpty()) {
      return false;
    } else {
      FlowStatus flowStatus = flowStatusList.get(0);
      Iterator<JobStatus> jobStatusIterator = flowStatus.getJobStatusIterator();

      while (jobStatusIterator.hasNext()) {
        JobStatus jobStatus = jobStatusIterator.next();
        if (JobStatusRetriever.isFlowStatus(jobStatus)) {
          return isJobRunning(jobStatus);
        }
      }
      return false;
    }
  }

  /**
   * @param jobStatus
   * @return true if the job associated with the {@link JobStatus} is RUNNING
   */
  private boolean isJobRunning(JobStatus jobStatus) {
    String status = jobStatus.getEventName().toUpperCase();
    return !FINISHED_STATUSES.contains(status);
  }
}
