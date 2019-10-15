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

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterators;


import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.metrics.event.TimingEvent;


/**
 * Retriever for {@link JobStatus}.
 */
@Alpha
public abstract class JobStatusRetriever implements LatestFlowExecutionIdTracker {
  public static final String EVENT_NAME_FIELD = "eventName";
  public static final String NA_KEY = "NA";

  public abstract Iterator<JobStatus> getJobStatusesForFlowExecution(String flowName, String flowGroup,
      long flowExecutionId);

  public abstract Iterator<JobStatus> getJobStatusesForFlowExecution(String flowName, String flowGroup,
      long flowExecutionId, String jobName, String jobGroup);

  public long getLatestExecutionIdForFlow(String flowName, String flowGroup) {
    List<Long> lastKExecutionIds = getLatestExecutionIdsForFlow(flowName, flowGroup, 1);
    return lastKExecutionIds != null && !lastKExecutionIds.isEmpty() ? lastKExecutionIds.get(0) : -1L;
  }

  /**
   * Get the latest {@link JobStatus}es that belongs to the same latest flow execution. Currently, latest flow execution
   * is decided by comparing {@link JobStatus#getFlowExecutionId()}.
   */
  public Iterator<JobStatus> getLatestJobStatusByFlowNameAndGroup(String flowName, String flowGroup) {
    long latestExecutionId = getLatestExecutionIdForFlow(flowName, flowGroup);

    return latestExecutionId == -1L ? Iterators.<JobStatus>emptyIterator()
        : getJobStatusesForFlowExecution(flowName, flowGroup, latestExecutionId);
  }


  /**
   *
   * @param jobState instance of {@link State}
   * @return deserialize {@link State} into a {@link JobStatus}.
   */
  protected JobStatus getJobStatus(State jobState) {
    String flowGroup = jobState.getProp(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD);
    String flowName = jobState.getProp(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD);
    long flowExecutionId = Long.parseLong(jobState.getProp(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD));
    String jobName = jobState.getProp(TimingEvent.FlowEventConstants.JOB_NAME_FIELD);
    String jobGroup = jobState.getProp(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD);
    String jobTag = jobState.getProp(TimingEvent.FlowEventConstants.JOB_TAG_FIELD);
    long jobExecutionId = Long.parseLong(jobState.getProp(TimingEvent.FlowEventConstants.JOB_EXECUTION_ID_FIELD, "0"));
    String eventName = jobState.getProp(JobStatusRetriever.EVENT_NAME_FIELD);
    long startTime = Long.parseLong(jobState.getProp(TimingEvent.JOB_START_TIME, "0"));
    long endTime = Long.parseLong(jobState.getProp(TimingEvent.JOB_END_TIME, "0"));
    String message = jobState.getProp(TimingEvent.METADATA_MESSAGE, "");
    String lowWatermark = jobState.getProp(TimingEvent.FlowEventConstants.LOW_WATERMARK_FIELD, "");
    String highWatermark = jobState.getProp(TimingEvent.FlowEventConstants.HIGH_WATERMARK_FIELD, "");
    long processedCount = Long.parseLong(jobState.getProp(TimingEvent.FlowEventConstants.PROCESSED_COUNT_FIELD, "0"));
    int maxAttempts = Integer.parseInt(jobState.getProp(TimingEvent.FlowEventConstants.MAX_ATTEMPTS_FIELD, "1"));
    int currentAttempts = Integer.parseInt(jobState.getProp(TimingEvent.FlowEventConstants.CURRENT_ATTEMPTS_FIELD, "1"));
    boolean shouldRetry = Boolean.parseBoolean(jobState.getProp(TimingEvent.FlowEventConstants.SHOULD_RETRY_FIELD, "false"));

    return JobStatus.builder().flowName(flowName).flowGroup(flowGroup).flowExecutionId(flowExecutionId).
        jobName(jobName).jobGroup(jobGroup).jobTag(jobTag).jobExecutionId(jobExecutionId).eventName(eventName).
        lowWatermark(lowWatermark).highWatermark(highWatermark).startTime(startTime).endTime(endTime).
        message(message).processedCount(processedCount).maxAttempts(maxAttempts).currentAttempts(currentAttempts).
        shouldRetry(shouldRetry).build();
  }

  public abstract StateStore<State> getStateStore();

  /**
   * Check if a {@link org.apache.gobblin.service.monitoring.JobStatus} is the special job status that represents the
   * entire flow's status
   */
  public static boolean isFlowStatus(org.apache.gobblin.service.monitoring.JobStatus jobStatus) {
    return jobStatus.getJobName() != null && jobStatus.getJobGroup() != null
        && jobStatus.getJobName().equals(JobStatusRetriever.NA_KEY) && jobStatus.getJobGroup().equals(JobStatusRetriever.NA_KEY);
  }
}
