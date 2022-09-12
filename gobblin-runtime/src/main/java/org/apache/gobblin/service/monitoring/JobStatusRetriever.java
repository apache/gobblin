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

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.typesafe.config.ConfigFactory;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.troubleshooter.Issue;
import org.apache.gobblin.runtime.troubleshooter.MultiContextIssueRepository;
import org.apache.gobblin.runtime.troubleshooter.TroubleshooterException;
import org.apache.gobblin.runtime.troubleshooter.TroubleshooterUtils;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Retriever for {@link JobStatus}.
 */
@Slf4j
public abstract class JobStatusRetriever implements LatestFlowExecutionIdTracker {
  public static final String EVENT_NAME_FIELD = "eventName";
  public static final String NA_KEY = "NA";

  @Getter
  protected final MetricContext metricContext;
  @Getter
  protected final Boolean dagManagerEnabled;

  private final MultiContextIssueRepository issueRepository;

  protected JobStatusRetriever(boolean dagManagerEnabled, MultiContextIssueRepository issueRepository) {
    this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(ConfigFactory.empty()), getClass());
    this.issueRepository = Objects.requireNonNull(issueRepository);
    this.dagManagerEnabled = dagManagerEnabled;
  }

  public abstract Iterator<JobStatus> getJobStatusesForFlowExecution(String flowName, String flowGroup,
      long flowExecutionId);

  public abstract Iterator<JobStatus> getJobStatusesForFlowExecution(String flowName, String flowGroup,
      long flowExecutionId, String jobName, String jobGroup);

  /**
   * Get the latest {@link FlowStatus}es of executions of flows belonging to this flow group.  Currently, latest flow execution
   * is decided by comparing {@link JobStatus#getFlowExecutionId()}.
   * @return `FlowStatus`es of `flowGroup`, ordered by ascending flowName, with all of each name adjacent and by descending flowExecutionId.
   *
   * NOTE: return `List`, not `Iterator` for non-side-effecting access.
   */
  public abstract List<FlowStatus> getFlowStatusesForFlowGroupExecutions(String flowGroup, int countJobStatusesPerFlowName);

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

    return latestExecutionId == -1L ? Iterators.emptyIterator()
        : getJobStatusesForFlowExecution(flowName, flowGroup, latestExecutionId);
  }

  /**
   *
   * @param jobState instance of {@link State}
   * @return deserialize {@link State} into a {@link JobStatus}.
   */
  protected JobStatus getJobStatus(State jobState) {
    String flowGroup = getFlowGroup(jobState);
    String flowName = getFlowName(jobState);
    long flowExecutionId = getFlowExecutionId(jobState);
    String jobName = getJobName(jobState);
    String jobGroup = getJobGroup(jobState);
    String jobTag = jobState.getProp(TimingEvent.FlowEventConstants.JOB_TAG_FIELD);
    long jobExecutionId = getJobExecutionId(jobState);
    String eventName = jobState.getProp(JobStatusRetriever.EVENT_NAME_FIELD);
    long orchestratedTime = Long.parseLong(jobState.getProp(TimingEvent.JOB_ORCHESTRATED_TIME, "0"));
    long startTime = Long.parseLong(jobState.getProp(TimingEvent.JOB_START_TIME, "0"));
    long endTime = Long.parseLong(jobState.getProp(TimingEvent.JOB_END_TIME, "0"));
    String message = jobState.getProp(TimingEvent.METADATA_MESSAGE, "");
    String lowWatermark = jobState.getProp(TimingEvent.FlowEventConstants.LOW_WATERMARK_FIELD, "");
    String highWatermark = jobState.getProp(TimingEvent.FlowEventConstants.HIGH_WATERMARK_FIELD, "");
    long processedCount = Long.parseLong(jobState.getProp(TimingEvent.FlowEventConstants.PROCESSED_COUNT_FIELD, "0"));
    int maxAttempts = Integer.parseInt(jobState.getProp(TimingEvent.FlowEventConstants.MAX_ATTEMPTS_FIELD, "1"));
    int currentAttempts = Integer.parseInt(jobState.getProp(TimingEvent.FlowEventConstants.CURRENT_ATTEMPTS_FIELD, "1"));
    int currentGeneration = Integer.parseInt(jobState.getProp(TimingEvent.FlowEventConstants.CURRENT_GENERATION_FIELD, "1"));
    boolean shouldRetry = Boolean.parseBoolean(jobState.getProp(TimingEvent.FlowEventConstants.SHOULD_RETRY_FIELD, "false"));
    int progressPercentage = jobState.getPropAsInt(TimingEvent.JOB_COMPLETION_PERCENTAGE, 0);
    long lastProgressEventTime = jobState.getPropAsLong(TimingEvent.JOB_LAST_PROGRESS_EVENT_TIME, 0);

    String contextId = TroubleshooterUtils.getContextIdForJob(jobState.getProperties());

    Supplier<List<Issue>> jobIssues = Suppliers.memoize(() -> {
      List<Issue> issues;
      try {
        issues = issueRepository.getAll(contextId);
      } catch (TroubleshooterException e) {
        log.warn("Cannot retrieve job issues", e);
        issues = Collections.emptyList();
      }
      return issues;
    });

    return JobStatus.builder().flowName(flowName).flowGroup(flowGroup).flowExecutionId(flowExecutionId).
        jobName(jobName).jobGroup(jobGroup).jobTag(jobTag).jobExecutionId(jobExecutionId).eventName(eventName).
        lowWatermark(lowWatermark).highWatermark(highWatermark).orchestratedTime(orchestratedTime).startTime(startTime).endTime(endTime).
        message(message).processedCount(processedCount).maxAttempts(maxAttempts).currentAttempts(currentAttempts).currentGeneration(currentGeneration).
        shouldRetry(shouldRetry).progressPercentage(progressPercentage).lastProgressEventTime(lastProgressEventTime).
        issues(jobIssues).build();
  }

  protected final String getFlowGroup(State jobState) {
    return jobState.getProp(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD);
  }

  protected final String getFlowName(State jobState) {
    return jobState.getProp(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD);
  }

  protected final long getFlowExecutionId(State jobState) {
    return Long.parseLong(jobState.getProp(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD));
  }

  protected final String getJobGroup(State jobState) {
    return jobState.getProp(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD);
  }

  protected final String getJobName(State jobState) {
    return jobState.getProp(TimingEvent.FlowEventConstants.JOB_NAME_FIELD);
  }

  protected final long getJobExecutionId(State jobState) {
    return Long.parseLong(jobState.getProp(TimingEvent.FlowEventConstants.JOB_EXECUTION_ID_FIELD, "0"));
  }

  protected Iterator<JobStatus> asJobStatuses(List<State> jobStatusStates) {
    return jobStatusStates.stream().map(this::getJobStatus).iterator();
  }

  protected List<FlowStatus> asFlowStatuses(List<FlowExecutionJobStateGrouping> flowExecutionGroupings) {
    return flowExecutionGroupings.stream().map(exec -> {
      List<JobStatus> jobStatuses = ImmutableList.copyOf(asJobStatuses(exec.getJobStates().stream().sorted(
          // rationalized order, to facilitate test assertions
          Comparator.comparing(this::getJobGroup).thenComparing(this::getJobName).thenComparing(this::getJobExecutionId)
      ).collect(Collectors.toList())));
      return new FlowStatus(exec.getFlowName(), exec.getFlowGroup(), exec.getFlowExecutionId(), jobStatuses.iterator(),
            getFlowStatusFromJobStatuses(dagManagerEnabled, jobStatuses.iterator()));
    }).collect(Collectors.toList());
  }

  @AllArgsConstructor
  @Getter
  protected static class FlowExecutionJobStateGrouping {
    private final String flowGroup;
    private final String flowName;
    private final long flowExecutionId;
    private final List<State> jobStates;
  }

  protected List<FlowExecutionJobStateGrouping> groupByFlowExecutionAndRetainLatest(
      String flowGroup, List<State> jobStatusStates, int maxCountPerFlowName) {
    Map<String, Map<Long, List<State>>> statesByFlowExecutionIdByName =
        jobStatusStates.stream().collect(Collectors.groupingBy(
            this::getFlowName,
            Collectors.groupingBy(this::getFlowExecutionId)));

    return statesByFlowExecutionIdByName.entrySet().stream().sorted(Map.Entry.comparingByKey()).flatMap(flowNameEntry -> {
      String flowName = flowNameEntry.getKey();
      Map<Long, List<State>> statesByFlowExecutionIdForName = flowNameEntry.getValue();

      List<Long> executionIds = Ordering.<Long>natural().greatestOf(statesByFlowExecutionIdForName.keySet(), maxCountPerFlowName);
      return executionIds.stream().map(executionId ->
          new FlowExecutionJobStateGrouping(flowGroup, flowName, executionId, statesByFlowExecutionIdForName.get(executionId)));
    }).collect(Collectors.toList());
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

  public static ExecutionStatus getFlowStatusFromJobStatuses(boolean dagManagerEnabled, Iterator<JobStatus> jobStatusIterator) {
    ExecutionStatus flowExecutionStatus = ExecutionStatus.$UNKNOWN;

    if (dagManagerEnabled) {
      while (jobStatusIterator.hasNext()) {
        JobStatus jobStatus = jobStatusIterator.next();
        // Check if this is the flow status instead of a single job status
        if (JobStatusRetriever.isFlowStatus(jobStatus)) {
          flowExecutionStatus = ExecutionStatus.valueOf(jobStatus.getEventName());
        }
      }
    } else {
      Set<ExecutionStatus> jobStatuses = new HashSet<>();
      while (jobStatusIterator.hasNext()) {
        JobStatus jobStatus = jobStatusIterator.next();
        // because in absence of DagManager we do not get all flow level events, we will ignore the flow level events
        // we actually get and purely calculate flow status based on flow statuses.
        if (!JobStatusRetriever.isFlowStatus(jobStatus)) {
          jobStatuses.add(ExecutionStatus.valueOf(jobStatus.getEventName()));
        }
      }

      List<ExecutionStatus> statusesInDescendingSalience = ImmutableList.of(ExecutionStatus.FAILED, ExecutionStatus.CANCELLED,
          ExecutionStatus.RUNNING, ExecutionStatus.ORCHESTRATED, ExecutionStatus.COMPLETE);
      flowExecutionStatus = statusesInDescendingSalience.stream().filter(jobStatuses::contains).findFirst().orElse(ExecutionStatus.$UNKNOWN);
    }

    return flowExecutionStatus;
  }
}
