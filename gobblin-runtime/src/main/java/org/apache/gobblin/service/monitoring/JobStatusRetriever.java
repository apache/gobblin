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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.typesafe.config.ConfigFactory;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;


import org.apache.gobblin.configuration.ConfigurationKeys;
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
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Retriever for {@link JobStatus}.
 */
@Slf4j
public abstract class JobStatusRetriever implements LatestFlowExecutionIdTracker {
  public static final String EVENT_NAME_FIELD = "eventName";
  public static final String NA_KEY = "NA";

  // File/blob movement writers report per-dataset copy metrics on the JobSummary event via these
  // flat metadata keys; the job-status monitor persists them into the job state, and they are
  // surfaced here on the JobStatistics REST surface. -1 = absent/unsupported for the counts;
  // the snapshot ids and partitions are comma-separated lists, null = absent/unreported.
  public static final String BYTES_WRITTEN_FIELD = "bytesCopied";
  public static final String RECORDS_WRITTEN_FIELD = "rowsCopied";
  public static final String FILES_COMMITTED_FIELD = "filesCommitted";
  public static final String SNAPSHOTS_COMMITTED_FIELD = "snapshotsCommitted";
  public static final String PARTITIONS_COMMITTED_FIELD = "partitionsCommitted";

  @Getter
  protected final MetricContext metricContext;

  private final MultiContextIssueRepository issueRepository;

  protected JobStatusRetriever(MultiContextIssueRepository issueRepository) {
    this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(ConfigFactory.empty()), getClass());
    this.issueRepository = Objects.requireNonNull(issueRepository);
  }

  public abstract Iterator<JobStatus> getJobStatusesForFlowExecution(String flowName, String flowGroup,
      long flowExecutionId);

  public abstract Iterator<JobStatus> getJobStatusesForFlowExecution(String flowName, String flowGroup,
      long flowExecutionId, String jobName, String jobGroup);

  /**
   * Read retained states without discarding backend metadata during conversion to {@link JobStatus}.
   * Includes the flow-summary record, if present. Callers must validate the decoded identity before using
   * execution handles: storage keys alone do not establish the identity of the serialized contents.
   *
   * <p>Unlike status-display APIs, read failures propagate rather than appearing as an empty result.</p>
   *
   * @throws IOException if this retriever has no state store or retained state cannot be read
   */
  public List<State> getJobStatusStatesForFlowExecution(String flowName, String flowGroup, long flowExecutionId)
      throws IOException {
    Objects.requireNonNull(flowName, "flowName");
    Objects.requireNonNull(flowGroup, "flowGroup");
    String separator = ServiceConfigKeys.STATE_STORE_KEY_SEPARATION_CHARACTER;
    String storeName = flowGroup + separator + flowName;
    String tablePrefix = flowExecutionId + separator;
    StateStore<State> stateStore = getStateStore();
    if (stateStore == null) {
      throw new IOException("Raw job status retrieval requires a state store: " + getClass().getName());
    }
    List<State> states = new ArrayList<>();
    for (String tableName : stateStore.getTableNames(storeName, name -> name.startsWith(tablePrefix))) {
      states.addAll(stateStore.getAll(storeName, tableName));
    }
    return states;
  }

  /**
   * Get the latest {@link FlowStatus}es of executions of flows belonging to this flow group.  Currently, latest flow execution
   * is decided by comparing {@link JobStatus#getFlowExecutionId()}.
   * @return `FlowStatus`es of `flowGroup`, ordered by ascending flowName, with all of each name adjacent and by descending flowExecutionId.
   *
   * NOTE: return `List`, not `Iterator` for non-side-effecting access.
   */
  public abstract List<FlowStatus> getFlowStatusesForFlowGroupExecutions(String flowGroup, int countJobStatusesPerFlowName);

  /**
   * Get all the  {@link FlowStatus}es of executions of flows belonging to this flow group and flowName.  Currently, latest flow execution
   * is decided by comparing {@link JobStatus#getFlowExecutionId()}.
   * @return `FlowStatus`es are ordered by descending flowExecutionId.
   **/
  public abstract List<FlowStatus> getAllFlowStatusesForFlowExecutionsOrdered(String flowGroup,String flowName);


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

    return latestExecutionId == -1L ? Collections.emptyIterator()
        : getJobStatusesForFlowExecution(flowName, flowGroup, latestExecutionId);
  }

  /**
   *
   * @param jobState instance of {@link State}
   * @return deserialize {@link State} into a {@link JobStatus}.
   */
  protected JobStatus getJobStatus(State jobState) {
    JobStatus.JobStatusBuilder jobStatusBuilder = createJobStatusBuilderFromState(jobState);

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

    jobStatusBuilder.issues(jobIssues);
    return jobStatusBuilder.build();
  }

  public static JobStatus.JobStatusBuilder createJobStatusBuilderFromState(State jobState) {
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
    long bytesWritten = jobState.getPropAsLong(BYTES_WRITTEN_FIELD, -1L);
    long recordsWritten = jobState.getPropAsLong(RECORDS_WRITTEN_FIELD, -1L);
    long filesCommitted = jobState.getPropAsLong(FILES_COMMITTED_FIELD, -1L);
    String snapshotsCommitted = jobState.getProp(SNAPSHOTS_COMMITTED_FIELD, null);
    String partitionsCommitted = jobState.getProp(PARTITIONS_COMMITTED_FIELD, null);

    return JobStatus.builder().flowName(flowName).flowGroup(flowGroup).flowExecutionId(flowExecutionId).jobName(jobName)
        .jobGroup(jobGroup).jobTag(jobTag).jobExecutionId(jobExecutionId).eventName(eventName).lowWatermark(lowWatermark)
        .highWatermark(highWatermark).orchestratedTime(orchestratedTime).startTime(startTime).endTime(endTime)
        .message(message).processedCount(processedCount).maxAttempts(maxAttempts).currentAttempts(currentAttempts)
        .currentGeneration(currentGeneration).shouldRetry(shouldRetry).progressPercentage(progressPercentage)
        .lastProgressEventTime(lastProgressEventTime)
        .bytesWritten(bytesWritten).recordsWritten(recordsWritten).filesCommitted(filesCommitted)
        .snapshotsCommitted(snapshotsCommitted).partitionsCommitted(partitionsCommitted);
  }

  protected static final String getFlowGroup(State jobState) {
    return jobState.getProp(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD);
  }

  protected static final String getFlowName(State jobState) {
    return jobState.getProp(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD);
  }

  protected static final long getFlowExecutionId(State jobState) {
    return Long.parseLong(jobState.getProp(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD));
  }

  protected static final String getJobGroup(State jobState) {
    return jobState.getProp(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD);
  }

  protected static final String getJobName(State jobState) {
    return jobState.getProp(TimingEvent.FlowEventConstants.JOB_NAME_FIELD);
  }

  /**
   * Resolves job execution id from job state. Used {@link ConfigurationKeys#GAAS_JOB_EXEC_ID_HASH} when present
   * or returns 0 as default.
   */
  protected static final long getJobExecutionId(State jobState) {
    String jobExecId = jobState.getProp(ConfigurationKeys.GAAS_JOB_EXEC_ID_HASH, "0");
    if (StringUtils.isNotBlank(jobExecId)) {
      try {
        return Long.parseLong(jobExecId);
      } catch (NumberFormatException e) {
        log.error("gaas.job.executionid.hash is not numeric, deriving long from string: {}", jobExecId, e);
      }
    }
    return 0;
  }

  protected Iterator<JobStatus> asJobStatuses(List<State> jobStatusStates) {
    return jobStatusStates.stream().map(this::getJobStatus).iterator();
  }

  protected List<FlowStatus> asFlowStatuses(List<FlowExecutionJobStateGrouping> flowExecutionGroupings) {
    return flowExecutionGroupings.stream().map(exec -> {
      List<JobStatus> jobStatuses = ImmutableList.copyOf(asJobStatuses(exec.getJobStates().stream().sorted(
          // rationalized order, to facilitate test assertions
          Comparator.comparing(JobStatusRetriever::getJobGroup)
              .thenComparing(JobStatusRetriever::getJobName)
              .thenComparing(JobStatusRetriever::getJobExecutionId)
      ).collect(Collectors.toList())));
      return new FlowStatus(exec.getFlowName(), exec.getFlowGroup(), exec.getFlowExecutionId(), jobStatuses.iterator(),
            getFlowStatusFromJobStatuses(jobStatuses.iterator()));
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
  /**
   * Groups job status states by flow execution IDs optionally limiting the number of executions per flow name.
   *
   * @param flowGroup The group to which the flow executions belong.
   * @param jobStatusStates List of job status states to process.
   * @param maxCountPerFlowName Maximum number of executions to retain per flow name.
   *                           If null, all executions are returned
   * @return List of FlowExecutionJobStateGrouping objects containing the latest job states
   *         grouped by flow execution ID and sorted by flow name in ascending order.
   */
  protected List<FlowExecutionJobStateGrouping> groupByFlowExecutionAndRetainLatest(
      String flowGroup, List<State> jobStatusStates, Integer maxCountPerFlowName) {
    Map<String, Map<Long, List<State>>> statesByFlowExecutionIdByName = jobStatusStates.stream().collect(
        Collectors.groupingBy(JobStatusRetriever::getFlowName, Collectors.groupingBy(JobStatusRetriever::getFlowExecutionId)));

    return statesByFlowExecutionIdByName.entrySet().stream().sorted(Map.Entry.comparingByKey()).flatMap(flowNameEntry -> {
      String flowName = flowNameEntry.getKey();
      Map<Long, List<State>> statesByFlowExecutionIdForName = flowNameEntry.getValue();

      List<Long> executionIds;
      if (maxCountPerFlowName != null) {
        // If maxCountPerFlowName is specified, limit the number of executions per flow name
        executionIds = Ordering.natural().greatestOf(statesByFlowExecutionIdForName.keySet(), maxCountPerFlowName);
      } else {
        // If maxCountPerFlowName is not specified (null), return all execution IDs sorted in descending order
        executionIds = new ArrayList<>(statesByFlowExecutionIdForName.keySet());
        executionIds.sort(Comparator.reverseOrder());
      }
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

  public static ExecutionStatus getFlowStatusFromJobStatuses(Iterator<JobStatus> jobStatusIterator) {
    ExecutionStatus flowExecutionStatus = ExecutionStatus.$UNKNOWN;

    while (jobStatusIterator.hasNext()) {
      JobStatus jobStatus = jobStatusIterator.next();
      // Check if this is the flow status instead of a single job status
      if (JobStatusRetriever.isFlowStatus(jobStatus)) {
        flowExecutionStatus = ExecutionStatus.valueOf(jobStatus.getEventName());
      }
    }

    return flowExecutionStatus;
  }
}
