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

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.codahale.metrics.MetricRegistry;
import com.google.gson.reflect.TypeToken;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.metrics.DatasetMetric;
import org.apache.gobblin.metrics.GaaSObservabilityEventExperimental;
import org.apache.gobblin.metrics.Issue;
import org.apache.gobblin.metrics.IssueSeverity;
import org.apache.gobblin.metrics.JobStatus;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.DatasetTaskSummary;
import org.apache.gobblin.runtime.troubleshooter.MultiContextIssueRepository;
import org.apache.gobblin.runtime.troubleshooter.TroubleshooterException;
import org.apache.gobblin.runtime.troubleshooter.TroubleshooterUtils;
import org.apache.gobblin.runtime.util.GsonUtils;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.orchestration.AzkabanProjectConfig;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;


/**
 * A class embedded within GaaS running in the JobStatusMonitor which emits GaaSObservabilityEvents after each job in a flow
 * This is an abstract class, we need a sub system like Kakfa, which support at least once delivery, to emit the event
 */
@Slf4j
public abstract class GaaSObservabilityEventProducer implements Closeable {
  public static final String GAAS_OBSERVABILITY_EVENT_PRODUCER_PREFIX = "GaaSObservabilityEventProducer.";
  public static final String GAAS_OBSERVABILITY_EVENT_PRODUCER_CLASS_KEY = GAAS_OBSERVABILITY_EVENT_PRODUCER_PREFIX + "class.name";
  public static final String DEFAULT_GAAS_OBSERVABILITY_EVENT_PRODUCER_CLASS = NoopGaaSObservabilityEventProducer.class.getName();
  public static final String ISSUES_READ_FAILED_METRIC_NAME =  GAAS_OBSERVABILITY_EVENT_PRODUCER_PREFIX + "getIssuesFailedCount";

  protected MetricContext metricContext;
  protected State state;
  protected MultiContextIssueRepository issueRepository;
  protected boolean instrumentationEnabled;
  ContextAwareMeter getIssuesFailedMeter;

  public GaaSObservabilityEventProducer(State state, MultiContextIssueRepository issueRepository, boolean instrumentationEnabled) {
    this.state = state;
    this.issueRepository = issueRepository;
    this.instrumentationEnabled = instrumentationEnabled;
    if (this.instrumentationEnabled) {
      this.metricContext = Instrumented.getMetricContext(state, getClass());
      this.getIssuesFailedMeter = this.metricContext.contextAwareMeter(MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,
          ISSUES_READ_FAILED_METRIC_NAME));
    }
  }

  public void emitObservabilityEvent(final State jobState) {
    GaaSObservabilityEventExperimental event = createGaaSObservabilityEvent(jobState);
    sendUnderlyingEvent(event);
  }

  /**
   * Emits the GaaSObservabilityEvent with the mechanism that the child class is built upon e.g. Kafka
   * @param event
   */
  abstract protected void sendUnderlyingEvent(GaaSObservabilityEventExperimental event);

  /**
   * Creates a GaaSObservabilityEvent which is derived from a final GaaS job pipeline state, which is combination of GTE job states in an ordered fashion
   * @param jobState
   * @return GaaSObservabilityEvent
   */
  private GaaSObservabilityEventExperimental createGaaSObservabilityEvent(final State jobState) {
    Long jobStartTime = jobState.contains(TimingEvent.JOB_START_TIME) ? jobState.getPropAsLong(TimingEvent.JOB_START_TIME) : null;
    Long jobEndTime = jobState.contains(TimingEvent.JOB_END_TIME) ? jobState.getPropAsLong(TimingEvent.JOB_END_TIME) : null;
    Long jobOrchestratedTime = jobState.contains(TimingEvent.JOB_ORCHESTRATED_TIME) ? jobState.getPropAsLong(TimingEvent.JOB_ORCHESTRATED_TIME) : null;
    Long jobPlanningPhaseStartTime = jobState.contains(TimingEvent.WORKUNIT_PLAN_START_TIME) ? jobState.getPropAsLong(TimingEvent.WORKUNIT_PLAN_START_TIME) : null;
    Long jobPlanningPhaseEndTime = jobState.contains(TimingEvent.WORKUNIT_PLAN_END_TIME) ? jobState.getPropAsLong(TimingEvent.WORKUNIT_PLAN_END_TIME) : null;
    Type datasetTaskSummaryType = new TypeToken<ArrayList<DatasetTaskSummary>>(){}.getType();
    List<DatasetTaskSummary> datasetTaskSummaries = jobState.contains(TimingEvent.DATASET_TASK_SUMMARIES) ?
        GsonUtils.GSON_WITH_DATE_HANDLING.fromJson(jobState.getProp(TimingEvent.DATASET_TASK_SUMMARIES), datasetTaskSummaryType) : null;
    List<DatasetMetric> datasetMetrics = datasetTaskSummaries != null ? datasetTaskSummaries.stream().map(
       DatasetTaskSummary::toDatasetMetric).collect(Collectors.toList()) : null;

    GaaSObservabilityEventExperimental.Builder builder = GaaSObservabilityEventExperimental.newBuilder();
    List<Issue> issueList = null;
    try {
      issueList = getIssuesForJob(issueRepository, jobState);
    } catch (Exception e) {
      // If issues cannot be fetched, increment metric but continue to try to emit the event
      log.error("Could not fetch issues while creating GaaSObservabilityEvent due to ", e);
      if (this.instrumentationEnabled) {
        this.getIssuesFailedMeter.mark();
      }
    }
    JobStatus status = convertExecutionStatusTojobState(jobState, ExecutionStatus.valueOf(jobState.getProp(JobStatusRetriever.EVENT_NAME_FIELD)));
    builder.setTimestamp(System.currentTimeMillis())
        .setFlowName(jobState.getProp(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD))
        .setFlowGroup(jobState.getProp(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD))
        .setFlowGraphEdgeId(jobState.getProp(TimingEvent.FlowEventConstants.FLOW_EDGE_FIELD, ""))
        .setFlowExecutionId(jobState.getPropAsLong(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD))
        .setLastFlowModificationTime(jobState.getPropAsLong(TimingEvent.FlowEventConstants.FLOW_MODIFICATION_TIME_FIELD, 0))
        .setJobName(jobState.getProp(TimingEvent.FlowEventConstants.JOB_NAME_FIELD))
        .setExecutorUrl(jobState.getProp(TimingEvent.METADATA_MESSAGE))
        .setExecutorId(jobState.getProp(TimingEvent.FlowEventConstants.SPEC_EXECUTOR_FIELD, ""))
        .setJobStartTime(jobStartTime)
        .setJobEndTime(jobEndTime)
        .setJobOrchestratedTime(jobOrchestratedTime)
        .setJobPlanningPhaseStartTime(jobPlanningPhaseStartTime)
        .setJobPlanningPhaseEndTime(jobPlanningPhaseEndTime)
        .setIssues(issueList)
        .setJobStatus(status)
        .setExecutionUserUrn(jobState.getProp(AzkabanProjectConfig.USER_TO_PROXY, null))
        .setDatasetsWritten(datasetMetrics)
        .setGaasId(this.state.getProp(ServiceConfigKeys.GOBBLIN_SERVICE_INSTANCE_NAME, null))
        .setJobProperties(jobState.getProp(JobExecutionPlan.JOB_PROPS_KEY, null));
    return builder.build();
  }

  private static JobStatus convertExecutionStatusTojobState(State state, ExecutionStatus executionStatus) {
    switch (executionStatus) {
      case FAILED:
        // TODO: Separate failure cases to SUBMISSION FAILURE and COMPILATION FAILURE, investigate events to populate these fields
        if (state.contains(TimingEvent.JOB_END_TIME)) {
          return JobStatus.EXECUTION_FAILURE;
        }
        return JobStatus.SUBMISSION_FAILURE;
      case COMPLETE:
        return JobStatus.SUCCEEDED;
      case CANCELLED:
        // TODO: If cancelled due to start SLA exceeded, consider grouping this as a submission failure?
        return JobStatus.CANCELLED;
      default:
        return null;
    }
  }

  private static List<Issue> getIssuesForJob(MultiContextIssueRepository issueRepository, State jobState) throws TroubleshooterException {
    return issueRepository.getAll(TroubleshooterUtils.getContextIdForJob(jobState.getProperties())).stream().map(
        issue -> new Issue(
            issue.getTime().toEpochSecond(),
            IssueSeverity.valueOf(issue.getSeverity().toString()),
            issue.getCode(),
            issue.getSummary(),
            issue.getDetails(),
            issue.getProperties()
        )).collect(Collectors.toList());
  }

  @Override
  public void close() throws IOException {
    // producer close will handle by the cache
    if (this.instrumentationEnabled) {
      this.metricContext.close();
    }
  }
}
