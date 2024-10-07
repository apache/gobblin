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
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.codahale.metrics.MetricRegistry;
import com.google.gson.reflect.TypeToken;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.metrics.DatasetMetric;
import org.apache.gobblin.metrics.FlowStatus;
import org.apache.gobblin.metrics.GaaSFlowObservabilityEvent;
import org.apache.gobblin.metrics.GaaSJobObservabilityEvent;
import org.apache.gobblin.metrics.Issue;
import org.apache.gobblin.metrics.IssueSeverity;
import org.apache.gobblin.metrics.JobStatus;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.OpenTelemetryMetrics;
import org.apache.gobblin.metrics.OpenTelemetryMetricsBase;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.DatasetTaskSummary;
import org.apache.gobblin.runtime.troubleshooter.MultiContextIssueRepository;
import org.apache.gobblin.runtime.troubleshooter.TroubleshooterException;
import org.apache.gobblin.runtime.troubleshooter.TroubleshooterUtils;
import org.apache.gobblin.runtime.util.GsonUtils;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.BaseFlowGraphHelper;
import org.apache.gobblin.service.modules.orchestration.AzkabanProjectConfig;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.spec.SerializationConstants;
import org.apache.gobblin.util.PropertiesUtils;


/**
 * A class embedded within GaaS running in the JobStatusMonitor which emits GaaSObservabilityEvents after each job in a flow
 * This is an abstract class, we need a sub system like Kafka, which support at least once delivery, to emit the event
 */
@Slf4j
public abstract class GaaSJobObservabilityEventProducer implements Closeable {
  public static final String GAAS_JOB_OBSERVABILITY_EVENT_PRODUCER_PREFIX = "GaaSJobObservabilityEventProducer.";
  public static final String GAAS_OBSERVABILITY_EVENT_PRODUCER_CLASS_KEY = GAAS_JOB_OBSERVABILITY_EVENT_PRODUCER_PREFIX + "class.name";
  public static final String DEFAULT_GAAS_OBSERVABILITY_EVENT_PRODUCER_CLASS = NoopGaaSJobObservabilityEventProducer.class.getName();
  public static final String EMIT_FLOW_OBSERVABILITY_EVENT = GAAS_JOB_OBSERVABILITY_EVENT_PRODUCER_PREFIX + "emitFlowLevelEvent";
  public static final String ISSUES_READ_FAILED_METRIC_NAME =  GAAS_JOB_OBSERVABILITY_EVENT_PRODUCER_PREFIX + "getIssuesFailedCount";
  public static final String GAAS_OBSERVABILITY_METRICS_GROUPNAME = GAAS_JOB_OBSERVABILITY_EVENT_PRODUCER_PREFIX + "metrics";
  public static final String GAAS_OBSERVABILITY_JOB_SUCCEEDED_METRIC_NAME = "jobSucceeded";
  private static final String DEFAULT_OPENTELEMETRY_ATTRIBUTE_VALUE = "-";

  protected MetricContext metricContext;
  protected State state;

  List<GaaSJobObservabilityEvent> eventCollector = new ArrayList<>();
  protected OpenTelemetryMetricsBase opentelemetryMetrics;
  protected ObservableLongMeasurement jobStatusMetric;
  protected MultiContextIssueRepository issueRepository;
  protected boolean instrumentationEnabled;
  protected boolean emitFlowObservabilityEvent;
  ContextAwareMeter getIssuesFailedMeter;

  public GaaSJobObservabilityEventProducer(State state, MultiContextIssueRepository issueRepository, boolean instrumentationEnabled) {
    this.state = state;
    this.issueRepository = issueRepository;
    this.instrumentationEnabled = instrumentationEnabled;
    this.emitFlowObservabilityEvent = this.state.getPropAsBoolean(EMIT_FLOW_OBSERVABILITY_EVENT, false);
    if (this.instrumentationEnabled) {
      this.metricContext = Instrumented.getMetricContext(state, getClass());
      this.getIssuesFailedMeter = this.metricContext.contextAwareMeter(MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,
          ISSUES_READ_FAILED_METRIC_NAME));
      setupMetrics(state);
    }
  }

  protected OpenTelemetryMetricsBase getOpentelemetryMetrics(State state) {
    return OpenTelemetryMetrics.getInstance(state);
  }


  private void setupMetrics(State state) {
    this.opentelemetryMetrics = getOpentelemetryMetrics(state);
    if (this.opentelemetryMetrics != null) {
      this.jobStatusMetric = this.opentelemetryMetrics.getMeter(GAAS_OBSERVABILITY_METRICS_GROUPNAME)
          .gaugeBuilder(GAAS_OBSERVABILITY_JOB_SUCCEEDED_METRIC_NAME)
          .ofLongs()
          .buildObserver();
      this.opentelemetryMetrics.getMeter(GAAS_OBSERVABILITY_METRICS_GROUPNAME)
          .batchCallback(() -> {
            for (GaaSJobObservabilityEvent event : this.eventCollector) {
              Attributes tags = getEventAttributes(event);
              int status = event.getJobStatus() == JobStatus.SUCCEEDED ? 1 : 0;
              this.jobStatusMetric.record(status, tags);
            }
            log.info("Submitted {} job status events", this.eventCollector.size());
            // Empty the list of events as they are all emitted at this point.
            this.eventCollector.clear();
          }, this.jobStatusMetric);
    }
  }

  public void emitObservabilityEvent(final State jobState) {
    GaaSJobObservabilityEvent event = createGaaSObservabilityEvent(jobState);
    if (jobState.getProp(TimingEvent.FlowEventConstants.JOB_NAME_FIELD).equals(JobStatusRetriever.NA_KEY) && this.emitFlowObservabilityEvent) {
      sendFlowLevelEvent(convertJobEventToFlowEvent(event, jobState));
    } else {
      sendJobLevelEvent(event);
    }
    this.eventCollector.add(event);
  }

  public Attributes getEventAttributes(GaaSJobObservabilityEvent event) {
    Attributes tags = Attributes.builder().put(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, getOrDefault(event.getFlowName(), DEFAULT_OPENTELEMETRY_ATTRIBUTE_VALUE))
        .put(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, getOrDefault(event.getFlowGroup(), DEFAULT_OPENTELEMETRY_ATTRIBUTE_VALUE))
        .put(TimingEvent.FlowEventConstants.JOB_NAME_FIELD, getOrDefault(event.getJobName(), DEFAULT_OPENTELEMETRY_ATTRIBUTE_VALUE))
        .put(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, event.getFlowExecutionId())
        .put(TimingEvent.FlowEventConstants.SPEC_EXECUTOR_FIELD, getOrDefault(event.getExecutorId(), DEFAULT_OPENTELEMETRY_ATTRIBUTE_VALUE))
        .put(TimingEvent.FlowEventConstants.FLOW_EDGE_FIELD, getOrDefault(event.getFlowEdgeId(), DEFAULT_OPENTELEMETRY_ATTRIBUTE_VALUE))
        .build();
    return tags;
  }

  /**
   * Returns the given string value if it is not empty (i.e., not null and not empty).
   * Otherwise, returns the specified default value.
   *
   * <p>This method utilizes {@link org.apache.commons.lang3.StringUtils#isNotEmpty(CharSequence)}
   * to check if the string is non-empty.</p>
   *
   * @param value the string to check
   * @param defaultValue the default value to return if the provided string is empty or null
   * @return the original string if it is not empty; otherwise, the provided default value
   */
  private String getOrDefault(String value, String defaultValue) {
    return StringUtils.isNotEmpty(value) ? value : defaultValue;
  }

  /**
   * Emits the GaaSJobObservabilityEvent with the mechanism that the child class is built upon e.g. Kafka
   * @param event
   */
  abstract protected void sendJobLevelEvent(GaaSJobObservabilityEvent event);

  abstract protected void sendFlowLevelEvent(GaaSFlowObservabilityEvent event);

  /**
   * Creates a GaaSJobObservabilityEvent which is derived from a final GaaS job pipeline state, which is combination of GTE job states in an ordered fashion
   * @param jobState
   * @return GaaSJobObservabilityEvent
   */
  private GaaSJobObservabilityEvent createGaaSObservabilityEvent(final State jobState) {
    Long jobStartTime = jobState.contains(TimingEvent.JOB_START_TIME) ? jobState.getPropAsLong(TimingEvent.JOB_START_TIME) : null;
    Long jobEndTime = jobState.contains(TimingEvent.JOB_END_TIME) ? jobState.getPropAsLong(TimingEvent.JOB_END_TIME) : null;
    Long jobOrchestratedTime = jobState.contains(TimingEvent.JOB_ORCHESTRATED_TIME) ? jobState.getPropAsLong(TimingEvent.JOB_ORCHESTRATED_TIME) : null;
    Long jobPlanningPhaseStartTime = jobState.contains(TimingEvent.WORKUNIT_PLAN_START_TIME) ? jobState.getPropAsLong(TimingEvent.WORKUNIT_PLAN_START_TIME) : null;
    Long jobPlanningPhaseEndTime = jobState.contains(TimingEvent.WORKUNIT_PLAN_END_TIME) ? jobState.getPropAsLong(TimingEvent.WORKUNIT_PLAN_END_TIME) : null;
    String flowGroup = jobState.getProp(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD);
    String flowName = jobState.getProp(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD);
    Properties jobProperties = new Properties();
    try {
      jobProperties = PropertiesUtils.deserialize(jobState.getProp(JobExecutionPlan.JOB_PROPS_KEY, ""));
    } catch (IOException e) {
      log.error("Could not deserialize job properties for flowGroup {} flowName {} while creating GaaSJobObservabilityEvent due to ", flowGroup, flowName, e);
    }

    String fullFlowEdge = jobState.getProp(TimingEvent.FlowEventConstants.FLOW_EDGE_FIELD, "");
    // Parse the flow edge from edge id that is stored in format sourceNode_destinationNode_flowEdgeId
    String edgeId = StringUtils.substringAfter(
        StringUtils.substringAfter(fullFlowEdge, jobProperties.getProperty(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, "")),
        BaseFlowGraphHelper.FLOW_EDGE_LABEL_JOINER_CHAR);

    Type datasetTaskSummaryType = new TypeToken<ArrayList<DatasetTaskSummary>>(){}.getType();
    List<DatasetTaskSummary> datasetTaskSummaries = jobState.contains(TimingEvent.DATASET_TASK_SUMMARIES) ?
        GsonUtils.GSON_WITH_DATE_HANDLING.fromJson(jobState.getProp(TimingEvent.DATASET_TASK_SUMMARIES), datasetTaskSummaryType) : null;
    List<DatasetMetric> datasetMetrics = datasetTaskSummaries != null ? datasetTaskSummaries.stream().map(
       DatasetTaskSummary::toDatasetMetric).collect(Collectors.toList()) : null;

    GaaSJobObservabilityEvent.Builder builder = GaaSJobObservabilityEvent.newBuilder();
    List<Issue> issueList = null;
    try {
      issueList = getIssuesForJob(issueRepository, jobState);
    } catch (Exception e) {
      // If issues cannot be fetched, increment metric but continue to try to emit the event
      log.error("Could not fetch issues while creating GaaSJobObservabilityEvent due to ", e);
      if (this.instrumentationEnabled) {
        this.getIssuesFailedMeter.mark();
      }
    }
    JobStatus status = convertExecutionStatusTojobState(jobState, ExecutionStatus.valueOf(jobState.getProp(JobStatusRetriever.EVENT_NAME_FIELD)));
    builder.setEventTimestamp(System.currentTimeMillis())
        .setFlowName(flowName)
        .setFlowGroup(flowGroup)
        .setFlowExecutionId(jobState.getPropAsLong(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD))
        .setLastFlowModificationTimestamp(jobState.getPropAsLong(TimingEvent.FlowEventConstants.FLOW_MODIFICATION_TIME_FIELD, 0))
        .setJobName(jobState.getProp(TimingEvent.FlowEventConstants.JOB_NAME_FIELD))
        .setExecutorUrl(jobState.getProp(TimingEvent.METADATA_MESSAGE))
        .setExecutorId(jobState.getProp(TimingEvent.FlowEventConstants.SPEC_EXECUTOR_FIELD, ""))
        .setJobStartTimestamp(jobStartTime)
        .setJobEndTimestamp(jobEndTime)
        .setJobOrchestratedTimestamp(jobOrchestratedTime)
        .setJobPlanningStartTimestamp(jobPlanningPhaseStartTime)
        .setJobPlanningEndTimestamp(jobPlanningPhaseEndTime)
        .setIssues(issueList)
        .setJobStatus(status)
        .setEffectiveUserUrn(jobState.getProp(AzkabanProjectConfig.USER_TO_PROXY, null))
        .setDatasetsMetrics(datasetMetrics)
        .setGaasId(this.state.getProp(ServiceConfigKeys.GOBBLIN_SERVICE_INSTANCE_NAME, null))
        .setJobProperties(GsonUtils.GSON_WITH_DATE_HANDLING.toJson(jobProperties))
        .setSourceNode(jobProperties.getProperty(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY, ""))
        .setDestinationNode(jobProperties.getProperty(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, ""))
        .setFlowEdgeId(!edgeId.isEmpty() ? edgeId : fullFlowEdge)
        .setExecutorUrn(null); //TODO: Fill with information from job execution
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

  private GaaSFlowObservabilityEvent convertJobEventToFlowEvent(GaaSJobObservabilityEvent jobEvent, State jobState) {
    GaaSFlowObservabilityEvent.Builder builder = GaaSFlowObservabilityEvent.newBuilder();
    builder.setEventTimestamp(jobEvent.getEventTimestamp())
        .setGaasId(jobEvent.getGaasId())
        .setFlowName(jobEvent.getFlowName())
        .setFlowGroup(jobEvent.getFlowGroup())
        .setFlowExecutionId(jobEvent.getFlowExecutionId())
        .setLastFlowModificationTimestamp(jobEvent.getLastFlowModificationTimestamp())
        .setSourceNode(jobEvent.getSourceNode())
        .setDestinationNode(jobEvent.getDestinationNode())
        .setEffectiveUserUrn(jobEvent.getEffectiveUserUrn())
        .setFlowStatus(FlowStatus.valueOf(jobEvent.getJobStatus().toString()))
        .setFlowStartTimestamp(jobState.getPropAsLong(SerializationConstants.FLOW_START_TIME_KEY, 0))
        .setFlowEndTimestamp(jobEvent.getJobEndTimestamp());
    return builder.build();
  }

  @Override
  public void close() throws IOException {
    // producer close will handle by the cache
    if (this.instrumentationEnabled) {
      this.metricContext.close();
    }
  }
}
