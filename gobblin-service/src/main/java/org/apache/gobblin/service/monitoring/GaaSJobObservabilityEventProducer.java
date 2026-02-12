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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Splitter;
import com.google.gson.reflect.TypeToken;
import org.apache.avro.Schema.Field;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
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
  private static final String DEFAULT_OPENTELEMETRY_ATTRIBUTE_VALUE = "NA";
  private static final Splitter COMMA_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

  /**
   * JSON map config: {@code <mdm_dim_key>: <gaas_obs_event_field_key>}.
   * This comes from orchestrator configs. This serves as default set of dimensions to emit.
   */
  public static final String JOB_SUCCEEDED_DIMENSIONS_MAP_KEY =
          "metrics.reporting.opentelemetry.jobSucceeded.dimensionsMap";

  /**
   * This comes from orchestrator configs.
   * Flag to enable/disable per-run extra dimensions for {@link #GAAS_OBSERVABILITY_JOB_SUCCEEDED_METRIC_NAME}.
   */
  public static final String JOB_SUCCEEDED_EXTRA_DIMENSIONS_ENABLED_KEY =
          "metrics.reporting.opentelemetry.jobSucceeded.extraDimensions.enabled";

  /**
   * This comes from common.properties
   * Job property (passed by template(/user)) listing extra dimension keys (comma-separated).
   */
  public static final String JOB_SUCCEEDED_EXTRA_DIMENSIONS_KEYS_JOBPROP =
          "metrics.reporting.opentelemetry.jobSucceeded.extraDimensions.keys";

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

  /**
   * Creates dimensions for the OpenTelemetry event based on the configured mapping of MDM dimension keys to GaaS observability event field keys.
   * If the config is not present or fails to parse, falls back to a default set of dimensions using hardcoded mappings.
   * The dimension values are read from the GaaS observability event.
   * If a value is missing or cannot be read, it defaults to "NA".
   * @param event
   * @return
   */
  public Attributes getEventAttributes(GaaSJobObservabilityEvent event) {
    Map<String, String> configuredMap = getConfiguredJobSucceededDimensionsMap(this.state);
    if (configuredMap == null || configuredMap.isEmpty()) {
      // Backward-compatible fallback to existing hardcoded default attributes
      return Attributes.builder().put(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, getOrDefault(event.getFlowName(), DEFAULT_OPENTELEMETRY_ATTRIBUTE_VALUE))
              .put(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, getOrDefault(event.getFlowGroup(), DEFAULT_OPENTELEMETRY_ATTRIBUTE_VALUE))
              .put(TimingEvent.FlowEventConstants.JOB_NAME_FIELD, getOrDefault(event.getJobName(), DEFAULT_OPENTELEMETRY_ATTRIBUTE_VALUE))
              .put(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, event.getFlowExecutionId())
              .put(TimingEvent.FlowEventConstants.SPEC_EXECUTOR_FIELD, getOrDefault(event.getExecutorId(), DEFAULT_OPENTELEMETRY_ATTRIBUTE_VALUE))
              .put(TimingEvent.FlowEventConstants.FLOW_EDGE_FIELD, getOrDefault(event.getFlowEdgeId(), DEFAULT_OPENTELEMETRY_ATTRIBUTE_VALUE))
              .build();
    }

    AttributesBuilder builder = Attributes.builder();
    for (Map.Entry<String, String> entry : configuredMap.entrySet()) {
      String mdmDimensionKey = entry.getKey();
      String obsEventFieldKey = entry.getValue();
      addObsEventFieldAttribute(builder, mdmDimensionKey, obsEventFieldKey, event);
    }

    if (this.state.getPropAsBoolean(JOB_SUCCEEDED_EXTRA_DIMENSIONS_ENABLED_KEY, false)) {
      addExtraDimensionsFromJobProperties(builder, configuredMap, event);
    }

    return builder.build();
  }

  /**
   * Reads the JSON string config for job succeeded dimensions map, which is expected to be in the format of
   *  {@code <mdm_dim_key>: <gaas_obs_event_field_key>}.
   * @param state
   * @return
   */
  private static Map<String, String> getConfiguredJobSucceededDimensionsMap(State state) {
    String raw = state.getProp(JOB_SUCCEEDED_DIMENSIONS_MAP_KEY, "");
    if (StringUtils.isBlank(raw)) {
      return null;
    }
    try {
      Type type = new TypeToken<Map<String, String>>() {}.getType();
      Map<String, String> parsed = GsonUtils.GSON.fromJson(raw, type);
      if (parsed == null || parsed.isEmpty()) {
        return null;
      }
      // Normalize into a deterministic insertion-order map.
      return new LinkedHashMap<>(parsed);
    } catch (Exception e) {
      log.warn("Failed parsing jobSucceeded dimensionsMap config `{}`; falling back to hardcoded defaults. Raw value: {}",
              JOB_SUCCEEDED_DIMENSIONS_MAP_KEY, raw, e);
      return null;
    }
  }

  /**
   * Adds an attribute to the OpenTelemetry event builder based on the mapping of MDM dimension key to GaaS observability event field key.
   * @param builder
   * @param mdmDimensionKey
   * @param obsEventFieldKey
   * @param event
   */
  private void addObsEventFieldAttribute(AttributesBuilder builder, String mdmDimensionKey, String obsEventFieldKey,
                                         GaaSJobObservabilityEvent event) {
    if (StringUtils.isBlank(mdmDimensionKey) || StringUtils.isBlank(obsEventFieldKey)) {
      return;
    }
    String normalizedEventKey = obsEventFieldKey.trim();

    Object value = getObsEventFieldValue(normalizedEventKey, event);
    if (value == null) {
      builder.put(mdmDimensionKey, DEFAULT_OPENTELEMETRY_ATTRIBUTE_VALUE);
      return;
    }
    if (value instanceof Number) {
      builder.put(mdmDimensionKey, ((Number) value).longValue());
      return;
    }
    if (value instanceof CharSequence) {
      builder.put(mdmDimensionKey, getOrDefault(value.toString(), DEFAULT_OPENTELEMETRY_ATTRIBUTE_VALUE));
      return;
    }
    builder.put(mdmDimensionKey, getOrDefault(String.valueOf(value), DEFAULT_OPENTELEMETRY_ATTRIBUTE_VALUE));
  }

  /**
   * Generic getter for {@link GaaSJobObservabilityEvent} fields used for dimension resolution.
   *
   * <p>Reads values by field name from the Avro schema (SpecificRecord {@code getSchema()/get(int)}),
   */
  private static Object getObsEventFieldValue(String obsEventFieldKey, GaaSJobObservabilityEvent event) {
    try {
      Field field = event.getSchema().getField(obsEventFieldKey);
      if (field == null) {
        return null;
      }
      return event.get(field.pos());
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Reads extra dimension keys from job properties, and adds them as attributes if they are not already present in the default dimensions.
   * The value for the extra dimension is read from job properties using the same key.
   * This allows users to specify additional dimensions on a per-job basis without needing to change the service configuration.
   * @param builder
   * @param defaultDims
   * @param event
   */
  private void addExtraDimensionsFromJobProperties(AttributesBuilder builder, Map<String, String> defaultDims,
                                                   GaaSJobObservabilityEvent event) {
    Map<String, String> jobProps = parseJobPropertiesJson(event);
    if (jobProps == null || jobProps.isEmpty()) {
      return;
    }
    String extraDimensionKeys = jobProps.getOrDefault(JOB_SUCCEEDED_EXTRA_DIMENSIONS_KEYS_JOBPROP, "");
    if (StringUtils.isBlank(extraDimensionKeys)) {
      return;
    }

    for (String key : COMMA_SPLITTER.split(extraDimensionKeys)) {
      if (StringUtils.isBlank(key)) {
        continue;
      }
      // Do not override service-default keys (map keys are the emitted OTel keys)
      if (defaultDims.containsKey(key)) {
        continue;
      }
      String value = jobProps.get(key);
      if (StringUtils.isBlank(value)) {
        continue;
      }
      builder.put(key, value);
    }
  }

  /**
   * Parses the job properties JSON string from the event into a Map. Returns null if the raw string is blank or if parsing fails.
   * @param event
   * @return
   */
  private static Map<String, String> parseJobPropertiesJson(GaaSJobObservabilityEvent event) {
    try {
      String raw = event.getJobProperties();
      if (StringUtils.isBlank(raw)) {
        return null;
      }
      Type type = new TypeToken<Map<String, String>>() {}.getType();
      return GsonUtils.GSON.fromJson(raw, type);
    } catch (Exception e) {
      return null;
    }
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
