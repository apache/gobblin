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

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;
import com.google.gson.JsonParser;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metrics.FlowStatus;
import org.apache.gobblin.metrics.GaaSFlowObservabilityEvent;
import org.apache.gobblin.metrics.GaaSJobObservabilityEvent;
import org.apache.gobblin.metrics.JobStatus;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.metrics.reporter.util.AvroBinarySerializer;
import org.apache.gobblin.metrics.reporter.util.AvroSerializer;
import org.apache.gobblin.metrics.reporter.util.NoopSchemaVersionWriter;
import org.apache.gobblin.runtime.DatasetTaskSummary;
import org.apache.gobblin.runtime.troubleshooter.InMemoryMultiContextIssueRepository;
import org.apache.gobblin.runtime.troubleshooter.Issue;
import org.apache.gobblin.runtime.troubleshooter.IssueSeverity;
import org.apache.gobblin.runtime.troubleshooter.MultiContextIssueRepository;
import org.apache.gobblin.runtime.troubleshooter.TroubleshooterUtils;
import org.apache.gobblin.runtime.util.GsonUtils;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.orchestration.AzkabanProjectConfig;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.spec.SerializationConstants;
import org.apache.gobblin.util.PropertiesUtils;


public class GaaSJobObservabilityProducerTest {

  private MultiContextIssueRepository issueRepository = new InMemoryMultiContextIssueRepository();

  @Test
  public void testCreateGaaSObservabilityEventWithFullMetadata() throws Exception {
    String flowGroup = "testFlowGroup1";
    String flowName = "testFlowName1";
    String jobName = String.format("%s_%s_%s", flowGroup, flowName, "testJobName1");
    String flowExecutionId = "1";
    this.issueRepository.put(
        TroubleshooterUtils.getContextIdForJob(flowGroup, flowName, flowExecutionId, jobName),
        createTestIssue("issueSummary", "issueCode", IssueSeverity.INFO)
    );
    List<DatasetTaskSummary> summaries = new ArrayList<>();
    DatasetTaskSummary dataset1 = new DatasetTaskSummary("/testFolder", 100, 1000, true, "PASSED");
    DatasetTaskSummary dataset2 = new DatasetTaskSummary("/testFolder2", 1000, 10000, false, "PASSED");
    summaries.add(dataset1);
    summaries.add(dataset2);
    Properties jobProps = new Properties();
    jobProps.setProperty("flow.executionId", "1681242538558");
    jobProps.setProperty("user.to.proxy", "newUser");
    jobProps.setProperty("gobblin.flow.sourceIdentifier", "sourceNode");
    jobProps.setProperty("gobblin.flow.destinationIdentifier", "destinationNode");

    State state = new State();
    state.setProp(ServiceConfigKeys.GOBBLIN_SERVICE_INSTANCE_NAME, "testCluster");
    MockGaaSJobObservabilityEventProducer producer = new MockGaaSJobObservabilityEventProducer(state, this.issueRepository, false);
    Map<String, String> gteEventMetadata = Maps.newHashMap();
    gteEventMetadata.put(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, flowGroup);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, flowName);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, flowExecutionId);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.JOB_NAME_FIELD, jobName);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD, flowName);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.FLOW_EDGE_FIELD, "sourceNode_destinationNode_flowEdge");
    gteEventMetadata.put(TimingEvent.FlowEventConstants.SPEC_EXECUTOR_FIELD, "specExecutor");
    gteEventMetadata.put(AzkabanProjectConfig.USER_TO_PROXY, "azkabanUser");
    gteEventMetadata.put(TimingEvent.METADATA_MESSAGE, "hostName");
    gteEventMetadata.put(TimingEvent.JOB_START_TIME, "20");
    gteEventMetadata.put(TimingEvent.JOB_END_TIME, "100");
    gteEventMetadata.put(JobStatusRetriever.EVENT_NAME_FIELD, ExecutionStatus.COMPLETE.name());
    gteEventMetadata.put(TimingEvent.JOB_ORCHESTRATED_TIME, "1");
    gteEventMetadata.put(TimingEvent.FlowEventConstants.FLOW_MODIFICATION_TIME_FIELD, "20");
    gteEventMetadata.put(TimingEvent.DATASET_TASK_SUMMARIES, GsonUtils.GSON_WITH_DATE_HANDLING.toJson(summaries));
    gteEventMetadata.put(JobExecutionPlan.JOB_PROPS_KEY, PropertiesUtils.serialize(jobProps));
    Properties jobStatusProps = new Properties();
    jobStatusProps.putAll(gteEventMetadata);
    producer.emitObservabilityEvent(new State(jobStatusProps));

    List<GaaSJobObservabilityEvent> emittedEvents = producer.getTestEmittedJobEvents();

    Assert.assertEquals(emittedEvents.size(), 1);
    Iterator<GaaSJobObservabilityEvent> iterator = emittedEvents.iterator();
    GaaSJobObservabilityEvent event = iterator.next();
    Assert.assertEquals(event.getFlowGroup(), flowGroup);
    Assert.assertEquals(event.getFlowName(), flowName);
    Assert.assertEquals(event.getJobName(), jobName);
    Assert.assertEquals(event.getFlowExecutionId(), Long.valueOf(flowExecutionId));
    Assert.assertEquals(event.getJobStatus(), JobStatus.SUCCEEDED);
    Assert.assertEquals(event.getExecutorUrl(), "hostName");
    Assert.assertEquals(event.getIssues().size(), 1);
    Assert.assertEquals(event.getFlowEdgeId(), "flowEdge");
    Assert.assertEquals(event.getSourceNode(), "sourceNode");
    Assert.assertEquals(event.getDestinationNode(), "destinationNode");
    Assert.assertEquals(event.getExecutorId(), "specExecutor");
    Assert.assertEquals(event.getEffectiveUserUrn(), "azkabanUser");
    Assert.assertEquals(event.getJobOrchestratedTimestamp(), Long.valueOf(1));
    Assert.assertEquals(event.getLastFlowModificationTimestamp(), Long.valueOf(20));
    Assert.assertEquals(event.getJobStartTimestamp(), Long.valueOf(20));
    Assert.assertEquals(event.getJobEndTimestamp(), Long.valueOf(100));
    Assert.assertEquals(event.getDatasetsMetrics().size(), 2);
    Assert.assertEquals(event.getDatasetsMetrics().get(0).getDatasetUrn(), dataset1.getDatasetUrn());
    Assert.assertEquals(event.getDatasetsMetrics().get(0).getEntitiesWritten(), Long.valueOf(dataset1.getRecordsWritten()));
    Assert.assertEquals(event.getDatasetsMetrics().get(0).getBytesWritten(), Long.valueOf(dataset1.getBytesWritten()));
    Assert.assertEquals(event.getDatasetsMetrics().get(0).getSuccessfullyCommitted(), Boolean.valueOf(dataset1.isSuccessfullyCommitted()));
    Assert.assertEquals(event.getDatasetsMetrics().get(1).getDatasetUrn(), dataset2.getDatasetUrn());
    Assert.assertEquals(event.getDatasetsMetrics().get(1).getEntitiesWritten(), Long.valueOf(dataset2.getRecordsWritten()));
    Assert.assertEquals(event.getDatasetsMetrics().get(1).getBytesWritten(), Long.valueOf(dataset2.getBytesWritten()));
    Assert.assertEquals(event.getDatasetsMetrics().get(1).getSuccessfullyCommitted(), Boolean.valueOf(dataset2.isSuccessfullyCommitted()));
    JsonParser.parseString(event.getJobProperties()); // Should not throw
    Assert.assertEquals(event.getGaasId(), "testCluster");
    AvroSerializer<GaaSJobObservabilityEvent> serializer = new AvroBinarySerializer<>(
        GaaSJobObservabilityEvent.SCHEMA$, new NoopSchemaVersionWriter()
    );
    serializer.serializeRecord(event);
  }

  @Test
  public void testCreateGaaSObservabilityEventWithPartialMetadata() throws Exception {
    String flowGroup = "testFlowGroup2";
    String flowName = "testFlowName2";
    String jobName = String.format("%s_%s_%s", flowGroup, flowName, "testJobName1");
    String flowExecutionId = "1";
    this.issueRepository.put(
        TroubleshooterUtils.getContextIdForJob(flowGroup, flowName, flowExecutionId, jobName),
        createTestIssue("issueSummary", "issueCode", IssueSeverity.INFO)
    );
    MockGaaSJobObservabilityEventProducer
        producer = new MockGaaSJobObservabilityEventProducer(new State(), this.issueRepository, false);
    Map<String, String> gteEventMetadata = Maps.newHashMap();
    gteEventMetadata.put(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, flowGroup);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, flowName);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, "1");
    gteEventMetadata.put(TimingEvent.FlowEventConstants.JOB_NAME_FIELD, jobName);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD, flowName);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.FLOW_EDGE_FIELD, "flowEdge");
    gteEventMetadata.put(TimingEvent.FlowEventConstants.SPEC_EXECUTOR_FIELD, "specExecutor");
    gteEventMetadata.put(JobStatusRetriever.EVENT_NAME_FIELD, ExecutionStatus.CANCELLED.name());

    Properties jobStatusProps = new Properties();
    jobStatusProps.putAll(gteEventMetadata);
    producer.emitObservabilityEvent(new State(jobStatusProps));

    List<GaaSJobObservabilityEvent> emittedEvents = producer.getTestEmittedJobEvents();

    Assert.assertEquals(emittedEvents.size(), 1);
    Iterator<GaaSJobObservabilityEvent> iterator = emittedEvents.iterator();
    GaaSJobObservabilityEvent event = iterator.next();
    Assert.assertEquals(event.getFlowGroup(), flowGroup);
    Assert.assertEquals(event.getFlowName(), flowName);
    Assert.assertEquals(event.getJobName(), jobName);
    Assert.assertEquals(event.getFlowExecutionId(), Long.valueOf(flowExecutionId));
    Assert.assertEquals(event.getJobStatus(), JobStatus.CANCELLED);
    Assert.assertEquals(event.getIssues().size(), 1);
    Assert.assertEquals(event.getFlowEdgeId(), "flowEdge");
    Assert.assertEquals(event.getExecutorId(), "specExecutor");
    Assert.assertEquals(event.getJobOrchestratedTimestamp(), null);
    Assert.assertEquals(event.getJobStartTimestamp(), null);
    Assert.assertEquals(event.getEffectiveUserUrn(), null);
    Assert.assertEquals(event.getExecutorUrl(), null);

    AvroSerializer<GaaSJobObservabilityEvent> serializer = new AvroBinarySerializer<>(
        GaaSJobObservabilityEvent.SCHEMA$, new NoopSchemaVersionWriter()
    );
    serializer.serializeRecord(event);
  }

  @Test
  public void testCreateGaaSObservabilityFlowEvent() throws Exception {
    String flowGroup = "testFlowGroup3";
    String flowName = "testFlowName3";
    String jobName = JobStatusRetriever.NA_KEY;
    String flowExecutionId = "1";
    this.issueRepository.put(
        TroubleshooterUtils.getContextIdForJob(flowGroup, flowName, flowExecutionId, jobName),
        createTestIssue("issueSummary", "issueCode", IssueSeverity.INFO)
    );
    State producerState = new State();
    producerState.setProp(GaaSJobObservabilityEventProducer.EMIT_FLOW_OBSERVABILITY_EVENT, "true");
    MockGaaSJobObservabilityEventProducer
        producer = new MockGaaSJobObservabilityEventProducer(producerState, this.issueRepository, false);
    Map<String, String> gteEventMetadata = Maps.newHashMap();
    gteEventMetadata.put(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, flowGroup);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, flowName);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, "1");
    gteEventMetadata.put(TimingEvent.FlowEventConstants.JOB_NAME_FIELD, jobName);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD, flowName);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.SPEC_EXECUTOR_FIELD, "specExecutor");
    gteEventMetadata.put(JobStatusRetriever.EVENT_NAME_FIELD, ExecutionStatus.COMPLETE.name());
    gteEventMetadata.put(SerializationConstants.FLOW_START_TIME_KEY, "1");

    Properties jobStatusProps = new Properties();
    jobStatusProps.putAll(gteEventMetadata);
    producer.emitObservabilityEvent(new State(jobStatusProps));

    List<GaaSFlowObservabilityEvent> emittedEvents = producer.getTestEmittedFlowEvents();

    Assert.assertEquals(emittedEvents.size(), 1);
    Iterator<GaaSFlowObservabilityEvent> iterator = emittedEvents.iterator();
    GaaSFlowObservabilityEvent event = iterator.next();
    Assert.assertEquals(event.getFlowGroup(), flowGroup);
    Assert.assertEquals(event.getFlowName(), flowName);
    Assert.assertEquals(event.getFlowExecutionId(), Long.valueOf(flowExecutionId));
    Assert.assertEquals(event.getFlowStatus(), FlowStatus.SUCCEEDED);
    Assert.assertNull(event.getEffectiveUserUrn());
    Assert.assertEquals(event.getFlowStartTimestamp(), Long.valueOf(1));

    AvroSerializer<GaaSFlowObservabilityEvent> serializer = new AvroBinarySerializer<>(
        GaaSFlowObservabilityEvent.SCHEMA$, new NoopSchemaVersionWriter()
    );
    serializer.serializeRecord(event);
  }

  @Test
  public void testEnableMetrics() throws Exception {
    String flowGroup = "testFlowGroup2";
    String flowName = "testFlowName2";
    String jobName = String.format("%s_%s_%s", flowGroup, flowName, "testJobName1");
    String flowExecutionId = "1";
    this.issueRepository.put(
        TroubleshooterUtils.getContextIdForJob(flowGroup, flowName, flowExecutionId, jobName),
        createTestIssue("issueSummary", "issueCode", IssueSeverity.INFO)
    );
    State producerState = new State();
    producerState.setProp(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_ENABLED, "true");
    producerState.setProp(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_ENDPOINT, "http://localhost:5000");

    MockGaaSJobObservabilityEventProducer
        producer = new MockGaaSJobObservabilityEventProducer(producerState, this.issueRepository, true);

    Map<String, String> gteEventMetadata = Maps.newHashMap();
    gteEventMetadata.put(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, flowGroup);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, flowName);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, "1");
    gteEventMetadata.put(TimingEvent.FlowEventConstants.JOB_NAME_FIELD, jobName);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD, flowName);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.FLOW_EDGE_FIELD, "flowEdge");
    gteEventMetadata.put(TimingEvent.FlowEventConstants.SPEC_EXECUTOR_FIELD, "specExecutor");
    gteEventMetadata.put(JobStatusRetriever.EVENT_NAME_FIELD, ExecutionStatus.CANCELLED.name());

    Properties jobStatusProps = new Properties();
    jobStatusProps.putAll(gteEventMetadata);

    // Ensure that this doesn't throw due to NPE
    producer.emitObservabilityEvent(new State(jobStatusProps));
  }

  @Test
  public void testMockProduceMetrics() throws Exception {
    String flowGroup = "testFlowGroup2";
    String flowName = "testFlowName2";
    String jobName = String.format("%s_%s_%s", flowGroup, flowName, "testJobName1");
    String flowExecutionId = "1";
    this.issueRepository.put(
        TroubleshooterUtils.getContextIdForJob(flowGroup, flowName, flowExecutionId, jobName),
        createTestIssue("issueSummary", "issueCode", IssueSeverity.INFO)
    );
    State producerState = new State();
    producerState.setProp(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_ENABLED, "true");

    MockGaaSJobObservabilityEventProducer
        producer = new MockGaaSJobObservabilityEventProducer(producerState, this.issueRepository, true);
    Map<String, String> gteEventMetadata = Maps.newHashMap();
    gteEventMetadata.put(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, flowGroup);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, flowName);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, "1");
    gteEventMetadata.put(TimingEvent.FlowEventConstants.JOB_NAME_FIELD, jobName);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD, flowName);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.FLOW_EDGE_FIELD, "flowEdge");
    gteEventMetadata.put(TimingEvent.FlowEventConstants.SPEC_EXECUTOR_FIELD, "specExecutor");
    gteEventMetadata.put(JobStatusRetriever.EVENT_NAME_FIELD, ExecutionStatus.CANCELLED.name());

    Map<String, String> gteEventMetadata2 = Maps.newHashMap();
    gteEventMetadata2.put(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, flowGroup);
    gteEventMetadata2.put(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, flowName);
    gteEventMetadata2.put(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, "2");
    gteEventMetadata2.put(TimingEvent.FlowEventConstants.JOB_NAME_FIELD, jobName);
    gteEventMetadata2.put(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD, flowName);
    gteEventMetadata2.put(TimingEvent.FlowEventConstants.FLOW_EDGE_FIELD, "flowEdge");
    gteEventMetadata2.put(TimingEvent.FlowEventConstants.SPEC_EXECUTOR_FIELD, "specExecutor");
    gteEventMetadata2.put(JobStatusRetriever.EVENT_NAME_FIELD, ExecutionStatus.COMPLETE.name());

    Properties jobStatusProps = new Properties();
    Properties jobStatusProps2 = new Properties();
    jobStatusProps.putAll(gteEventMetadata);    // Ensure that this doesn't throw due to NPE
    producer.emitObservabilityEvent(new State(jobStatusProps));
    jobStatusProps2.putAll(gteEventMetadata2);
    producer.emitObservabilityEvent(new State(jobStatusProps2));
    Collection<MetricData> metrics = producer.getOpentelemetryMetrics().metricReader.collectAllMetrics();
    // Check number of meters
    Assert.assertEquals(metrics.size(), 1);
    Map<String, MetricData > metricsByName = metrics.stream().collect(Collectors.toMap(metric -> metric.getName(), metricData -> metricData));
    MetricData jobStatusMetric = metricsByName.get("jobSucceeded");
    // Check the attributes of the metrics
    List<LongPointData> datapoints = jobStatusMetric.getLongGaugeData().getPoints().stream().collect(Collectors.toList());
    Assert.assertEquals(datapoints.size(), 2);
    // Check that the values are different for the two events (order not guaranteed for the same collection event)
    Assert.assertNotEquals(datapoints.get(0).getValue(), datapoints.get(1).getValue());
    Assert.assertNotEquals(datapoints.get(0).getAttributes().asMap().get(AttributeKey.longKey("flowExecutionId")),
        datapoints.get(1).getAttributes().asMap().get(AttributeKey.longKey("flowExecutionId")));

    // Check common string tag
    Assert.assertEquals(datapoints.get(0).getAttributes().asMap().get(AttributeKey.stringKey("flowGroup")), flowGroup);
    Assert.assertEquals(datapoints.get(1).getAttributes().asMap().get(AttributeKey.stringKey("flowGroup")), flowGroup);
    datapoints.forEach(point -> {
      if (point.getAttributes().asMap().get(AttributeKey.longKey("flowExecutionId")).equals(1L)) {
        Assert.assertEquals(point.getValue(), 0); // Cancelled job should show up as a 0
      } else if (point.getAttributes().asMap().get(AttributeKey.longKey("flowExecutionId")).equals(2L)) {
        Assert.assertEquals(point.getValue(), 1L); // Completed job should show up as a 1
      }
      Assert.assertEquals(point.getAttributes().asMap().get(AttributeKey.stringKey("flowName")), flowName);
      Assert.assertEquals(point.getAttributes().asMap().get(AttributeKey.stringKey("jobName")), jobName);
      Assert.assertEquals(point.getAttributes().asMap().get(AttributeKey.stringKey("flowEdge")), "flowEdge");
      Assert.assertEquals(point.getAttributes().asMap().get(AttributeKey.stringKey("specExecutor")), "specExecutor");
    });
  }

  private Issue createTestIssue(String summary, String code, IssueSeverity severity) {
    return Issue.builder().summary(summary).code(code).time(ZonedDateTime.now()).severity(severity).build();
  }
}
