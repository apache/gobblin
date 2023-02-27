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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;
import org.testng.Assert;

import com.google.common.collect.Maps;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metrics.GaaSObservabilityEventExperimental;
import org.apache.gobblin.metrics.JobStatus;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.metrics.reporter.util.AvroBinarySerializer;
import org.apache.gobblin.metrics.reporter.util.AvroSerializer;
import org.apache.gobblin.metrics.reporter.util.NoopSchemaVersionWriter;
import org.apache.gobblin.runtime.troubleshooter.InMemoryMultiContextIssueRepository;
import org.apache.gobblin.runtime.troubleshooter.Issue;
import org.apache.gobblin.runtime.troubleshooter.IssueSeverity;
import org.apache.gobblin.runtime.troubleshooter.MultiContextIssueRepository;
import org.apache.gobblin.runtime.troubleshooter.TroubleshooterUtils;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.orchestration.AzkabanProjectConfig;


public class GaaSObservabilityProducerTest {

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
    MockGaaSObservabilityEventProducer producer = new MockGaaSObservabilityEventProducer(new State(), this.issueRepository);
    Map<String, String> gteEventMetadata = Maps.newHashMap();
    gteEventMetadata.put(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, flowGroup);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, flowName);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, flowExecutionId);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.JOB_NAME_FIELD, jobName);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD, flowName);
    gteEventMetadata.put(TimingEvent.FlowEventConstants.FLOW_EDGE_FIELD, "flowEdge");
    gteEventMetadata.put(TimingEvent.FlowEventConstants.SPEC_EXECUTOR_FIELD, "specExecutor");
    gteEventMetadata.put(AzkabanProjectConfig.USER_TO_PROXY, "azkabanUser");
    gteEventMetadata.put(TimingEvent.METADATA_MESSAGE, "hostName");
    gteEventMetadata.put(TimingEvent.METADATA_START_TIME, "20");
    gteEventMetadata.put(TimingEvent.METADATA_END_TIME, "100");
    gteEventMetadata.put(JobStatusRetriever.EVENT_NAME_FIELD, ExecutionStatus.COMPLETE.name());
    gteEventMetadata.put(TimingEvent.JOB_ORCHESTRATED_TIME, "1");

    Properties jobStatusProps = new Properties();
    jobStatusProps.putAll(gteEventMetadata);
    producer.emitObservabilityEvent(new State(jobStatusProps));

    List<GaaSObservabilityEventExperimental> emittedEvents = producer.getTestEmittedEvents();

    Assert.assertEquals(emittedEvents.size(), 1);
    Iterator<GaaSObservabilityEventExperimental> iterator = emittedEvents.iterator();
    GaaSObservabilityEventExperimental event = iterator.next();
    Assert.assertEquals(event.getFlowGroup(), flowGroup);
    Assert.assertEquals(event.getFlowName(), flowName);
    Assert.assertEquals(event.getJobName(), jobName);
    Assert.assertEquals(event.getFlowExecutionId(), Long.valueOf(flowExecutionId));
    Assert.assertEquals(event.getJobStatus(), JobStatus.SUCCEEDED);
    Assert.assertEquals(event.getExecutorUrl(), "hostName");
    Assert.assertEquals(event.getIssues().size(), 1);
    Assert.assertEquals(event.getFlowGraphEdgeId(), "flowEdge");
    Assert.assertEquals(event.getExecutorId(), "specExecutor");
    Assert.assertEquals(event.getExecutionUserUrn(), "azkabanUser");
    Assert.assertEquals(event.getJobOrchestratedTime(), Long.valueOf(1));
    Assert.assertEquals(event.getJobStartTime(), Long.valueOf(20));

    AvroSerializer<GaaSObservabilityEventExperimental> serializer = new AvroBinarySerializer<>(
        GaaSObservabilityEventExperimental.SCHEMA$, new NoopSchemaVersionWriter()
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
    MockGaaSObservabilityEventProducer producer = new MockGaaSObservabilityEventProducer(new State(), this.issueRepository);
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

    List<GaaSObservabilityEventExperimental> emittedEvents = producer.getTestEmittedEvents();

    Assert.assertEquals(emittedEvents.size(), 1);
    Iterator<GaaSObservabilityEventExperimental> iterator = emittedEvents.iterator();
    GaaSObservabilityEventExperimental event = iterator.next();
    Assert.assertEquals(event.getFlowGroup(), flowGroup);
    Assert.assertEquals(event.getFlowName(), flowName);
    Assert.assertEquals(event.getJobName(), jobName);
    Assert.assertEquals(event.getFlowExecutionId(), Long.valueOf(flowExecutionId));
    Assert.assertEquals(event.getJobStatus(), JobStatus.CANCELLED);
    Assert.assertEquals(event.getIssues().size(), 1);
    Assert.assertEquals(event.getFlowGraphEdgeId(), "flowEdge");
    Assert.assertEquals(event.getExecutorId(), "specExecutor");
    Assert.assertEquals(event.getJobOrchestratedTime(), null);
    Assert.assertEquals(event.getJobStartTime(), null);
    Assert.assertEquals(event.getExecutionUserUrn(), null);
    Assert.assertEquals(event.getExecutorUrl(), null);

    AvroSerializer<GaaSObservabilityEventExperimental> serializer = new AvroBinarySerializer<>(
        GaaSObservabilityEventExperimental.SCHEMA$, new NoopSchemaVersionWriter()
    );
    serializer.serializeRecord(event);
  }

  private Issue createTestIssue(String summary, String code, IssueSeverity severity) {
    return Issue.builder().summary(summary).code(code).time(ZonedDateTime.now()).severity(severity).build();
  }
}
