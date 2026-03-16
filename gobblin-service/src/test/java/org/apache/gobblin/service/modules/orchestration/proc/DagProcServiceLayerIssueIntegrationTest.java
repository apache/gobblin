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

package org.apache.gobblin.service.modules.orchestration.proc;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.apache.gobblin.metrics.RootMetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.GobblinEventBuilder;
import org.apache.gobblin.runtime.troubleshooter.IssueSeverity;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;


/**
 * Tests for {@link OrchestratorIssueEmitter} issue emission.
 */
public class DagProcServiceLayerIssueIntegrationTest {

  @Test
  public void testGenerateIssueCodeIsDeterministic() {
    String code1 = OrchestratorIssueEmitter.generateIssueCode("Flow failed because job X failed");
    String code2 = OrchestratorIssueEmitter.generateIssueCode("Flow failed because job X failed");
    Assert.assertEquals(code1, code2, "Same summary should produce same issue code");
  }

  @Test
  public void testGenerateIssueCodeHasCorrectPrefix() {
    String code = OrchestratorIssueEmitter.generateIssueCode("test summary");
    Assert.assertTrue(code.startsWith("S"), "Issue code should start with S prefix");
    Assert.assertEquals(code.length(), 7, "Issue code should be S + 6 hex chars");
  }

  @Test
  public void testDifferentSummariesProduceDifferentCodes() {
    String code1 = OrchestratorIssueEmitter.generateIssueCode("Flow failed because job X failed");
    String code2 = OrchestratorIssueEmitter.generateIssueCode("DAG not found for kill request");
    Assert.assertNotEquals(code1, code2, "Different summaries should produce different codes");
  }

  @Test
  public void testEmitFlowIssueWithDagId() {
    EventSubmitter spySubmitter = Mockito.spy(
        new EventSubmitter.Builder(RootMetricContext.get(), "org.apache.gobblin.service").build());

    Dag.DagId dagId = new Dag.DagId("test-group", "test-flow", 12345L);

    OrchestratorIssueEmitter.emitFlowIssue(spySubmitter, dagId, IssueSeverity.ERROR, "Test flow issue");

    ArgumentCaptor<GobblinEventBuilder> captor = ArgumentCaptor.forClass(GobblinEventBuilder.class);
    Mockito.verify(spySubmitter).submit(captor.capture());

    GobblinEventBuilder captured = captor.getValue();
    Assert.assertEquals(captured.getMetadata().get("flowGroup"), "test-group");
    Assert.assertEquals(captured.getMetadata().get("flowName"), "test-flow");
    Assert.assertEquals(captured.getMetadata().get("flowExecutionId"), "12345");
    Assert.assertEquals(captured.getMetadata().get("jobName"), JobStatusRetriever.NA_KEY);
    Assert.assertEquals(captured.getMetadata().get("issueSource"), "service-layer");
  }

  @Test
  public void testEmitJobIssueWithDagId() {
    EventSubmitter spySubmitter = Mockito.spy(
        new EventSubmitter.Builder(RootMetricContext.get(), "org.apache.gobblin.service").build());

    Dag.DagId dagId = new Dag.DagId("test-group", "test-flow", 12345L);

    OrchestratorIssueEmitter.emitJobIssue(spySubmitter, dagId, "my-job",
        IssueSeverity.ERROR, "Job submission failed");

    ArgumentCaptor<GobblinEventBuilder> captor = ArgumentCaptor.forClass(GobblinEventBuilder.class);
    Mockito.verify(spySubmitter).submit(captor.capture());

    GobblinEventBuilder captured = captor.getValue();
    Assert.assertEquals(captured.getMetadata().get("flowGroup"), "test-group");
    Assert.assertEquals(captured.getMetadata().get("flowName"), "test-flow");
    Assert.assertEquals(captured.getMetadata().get("jobName"), "my-job");
  }

  @Test
  public void testEmitFlowIssueWithStringIds() {
    EventSubmitter spySubmitter = Mockito.spy(
        new EventSubmitter.Builder(RootMetricContext.get(), "org.apache.gobblin.service").build());

    OrchestratorIssueEmitter.emitFlowIssue(spySubmitter, "fg", "fn", "99999",
        IssueSeverity.WARN, "Concurrent execution blocked");

    ArgumentCaptor<GobblinEventBuilder> captor = ArgumentCaptor.forClass(GobblinEventBuilder.class);
    Mockito.verify(spySubmitter).submit(captor.capture());

    GobblinEventBuilder captured = captor.getValue();
    Assert.assertEquals(captured.getMetadata().get("flowGroup"), "fg");
    Assert.assertEquals(captured.getMetadata().get("flowName"), "fn");
    Assert.assertEquals(captured.getMetadata().get("flowExecutionId"), "99999");
  }

  @Test
  public void testEmitDoesNotThrowOnNullEventSubmitter() {
    // Should not throw - emit catches all exceptions internally
    OrchestratorIssueEmitter.emitFlowIssue(null, "fg", "fn", "123",
        IssueSeverity.ERROR, "test");
  }
}
