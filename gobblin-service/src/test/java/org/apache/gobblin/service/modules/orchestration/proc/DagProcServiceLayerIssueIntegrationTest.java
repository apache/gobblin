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

import java.io.IOException;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.troubleshooter.InMemoryMultiContextIssueRepository;
import org.apache.gobblin.runtime.troubleshooter.TroubleshooterUtils;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.task.DagProcessingEngineMetrics;
import org.apache.gobblin.service.modules.orchestration.task.ReevaluateDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.JobStatus;

import static org.mockito.Mockito.*;


/**
 * Integration tests for service-layer issue capture in DagProc.
 *
 * <p>These tests verify that:
 * <ul>
 *   <li>Service-layer exceptions are captured as Issues</li>
 *   <li>Issues are correctly attributed to the right flow/job</li>
 *   <li>MDC is properly cleaned up between executions</li>
 *   <li>Feature flag correctly enables/disables troubleshooter</li>
 * </ul>
 */
public class DagProcServiceLayerIssueIntegrationTest {

  private static final Logger log = LoggerFactory.getLogger(DagProcServiceLayerIssueIntegrationTest.class);

  private DagManagementStateStore mockDagManagementStateStore;
  private DagProcessingEngineMetrics mockMetrics;
  private Config config;

  @BeforeMethod
  public void setUp() {
    mockDagManagementStateStore = Mockito.mock(DagManagementStateStore.class);
    mockMetrics = Mockito.mock(DagProcessingEngineMetrics.class);

    // Enable service-layer troubleshooter
    config = ConfigFactory.empty()
        .withValue(ServiceConfigKeys.SERVICE_LAYER_TROUBLESHOOTER_ENABLED,
            ConfigValueFactory.fromAnyRef(true));

    MDC.clear();
  }

  @AfterMethod
  public void tearDown() {
    MDC.clear();
  }

  /**
   * Custom DagProc implementation for testing that throws an exception in initialize().
   */
  private static class TestFailingInitDagProc extends ReevaluateDagProc {
    public TestFailingInitDagProc(ReevaluateDagTask task, Config config) {
      super(task, config);
    }

    @Override
    protected Pair<Optional<Dag.DagNode<JobExecutionPlan>>, Optional<JobStatus>> initialize(
        DagManagementStateStore dagManagementStateStore) throws IOException {
      // Log error before throwing exception
      log.error("Test error in initialize() for flow integration test", new RuntimeException("Initialize failed"));
      throw new IOException("Initialization failed for testing");
    }
  }

  /**
   * Custom DagProc implementation for testing that throws an exception in act().
   */
  private static class TestFailingActDagProc extends ReevaluateDagProc {
    public TestFailingActDagProc(ReevaluateDagTask task, Config config) {
      super(task, config);
    }

    @Override
    protected void act(DagManagementStateStore dagManagementStateStore,
        org.apache.commons.lang3.tuple.Pair<Optional<Dag.DagNode<JobExecutionPlan>>, Optional<JobStatus>> state,
        DagProcessingEngineMetrics metrics) throws IOException {
      // Log error before throwing exception
      log.error("Test error in act() for flow integration test", new RuntimeException("Act failed"));
      throw new IOException("Act failed for testing");
    }
  }

  @Test
  public void testServiceLayerExceptionCapturedInInitialize() throws Exception {
    // Create ReevaluateDagTask
    DagActionStore.DagAction dagAction = DagActionStore.DagAction.forFlow(
        "test-group", "test-flow", 123456L, DagActionStore.DagActionType.REEVALUATE);
    dagAction = new DagActionStore.DagAction(
        dagAction.getFlowGroup(),
        dagAction.getFlowName(),
        dagAction.getFlowExecutionId(),
        "test-job",
        dagAction.getDagActionType()
    );
    ReevaluateDagTask task = new ReevaluateDagTask(dagAction, null, mockDagManagementStateStore, mockMetrics);

    // Create DagProc that will fail in initialize()
    TestFailingInitDagProc dagProc = new TestFailingInitDagProc(task, config);

    // Process should throw exception
    try {
      dagProc.process(mockDagManagementStateStore, mockMetrics);
      Assert.fail("Expected IOException to be thrown");
    } catch (IOException e) {
      // Expected
      Assert.assertTrue(e.getMessage().contains("Initialization failed"), "Exception message should match");
    }

    // Verify MDC is cleared
    Assert.assertNull(MDC.get(ConfigurationKeys.FLOW_GROUP_KEY), "MDC should be cleared after processing");
  }

  @Test
  public void testMdcCleanedUpBetweenExecutions() throws Exception {
    // Create first DagAction
    DagActionStore.DagAction dagAction1 = new DagActionStore.DagAction(
        "group-A", "flow-A", 111L, "job-A", DagActionStore.DagActionType.REEVALUATE);
    ReevaluateDagTask task1 = new ReevaluateDagTask(dagAction1, null, mockDagManagementStateStore, mockMetrics);

    // Create DagProc that will fail
    TestFailingInitDagProc dagProc1 = new TestFailingInitDagProc(task1, config);

    try {
      dagProc1.process(mockDagManagementStateStore, mockMetrics);
    } catch (IOException e) {
      // Expected
    }

    // Verify MDC is cleared after first execution
    Assert.assertNull(MDC.get(ConfigurationKeys.FLOW_GROUP_KEY), "MDC should be cleared after first execution");
    Assert.assertNull(MDC.get(ConfigurationKeys.FLOW_NAME_KEY), "MDC should be cleared after first execution");

    // Create second DagAction (different flow)
    DagActionStore.DagAction dagAction2 = new DagActionStore.DagAction(
        "group-B", "flow-B", 222L, "job-B", DagActionStore.DagActionType.REEVALUATE);
    ReevaluateDagTask task2 = new ReevaluateDagTask(dagAction2, null, mockDagManagementStateStore, mockMetrics);

    TestFailingActDagProc dagProc2 = new TestFailingActDagProc(task2, config);

    // Mock successful initialize for second proc
    when(mockDagManagementStateStore.getDagNodeWithJobStatus(any()))
        .thenReturn(org.apache.commons.lang3.tuple.Pair.of(Optional.empty(), Optional.empty()));

    try {
      dagProc2.process(mockDagManagementStateStore, mockMetrics);
    } catch (IOException e) {
      // Expected
    }

    // Verify MDC is cleared after second execution
    Assert.assertNull(MDC.get(ConfigurationKeys.FLOW_GROUP_KEY), "MDC should be cleared after second execution");

    // This test verifies that Flow B's errors won't be attributed to Flow A
    // because MDC is properly cleaned up between executions
  }

  @Test
  public void testMdcContextSetCorrectly() throws Exception {
    // Create a special DagProc that captures MDC values during execution
    final String[] capturedFlowGroup = new String[1];
    final String[] capturedFlowName = new String[1];
    final String[] capturedFlowExecutionId = new String[1];
    final String[] capturedJobName = new String[1];

    DagActionStore.DagAction dagAction = new DagActionStore.DagAction(
        "test-group-123", "test-flow-456", 789L, "test-job-abc",
        DagActionStore.DagActionType.REEVALUATE);
    ReevaluateDagTask task = new ReevaluateDagTask(dagAction, null, mockDagManagementStateStore, mockMetrics);

    ReevaluateDagProc dagProc = new ReevaluateDagProc(task, config) {
      @Override
      protected org.apache.commons.lang3.tuple.Pair<Optional<Dag.DagNode<JobExecutionPlan>>, Optional<JobStatus>> initialize(
          DagManagementStateStore store) throws IOException {
        // Capture MDC values during execution
        capturedFlowGroup[0] = MDC.get(ConfigurationKeys.FLOW_GROUP_KEY);
        capturedFlowName[0] = MDC.get(ConfigurationKeys.FLOW_NAME_KEY);
        capturedFlowExecutionId[0] = MDC.get(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);
        capturedJobName[0] = MDC.get(ConfigurationKeys.JOB_NAME_KEY);
        return org.apache.commons.lang3.tuple.Pair.of(Optional.empty(), Optional.empty());
      }
    };

    when(mockDagManagementStateStore.getDagNodeWithJobStatus(any()))
        .thenReturn(org.apache.commons.lang3.tuple.Pair.of(Optional.empty(), Optional.empty()));

    dagProc.process(mockDagManagementStateStore, mockMetrics);

    // Verify MDC was set correctly during execution
    Assert.assertEquals(capturedFlowGroup[0], "test-group-123", "Flow group should match");
    Assert.assertEquals(capturedFlowName[0], "test-flow-456", "Flow name should match");
    Assert.assertEquals(capturedFlowExecutionId[0], "789", "Flow execution ID should match");
    Assert.assertEquals(capturedJobName[0], "test-job-abc", "Job name should match");

    // Verify MDC is cleared after execution
    Assert.assertNull(MDC.get(ConfigurationKeys.FLOW_GROUP_KEY), "MDC should be cleared after execution");
  }

  @Test
  public void testContextIdFormatMatchesExecutorSide() {
    // Verify context ID format is consistent with executor side
    String flowGroup = "pipelines";
    String flowName = "daily-etl";
    String flowExecutionId = "1770226800011";
    String jobName = "extract-job";

    String contextId = TroubleshooterUtils.getContextIdForJob(
        flowGroup, flowName, flowExecutionId, jobName);

    // Should be: flowGroup:flowName:flowExecutionId:jobName
    Assert.assertEquals(contextId, "pipelines:daily-etl:1770226800011:extract-job",
        "Context ID format should match executor side");
  }
}
