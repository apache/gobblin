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

package org.apache.gobblin.service.modules.utils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagManagerTest;
import org.apache.gobblin.service.modules.orchestration.DagProcessingEngine;
import org.apache.gobblin.service.modules.orchestration.DagTestUtils;
import org.apache.gobblin.service.modules.orchestration.MySqlDagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.MySqlDagManagementStateStoreTest;
import org.apache.gobblin.service.modules.orchestration.proc.LaunchDagProcTest;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.FlowStatus;
import org.apache.gobblin.service.monitoring.JobStatus;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


/**
 * Test functionality provided by the helper class re-used between the DagProcs and Orchestrator for flow compilation.
 */
public class FlowCompilationValidationHelperTest {
  private final Long jobSpecFlowExecutionId = 1234L;
  private Dag<JobExecutionPlan> jobExecutionPlanDag;
  private MySqlDagManagementStateStore dagManagementStateStore;

  @BeforeClass
  public void setup() throws Exception {
    String dagId = "testDag";
    jobExecutionPlanDag =  DagTestUtils.buildDag(dagId, jobSpecFlowExecutionId);
  }

  @BeforeMethod
  public void resetDMSS() throws Exception {
    ITestMetastoreDatabase testMetastoreDatabase = TestMetastoreDatabaseFactory.get();
    this.dagManagementStateStore = spy(MySqlDagManagementStateStoreTest.getDummyDMSS(testMetastoreDatabase));
    LaunchDagProcTest.mockDMSSCommonBehavior(this.dagManagementStateStore);
  }

  /*
    Tests that addFlowExecutionIdIfAbsent adds the jobSpec flowExecutionId to a flowMetadata object when it is absent
   */
  @Test
  public void testAddFlowExecutionIdWhenAbsent() {
    HashMap<String, String> flowMetadata = new HashMap<>();
    FlowCompilationValidationHelper.addFlowExecutionIdIfAbsent(flowMetadata, jobExecutionPlanDag);
    Assert.assertEquals(flowMetadata.get(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD), String.valueOf(jobSpecFlowExecutionId));
  }

  /*
    Tests that addFlowExecutionIdIfAbsent does not update an existing flowExecutionId in a flowMetadata object
   */
  @Test
  public void testSkipAddingFlowExecutionIdWhenPresent() {
    HashMap<String, String> flowMetadata = new HashMap<>();
    String existingFlowExecutionId = "9999";
    flowMetadata.put(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, existingFlowExecutionId);
    FlowCompilationValidationHelper.addFlowExecutionIdIfAbsent(flowMetadata,jobExecutionPlanDag);
    Assert.assertEquals(flowMetadata.get(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD),
        existingFlowExecutionId);
  }

  @Test
  public void testConcurrentFlowPreviousFlowWithNonTerminalStatusWithNoDag() throws IOException {
    List<FlowStatus> list = new ArrayList<>();
    String flowGroup = "fg";
    String flowName = "fn";
    long previousFlowExecutionId = 12345L;
    JobStatus jobStatus = JobStatus.builder().flowGroup(flowGroup).flowName(flowName).flowExecutionId(previousFlowExecutionId)
        .jobName(JobStatusRetriever.NA_KEY).jobGroup(JobStatusRetriever.NA_KEY).eventName(ExecutionStatus.COMPILED.name()).build();
    Iterator<JobStatus> jobStatusIterator = Lists.newArrayList(jobStatus).iterator();
    list.add(new FlowStatus(flowName, flowGroup, previousFlowExecutionId, jobStatusIterator, ExecutionStatus.COMPILED));
    when(this.dagManagementStateStore.getAllFlowStatusesForFlow(anyString(), anyString())).thenReturn(list);

    Assert.assertFalse(FlowCompilationValidationHelper.isPriorFlowExecutionRunning(flowGroup, flowName,
        previousFlowExecutionId, this.dagManagementStateStore));
  }

  @Test
  public void testConcurrentFlowPreviousExecutionWithNonTerminalStatusRunningBeyondJobStartDeadline()
      throws IOException, URISyntaxException {
    String flowGroup = "fg";
    String flowName = "fn";
    long jobStartDeadline = 10L;
    // extra minus 1 because sometimes assertion reach within a millisecond and makes the flow running within the deadline
    long flowStartTime = System.currentTimeMillis() - jobStartDeadline - 1;
    long currentFlowExecutionId = System.currentTimeMillis() ;

    insertFlowIntoDMSSMock(flowGroup, flowName, flowStartTime, ExecutionStatus.PENDING,
        ConfigFactory.empty()
        .withValue(ConfigurationKeys.GOBBLIN_JOB_START_DEADLINE_TIME_UNIT, ConfigValueFactory.fromAnyRef(TimeUnit.MILLISECONDS.name()))
        .withValue(ConfigurationKeys.GOBBLIN_JOB_START_DEADLINE_TIME, ConfigValueFactory.fromAnyRef(jobStartDeadline)));

    Assert.assertFalse(FlowCompilationValidationHelper.isPriorFlowExecutionRunning(flowGroup, flowName,
        currentFlowExecutionId, this.dagManagementStateStore));
  }

  @Test
  public void testConcurrentFlowPreviousExecutionWithNonTerminalStatusRunningBeyondFlowFinishDeadline()
      throws IOException, URISyntaxException {
    String flowGroup = "fg";
    String flowName = "fn";
    long flowFinishDeadline = 30L;
    long flowStartTime = System.currentTimeMillis() - flowFinishDeadline - 1;
    long currentFlowExecutionId = System.currentTimeMillis() ;

    insertFlowIntoDMSSMock(flowGroup, flowName, flowStartTime, ExecutionStatus.PENDING_RESUME,
        ConfigFactory.empty()
            .withValue(ConfigurationKeys.GOBBLIN_FLOW_FINISH_DEADLINE_TIME_UNIT, ConfigValueFactory.fromAnyRef(TimeUnit.MILLISECONDS.name()))
            .withValue(ConfigurationKeys.GOBBLIN_FLOW_FINISH_DEADLINE_TIME, ConfigValueFactory.fromAnyRef(flowFinishDeadline)));

    Assert.assertFalse(FlowCompilationValidationHelper.isPriorFlowExecutionRunning(flowGroup, flowName,
        currentFlowExecutionId, this.dagManagementStateStore));
  }

  @Test
  public void testConcurrentFlowPreviousExecutionWithNonTerminalStatusRunningWithinFlowFinishDeadline()
      throws IOException, URISyntaxException {
    String flowGroup = "fg";
    String flowName = "fn";
    long flowFinishDeadline = 10000L;
    long flowStartTime = System.currentTimeMillis() - 1 ;  // giving test flowFinishDeadline + 1 ms to finish
    long currentFlowExecutionId = System.currentTimeMillis() ;

    insertFlowIntoDMSSMock(flowGroup, flowName, flowStartTime, ExecutionStatus.RUNNING,
        ConfigFactory.empty()
            .withValue(ConfigurationKeys.GOBBLIN_FLOW_FINISH_DEADLINE_TIME_UNIT, ConfigValueFactory.fromAnyRef(TimeUnit.MILLISECONDS.name()))
            .withValue(ConfigurationKeys.GOBBLIN_FLOW_FINISH_DEADLINE_TIME, ConfigValueFactory.fromAnyRef(flowFinishDeadline)));

    Assert.assertTrue(FlowCompilationValidationHelper.isPriorFlowExecutionRunning(flowGroup, flowName,
        currentFlowExecutionId, this.dagManagementStateStore));
  }

  @Test
  public void testConcurrentFlowNoPreviousExecutionRunning() throws IOException, URISyntaxException {
    String flowGroup = "fg";
    String flowName = "fn";
    long flowStartTime = System.currentTimeMillis();  // giving test flowFinishDeadline to finish
    insertFlowIntoDMSSMock(flowGroup, flowName, flowStartTime, ExecutionStatus.PENDING,
        ConfigFactory.empty()
            .withValue(ConfigurationKeys.GOBBLIN_JOB_START_DEADLINE_TIME_UNIT, ConfigValueFactory.fromAnyRef(TimeUnit.MILLISECONDS.name()))
            .withValue(ConfigurationKeys.GOBBLIN_JOB_START_DEADLINE_TIME, ConfigValueFactory.fromAnyRef(flowStartTime)));

    // change the mock to not return any previous flow status
    when(this.dagManagementStateStore.getAllFlowStatusesForFlow(anyString(), anyString())).thenReturn(Collections.emptyList());

    Assert.assertFalse(FlowCompilationValidationHelper.isPriorFlowExecutionRunning(flowGroup, flowName,
        flowStartTime, this.dagManagementStateStore));
  }

  @Test
  public void testSameFlowExecAlreadyCompiledWithinJobStartDeadline() throws IOException, URISyntaxException {
    String flowGroup = "fg";
    String flowName = "fn";
    long jobStartDeadline = 10000L;
    long flowStartTime = System.currentTimeMillis();

    insertFlowIntoDMSSMock(flowGroup, flowName, flowStartTime, ExecutionStatus.COMPILED,
        ConfigFactory.empty()
            .withValue(ConfigurationKeys.GOBBLIN_JOB_START_DEADLINE_TIME_UNIT, ConfigValueFactory.fromAnyRef(TimeUnit.MILLISECONDS.name()))
            .withValue(ConfigurationKeys.GOBBLIN_JOB_START_DEADLINE_TIME, ConfigValueFactory.fromAnyRef(jobStartDeadline)));

    // flowStartTime = currentFlowExecutionId
    Assert.assertFalse(FlowCompilationValidationHelper.isPriorFlowExecutionRunning(flowGroup, flowName,
        flowStartTime, this.dagManagementStateStore));
  }

  private void insertFlowIntoDMSSMock(String flowGroup, String flowName, long flowStartTime, ExecutionStatus executionStatus, Config config)
      throws URISyntaxException, IOException {
    List<FlowStatus> list = new ArrayList<>();
    JobStatus jobStatus = JobStatus.builder().flowGroup(flowGroup).flowName(flowName).flowExecutionId(flowStartTime)
        .jobName(JobStatusRetriever.NA_KEY).jobGroup(JobStatusRetriever.NA_KEY).eventName(executionStatus.name()).build();
    Iterator<JobStatus> jobStatusIterator = Lists.newArrayList(jobStatus).iterator();
    list.add(new FlowStatus(flowName, flowGroup, flowStartTime, jobStatusIterator, executionStatus));
    when(this.dagManagementStateStore.getAllFlowStatusesForFlow(anyString(), anyString())).thenReturn(list);
    Dag<JobExecutionPlan> dag = DagManagerTest.buildDag("1", flowStartTime,
        DagProcessingEngine.FailureOption.FINISH_ALL_POSSIBLE.name(), 5, "user5", config
            .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
            .withValue(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY, ConfigValueFactory.fromAnyRef(
                MySqlDagManagementStateStoreTest.TEST_SPEC_EXECUTOR_URI)));
    dag.getNodes().forEach(node -> node.getValue().setFlowStartTime(flowStartTime));
    this.dagManagementStateStore.addDag(dag);
  }
}
