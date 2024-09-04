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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.core.GobblinServiceManager;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagProcessingEngine;
import org.apache.gobblin.service.modules.orchestration.DagTestUtils;
import org.apache.gobblin.service.modules.orchestration.MySqlDagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.MySqlDagManagementStateStoreTest;
import org.apache.gobblin.service.modules.orchestration.task.DagProcessingEngineMetrics;
import org.apache.gobblin.service.modules.orchestration.task.EnforceFlowFinishDeadlineDagTask;
import org.apache.gobblin.service.modules.orchestration.task.EnforceJobStartDeadlineDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.JobStatus;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;


public class EnforceDeadlineDagProcsTest {
  private ITestMetastoreDatabase testMetastoreDatabase;
  private final MockedStatic<GobblinServiceManager> mockedGobblinServiceManager = Mockito.mockStatic(GobblinServiceManager.class);
  private DagProcessingEngineMetrics mockedDagProcEngineMetrics;

  @BeforeClass
  public void setUp() throws Exception {
    this.testMetastoreDatabase = TestMetastoreDatabaseFactory.get();
    this.mockedDagProcEngineMetrics = Mockito.mock(DagProcessingEngineMetrics.class);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() throws Exception {
    // `.close()` to avoid (in the aggregate, across multiple suites) - java.sql.SQLNonTransientConnectionException: Too many connections
    this.testMetastoreDatabase.close();
    this.mockedGobblinServiceManager.close();
  }

  /*
    This test simulate submitting a dag with a very short job start deadline that will definitely be breached,
    resulting in the job requiring to be killed
   */
  @Test
  public void enforceJobStartDeadlineTest() throws Exception {
    String flowGroup = "fg";
    String flowName = "fn";
    long flowExecutionId = System.currentTimeMillis();
    MySqlDagManagementStateStore dagManagementStateStore = spy(MySqlDagManagementStateStoreTest.getDummyDMSS(this.testMetastoreDatabase));
    LaunchDagProcTest.mockDMSSCommonBehavior(dagManagementStateStore);
    DagActionStore.DagAction dagAction = new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, "job0",
        DagActionStore.DagActionType.ENFORCE_JOB_START_DEADLINE);
    Dag<JobExecutionPlan> dag = DagTestUtils.buildDag("1", flowExecutionId, DagProcessingEngine.FailureOption.FINISH_ALL_POSSIBLE.name(),
        5, "user5", ConfigFactory.empty()
            .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
            .withValue(ConfigurationKeys.GOBBLIN_JOB_START_DEADLINE_TIME_UNIT, ConfigValueFactory.fromAnyRef(TimeUnit.MILLISECONDS.name()))
            .withValue(ConfigurationKeys.GOBBLIN_JOB_START_DEADLINE_TIME, ConfigValueFactory.fromAnyRef(1L))
            .withValue(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY, ConfigValueFactory.fromAnyRef(
                MySqlDagManagementStateStoreTest.TEST_SPEC_EXECUTOR_URI)));
    List<SpecProducer<Spec>> specProducers = ReevaluateDagProcTest.getDagSpecProducers(dag);
    JobStatus jobStatus = JobStatus.builder().flowName(flowName).flowGroup(flowGroup).jobGroup(flowGroup).jobName("job0").flowExecutionId(flowExecutionId).
        message("Test message").eventName(ExecutionStatus.ORCHESTRATED.name()).startTime(flowExecutionId).shouldRetry(false).orchestratedTime(flowExecutionId).build();

    doReturn(Optional.of(dag)).when(dagManagementStateStore).getDag(any());
    doReturn(Pair.of(Optional.of(dag.getStartNodes().get(0)), Optional.of(jobStatus))).when(dagManagementStateStore).getDagNodeWithJobStatus(any());

    dagManagementStateStore.addDag(dag);  // simulate having a dag that has not yet started running
    dagManagementStateStore.addDagAction(dagAction);

    EnforceJobStartDeadlineDagProc enforceJobStartDeadlineDagProc = new EnforceJobStartDeadlineDagProc(
        new EnforceJobStartDeadlineDagTask(new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId,
            "job0", DagActionStore.DagActionType.ENFORCE_JOB_START_DEADLINE), null,
            dagManagementStateStore, mockedDagProcEngineMetrics), ConfigFactory.empty());
    enforceJobStartDeadlineDagProc.process(dagManagementStateStore, mockedDagProcEngineMetrics);

    int expectedNumOfDeleteDagNodeStates = 1; // the one dag node corresponding to the EnforceStartDeadlineDagProc
    Mockito.verify(specProducers.get(0), Mockito.times(1)).cancelJob(any(), any());
    specProducers.stream().skip(expectedNumOfDeleteDagNodeStates) // separately verified `specProducers.get(0)`
        .forEach(sp -> Mockito.verify(sp, Mockito.never()).cancelJob(any(), any()));
  }

  /*
  This test simulate deletion of a dag action (by not adding it into DMSS).
  Absence of dag action signals that deadline enforcement is not required and hence the test verifies no further action
  is taken by deadline dag proc.
   */
  @Test
  public void enforceJobStartDeadlineTestWithMissingDagAction() throws Exception {
    String flowGroup = "fg";
    String flowName = "fn";
    long flowExecutionId = System.currentTimeMillis();
    MySqlDagManagementStateStore dagManagementStateStore = spy(MySqlDagManagementStateStoreTest.getDummyDMSS(this.testMetastoreDatabase));
    LaunchDagProcTest.mockDMSSCommonBehavior(dagManagementStateStore);
    DagActionStore.DagAction dagAction = new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, "job0",
        DagActionStore.DagActionType.ENFORCE_JOB_START_DEADLINE);
    Dag<JobExecutionPlan> dag = DagTestUtils.buildDag("1", flowExecutionId, DagProcessingEngine.FailureOption.FINISH_ALL_POSSIBLE.name(),
        5, "user5", ConfigFactory.empty()
            .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
            .withValue(ConfigurationKeys.GOBBLIN_JOB_START_DEADLINE_TIME_UNIT, ConfigValueFactory.fromAnyRef(TimeUnit.MILLISECONDS.name()))
            .withValue(ConfigurationKeys.GOBBLIN_JOB_START_DEADLINE_TIME, ConfigValueFactory.fromAnyRef(1L))
            .withValue(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY, ConfigValueFactory.fromAnyRef(
                MySqlDagManagementStateStoreTest.TEST_SPEC_EXECUTOR_URI)));
    List<SpecProducer<Spec>> specProducers = ReevaluateDagProcTest.getDagSpecProducers(dag);
    JobStatus jobStatus = JobStatus.builder().flowName(flowName).flowGroup(flowGroup).jobGroup(flowGroup).jobName("job0").flowExecutionId(flowExecutionId).
        message("Test message").eventName(ExecutionStatus.ORCHESTRATED.name()).startTime(flowExecutionId).shouldRetry(false).orchestratedTime(flowExecutionId).build();

    doReturn(Optional.of(dag)).when(dagManagementStateStore).getDag(any());
    doReturn(Pair.of(Optional.of(dag.getStartNodes().get(0)), Optional.of(jobStatus))).when(dagManagementStateStore).getDagNodeWithJobStatus(any());

    dagManagementStateStore.addDag(dag);  // simulate having a dag that has not yet started running

    EnforceJobStartDeadlineDagProc enforceJobStartDeadlineDagProc = new EnforceJobStartDeadlineDagProc(
        new EnforceJobStartDeadlineDagTask(new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId,
            "job0", DagActionStore.DagActionType.ENFORCE_JOB_START_DEADLINE), null,
            dagManagementStateStore, mockedDagProcEngineMetrics), ConfigFactory.empty());
    enforceJobStartDeadlineDagProc.process(dagManagementStateStore, mockedDagProcEngineMetrics);

    // no job cancelled because we simulated (by not adding) missing dag action
    specProducers.forEach(sp -> Mockito.verify(sp, Mockito.never()).cancelJob(any(), any()));
  }

  /*
   This test simulate submitting a dag with a very short flow finish deadline that will definitely be breached,
    resulting in the dag requiring to be killed
 */
  @Test
  public void enforceFlowFinishDeadlineTest() throws Exception {
    String flowGroup = "fg";
    String flowName = "fn";
    long flowExecutionId = System.currentTimeMillis();
    MySqlDagManagementStateStore dagManagementStateStore = spy(MySqlDagManagementStateStoreTest.getDummyDMSS(this.testMetastoreDatabase));
    LaunchDagProcTest.mockDMSSCommonBehavior(dagManagementStateStore);
    DagActionStore.DagAction dagAction = new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, "job0",
        DagActionStore.DagActionType.ENFORCE_FLOW_FINISH_DEADLINE);
    int numOfDagNodes = 5;
    Dag<JobExecutionPlan> dag = DagTestUtils.buildDag("1", flowExecutionId, DagProcessingEngine.FailureOption.FINISH_ALL_POSSIBLE.name(),
        numOfDagNodes, "user5", ConfigFactory.empty()
            .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
            .withValue(ConfigurationKeys.GOBBLIN_FLOW_FINISH_DEADLINE_TIME_UNIT, ConfigValueFactory.fromAnyRef(TimeUnit.MILLISECONDS.name()))
            .withValue(ConfigurationKeys.GOBBLIN_FLOW_FINSIH_DEADLINE_TIME, ConfigValueFactory.fromAnyRef(1L))
            .withValue(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY, ConfigValueFactory.fromAnyRef(
                MySqlDagManagementStateStoreTest.TEST_SPEC_EXECUTOR_URI)));
    List<SpecProducer<Spec>> specProducers = ReevaluateDagProcTest.getDagSpecProducers(dag);
    JobStatus jobStatus = JobStatus.builder().flowName(flowName).flowGroup(flowGroup).jobGroup(flowGroup).jobName("job0").flowExecutionId(flowExecutionId).
        message("Test message").eventName(ExecutionStatus.RUNNING.name()).startTime(flowExecutionId).shouldRetry(false).orchestratedTime(flowExecutionId).build();

    doReturn(Optional.of(dag)).when(dagManagementStateStore).getDag(any());
    doReturn(Pair.of(Optional.of(dag.getStartNodes().get(0)), Optional.of(jobStatus))).when(dagManagementStateStore).getDagNodeWithJobStatus(any());

    dagManagementStateStore.addDag(dag);  // simulate having a dag that is in running state
    dagManagementStateStore.addDagAction(dagAction);

    EnforceFlowFinishDeadlineDagProc enforceFlowFinishDeadlineDagProc = new EnforceFlowFinishDeadlineDagProc(
        new EnforceFlowFinishDeadlineDagTask(new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId,
            "job0", DagActionStore.DagActionType.ENFORCE_FLOW_FINISH_DEADLINE), null,
            dagManagementStateStore, mockedDagProcEngineMetrics), ConfigFactory.empty());
    enforceFlowFinishDeadlineDagProc.process(dagManagementStateStore, mockedDagProcEngineMetrics);

    specProducers.forEach(sp -> Mockito.verify(sp, Mockito.times(1)).cancelJob(any(), any()));
  }
}
