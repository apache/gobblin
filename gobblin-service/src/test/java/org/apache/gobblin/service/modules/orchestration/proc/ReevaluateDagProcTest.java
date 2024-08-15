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
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.metrics.RootMetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagTestUtils;
import org.apache.gobblin.service.modules.orchestration.DagUtils;
import org.apache.gobblin.service.modules.orchestration.DagProcessingEngine;
import org.apache.gobblin.service.modules.orchestration.MySqlDagManagementStateStoreTest;
import org.apache.gobblin.service.modules.orchestration.task.DagProcessingEngineMetrics;
import org.apache.gobblin.service.modules.orchestration.task.ReevaluateDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.JobStatus;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.powermock.reflect.Whitebox.setInternalState;


@RunWith(PowerMockRunner.class)
@PrepareForTest(EventSubmitter.class)
public class ReevaluateDagProcTest {
  private final long flowExecutionId = System.currentTimeMillis();
  private final String flowGroup = "fg";
  private ITestMetastoreDatabase testMetastoreDatabase;
  private DagManagementStateStore dagManagementStateStore;
  private DagProcessingEngineMetrics mockedDagProcEngineMetrics;
  private MockedStatic<DagProc> dagProc;
  private EventSubmitter mockedEventSubmitter;

  @BeforeClass
  public void setUpClass() throws Exception {
    this.testMetastoreDatabase = TestMetastoreDatabaseFactory.get();
    this.dagProc = mockStatic(DagProc.class);
  }

  @BeforeMethod
  public void setUp() throws Exception {
    this.dagManagementStateStore = spy(MySqlDagManagementStateStoreTest.getDummyDMSS(this.testMetastoreDatabase));
    LaunchDagProcTest.mockDMSSCommonBehavior(dagManagementStateStore);
    this.mockedDagProcEngineMetrics = Mockito.mock(DagProcessingEngineMetrics.class);
    this.mockedEventSubmitter = spy(new EventSubmitter.Builder(RootMetricContext.get(), "org.apache.gobblin.service").build());
    setInternalState(DagProc.class, "eventSubmitter", this.mockedEventSubmitter);
  }

  @AfterClass(alwaysRun = true)
  public void tearDownClass() throws Exception {
    // `.close()` to avoid (in the aggregate, across multiple suites) - java.sql.SQLNonTransientConnectionException: Too many connections
    this.testMetastoreDatabase.close();
    this.dagProc.close();
  }

  @Test
  public void testOneNextJobToRun() throws Exception {
    String flowName = "fn";
    Dag.DagId dagId = new Dag.DagId(flowGroup, flowName, flowExecutionId);
    Dag<JobExecutionPlan> dag = DagTestUtils.buildDag("1", flowExecutionId, DagProcessingEngine.FailureOption.FINISH_ALL_POSSIBLE.name(),
        2, "user5", ConfigFactory.empty()
            .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
            .withValue(ConfigurationKeys.JOB_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY, ConfigValueFactory.fromAnyRef(
                MySqlDagManagementStateStoreTest.TEST_SPEC_EXECUTOR_URI))
    );
    List<SpecProducer<Spec>> specProducers = getDagSpecProducers(dag);
    JobStatus jobStatus = JobStatus.builder().flowName(flowName).flowGroup(flowGroup).jobGroup(flowGroup).jobName("job0")
        .flowExecutionId(flowExecutionId).message("Test message").eventName(ExecutionStatus.COMPLETE.name())
        .startTime(flowExecutionId).shouldRetry(false).orchestratedTime(flowExecutionId).build();
    dagManagementStateStore.addDag(dag);

    /*
    We cannot check the spec producers if addSpec is called on them because spec producer object changes in writing/reading
    from DMSS. So, in order to verify job submission by checking the mock invocations on spec producers, we will have to
    fix the dag, which includes SpecProducer, by mocking, when we read a dag from DMSS
    */
    doReturn(new ImmutablePair<>(Optional.of(dag.getStartNodes().get(0)), Optional.of(jobStatus)))
        .when(dagManagementStateStore).getDagNodeWithJobStatus(any());
    doReturn(Optional.of(dag)). when(dagManagementStateStore).getDag(dagId);

    ReevaluateDagProc
        reEvaluateDagProc = new ReevaluateDagProc(new ReevaluateDagTask(new DagActionStore.DagAction(flowGroup, flowName,
        flowExecutionId, "job0", DagActionStore.DagActionType.REEVALUATE), null,
        dagManagementStateStore, mockedDagProcEngineMetrics), ConfigFactory.empty());
    reEvaluateDagProc.process(dagManagementStateStore, mockedDagProcEngineMetrics);
    // next job is sent to spec producer
    Mockito.verify(specProducers.get(1), Mockito.times(1)).addSpec(any());
    // there are two invocations, one after setting status and other after sending new job to specProducer
    Mockito.verify(this.dagManagementStateStore, Mockito.times(2)).updateDagNode(any());

    // assert that the first job is completed
    Assert.assertEquals(ExecutionStatus.COMPLETE,
        this.dagManagementStateStore.getDag(dagId).get().getStartNodes().get(0).getValue().getExecutionStatus());

    // note that only assertFalse can be tested on DagProcUtils.isDagFinished, because if it had returned true, dag must have been cleaned
    Assert.assertFalse(DagProcUtils.isDagFinished(this.dagManagementStateStore.getDag(dagId).get()));
  }

  // test when there does not exist a next job in the dag when the current job's reevaluate dag action is processed
  @Test
  public void testNoNextJobToRun() throws Exception {
    String flowName = "fn2";
    Dag.DagId dagId = new Dag.DagId(flowGroup, flowName, flowExecutionId);
    Dag<JobExecutionPlan> dag = DagTestUtils.buildDag("2", flowExecutionId, DagProcessingEngine.FailureOption.FINISH_ALL_POSSIBLE.name(),
        1, "user5", ConfigFactory.empty()
            .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
            .withValue(ConfigurationKeys.JOB_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY, ConfigValueFactory.fromAnyRef(
                MySqlDagManagementStateStoreTest.TEST_SPEC_EXECUTOR_URI))
    );
    JobStatus jobStatus = JobStatus.builder().flowName(flowName).flowGroup(flowGroup).jobGroup(flowGroup).jobName("job0")
        .flowExecutionId(flowExecutionId).message("Test message").eventName(ExecutionStatus.COMPLETE.name())
        .startTime(flowExecutionId).shouldRetry(false).orchestratedTime(flowExecutionId).build();
    dagManagementStateStore.addDag(dag);

    Dag<JobExecutionPlan> mockedDag = DagTestUtils.buildDag("2", flowExecutionId, DagProcessingEngine.FailureOption.FINISH_ALL_POSSIBLE.name(),
        1, "user5", ConfigFactory.empty()
            .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
            .withValue(ConfigurationKeys.JOB_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY, ConfigValueFactory.fromAnyRef(
                MySqlDagManagementStateStoreTest.TEST_SPEC_EXECUTOR_URI))
    );
    // mock getDagNodeWithJobStatus() to return a dagNode with status completed
    mockedDag.getNodes().get(0).getValue().setExecutionStatus(ExecutionStatus.COMPLETE);
    doReturn(new ImmutablePair<>(Optional.of(mockedDag.getNodes().get(0)), Optional.of(jobStatus)))
        .when(dagManagementStateStore).getDagNodeWithJobStatus(any());

    Assert.assertFalse(DagProcUtils.isDagFinished(this.dagManagementStateStore.getDag(dagId).get()));

    List<SpecProducer<Spec>> specProducers = getDagSpecProducers(dag);

    ReevaluateDagProc
        reEvaluateDagProc = new ReevaluateDagProc(new ReevaluateDagTask(new DagActionStore.DagAction(flowGroup, flowName,
        flowExecutionId, "job0", DagActionStore.DagActionType.REEVALUATE), null,
        dagManagementStateStore, mockedDagProcEngineMetrics), ConfigFactory.empty());
    reEvaluateDagProc.process(dagManagementStateStore, mockedDagProcEngineMetrics);

    // no new job to launch for this one job flow
    specProducers.forEach(sp -> Mockito.verify(sp, Mockito.never()).addSpec(any()));

    // dag is deleted because the only job in the dag is completed
    Assert.assertEquals(Mockito.mockingDetails(dagManagementStateStore).getInvocations().stream()
        .filter(a -> a.getMethod().getName().equals("deleteDag")).count(), 1);

    Assert.assertEquals(Mockito.mockingDetails(this.dagManagementStateStore).getInvocations().stream()
        .filter(a -> a.getMethod().getName().equals("deleteDagAction")).count(), 1);
  }

  @Test
  public void testCurrentJobToRun() throws Exception {
    String flowName = "fn3";
    Dag<JobExecutionPlan> dag = DagTestUtils.buildDag("1", flowExecutionId, DagProcessingEngine.FailureOption.FINISH_ALL_POSSIBLE.name(),
        2, "user5", ConfigFactory.empty()
            .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
            .withValue(ConfigurationKeys.JOB_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY, ConfigValueFactory.fromAnyRef(
                MySqlDagManagementStateStoreTest.TEST_SPEC_EXECUTOR_URI))
    );
    Dag.DagId dagId = DagUtils.generateDagId(dag);
    List<SpecProducer<Spec>> specProducers = getDagSpecProducers(dag);
    dagManagementStateStore.addDag(dag);
    doReturn(new ImmutablePair<>(Optional.of(dag.getNodes().get(0)), Optional.empty()))
        .when(dagManagementStateStore).getDagNodeWithJobStatus(any());

    ReevaluateDagProc
        reEvaluateDagProc = new ReevaluateDagProc(new ReevaluateDagTask(new DagActionStore.DagAction(flowGroup, flowName,
        flowExecutionId, "job0", DagActionStore.DagActionType.REEVALUATE), null,
        dagManagementStateStore, mockedDagProcEngineMetrics), ConfigFactory.empty());
    reEvaluateDagProc.process(dagManagementStateStore, mockedDagProcEngineMetrics);

    int numOfLaunchedJobs = 1; // only the current job
    // only the current job should have run
    Mockito.verify(specProducers.get(0), Mockito.times(1)).addSpec(any());

    specProducers.stream().skip(numOfLaunchedJobs) // separately verified `specProducers.get(0)`
        .forEach(sp -> Mockito.verify(sp, Mockito.never()).addSpec(any()));

    Mockito.verify(dagManagementStateStore, Mockito.never()).deleteDagAction(any());
    Mockito.verify(dagManagementStateStore, Mockito.never()).addJobDagAction(any(), any(), anyLong(), any(),
        eq(DagActionStore.DagActionType.REEVALUATE));

    Assert.assertFalse(DagProcUtils.isDagFinished(this.dagManagementStateStore.getDag(dagId).get()));
  }

  @Test
  public void testMultipleNextJobToRun() throws Exception {
    String flowName = "fn4";
    Dag<JobExecutionPlan> dag = LaunchDagProcTest.buildDagWithMultipleNodesAtDifferentLevels("1", flowExecutionId,
        DagProcessingEngine.FailureOption.FINISH_ALL_POSSIBLE.name(), "user5", ConfigFactory.empty()
            .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
            .withValue(ConfigurationKeys.JOB_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY, ConfigValueFactory.fromAnyRef(
                MySqlDagManagementStateStoreTest.TEST_SPEC_EXECUTOR_URI))
    );
    Dag.DagId dagId = DagUtils.generateDagId(dag);
    JobStatus jobStatus = JobStatus.builder().flowName(flowName).flowGroup(flowGroup).jobGroup(flowGroup)
        .jobName("job3").flowExecutionId(flowExecutionId).message("Test message").eventName(ExecutionStatus.COMPLETE.name())
        .startTime(flowExecutionId).shouldRetry(false).orchestratedTime(flowExecutionId).build();
    dagManagementStateStore.addDag(dag);

    doReturn(new ImmutablePair<>(Optional.of(dag.getStartNodes().get(0)), Optional.of(jobStatus)))
        .when(dagManagementStateStore).getDagNodeWithJobStatus(any());

    // mocked job status for the first four jobs
    dag.getNodes().get(0).getValue().setExecutionStatus(ExecutionStatus.COMPLETE);
    dag.getNodes().get(1).getValue().setExecutionStatus(ExecutionStatus.COMPLETE);
    dag.getNodes().get(2).getValue().setExecutionStatus(ExecutionStatus.COMPLETE);
    dag.getNodes().get(3).getValue().setExecutionStatus(ExecutionStatus.COMPLETE);

    doReturn(Optional.of(dag)).when(dagManagementStateStore).getDag(any());

    ReevaluateDagProc
        reEvaluateDagProc = new ReevaluateDagProc(new ReevaluateDagTask(new DagActionStore.DagAction(flowGroup, flowName,
        flowExecutionId, "job3", DagActionStore.DagActionType.REEVALUATE), null,
        dagManagementStateStore, mockedDagProcEngineMetrics), ConfigFactory.empty());
    List<SpecProducer<Spec>> specProducers = getDagSpecProducers(dag);
    // process 4th job
    reEvaluateDagProc.process(dagManagementStateStore, mockedDagProcEngineMetrics);

    int numOfLaunchedJobs = 2; // = number of jobs that should launch when 4th job passes, i.e. 5th and 6th job
    // parallel jobs are launched through reevaluate dag action
    Mockito.verify(dagManagementStateStore, Mockito.times(numOfLaunchedJobs))
        .addJobDagAction(eq(flowGroup), eq(flowName), eq(flowExecutionId), any(), eq(DagActionStore.DagActionType.REEVALUATE));

    // when there are parallel jobs to launch, they are not directly sent to spec producers, instead reevaluate dag action is created
    specProducers.forEach(sp -> Mockito.verify(sp, Mockito.never()).addSpec(any()));

    Assert.assertFalse(DagProcUtils.isDagFinished(this.dagManagementStateStore.getDag(dagId).get()));
  }

  @Test
  public void testRetryCurrentFailedJob() throws Exception {
    String flowName = "fn5";
    Dag<JobExecutionPlan> dag = DagTestUtils.buildDag("1", flowExecutionId, DagProcessingEngine.FailureOption.FINISH_ALL_POSSIBLE.name(),
        2, "user5", ConfigFactory.empty()
            .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
            .withValue(ConfigurationKeys.JOB_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY, ConfigValueFactory.fromAnyRef(
                MySqlDagManagementStateStoreTest.TEST_SPEC_EXECUTOR_URI)));
    Dag.DagId dagId = DagUtils.generateDagId(dag);
    List<SpecProducer<Spec>> specProducers = getDagSpecProducers(dag);
    dagManagementStateStore.addDag(dag);
    // a job status with shouldRetry=true, it should have execution status = PENDING_RETRY
    JobStatus jobStatus = JobStatus.builder().flowName(flowName).flowGroup(flowGroup).jobGroup(flowGroup)
        .jobName("job0").flowExecutionId(flowExecutionId).message("Test message").eventName(ExecutionStatus.PENDING_RETRY.name())
        .startTime(flowExecutionId).shouldRetry(true).orchestratedTime(flowExecutionId).build();
    doReturn(new ImmutablePair<>(Optional.of(dag.getNodes().get(0)), Optional.of(jobStatus)))
        .when(dagManagementStateStore).getDagNodeWithJobStatus(any());

    ReevaluateDagProc reEvaluateDagProc = new ReevaluateDagProc(new ReevaluateDagTask(new DagActionStore.DagAction(
        flowGroup, flowName, flowExecutionId, "job0", DagActionStore.DagActionType.REEVALUATE), null,
        dagManagementStateStore, mockedDagProcEngineMetrics), ConfigFactory.empty());
    reEvaluateDagProc.process(dagManagementStateStore, mockedDagProcEngineMetrics);

    int numOfLaunchedJobs = 1; // only the current job
    // only the current job, that was failed, should have been retried by the reevaluate dag proc, because jobStatus has shouldRetry=true
    Mockito.verify(specProducers.get(0), Mockito.times(1)).addSpec(any());

    specProducers.stream().skip(numOfLaunchedJobs) // separately verified `specProducers.get(0)`
        .forEach(sp -> Mockito.verify(sp, Mockito.never()).addSpec(any()));

    Assert.assertFalse(DagProcUtils.isDagFinished(this.dagManagementStateStore.getDag(dagId).get()));
    Mockito.verify(dagManagementStateStore, Mockito.never()).deleteDagAction(any());
    Mockito.verify(dagManagementStateStore, Mockito.never()).deleteDag(any());
    Mockito.verify(dagManagementStateStore, Mockito.never()).addJobDagAction(any(), any(), anyLong(), any(),
        eq(DagActionStore.DagActionType.REEVALUATE));
  }

  @Test
  public void testCancelledJob() throws Exception {
    String flowName = "fn5";
    Dag<JobExecutionPlan> dag = DagManagerTest.buildDag("1", flowExecutionId, DagManager.FailureOption.FINISH_ALL_POSSIBLE.name(),
        3, "user5", ConfigFactory.empty()
            .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
            .withValue(ConfigurationKeys.JOB_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY, ConfigValueFactory.fromAnyRef(
                MySqlDagManagementStateStoreTest.TEST_SPEC_EXECUTOR_URI)));
    DagManager.DagId dagId = DagManagerUtils.generateDagId(dag);

    List<SpecProducer<Spec>> specProducers = getDagSpecProducers(dag);
    dagManagementStateStore.addDag(dag);
    JobStatus jobStatus1 = JobStatus.builder().flowName(flowName).flowGroup(flowGroup).jobGroup(flowGroup)
        .jobName("job0").flowExecutionId(flowExecutionId).message("Test message").eventName(ExecutionStatus.CANCELLED.name())
        .startTime(flowExecutionId).shouldRetry(false).orchestratedTime(flowExecutionId).build();
    JobStatus jobStatus2 = JobStatus.builder().flowName(flowName).flowGroup(flowGroup).jobGroup(flowGroup)
        .jobName("job1").flowExecutionId(flowExecutionId).message("Test message").eventName(ExecutionStatus.SKIPPED.name())
        .startTime(flowExecutionId).shouldRetry(false).orchestratedTime(flowExecutionId).build();
    JobStatus jobStatus3 = JobStatus.builder().flowName(flowName).flowGroup(flowGroup).jobGroup(flowGroup)
        .jobName("job2").flowExecutionId(flowExecutionId).message("Test message").eventName(ExecutionStatus.SKIPPED.name())
        .startTime(flowExecutionId).shouldRetry(false).orchestratedTime(flowExecutionId).build();
    doReturn(new ImmutablePair<>(Optional.of(dag.getNodes().get(0)), Optional.of(jobStatus1)))
        .when(dagManagementStateStore).getDagNodeWithJobStatus(any());

    ReevaluateDagProc reEvaluateDagProc1 = new ReevaluateDagProc(new ReevaluateDagTask(new DagActionStore.DagAction(
        flowGroup, flowName, flowExecutionId, "job0", DagActionStore.DagActionType.REEVALUATE), null,
        dagManagementStateStore, mockedDagProcEngineMetrics), ConfigFactory.empty());
    reEvaluateDagProc1.process(dagManagementStateStore, mockedDagProcEngineMetrics);

    // job cancelled, so no more jobs to launch
    specProducers.forEach(sp -> Mockito.verify(sp, Mockito.never()).addSpec(any()));

    // dag should still have the running jobs that are skipped
    Assert.assertTrue(dagManagementStateStore.hasRunningJobs(dagId));
    Mockito.verify(dagManagementStateStore, Mockito.never()).deleteDag(any());
    Mockito.verify(dagManagementStateStore, Mockito.never()).markDagFailed(any());
    // and also remove the ENFORCE_FLOW_FINISH_DEADLINE dag action
    Mockito.verify(dagManagementStateStore, Mockito.never()).deleteDagAction(any());
    // JOB_SKIPPED is emitted twice, once for each of the child job
    Mockito.verify(this.mockedEventSubmitter, Mockito.times(2))
        .submit(eq(TimingEvent.LauncherTimings.JOB_SKIPPED), anyMap());

    doReturn(new ImmutablePair<>(Optional.of(dag.getNodes().get(1)), Optional.of(jobStatus2)))
        .when(dagManagementStateStore).getDagNodeWithJobStatus(any());
    ReevaluateDagProc reEvaluateDagProc2 = new ReevaluateDagProc(new ReevaluateDagTask(new DagActionStore.DagAction(
        flowGroup, flowName, flowExecutionId, "job1", DagActionStore.DagActionType.REEVALUATE), null,
        dagManagementStateStore, mockedDagProcEngineMetrics), ConfigFactory.empty());
    reEvaluateDagProc2.process(dagManagementStateStore, mockedDagProcEngineMetrics);

    // dag should still have the running jobs that are skipped
    Assert.assertTrue(dagManagementStateStore.hasRunningJobs(dagId));
    Mockito.verify(dagManagementStateStore, Mockito.never()).deleteDag(any());
    Mockito.verify(dagManagementStateStore, Mockito.never()).markDagFailed(any());
    // and also remove the ENFORCE_FLOW_FINISH_DEADLINE dag action
    Mockito.verify(dagManagementStateStore, Mockito.never()).deleteDagAction(any());
    // JOB_SKIPPED is emitted twice, once for each of the child job
    Mockito.verify(this.mockedEventSubmitter, Mockito.times(2))
        .submit(eq(TimingEvent.LauncherTimings.JOB_SKIPPED), anyMap());


    doReturn(new ImmutablePair<>(Optional.of(dag.getNodes().get(2)), Optional.of(jobStatus3)))
        .when(dagManagementStateStore).getDagNodeWithJobStatus(any());
    ReevaluateDagProc reEvaluateDagProc3 = new ReevaluateDagProc(new ReevaluateDagTask(new DagActionStore.DagAction(
        flowGroup, flowName, flowExecutionId, "job2", DagActionStore.DagActionType.REEVALUATE), null,
        dagManagementStateStore, mockedDagProcEngineMetrics), ConfigFactory.empty());
    reEvaluateDagProc3.process(dagManagementStateStore, mockedDagProcEngineMetrics);

    // this final relaunch dag proc should mark the dag failed
    Mockito.verify(dagManagementStateStore, Mockito.never()).deleteDag(any());
    Mockito.verify(dagManagementStateStore, Mockito.times(1)).markDagFailed(any());
    // and also remove the ENFORCE_FLOW_FINISH_DEADLINE dag action
    Mockito.verify(dagManagementStateStore, Mockito.times(1)).deleteDagAction(
        argThat(dagAction -> dagAction.getDagActionType() == DagActionStore.DagActionType.ENFORCE_FLOW_FINISH_DEADLINE));

    // this finished dag should not create any new dag actions
    Mockito.verify(dagManagementStateStore, Mockito.never()).addJobDagAction(any(), any(), anyLong(), any(), any());
    Mockito.verify(this.mockedEventSubmitter, Mockito.times(2)).submit(any(), anyMap());
  }

  @Test
  public void testFailedJob() throws Exception {
    String flowName = "fn5";
    Dag<JobExecutionPlan> dag = DagManagerTest.buildDag("1", flowExecutionId, DagManager.FailureOption.FINISH_ALL_POSSIBLE.name(),
        3, "user5", ConfigFactory.empty()
            .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
            .withValue(ConfigurationKeys.JOB_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY, ConfigValueFactory.fromAnyRef(
                MySqlDagManagementStateStoreTest.TEST_SPEC_EXECUTOR_URI)));
    DagManager.DagId dagId = DagManagerUtils.generateDagId(dag);

    List<SpecProducer<Spec>> specProducers = getDagSpecProducers(dag);
    dagManagementStateStore.addDag(dag);
    JobStatus jobStatus = JobStatus.builder().flowName(flowName).flowGroup(flowGroup).jobGroup(flowGroup)
        .jobName("job0").flowExecutionId(flowExecutionId).message("Test message").eventName(ExecutionStatus.FAILED.name())
        .startTime(flowExecutionId).shouldRetry(false).orchestratedTime(flowExecutionId).build();
    doReturn(new ImmutablePair<>(Optional.of(dag.getNodes().get(0)), Optional.of(jobStatus)))
        .when(dagManagementStateStore).getDagNodeWithJobStatus(any());

    ReevaluateDagProc reEvaluateDagProc = new ReevaluateDagProc(new ReevaluateDagTask(new DagActionStore.DagAction(
        flowGroup, flowName, flowExecutionId, "job0", DagActionStore.DagActionType.REEVALUATE), null,
        dagManagementStateStore, mockedDagProcEngineMetrics), ConfigFactory.empty());
    reEvaluateDagProc.process(dagManagementStateStore, mockedDagProcEngineMetrics);

    // job cancelled, so no more jobs to launch
    specProducers.forEach(sp -> Mockito.verify(sp, Mockito.never()).addSpec(any()));

    // JOB_SKIPPED is emitted twice, once for each of the child job
    Mockito.verify(this.mockedEventSubmitter, Mockito.times(2))
        .submit(eq(TimingEvent.LauncherTimings.JOB_SKIPPED), anyMap());

    // dag should not have any running jobs now
    Assert.assertFalse(dagManagementStateStore.hasRunningJobs(dagId));

    // job cancellation should mark the dag failed
    Mockito.verify(dagManagementStateStore, Mockito.never()).deleteDag(any());
    Mockito.verify(dagManagementStateStore, Mockito.times(1)).markDagFailed(any());

    // and also remove the ENFORCE_FLOW_FINISH_DEADLINE dag action
    Mockito.verify(dagManagementStateStore, Mockito.times(1)).deleteDagAction(
        argThat(dagAction -> dagAction.getDagActionType() == DagActionStore.DagActionType.ENFORCE_FLOW_FINISH_DEADLINE));
    // this finished dag should not create any new dag actions
    Mockito.verify(dagManagementStateStore, Mockito.never()).addJobDagAction(any(), any(), anyLong(), any(), any());
  }

  public static List<SpecProducer<Spec>> getDagSpecProducers(Dag<JobExecutionPlan> dag) {
    return dag.getNodes().stream().map(n -> {
      try {
        return DagUtils.getSpecProducer(n);
      } catch (ExecutionException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());
  }
}
