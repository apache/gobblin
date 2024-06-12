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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
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
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.core.GobblinServiceManager;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagActionReminderScheduler;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.service.modules.orchestration.DagManagerTest;
import org.apache.gobblin.service.modules.orchestration.DagManagerUtils;
import org.apache.gobblin.service.modules.orchestration.MostlyMySqlDagManagementStateStoreTest;
import org.apache.gobblin.service.modules.orchestration.task.ReevaluateDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.JobStatus;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;


public class ReevaluateDagProcTest {
  private final long flowExecutionId = System.currentTimeMillis();
  private final String flowGroup = "fg";

  private ITestMetastoreDatabase testMetastoreDatabase;
  private DagManagementStateStore dagManagementStateStore;
  private MockedStatic<GobblinServiceManager> mockedGobblinServiceManager;
  private DagActionReminderScheduler dagActionReminderScheduler;

  @BeforeClass
  public void setUpClass() throws Exception {
    this.testMetastoreDatabase = TestMetastoreDatabaseFactory.get();
    this.mockedGobblinServiceManager = Mockito.mockStatic(GobblinServiceManager.class);
  }

  @BeforeMethod
  public void setUp() throws Exception {
    this.dagManagementStateStore = spy(MostlyMySqlDagManagementStateStoreTest.getDummyDMSS(this.testMetastoreDatabase));
    mockDMSSCommonBehavior(dagManagementStateStore);
    this.dagActionReminderScheduler = mock(DagActionReminderScheduler.class);
    this.mockedGobblinServiceManager.when(() -> GobblinServiceManager.getClass(DagActionReminderScheduler.class)).thenReturn(this.dagActionReminderScheduler);
  }

  private void mockDMSSCommonBehavior(DagManagementStateStore dagManagementStateStore) throws IOException, SpecNotFoundException {
    doReturn(FlowSpec.builder().build()).when(dagManagementStateStore).getFlowSpec(any());
    doNothing().when(dagManagementStateStore).tryAcquireQuota(any());
    doNothing().when(dagManagementStateStore).addDagNodeState(any(), any());
    doNothing().when(dagManagementStateStore).deleteDagNodeState(any(), any());
    doReturn(true).when(dagManagementStateStore).releaseQuota(any());
  }

  @AfterClass(alwaysRun = true)
  public void tearDownClass() throws Exception {
    this.mockedGobblinServiceManager.close();
    // `.close()` to avoid (in the aggregate, across multiple suites) - java.sql.SQLNonTransientConnectionException: Too many connections
    this.testMetastoreDatabase.close();
  }

  @Test
  public void testOneNextJobToRun() throws Exception {
    String flowName = "fn";
    Dag<JobExecutionPlan> dag = DagManagerTest.buildDag("1", flowExecutionId, DagManager.FailureOption.FINISH_ALL_POSSIBLE.name(),
        2, "user5", ConfigFactory.empty()
            .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
            .withValue(ConfigurationKeys.JOB_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
    );
    List<SpecProducer<Spec>> specProducers = getDagSpecProducers(dag);
    JobStatus jobStatus = JobStatus.builder().flowName(flowName).flowGroup(flowGroup).jobGroup(flowGroup).jobName("job0").flowExecutionId(flowExecutionId).
        message("Test message").eventName(ExecutionStatus.COMPLETE.name()).startTime(flowExecutionId).shouldRetry(false).orchestratedTime(flowExecutionId).build();

    doReturn(Optional.of(dag)).when(dagManagementStateStore).getDag(any());
    doReturn(new ImmutablePair<>(Optional.of(dag.getStartNodes().get(0)), Optional.of(jobStatus))).when(dagManagementStateStore).getDagNodeWithJobStatus(any());

    ReevaluateDagProc
        reEvaluateDagProc = new ReevaluateDagProc(new ReevaluateDagTask(new DagActionStore.DagAction(flowGroup, flowName,
        String.valueOf(flowExecutionId), "job0", DagActionStore.DagActionType.REEVALUATE), null, dagManagementStateStore));
    reEvaluateDagProc.process(dagManagementStateStore);

    long addSpecCount = specProducers.stream()
        .mapToLong(p -> Mockito.mockingDetails(p)
            .getInvocations()
            .stream()
            .filter(a -> a.getMethod().getName().equals("addSpec"))
            .count())
        .sum();

    // next job is sent to spec producer
    Assert.assertEquals(addSpecCount, 1L);

    // current job's state is deleted
    Assert.assertEquals(Mockito.mockingDetails(dagManagementStateStore).getInvocations().stream()
        .filter(a -> a.getMethod().getName().equals("deleteDagNodeState")).count(), 1);

    Assert.assertEquals(Mockito.mockingDetails(this.dagActionReminderScheduler).getInvocations().stream()
        .filter(a -> a.getMethod().getName().equals("unscheduleReminderJob")).count(), 1);

    // when there is no more job to run in re-evaluate dag proc, it deletes enforce_flow_finish_dag_action also
    Assert.assertEquals(Mockito.mockingDetails(this.dagManagementStateStore).getInvocations().stream()
        .filter(a -> a.getMethod().getName().equals("deleteDagAction")).count(), 1);
  }

  // test when there does not exist a next job in the dag when the current job's reevaluate dag action is processed
  @Test
  public void testNoNextJobToRun() throws Exception {
    String flowName = "fn2";
    Dag<JobExecutionPlan> dag = DagManagerTest.buildDag("2", flowExecutionId, DagManager.FailureOption.FINISH_ALL_POSSIBLE.name(),
        1, "user5", ConfigFactory.empty()
            .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
            .withValue(ConfigurationKeys.JOB_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
    );
    JobStatus jobStatus = JobStatus.builder().flowName(flowName).flowGroup(flowGroup).jobGroup(flowGroup).jobName("job0").flowExecutionId(flowExecutionId).
        message("Test message").eventName(ExecutionStatus.COMPLETE.name()).startTime(flowExecutionId).shouldRetry(false).orchestratedTime(flowExecutionId).build();

    doReturn(Optional.of(dag)).when(dagManagementStateStore).getDag(any());
    doReturn(new ImmutablePair<>(Optional.of(dag.getStartNodes().get(0)), Optional.of(jobStatus))).when(dagManagementStateStore).getDagNodeWithJobStatus(any());
    doReturn(true).when(dagManagementStateStore).releaseQuota(any());

    List<SpecProducer<Spec>> specProducers = getDagSpecProducers(dag);

    long addSpecCount = specProducers.stream()
        .mapToLong(p -> Mockito.mockingDetails(p)
            .getInvocations()
            .stream()
            .filter(a -> a.getMethod().getName().equals("addSpec"))
            .count())
        .sum();

    ReevaluateDagProc
        reEvaluateDagProc = new ReevaluateDagProc(new ReevaluateDagTask(new DagActionStore.DagAction(flowGroup, flowName,
        String.valueOf(flowExecutionId), "job0", DagActionStore.DagActionType.REEVALUATE), null, dagManagementStateStore));
    reEvaluateDagProc.process(dagManagementStateStore);

    // no new job to launch for this one job flow
    Assert.assertEquals(addSpecCount, 0L);

    // current job's state is deleted
    Assert.assertEquals(Mockito.mockingDetails(dagManagementStateStore).getInvocations().stream()
        .filter(a -> a.getMethod().getName().equals("deleteDagNodeState")).count(), 1);

    // dag is deleted because the only job in the dag is completed
    Assert.assertEquals(Mockito.mockingDetails(dagManagementStateStore).getInvocations().stream()
        .filter(a -> a.getMethod().getName().equals("deleteDag")).count(), 1);

    Assert.assertEquals(Mockito.mockingDetails(this.dagActionReminderScheduler).getInvocations().stream()
        .filter(a -> a.getMethod().getName().equals("unscheduleReminderJob")).count(), 1);

    Assert.assertEquals(Mockito.mockingDetails(this.dagManagementStateStore).getInvocations().stream()
        .filter(a -> a.getMethod().getName().equals("deleteDagAction")).count(), 1);
  }

  @Test
  public void testCurrentJobToRun() throws Exception {
    String flowName = "fn3";
    Dag<JobExecutionPlan> dag = DagManagerTest.buildDag("1", flowExecutionId, DagManager.FailureOption.FINISH_ALL_POSSIBLE.name(),
        2, "user5", ConfigFactory.empty()
            .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
            .withValue(ConfigurationKeys.JOB_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
    );
    List<Dag.DagNode<JobExecutionPlan>> startDagNodes = dag.getStartNodes();
    List<SpecProducer<Spec>> specProducers = getDagSpecProducers(dag);

    doReturn(Optional.of(dag)).when(dagManagementStateStore).getDag(any());
    doReturn(new ImmutablePair<>(Optional.of(startDagNodes.get(0)), Optional.empty()))
        .when(dagManagementStateStore).getDagNodeWithJobStatus(any());

    ReevaluateDagProc
        reEvaluateDagProc = new ReevaluateDagProc(new ReevaluateDagTask(new DagActionStore.DagAction(flowGroup, flowName,
        String.valueOf(flowExecutionId), "job0", DagActionStore.DagActionType.REEVALUATE), null,
        dagManagementStateStore));
    reEvaluateDagProc.process(dagManagementStateStore);

    int numOfLaunchedJobs = 1; // only the current job
    // only the current job should have run
    Mockito.verify(specProducers.get(0), Mockito.times(1)).addSpec(any());

    specProducers.stream().skip(numOfLaunchedJobs) // separately verified `specProducers.get(0)`
        .forEach(sp -> Mockito.verify(sp, Mockito.never()).addSpec(any()));

    // no job's state is deleted because that happens when the job finishes triggered the reevaluate dag proc
    Mockito.verify(dagManagementStateStore, Mockito.never()).deleteDagNodeState(any(), any());
    Mockito.verify(dagManagementStateStore, Mockito.never()).deleteDagAction(any());
    Mockito.verify(dagManagementStateStore, Mockito.never()).addJobDagAction(any(), any(), any(), any(),
        eq(DagActionStore.DagActionType.REEVALUATE));
    Mockito.verify(dagActionReminderScheduler, Mockito.never()).unscheduleReminderJob(any());
  }

  @Test
  public void testMultipleNextJobToRun() throws Exception {
    String flowName = "fn4";
    Dag<JobExecutionPlan> dag = LaunchDagProcTest.buildDagWithMultipleNodesAtDifferentLevels("1", String.valueOf(flowExecutionId),
        DagManager.FailureOption.FINISH_ALL_POSSIBLE.name(), "user5", ConfigFactory.empty()
            .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
            .withValue(ConfigurationKeys.JOB_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
    );
    JobStatus jobStatus = JobStatus.builder().flowName(flowName).flowGroup(flowGroup).jobGroup(flowGroup)
        .jobName("job3").flowExecutionId(flowExecutionId).message("Test message").eventName(ExecutionStatus.COMPLETE.name())
        .startTime(flowExecutionId).shouldRetry(false).orchestratedTime(flowExecutionId).build();

    doReturn(Optional.of(dag)).when(dagManagementStateStore).getDag(any());
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
        String.valueOf(flowExecutionId), "job3", DagActionStore.DagActionType.REEVALUATE), null, dagManagementStateStore));
    List<SpecProducer<Spec>> specProducers = getDagSpecProducers(dag);
    // process 4th job
    reEvaluateDagProc.process(dagManagementStateStore);

    int numOfLaunchedJobs = 2; // = number of jobs that should launch when 4th job passes, i.e. 5th and 6th job
    // parallel jobs are launched through reevaluate dag action
    Mockito.verify(dagManagementStateStore, Mockito.times(numOfLaunchedJobs))
        .addJobDagAction(eq(flowGroup), eq(flowName), eq(String.valueOf(flowExecutionId)), any(), eq(DagActionStore.DagActionType.REEVALUATE));

    // when there are parallel jobs to launch, they are not directly sent to spec producers, instead reevaluate dag action is created
    specProducers.forEach(sp -> Mockito.verify(sp, Mockito.never()).addSpec(any()));
  }

  public static List<SpecProducer<Spec>> getDagSpecProducers(Dag<JobExecutionPlan> dag) {
    return dag.getNodes().stream().map(n -> {
      try {
        return DagManagerUtils.getSpecProducer(n);
      } catch (ExecutionException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());
  }
}
