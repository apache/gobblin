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
import java.net.URISyntaxException;
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
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.service.modules.orchestration.DagManagerTest;
import org.apache.gobblin.service.modules.orchestration.DagManagerUtils;
import org.apache.gobblin.service.modules.orchestration.MySqlDagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.MySqlDagManagementStateStoreTest;
import org.apache.gobblin.service.modules.orchestration.MysqlDagActionStore;
import org.apache.gobblin.service.modules.orchestration.task.DagProcessingEngineMetrics;
import org.apache.gobblin.service.modules.orchestration.task.KillDagTask;
import org.apache.gobblin.service.modules.orchestration.task.LaunchDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.utils.FlowCompilationValidationHelper;
import org.apache.gobblin.service.monitoring.JobStatus;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.powermock.reflect.Whitebox.setInternalState;


@RunWith(PowerMockRunner.class)
@PrepareForTest(EventSubmitter.class)
public class KillDagProcTest {
  private MySqlDagManagementStateStore dagManagementStateStore;
  private ITestMetastoreDatabase testDb;
  private DagProcessingEngineMetrics mockedDagProcEngineMetrics;
  private MockedStatic<DagProc> dagProc;
  private EventSubmitter mockedEventSubmitter;

  @BeforeClass
  public void setUp() throws Exception {
    this.testDb = TestMetastoreDatabaseFactory.get();
    this.dagManagementStateStore = spy(MySqlDagManagementStateStoreTest.getDummyDMSS(this.testDb));
    LaunchDagProcTest.mockDMSSCommonBehavior(this.dagManagementStateStore);
    this.mockedDagProcEngineMetrics = Mockito.mock(DagProcessingEngineMetrics.class);
    this.dagProc = mockStatic(DagProc.class);
  }

  @BeforeMethod
  public void resetMocks() {
    this.mockedEventSubmitter = spy(new EventSubmitter.Builder(RootMetricContext.get(), "org.apache.gobblin.service").build());
    setInternalState(DagProc.class, "eventSubmitter", this.mockedEventSubmitter);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() throws Exception {
    if (this.testDb != null) {
      // `.close()` to avoid (in the aggregate, across multiple suites) - java.sql.SQLNonTransientConnectionException: Too many connections
      this.testDb.close();
    }
    this.dagProc.close();
  }

  @Test
  public void killDag() throws IOException, URISyntaxException, InterruptedException {
    long flowExecutionId = System.currentTimeMillis();
    Dag<JobExecutionPlan> dag = DagManagerTest.buildDag("1", flowExecutionId, DagManager.FailureOption.FINISH_ALL_POSSIBLE.name(),
        5, "user5", ConfigFactory.empty()
            .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef("fg"))
            .withValue(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY, ConfigValueFactory.fromAnyRef(
                MySqlDagManagementStateStoreTest.TEST_SPEC_EXECUTOR_URI)));
    FlowCompilationValidationHelper flowCompilationValidationHelper = mock(FlowCompilationValidationHelper.class);
    doReturn(Optional.of(dag)).when(dagManagementStateStore).getDag(any());
    doReturn(com.google.common.base.Optional.of(dag)).when(flowCompilationValidationHelper).createExecutionPlanIfValid(any());

    LaunchDagProc launchDagProc = new LaunchDagProc(new LaunchDagTask(new DagActionStore.DagAction("fg", "flow1",
        flowExecutionId, MysqlDagActionStore.NO_JOB_NAME_DEFAULT, DagActionStore.DagActionType.LAUNCH),
        null, this.dagManagementStateStore, mockedDagProcEngineMetrics), flowCompilationValidationHelper, ConfigFactory.empty());
    launchDagProc.process(this.dagManagementStateStore, this.mockedDagProcEngineMetrics);

    List<SpecProducer<Spec>> specProducers = dag.getNodes().stream().map(n -> {
      try {
        return DagManagerUtils.getSpecProducer(n);
      } catch (ExecutionException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());

    KillDagProc killDagProc = new KillDagProc(new KillDagTask(new DagActionStore.DagAction("fg", "flow1",
       flowExecutionId, MysqlDagActionStore.NO_JOB_NAME_DEFAULT, DagActionStore.DagActionType.KILL),
        null, this.dagManagementStateStore, mockedDagProcEngineMetrics), ConfigFactory.empty());
    killDagProc.process(this.dagManagementStateStore, this.mockedDagProcEngineMetrics);

    int numOfCancelledJobs = 5; // all jobs in the dag
    int numOfCancelledFlows = 1;
    long cancelJobCount = specProducers.stream()
        .mapToLong(p -> Mockito.mockingDetails(p)
            .getInvocations()
            .stream()
            .filter(a -> a.getMethod().getName().equals("cancelJob"))
            .count())
        .sum();
    // kill dag proc tries to cancel all the dag nodes
    Assert.assertEquals(cancelJobCount, numOfCancelledJobs);

    Mockito.verify(this.mockedEventSubmitter, Mockito.times(numOfCancelledJobs))
        .submit(eq(TimingEvent.LauncherTimings.JOB_CANCEL), anyMap());
    Mockito.verify(this.mockedEventSubmitter, Mockito.times(numOfCancelledFlows))
        .submit(eq(TimingEvent.FlowTimings.FLOW_CANCELLED), anyMap());
  }

  @Test
  public void killDagNode() throws IOException, URISyntaxException, InterruptedException {
    long flowExecutionId = System.currentTimeMillis();
    Dag<JobExecutionPlan> dag = DagManagerTest.buildDag("2", flowExecutionId, DagManager.FailureOption.FINISH_ALL_POSSIBLE.name(),
        5, "user5", ConfigFactory.empty()
            .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef("fg"))
            .withValue(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY, ConfigValueFactory.fromAnyRef(
                MySqlDagManagementStateStoreTest.TEST_SPEC_EXECUTOR_URI)));
    FlowCompilationValidationHelper flowCompilationValidationHelper = mock(FlowCompilationValidationHelper.class);
    JobStatus
        jobStatus = JobStatus.builder().flowName("job0").flowGroup("fg").jobGroup("fg").jobName("job0").flowExecutionId(flowExecutionId).
        message("Test message").eventName(ExecutionStatus.COMPLETE.name()).startTime(flowExecutionId).shouldRetry(false).orchestratedTime(flowExecutionId).build();

    doReturn(Optional.of(dag)).when(dagManagementStateStore).getDag(any());
    doReturn(new ImmutablePair<>(Optional.of(dag.getStartNodes().get(0)), Optional.of(jobStatus))).when(dagManagementStateStore).getDagNodeWithJobStatus(any());
    doReturn(com.google.common.base.Optional.of(dag)).when(flowCompilationValidationHelper).createExecutionPlanIfValid(any());

    LaunchDagProc launchDagProc = new LaunchDagProc(new LaunchDagTask(new DagActionStore.DagAction("fg", "flow2",
        flowExecutionId, MysqlDagActionStore.NO_JOB_NAME_DEFAULT, DagActionStore.DagActionType.LAUNCH),
        null, this.dagManagementStateStore, this.mockedDagProcEngineMetrics), flowCompilationValidationHelper,
        ConfigFactory.empty());
    launchDagProc.process(this.dagManagementStateStore, this.mockedDagProcEngineMetrics);

    List<SpecProducer<Spec>> specProducers = dag.getNodes().stream().map(n -> {
      try {
        return DagManagerUtils.getSpecProducer(n);
      } catch (ExecutionException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());

    KillDagProc killDagProc = new KillDagProc(new KillDagTask(new DagActionStore.DagAction("fg", "flow2",
        flowExecutionId, "job2", DagActionStore.DagActionType.KILL),
        null, this.dagManagementStateStore, this.mockedDagProcEngineMetrics), ConfigFactory.empty());
    killDagProc.process(this.dagManagementStateStore, this.mockedDagProcEngineMetrics);

    int numOfCancelledJobs = 1; // the only job that was cancelled
    int numOfCancelledFlows = 1;
    long cancelJobCount = specProducers.stream()
        .mapToLong(p -> Mockito.mockingDetails(p)
            .getInvocations()
            .stream()
            .filter(a -> a.getMethod().getName().equals("cancelJob"))
            .count())
        .sum();
    // kill dag proc tries to cancel only the exact dag node that was provided
    Assert.assertEquals(cancelJobCount, numOfCancelledJobs);

    Mockito.verify(this.mockedEventSubmitter, Mockito.times(numOfCancelledJobs))
        .submit(eq(TimingEvent.LauncherTimings.JOB_CANCEL), anyMap());
    Mockito.verify(this.mockedEventSubmitter, Mockito.times(numOfCancelledFlows))
        .submit(eq(TimingEvent.FlowTimings.FLOW_CANCELLED), anyMap());
  }
}
