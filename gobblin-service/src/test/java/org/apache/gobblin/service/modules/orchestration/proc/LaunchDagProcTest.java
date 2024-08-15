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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.metrics.RootMetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.runtime.spec_executorInstance.MockedSpecExecutor;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.AzkabanProjectConfig;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagProcessingEngine;
import org.apache.gobblin.service.modules.orchestration.DagTestUtils;
import org.apache.gobblin.service.modules.orchestration.DagUtils;
import org.apache.gobblin.service.modules.orchestration.MySqlDagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.MySqlDagManagementStateStoreTest;
import org.apache.gobblin.service.modules.orchestration.task.DagProcessingEngineMetrics;
import org.apache.gobblin.service.modules.orchestration.task.LaunchDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.spec.JobExecutionPlanDagFactory;
import org.apache.gobblin.service.modules.utils.FlowCompilationValidationHelper;
import org.apache.gobblin.util.ConfigUtils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.powermock.reflect.Whitebox.setInternalState;


@RunWith(PowerMockRunner.class)
@PrepareForTest(EventSubmitter.class)
public class LaunchDagProcTest {
  private ITestMetastoreDatabase testMetastoreDatabase;
  private MySqlDagManagementStateStore dagManagementStateStore;
  private DagProcessingEngineMetrics mockedDagProcEngineMetrics;
  private MockedStatic<DagProc> dagProc;
  private EventSubmitter mockedEventSubmitter;

  @BeforeClass
  public void setUp() throws Exception {
    this.testMetastoreDatabase = TestMetastoreDatabaseFactory.get();
    this.dagProc = mockStatic(DagProc.class);
  }

  /**
   * Reset DagManagementStateStore between tests so that Mockito asserts are done on a fresh state.
   */
  @BeforeMethod
  public void resetDMSS() throws Exception {
    this.dagManagementStateStore = spy(MySqlDagManagementStateStoreTest.getDummyDMSS(this.testMetastoreDatabase));
    mockDMSSCommonBehavior(this.dagManagementStateStore);
    this.mockedDagProcEngineMetrics = Mockito.mock(DagProcessingEngineMetrics.class);
    this.mockedEventSubmitter = spy(new EventSubmitter.Builder(RootMetricContext.get(), "org.apache.gobblin.service").build());
    setInternalState(DagProc.class, "eventSubmitter", this.mockedEventSubmitter);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() throws Exception {
    // `.close()` to avoid (in the aggregate, across multiple suites) - java.sql.SQLNonTransientConnectionException: Too many connections
    this.testMetastoreDatabase.close();
    this.dagProc.close();
  }

  @Test
  public void launchDag() throws IOException, InterruptedException, URISyntaxException {
    String flowGroup = "fg";
    String flowName = "fn";
    long flowExecutionId = 12345L;
    Dag<JobExecutionPlan> dag = DagTestUtils.buildDag("1", flowExecutionId,
        DagProcessingEngine.FailureOption.FINISH_ALL_POSSIBLE.name(), 5, "user5", ConfigFactory.empty()
            .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
            .withValue(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY, ConfigValueFactory.fromAnyRef(
            MySqlDagManagementStateStoreTest.TEST_SPEC_EXECUTOR_URI)));
    Dag.DagId dagId = DagUtils.generateDagId(dag);
    FlowCompilationValidationHelper flowCompilationValidationHelper = mock(FlowCompilationValidationHelper.class);
    doReturn(com.google.common.base.Optional.of(dag)).when(flowCompilationValidationHelper).createExecutionPlanIfValid(any());
    List<SpecProducer<Spec>> specProducers = ReevaluateDagProcTest.getDagSpecProducers(dag);
    LaunchDagProc launchDagProc = new LaunchDagProc(
        new LaunchDagTask(new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, "job0",
            DagActionStore.DagActionType.LAUNCH), null, this.dagManagementStateStore,
            this.mockedDagProcEngineMetrics),
        flowCompilationValidationHelper, ConfigFactory.empty());

    launchDagProc.process(this.dagManagementStateStore, mockedDagProcEngineMetrics);

    int numOfLaunchedJobs = 1; // = number of start nodes
    Mockito.verify(specProducers.get(0), Mockito.times(1)).addSpec(any());

    specProducers.stream().skip(numOfLaunchedJobs) // separately verified `specProducers.get(0)`
        .forEach(sp -> Mockito.verify(sp, Mockito.never()).addSpec(any()));

    Mockito.verify(this.dagManagementStateStore, Mockito.times(numOfLaunchedJobs))
        .addJobDagAction(any(), any(), anyLong(), eq(DagActionStore.NO_JOB_NAME_DEFAULT), eq(DagActionStore.DagActionType.ENFORCE_FLOW_FINISH_DEADLINE));

    // FLOW_RUNNING is emitted exactly once per flow during the execution of LaunchDagProc
    Mockito.verify(this.mockedEventSubmitter, Mockito.times(1))
        .submit(eq(TimingEvent.FlowTimings.FLOW_RUNNING), anyMap());

    Assert.assertFalse(DagProcUtils.isDagFinished(this.dagManagementStateStore.getDag(dagId).get()));
  }

  @Test
  public void launchDagWithMultipleParallelJobs() throws IOException, InterruptedException, URISyntaxException {
    String flowGroup = "fg";
    String flowName = "fn2";
    long flowExecutionId = 12345L;
    Dag<JobExecutionPlan> dag = buildDagWithMultipleNodesAtDifferentLevels("1", flowExecutionId,
        DagProcessingEngine.FailureOption.FINISH_ALL_POSSIBLE.name(),"user5", ConfigFactory.empty()
            .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.FLOW_NAME_KEY,  ConfigValueFactory.fromAnyRef(flowName))
            .withValue(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY, ConfigValueFactory.fromAnyRef(
                MySqlDagManagementStateStoreTest.TEST_SPEC_EXECUTOR_URI)));
    Dag.DagId dagId = DagUtils.generateDagId(dag);
    FlowCompilationValidationHelper flowCompilationValidationHelper = mock(FlowCompilationValidationHelper.class);
    doReturn(com.google.common.base.Optional.of(dag)).when(flowCompilationValidationHelper).createExecutionPlanIfValid(any());
    LaunchDagProc launchDagProc = new LaunchDagProc(
        new LaunchDagTask(new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId,
            "jn", DagActionStore.DagActionType.LAUNCH), null, this.dagManagementStateStore,
            this.mockedDagProcEngineMetrics),
        flowCompilationValidationHelper, ConfigFactory.empty());

    launchDagProc.process(this.dagManagementStateStore, mockedDagProcEngineMetrics);
    int numOfLaunchedJobs = 3; // = number of start nodes
    // parallel jobs are launched through reevaluate dag action
    Mockito.verify(this.dagManagementStateStore, Mockito.times(numOfLaunchedJobs))
        .addJobDagAction(eq(flowGroup), eq(flowName), eq(flowExecutionId), any(), eq(DagActionStore.DagActionType.REEVALUATE));

    // FLOW_RUNNING is emitted exactly once per flow during the execution of LaunchDagProc
    Mockito.verify(this.mockedEventSubmitter, Mockito.times(1))
        .submit(eq(TimingEvent.FlowTimings.FLOW_RUNNING), anyMap());

    Assert.assertFalse(DagProcUtils.isDagFinished(this.dagManagementStateStore.getDag(dagId).get()));
  }

  // This creates a dag like this
  //  D1  D2 D3
  //    \ | /
  //     DN4
  //    /   \
  //  D5     D6

  public static Dag<JobExecutionPlan> buildDagWithMultipleNodesAtDifferentLevels(String id, long flowExecutionId,
      String flowFailureOption, String proxyUser, Config additionalConfig) throws URISyntaxException {
    List<JobExecutionPlan> jobExecutionPlans = new ArrayList<>();

    for (int i = 0; i < 6; i++) {
      String suffix = Integer.toString(i);
      Config jobConfig = ConfigBuilder.create().
          addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "group" + id).
          addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "flow" + id).
          addPrimitive(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, flowExecutionId).
          addPrimitive(ConfigurationKeys.JOB_GROUP_KEY, "group" + id).
          addPrimitive(ConfigurationKeys.JOB_NAME_KEY, "job" + suffix).
          addPrimitive(ConfigurationKeys.FLOW_FAILURE_OPTION, flowFailureOption).
          addPrimitive(AzkabanProjectConfig.USER_TO_PROXY, proxyUser).build();
      jobConfig = additionalConfig.withFallback(jobConfig);
      if (i == 3) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job0,job1,job2"));
      } else if ((i == 4) || (i == 5)) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job3"));
      }
      JobSpec js = JobSpec.builder("test_job" + suffix).withVersion(suffix).withConfig(jobConfig).
          withTemplate(new URI("job" + suffix)).build();
      SpecExecutor specExecutor = MockedSpecExecutor.createDummySpecExecutor(new URI(
          ConfigUtils.getString(additionalConfig, ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY,"job" + i)));
      JobExecutionPlan jobExecutionPlan = new JobExecutionPlan(js, specExecutor);
      jobExecutionPlans.add(jobExecutionPlan);
    }
    return new JobExecutionPlanDagFactory().createDag(jobExecutionPlans);
  }

  public static void mockDMSSCommonBehavior(DagManagementStateStore dagManagementStateStore) throws IOException, SpecNotFoundException {
    doReturn(FlowSpec.builder().build()).when(dagManagementStateStore).getFlowSpec(any());
    doNothing().when(dagManagementStateStore).removeFlowSpec(any(), any(), anyBoolean());
    doNothing().when(dagManagementStateStore).tryAcquireQuota(any());
    doReturn(true).when(dagManagementStateStore).releaseQuota(any());
  }

  public static TopologySpec buildNaiveTopologySpec(String specUriInString) {
    Config specExecConfig = MockedSpecExecutor.makeDummyConfigsForSpecExecutor(specUriInString);
    SpecExecutor specExecutorInstanceProducer = new MockedSpecExecutor(specExecConfig);
    TopologySpec.Builder topologySpecBuilder = TopologySpec
        .builder(new Path(specExecConfig.getString("specStore.fs.dir")).toUri())
        .withConfig(specExecConfig)
        .withDescription("test")
        .withVersion("1")
        .withSpecExecutor(specExecutorInstanceProducer);

    return topologySpecBuilder.build();
  }
}
