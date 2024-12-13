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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.runtime.spec_executorInstance.InMemorySpecExecutor;
import org.apache.gobblin.runtime.spec_executorInstance.MockedSpecExecutor;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.flowgraph.FlowGraphConfigurationKeys;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagManagerMetrics;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DagProcUtilsTest {

  DagManagementStateStore dagManagementStateStore;
  SpecExecutor mockSpecExecutor;

  @BeforeMethod
  public void setUp() {
    dagManagementStateStore = Mockito.mock(DagManagementStateStore.class);
    mockSpecExecutor = new MockedSpecExecutor(Mockito.mock(Config.class));
  }

  @Test
  public void testSubmitNextNodesSuccess() throws URISyntaxException, IOException {
    Dag.DagId dagId = new Dag.DagId("testFlowGroup", "testFlowName", 2345678);
    List<JobExecutionPlan> jobExecutionPlans = getJobExecutionPlans();
    List<Dag.DagNode<JobExecutionPlan>> dagNodeList = jobExecutionPlans.stream()
        .map(Dag.DagNode<JobExecutionPlan>::new)
        .collect(Collectors.toList());
    Dag<JobExecutionPlan> dag = new Dag<>(dagNodeList);
    Mockito.doNothing().when(dagManagementStateStore).addJobDagAction(Mockito.anyString(), Mockito.anyString(), Mockito.anyLong(), Mockito.anyString(), Mockito.any());
    DagProcUtils.submitNextNodes(dagManagementStateStore, dag, dagId);
    for (JobExecutionPlan jobExecutionPlan : jobExecutionPlans) {
      Mockito.verify(dagManagementStateStore, Mockito.times(1))
          .addJobDagAction(jobExecutionPlan.getFlowGroup(), jobExecutionPlan.getFlowName(),
              jobExecutionPlan.getFlowExecutionId(), jobExecutionPlan.getJobName(),
              DagActionStore.DagActionType.REEVALUATE);
    }
    Mockito.verifyNoMoreInteractions(dagManagementStateStore);
  }

  @Test
  public void testWhenSubmitToExecutorSuccess() throws URISyntaxException, IOException {
    Dag.DagId dagId = new Dag.DagId("flowGroup1", "flowName1", 2345680);
    List<Dag.DagNode<JobExecutionPlan>> dagNodeList = new ArrayList<>();
    JobExecutionPlan jobExecutionPlan = getJobExecutionPlans().get(0);
    Dag.DagNode<JobExecutionPlan> dagNode = new Dag.DagNode<>(jobExecutionPlan);
    dagNodeList.add(dagNode);
    Dag<JobExecutionPlan> dag = new Dag<>(dagNodeList);
    DagManagerMetrics metrics = Mockito.mock(DagManagerMetrics.class);
    Mockito.when(dagManagementStateStore.getDagManagerMetrics()).thenReturn(metrics);
    Mockito.doNothing().when(metrics).incrementRunningJobMetrics(dagNode);
    Mockito.doNothing().when(metrics).incrementJobsSentToExecutor(dagNode);
    DagProcUtils.submitNextNodes(dagManagementStateStore, dag, dagId);
    Mockito.verify(dagManagementStateStore, Mockito.times(2)).getDagManagerMetrics();
    Mockito.verify(dagManagementStateStore, Mockito.times(1)).tryAcquireQuota(Collections.singleton(dagNode));
    Mockito.verify(dagManagementStateStore, Mockito.times(1)).updateDagNode(dagNode);
    Mockito.verify(dagManagementStateStore, Mockito.times(1)).addDagAction(Mockito.any(DagActionStore.DagAction.class));

    Mockito.verify(metrics, Mockito.times(1)).incrementRunningJobMetrics(dagNode);
    Mockito.verify(metrics, Mockito.times(1)).incrementJobsSentToExecutor(dagNode);
    Mockito.verifyNoMoreInteractions(dagManagementStateStore);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testWhenSubmitToExecutorGivesRuntimeException() throws URISyntaxException, IOException, ExecutionException, InterruptedException{
    Dag.DagId dagId = new Dag.DagId("flowGroup3", "flowName3", 2345678);
    List<Dag.DagNode<JobExecutionPlan>> dagNodeList = new ArrayList<>();
    JobExecutionPlan jobExecutionPlan = getJobExecutionPlans().get(2);
    Dag.DagNode<JobExecutionPlan> dagNode = new Dag.DagNode<>(jobExecutionPlan);
    dagNodeList.add(dagNode);
    Dag<JobExecutionPlan> dag = new Dag<>(dagNodeList);
    SpecProducer<Spec> mockedSpecProducer = mockSpecExecutor.getProducer().get();
    Mockito.doThrow(RuntimeException.class).when(mockedSpecProducer).addSpec(Mockito.any(JobSpec.class));
    DagManagerMetrics metrics = Mockito.mock(DagManagerMetrics.class);
    Mockito.when(dagManagementStateStore.getDagManagerMetrics()).thenReturn(metrics);
    Mockito.doNothing().when(metrics).incrementRunningJobMetrics(dagNode);
    DagProcUtils.submitNextNodes(dagManagementStateStore, dag, dagId);
    Mockito.verify(mockedSpecProducer, Mockito.times(1)).addSpec(Mockito.any(JobSpec.class));
    Mockito.verify(dagManagementStateStore, Mockito.times(1)).getDagManagerMetrics();
    Mockito.verify(metrics, Mockito.times(1)).incrementRunningJobMetrics(dagNode);
    Mockito.verifyNoMoreInteractions(dagManagementStateStore);
  }

  private List<JobExecutionPlan> getJobExecutionPlans() throws URISyntaxException {
    Config flowConfig1 = ConfigBuilder.create().addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "flowName1")
        .addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "flowGroup1").build();
    Config flowConfig2 = ConfigBuilder.create().addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "flowName2")
        .addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "flowGroup2").build();
    Config flowConfig3 = ConfigBuilder.create().addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "flowName3")
        .addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "flowGroup3").build();
    List<Config> flowConfigs = Arrays.asList(flowConfig1, flowConfig2, flowConfig3);

    Config jobConfig1 = ConfigBuilder.create().addPrimitive(ConfigurationKeys.JOB_NAME_KEY, "job1")
        .addPrimitive(FlowGraphConfigurationKeys.FLOW_EDGE_ID_KEY, "source:destination:edgeName1").build();
    Config jobConfig2 = ConfigBuilder.create().addPrimitive(ConfigurationKeys.JOB_NAME_KEY, "job2")
        .addPrimitive(FlowGraphConfigurationKeys.FLOW_EDGE_ID_KEY, "source:destination:edgeName2").build();
    Config jobConfig3 = ConfigBuilder.create().addPrimitive(ConfigurationKeys.JOB_NAME_KEY, "job1")
        .addPrimitive(FlowGraphConfigurationKeys.FLOW_EDGE_ID_KEY, "source:destination:edgeName3").build();
    List<Config> jobConfigs = Arrays.asList(jobConfig1, jobConfig2, jobConfig3);
    List<JobExecutionPlan> jobExecutionPlans = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      Config jobConfig = jobConfigs.get(i);
      FlowSpec flowSpec = FlowSpec.builder("testFlowSpec").withConfig(flowConfigs.get(i)).build();
      if (i == 2) {
        jobExecutionPlans.add(new JobExecutionPlan.Factory().createPlan(flowSpec,
            jobConfig.withValue(ConfigurationKeys.JOB_TEMPLATE_PATH, ConfigValueFactory.fromAnyRef("testUri")),
            mockSpecExecutor, 0L, ConfigFactory.empty()));
      } else {
        jobExecutionPlans.add(new JobExecutionPlan.Factory().createPlan(flowSpec,
            jobConfig.withValue(ConfigurationKeys.JOB_TEMPLATE_PATH, ConfigValueFactory.fromAnyRef("testUri")),
            new InMemorySpecExecutor(ConfigFactory.empty()), 0L, ConfigFactory.empty()));
      }
    }
    return jobExecutionPlans;
  }
}