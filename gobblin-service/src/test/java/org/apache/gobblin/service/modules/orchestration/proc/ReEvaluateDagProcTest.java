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
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.service.modules.orchestration.DagManagerTest;
import org.apache.gobblin.service.modules.orchestration.DagManagerUtils;
import org.apache.gobblin.service.modules.orchestration.MostlyMySqlDagManagementStateStoreTest;
import org.apache.gobblin.service.modules.orchestration.task.ReEvaluateDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.JobStatus;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;


public class ReEvaluateDagProcTest {
  JobStatusRetriever jobStatusRetriever = mock(JobStatusRetriever.class);

  void mockDMSS(DagManagementStateStore dagManagementStateStore) throws IOException, SpecNotFoundException {
    doReturn(FlowSpec.builder().build()).when(dagManagementStateStore).getFlowSpec(any());
    doNothing().when(dagManagementStateStore).tryAcquireQuota(any());
    doNothing().when(dagManagementStateStore).addDagNodeState(any(), any());
    doReturn(true).when(dagManagementStateStore).releaseQuota(any());
  }

  @Test
  public void testOneNextJobToRun() throws Exception {
    DagManagementStateStore dagManagementStateStore = spy(MostlyMySqlDagManagementStateStoreTest.getDummyDMSS(TestMetastoreDatabaseFactory.get()));
    mockDMSS(dagManagementStateStore);
    long flowExecutionId = 12345L;
    String flowGroup = "fg";
    String flowName = "fn";
    Dag<JobExecutionPlan> dag = DagManagerTest.buildDag("1", flowExecutionId, DagManager.FailureOption.FINISH_ALL_POSSIBLE.name(),
        2, "user5", ConfigFactory.empty()
            .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
            .withValue(ConfigurationKeys.JOB_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
    );
    doReturn(Optional.of(dag)).when(dagManagementStateStore).getDag(any());
    doReturn(Optional.of(dag.getStartNodes().get(0))).when(dagManagementStateStore).getDagNode(any());
    doReturn(Optional.of(dag)).when(dagManagementStateStore).getParentDag(any());
    doNothing().when(dagManagementStateStore).deleteDagNodeState(any(), any());
    Iterator<JobStatus> jobStatusIterator = DagManagerTest.getMockJobStatus(flowName, flowGroup,
        flowExecutionId, flowGroup, "job0", String.valueOf(ExecutionStatus.COMPLETE));
    doReturn(jobStatusIterator).when(this.jobStatusRetriever).getJobStatusesForFlowExecution(flowName, flowGroup,
        flowExecutionId, "job0", flowGroup);
    List<SpecProducer<Spec>> specProducers = dag.getNodes().stream().map(n -> {
      try {
        return DagManagerUtils.getSpecProducer(n);
      } catch (ExecutionException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());

    ReEvaluateDagProc reEvaluateDagProc = new ReEvaluateDagProc(new ReEvaluateDagTask(new DagActionStore.DagAction(flowGroup, flowName,
        String.valueOf(flowExecutionId), "job0", DagActionStore.DagActionType.REEVALUATE), null), this.jobStatusRetriever);
    reEvaluateDagProc.process(dagManagementStateStore);

    long addSpecCount = specProducers.stream()
        .mapToLong(p -> Mockito.mockingDetails(p)
            .getInvocations()
            .stream()
            .filter(a -> a.getMethod().getName().equals("addSpec"))
            .count())
        .sum();

    Assert.assertEquals(addSpecCount, 1L);
    Assert.assertEquals(Mockito.mockingDetails(dagManagementStateStore).getInvocations().stream()
        .filter(a -> a.getMethod().getName().equals("deleteDagNodeState")).count(), 1);
  }

  @Test
  public void testNoNextJobToRun() throws Exception {
    DagManagementStateStore dagManagementStateStore = spy(MostlyMySqlDagManagementStateStoreTest.getDummyDMSS(TestMetastoreDatabaseFactory.get()));
    mockDMSS(dagManagementStateStore);
    long flowExecutionId = 123456L;
    String flowGroup = "fg";
    String flowName = "fn";
    Dag<JobExecutionPlan> dag = DagManagerTest.buildDag("2", flowExecutionId, DagManager.FailureOption.FINISH_ALL_POSSIBLE.name(),
        1, "user5", ConfigFactory.empty()
            .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
            .withValue(ConfigurationKeys.JOB_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
    );
    doReturn(Optional.of(dag)).when(dagManagementStateStore).getDag(any());
    doReturn(Optional.of(dag.getStartNodes().get(0))).when(dagManagementStateStore).getDagNode(any());
    doReturn(Optional.of(dag)).when(dagManagementStateStore).getParentDag(any());
    doReturn(true).when(dagManagementStateStore).releaseQuota(any());
    doNothing().when(dagManagementStateStore).deleteDagNodeState(any(), any());
    Iterator<JobStatus> jobStatusIterator = DagManagerTest.getMockJobStatus(flowName, flowGroup,
        flowExecutionId, flowGroup, "job0", String.valueOf(ExecutionStatus.COMPLETE));
    doReturn(jobStatusIterator).when(this.jobStatusRetriever).getJobStatusesForFlowExecution(flowName, flowGroup,
        flowExecutionId, "job0", flowGroup);
    List<SpecProducer<Spec>> specProducers = dag.getNodes().stream().map(n -> {
      try {
        return DagManagerUtils.getSpecProducer(n);
      } catch (ExecutionException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());

    long addSpecCount = specProducers.stream()
        .mapToLong(p -> Mockito.mockingDetails(p)
            .getInvocations()
            .stream()
            .filter(a -> a.getMethod().getName().equals("addSpec"))
            .count())
        .sum();

    ReEvaluateDagProc reEvaluateDagProc = new ReEvaluateDagProc(new ReEvaluateDagTask(new DagActionStore.DagAction(flowGroup, flowName,
        String.valueOf(flowExecutionId), "job0", DagActionStore.DagActionType.REEVALUATE), null), this.jobStatusRetriever);
    reEvaluateDagProc.process(dagManagementStateStore);

    Assert.assertEquals(addSpecCount, 0L);
    Assert.assertEquals(Mockito.mockingDetails(dagManagementStateStore).getInvocations().stream()
        .filter(a -> a.getMethod().getName().equals("deleteDagNodeState")).count(), 1);
    Assert.assertEquals(Mockito.mockingDetails(dagManagementStateStore).getInvocations().stream()
        .filter(a -> a.getMethod().getName().equals("deleteDag")).count(), 1);
  }
}
