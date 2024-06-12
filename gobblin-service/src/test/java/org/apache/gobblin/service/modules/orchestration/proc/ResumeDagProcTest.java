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

import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.service.modules.orchestration.DagManagerTest;
import org.apache.gobblin.service.modules.orchestration.DagManagerUtils;
import org.apache.gobblin.service.modules.orchestration.MostlyMySqlDagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.MostlyMySqlDagManagementStateStoreTest;
import org.apache.gobblin.service.modules.orchestration.MysqlDagActionStore;
import org.apache.gobblin.service.modules.orchestration.task.ResumeDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;


public class ResumeDagProcTest {
  private MostlyMySqlDagManagementStateStore dagManagementStateStore;
  private ITestMetastoreDatabase testDb;

  @BeforeClass
  public void setUp() throws Exception {
    testDb = TestMetastoreDatabaseFactory.get();
    this.dagManagementStateStore = spy(MostlyMySqlDagManagementStateStoreTest.getDummyDMSS(testDb));
    doReturn(FlowSpec.builder().build()).when(this.dagManagementStateStore).getFlowSpec(any());
    doNothing().when(this.dagManagementStateStore).tryAcquireQuota(any());
    doNothing().when(this.dagManagementStateStore).addDagNodeState(any(), any());
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() throws Exception {
    if (testDb != null) {
      testDb.close();
    }
  }

  /*
  This test creates a failed dag and launches a resume dag proc for it. It then verifies that the next jobs are set to run.
   */
  @Test
  public void resumeDag()
      throws IOException, URISyntaxException, ExecutionException, InterruptedException {
    long flowExecutionId = 12345L;
    String flowGroup = "fg";
    String flowName = "fn";
    Dag<JobExecutionPlan> dag = DagManagerTest.buildDag("1", flowExecutionId, DagManager.FailureOption.FINISH_ALL_POSSIBLE.name(),
        5, "user5", ConfigFactory.empty().withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName)));
    // simulate a failed dag in store
    dag.getNodes().get(0).getValue().setExecutionStatus(ExecutionStatus.COMPLETE);
    dag.getNodes().get(1).getValue().setExecutionStatus(ExecutionStatus.FAILED);
    dag.getNodes().get(2).getValue().setExecutionStatus(ExecutionStatus.COMPLETE);
    dag.getNodes().get(4).getValue().setExecutionStatus(ExecutionStatus.COMPLETE);
    doReturn(Optional.of(dag)).when(dagManagementStateStore).getFailedDag(any());

    ResumeDagProc resumeDagProc = new ResumeDagProc(new ResumeDagTask(new DagActionStore.DagAction(flowGroup, flowName,
        flowExecutionId, MysqlDagActionStore.NO_JOB_NAME_DEFAULT, DagActionStore.DagActionType.RESUME),
        null, this.dagManagementStateStore));
    resumeDagProc.process(this.dagManagementStateStore);

    SpecProducer<Spec> specProducer = DagManagerUtils.getSpecProducer(dag.getNodes().get(1));
    List<SpecProducer<Spec>> otherSpecProducers = dag.getNodes().stream().map(node -> {
      try {
        return DagManagerUtils.getSpecProducer(node);
      } catch (ExecutionException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }).filter(sp -> specProducer != sp).collect(Collectors.toList());
    int expectedNumOfResumedJobs = 1; // = number of resumed nodes

    Mockito.verify(specProducer, Mockito.times(expectedNumOfResumedJobs)).addSpec(any());
    Mockito.verify(this.dagManagementStateStore, Mockito.times(expectedNumOfResumedJobs)).addDagNodeState(any(), any());
    otherSpecProducers.forEach(sp -> Mockito.verify(sp, Mockito.never()).addSpec(any()));
  }
}
