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

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagTestUtils;
import org.apache.gobblin.service.modules.orchestration.DagUtils;
import org.apache.gobblin.service.modules.orchestration.DagProcessingEngine;
import org.apache.gobblin.service.modules.orchestration.MySqlDagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.MySqlDagManagementStateStoreTest;
import org.apache.gobblin.service.modules.orchestration.MysqlDagActionStore;
import org.apache.gobblin.service.modules.orchestration.task.DagProcessingEngineMetrics;
import org.apache.gobblin.service.modules.orchestration.task.ResumeDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;


public class ResumeDagProcTest {
  private MySqlDagManagementStateStore dagManagementStateStore;
  private ITestMetastoreDatabase testDb;
  private DagProcessingEngineMetrics mockedDagProcEngineMetrics;

  @BeforeClass
  public void setUp() throws Exception {
    testDb = TestMetastoreDatabaseFactory.get();
    this.dagManagementStateStore = spy(MySqlDagManagementStateStoreTest.getDummyDMSS(testDb));
    LaunchDagProcTest.mockDMSSCommonBehavior(this.dagManagementStateStore);
    this.mockedDagProcEngineMetrics = Mockito.mock(DagProcessingEngineMetrics.class);
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
  public void resumeDag() throws IOException, URISyntaxException {
    long flowExecutionId = 12345L;
    String flowGroup = "fg";
    String flowName = "fn";
    Dag<JobExecutionPlan> dag = DagTestUtils.buildDag("1", flowExecutionId, DagProcessingEngine.FailureOption.FINISH_ALL_POSSIBLE.name(),
        5, "user5", ConfigFactory.empty()
            .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
            .withValue(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY, ConfigValueFactory.fromAnyRef(
                MySqlDagManagementStateStoreTest.TEST_SPEC_EXECUTOR_URI)));
    Dag.DagId dagId = DagUtils.generateDagId(dag);
    // simulate a failed dag in store
    dag.getNodes().get(0).getValue().setExecutionStatus(ExecutionStatus.COMPLETE);
    dag.getNodes().get(1).getValue().setExecutionStatus(ExecutionStatus.FAILED);
    dag.getNodes().get(2).getValue().setExecutionStatus(ExecutionStatus.COMPLETE);
    dag.getNodes().get(4).getValue().setExecutionStatus(ExecutionStatus.COMPLETE);
    this.dagManagementStateStore.addDag(dag);
    // simulate it as a failed dag
    this.dagManagementStateStore.markDagFailed(dagId);

    ResumeDagProc resumeDagProc = new ResumeDagProc(new ResumeDagTask(new DagActionStore.DagAction(flowGroup, flowName,
        flowExecutionId, MysqlDagActionStore.NO_JOB_NAME_DEFAULT, DagActionStore.DagActionType.RESUME),
        null, this.dagManagementStateStore, mockedDagProcEngineMetrics), ConfigFactory.empty());
    resumeDagProc.process(this.dagManagementStateStore, mockedDagProcEngineMetrics);

    int expectedNumOfResumedJobs = 1; // = number of resumed nodes

    /* only the current job should have run
     we cannot check the spec producers if addSpec is called on them, because in resumeDagProc, a dag is stored in DMSS
     and retrieved, hence it goes through serialization/deserialization; in this process  the SpecProducer objects in
     a dag node are recreated and addSpec is called on new objects.
     warning - in unit tests, a test dag is created through different ways (e.g. DagManagerTest.buildDag() or DagTestUtils.buildDag()
     different methods create different spec executors (e.g. MockedSpecExecutor.createDummySpecExecutor() or
     buildNaiveTopologySpec().getSpecExecutor() respectively.
     the result will be that after serializing/deserializing the test dag, the spec executor (and producer) type may change */

    Mockito.verify(this.dagManagementStateStore, Mockito.times(expectedNumOfResumedJobs)).updateDagNode(any());

    Assert.assertFalse(DagProcUtils.isDagFinished(this.dagManagementStateStore.getDag(dagId).get()));
  }
}
