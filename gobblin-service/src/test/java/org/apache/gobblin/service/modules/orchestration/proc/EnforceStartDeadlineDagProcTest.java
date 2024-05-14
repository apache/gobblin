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
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.mockito.MockedStatic;
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
import org.apache.gobblin.service.modules.core.GobblinServiceManager;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.service.modules.orchestration.DagManagerTest;
import org.apache.gobblin.service.modules.orchestration.MostlyMySqlDagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.MostlyMySqlDagManagementStateStoreTest;
import org.apache.gobblin.service.modules.orchestration.task.EnforceStartDeadlineDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.JobStatus;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;


public class EnforceStartDeadlineDagProcTest {
  private ITestMetastoreDatabase testMetastoreDatabase;
  private MostlyMySqlDagManagementStateStore dagManagementStateStore;
  private final MockedStatic<GobblinServiceManager> mockedGobblinServiceManager = Mockito.mockStatic(GobblinServiceManager.class);

  @BeforeClass
  public void setUp() throws Exception {
    this.testMetastoreDatabase = TestMetastoreDatabaseFactory.get();
    this.dagManagementStateStore = spy(MostlyMySqlDagManagementStateStoreTest.getDummyDMSS(this.testMetastoreDatabase));
    doNothing().when(this.dagManagementStateStore).tryAcquireQuota(any());
    doNothing().when(this.dagManagementStateStore).addDagNodeState(any(), any());
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() throws Exception {
    // `.close()` to avoid (in the aggregate, across multiple suites) - java.sql.SQLNonTransientConnectionException: Too many connections
    this.testMetastoreDatabase.close();
    this.mockedGobblinServiceManager.close();
  }

  @Test
  public void enforceStartDeadlineTest() throws IOException, URISyntaxException {
    String flowGroup = "fg";
    String flowName = "fn";
    long flowExecutionId = System.currentTimeMillis();
    Dag<JobExecutionPlan> dag = DagManagerTest.buildDag("1", flowExecutionId, DagManager.FailureOption.FINISH_ALL_POSSIBLE.name(),
        5, "user5", ConfigFactory.empty()
            .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
            .withValue(ConfigurationKeys.GOBBLIN_JOB_START_SLA_TIME_UNIT, ConfigValueFactory.fromAnyRef(TimeUnit.MILLISECONDS.name()))
            .withValue(ConfigurationKeys.GOBBLIN_JOB_START_SLA_TIME, ConfigValueFactory.fromAnyRef(1L)));
    JobStatus jobStatus = JobStatus.builder().flowName(flowName).flowGroup(flowGroup).jobGroup(flowGroup).jobName("job0").flowExecutionId(flowExecutionId).
        message("Test message").eventName(ExecutionStatus.ORCHESTRATED.name()).startTime(flowExecutionId).shouldRetry(false).orchestratedTime(flowExecutionId).build();
    doReturn(Optional.of(dag)).when(this.dagManagementStateStore).getDag(any());
    doReturn(Pair.of(Optional.of(dag.getStartNodes().get(0)), Optional.of(jobStatus))).when(this.dagManagementStateStore).getDagNodeWithJobStatus(any());
    this.mockedGobblinServiceManager.when(() -> GobblinServiceManager.getClass(DagActionStore.class)).thenReturn(mock(DagActionStore.class));
    this.dagManagementStateStore.checkpointDag(dag);  // simulate having a dag that has not yet started running

    EnforceStartDeadlineDagProc enforceStartDeadlineDagProc = new EnforceStartDeadlineDagProc(
        new EnforceStartDeadlineDagTask(new DagActionStore.DagAction(flowGroup, flowName, String.valueOf(flowExecutionId),
            "job0", DagActionStore.DagActionType.ENFORCE_START_DEADLINE), null, mock(DagActionStore.class)));
    enforceStartDeadlineDagProc.process(this.dagManagementStateStore);

    int expectedNumOfDeleteDagNodeStates = 1; // the one dag node corresponding to the EnforceStartDeadlineDagProc
    Assert.assertEquals(expectedNumOfDeleteDagNodeStates,
        Mockito.mockingDetails(this.dagManagementStateStore).getInvocations().stream()
            .filter(a -> a.getMethod().getName().equals("deleteDagNodeState")).count());
  }
}
