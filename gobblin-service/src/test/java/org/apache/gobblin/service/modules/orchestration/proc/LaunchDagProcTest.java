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

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagManagementTaskStreamImpl;
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.service.modules.orchestration.DagManagerTest;
import org.apache.gobblin.service.modules.orchestration.MostlyMySqlDagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.MostlyMySqlDagManagementStateStoreTest;
import org.apache.gobblin.service.modules.orchestration.task.LaunchDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.utils.FlowCompilationValidationHelper;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;


public class LaunchDagProcTest {
  MostlyMySqlDagManagementStateStore dagManagementStateStore;
  DagManagementTaskStreamImpl dagManagementTaskStream;
  @BeforeClass
  public void setUp() throws Exception {
    this.dagManagementTaskStream = new DagManagementTaskStreamImpl(ConfigBuilder.create().build(), Optional.empty());
    this.dagManagementStateStore = spy(MostlyMySqlDagManagementStateStoreTest.getDummyDMSS(TestMetastoreDatabaseFactory.get()));
    doReturn(FlowSpec.builder().build()).when(this.dagManagementStateStore).getFlowSpec(any());
    doNothing().when(this.dagManagementStateStore).tryAcquireQuota(any());
    doNothing().when(this.dagManagementStateStore).addDagNodeState(any(), any());
  }
  @Test
  public void launchDag()
      throws IOException, InterruptedException, URISyntaxException {
    // this creates a dag with 3 start nodes
    Dag<JobExecutionPlan> dag1 = DagManagerTest.buildDagWithMultipleNodesAtDifferentLevels("1", System.currentTimeMillis(), DagManager.FailureOption.FINISH_ALL_POSSIBLE.name(),
        "user5", ConfigFactory.empty().withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef("group2")));
    FlowCompilationValidationHelper flowCompilationValidationHelper = mock(FlowCompilationValidationHelper.class);
    doReturn(com.google.common.base.Optional.of(dag1)).when(flowCompilationValidationHelper).createExecutionPlanIfValid(any());
    LaunchDagProc launchDagProc = new LaunchDagProc(new LaunchDagTask(new DagActionStore.DagAction("fg", "fn",
        "12345", DagActionStore.FlowActionType.LAUNCH), null), flowCompilationValidationHelper);

    launchDagProc.process(this.dagManagementStateStore);
    int expectedNumOfSavingDagNodeStates = 3; // = number of start nodes
    Assert.assertEquals(expectedNumOfSavingDagNodeStates,
        Mockito.mockingDetails(this.dagManagementStateStore).getInvocations().stream()
            .filter(a -> a.getMethod().getName().equals("addDagNodeState")).count());
    System.out.println();
  }
}
