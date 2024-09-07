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

package org.apache.gobblin.service.modules.orchestration.task;

import java.io.IOException;
import java.net.URISyntaxException;

import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.GobblinServiceManagerTest;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.LeaseAttemptStatus;
import org.apache.gobblin.service.modules.orchestration.MultiActiveLeaseArbiter;

import static org.apache.gobblin.service.modules.orchestration.OrchestratorTest.createBasicFlowSpecForFlowId;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;


public class LaunchDagTaskTest {
  DagActionStore.DagAction dagAction;
  LeaseAttemptStatus.LeaseObtainedStatus leaseObtainedStatus;
  DagManagementStateStore dagManagementStateStore;

  @BeforeClass
  public void setUp() throws URISyntaxException, SpecNotFoundException, IOException {
    String flowGroup = "group1";
    dagAction = new DagActionStore.DagAction(flowGroup, "name1",
        1L, "", DagActionStore.DagActionType.LAUNCH);

    MultiActiveLeaseArbiter mockedLeaseArbiter = Mockito.mock(MultiActiveLeaseArbiter.class);
    Mockito.when(mockedLeaseArbiter.recordLeaseSuccess(any())).thenReturn(true);

    leaseObtainedStatus = new LeaseAttemptStatus.LeaseObtainedStatus(
        new DagActionStore.LeaseParams(dagAction, true, 1),
        0, 5, mockedLeaseArbiter);
    dagManagementStateStore = Mockito.mock(DagManagementStateStore.class);

    FlowId flowId = GobblinServiceManagerTest.createFlowIdWithUniqueName(flowGroup);
    FlowSpec adhocSpec = createBasicFlowSpecForFlowId(flowId);
    Mockito.when(dagManagementStateStore.getFlowSpec(any())).thenReturn(adhocSpec);
  }

  /*
  Validate that after concluding an adhoc flow its flowSpec will be removed
   */
  @Test
  public void concludeRemovesAdhocFlowSpec() throws IOException {
    LaunchDagTask dagTask = new LaunchDagTask(dagAction, leaseObtainedStatus, dagManagementStateStore,
        Mockito.mock(DagProcessingEngineMetrics.class));
    dagTask.conclude();
    Mockito.verify(dagManagementStateStore, Mockito.times(1)).deleteDagAction(any());
    Mockito.verify(dagManagementStateStore, Mockito.times(1)).removeFlowSpec(any(), any(), anyBoolean());
  }
}
