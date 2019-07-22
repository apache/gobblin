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

package org.apache.gobblin.service.modules.orchestration;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.typesafe.config.Config;

import javax.annotation.Nullable;

import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.FlowConfigResourceLocalHandler;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;
import org.apache.gobblin.service.monitoring.KafkaJobStatusMonitor;
import org.apache.gobblin.testing.AssertWithBackoff;
import org.apache.gobblin.util.ConfigUtils;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class DagManagerFlowTest {
  DagManager dagManager;

  @BeforeClass
  public void setUp() {
    Properties props = new Properties();
    props.put(DagManager.JOB_STATUS_POLLING_INTERVAL_KEY, 1);
    dagManager = new MockedDagManager(ConfigUtils.propertiesToConfig(props), false);
    dagManager.setActive(true);
  }

  @Test
  void testAddDeleteSpec() throws Exception {
    Dag<JobExecutionPlan> dag1 = DagManagerTest.buildDag("0", 123456780L, "FINISH_RUNNING", 1);
    Dag<JobExecutionPlan> dag2 = DagManagerTest.buildDag("1", 123456781L, "FINISH_RUNNING", 1);
    Dag<JobExecutionPlan> dag3 = DagManagerTest.buildDag("2", 123456782L, "FINISH_RUNNING", 1);

    // mock add spec
    dagManager.addDag(dag1);
    dagManager.addDag(dag2);
    dagManager.addDag(dag3);

    // check existence of dag in dagToJobs map
    AssertWithBackoff.create().maxSleepMs(5000).backoffFactor(1).
        assertTrue(input -> dagManager.dagManagerThreads[0].dagToJobs.containsKey(DagManagerUtils.generateDagId(dag1)), "Waiting for the map to update");
    AssertWithBackoff.create().maxSleepMs(1000).backoffFactor(1).
        assertTrue(input -> dagManager.dagManagerThreads[1].dagToJobs.containsKey(DagManagerUtils.generateDagId(dag2)), "Waiting for the map to update");
    AssertWithBackoff.create().maxSleepMs(1000).backoffFactor(1).
        assertTrue(input -> dagManager.dagManagerThreads[2].dagToJobs.containsKey(DagManagerUtils.generateDagId(dag3)), "Waiting for the map to update");

    // mock delete spec
    dagManager.stopDag(FlowConfigResourceLocalHandler.FlowUriUtils.createFlowSpecUri(new FlowId().setFlowGroup("group0").setFlowName("flow0")));
    dagManager.stopDag(FlowConfigResourceLocalHandler.FlowUriUtils.createFlowSpecUri(new FlowId().setFlowGroup("group1").setFlowName("flow1")));
    dagManager.stopDag(FlowConfigResourceLocalHandler.FlowUriUtils.createFlowSpecUri(new FlowId().setFlowGroup("group2").setFlowName("flow2")));

    // verify deleteSpec() of specProducer is called once
    AssertWithBackoff.create().maxSleepMs(5000).backoffFactor(1).assertTrue(new DeletePredicate(dag1), "Waiting for the map to update");
    AssertWithBackoff.create().maxSleepMs(1000).backoffFactor(1).assertTrue(new DeletePredicate(dag2), "Waiting for the map to update");
    AssertWithBackoff.create().maxSleepMs(1000).backoffFactor(1).assertTrue(new DeletePredicate(dag3), "Waiting for the map to update");

    // mock flow cancellation tracking event
    Mockito.doReturn(DagManagerTest.getMockJobStatus("flow0", "group0", 123456780L, "group0", "job0", String.valueOf(
        ExecutionStatus.CANCELLED)))
        .when(dagManager.getJobStatusRetriever()).getJobStatusesForFlowExecution("flow0", "group0", 123456780L, "job0", "group0");

    Mockito.doReturn(DagManagerTest.getMockJobStatus("flow1", "group1", 123456781L, "group1", "job0", String.valueOf(
        ExecutionStatus.CANCELLED)))
        .when(dagManager.getJobStatusRetriever()).getJobStatusesForFlowExecution("flow1", "group1", 123456781L, "job0", "group1");

    Mockito.doReturn(DagManagerTest.getMockJobStatus("flow2", "group2", 123456782L, "group2", "job0", String.valueOf(
        ExecutionStatus.CANCELLED)))
        .when(dagManager.getJobStatusRetriever()).getJobStatusesForFlowExecution("flow2", "group2", 123456782L, "job0", "group2");

    // check removal of dag in dagToJobs map
    AssertWithBackoff.create().maxSleepMs(5000).backoffFactor(1).
        assertTrue(input -> !dagManager.dagManagerThreads[0].dagToJobs.containsKey(DagManagerUtils.generateDagId(dag1)), "Waiting for the map to update");
    AssertWithBackoff.create().maxSleepMs(1000).backoffFactor(1).
        assertTrue(input -> !dagManager.dagManagerThreads[1].dagToJobs.containsKey(DagManagerUtils.generateDagId(dag2)), "Waiting for the map to update");
    AssertWithBackoff.create().maxSleepMs(1000).backoffFactor(1).
        assertTrue(input -> !dagManager.dagManagerThreads[2].dagToJobs.containsKey(DagManagerUtils.generateDagId(dag3)), "Waiting for the map to update");
  }
}

class DeletePredicate implements Predicate {
  private final Dag<JobExecutionPlan> dag;
  public DeletePredicate(Dag<JobExecutionPlan> dag) {
    this.dag = dag;
  }

  @Override
  public boolean apply(@Nullable Object input) {
    try {
      verify(dag.getNodes().get(0).getValue().getSpecExecutor().getProducer().get()).deleteSpec(any(), any());
    } catch (Throwable e) {
      return false;
    }
    return true;
  }
}

class MockedDagManager extends DagManager {

  public MockedDagManager(Config config, boolean instrumentationEnabled) {
    super(config, instrumentationEnabled);
  }

  @Override
  JobStatusRetriever createJobStatusRetriever(Config config) {
    JobStatusRetriever mockedJbStatusRetriever = Mockito.mock(JobStatusRetriever.class);
    Mockito.doReturn(Collections.emptyIterator()).when(mockedJbStatusRetriever).getJobStatusesForFlowExecution(anyString(), anyString(), anyLong(), anyString(), anyString());
    when(mockedJbStatusRetriever.getLatestExecutionIdsForFlow(eq("flow0"), eq("group0"), anyInt())).thenReturn(Collections.singletonList(123456780L));
    when(mockedJbStatusRetriever.getLatestExecutionIdsForFlow(eq("flow1"), eq("group1"), anyInt())).thenReturn(Collections.singletonList(123456781L));
    when(mockedJbStatusRetriever.getLatestExecutionIdsForFlow(eq("flow2"), eq("group2"), anyInt())).thenReturn(Collections.singletonList(123456782L));
    return  mockedJbStatusRetriever;
  }

  @Override
  KafkaJobStatusMonitor createJobStatusMonitor(Config config) {
    return null;
  }

  @Override
  DagStateStore createDagStateStore(Config config, Map<URI, TopologySpec> topologySpecMap) {
    DagStateStore mockedDagStateStore = Mockito.mock(DagStateStore.class);

    try {
      doNothing().when(mockedDagStateStore).writeCheckpoint(any());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return mockedDagStateStore;
  }
}
