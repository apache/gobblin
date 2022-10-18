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

import com.google.common.base.Optional;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.dag_action_store.MysqlDagActionStore;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import javax.annotation.Nullable;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;
import org.apache.gobblin.testing.AssertWithBackoff;
import org.apache.gobblin.util.ConfigUtils;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class DagManagerFlowTest {
  MockedDagManager dagManager;
  int dagNumThreads;
  static final String ERROR_MESSAGE = "Waiting for the map to update";
  private static final String USER = "testUser";
  private static final String PASSWORD = "testPassword";
  private static final String TABLE = "dag_action_store";
  private static final String flowGroup = "testFlowGroup";
  private static final String flowName = "testFlowName";
  private static final String flowExecutionId = "12345677";
  private static final String flowExecutionId_2 = "12345678";
  private DagActionStore dagActionStore;

  @BeforeClass
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.put(DagManager.JOB_STATUS_POLLING_INTERVAL_KEY, 1);
    ITestMetastoreDatabase testDb = TestMetastoreDatabaseFactory.get();

    Config config = ConfigBuilder.create()
        .addPrimitive("MysqlDagActionStore." + ConfigurationKeys.STATE_STORE_DB_URL_KEY, testDb.getJdbcUrl())
        .addPrimitive("MysqlDagActionStore." + ConfigurationKeys.STATE_STORE_DB_USER_KEY, USER)
        .addPrimitive("MysqlDagActionStore." + ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, PASSWORD)
        .addPrimitive("MysqlDagActionStore." + ConfigurationKeys.STATE_STORE_DB_TABLE_KEY, TABLE)
        .build();

    dagActionStore = new MysqlDagActionStore(config);
    dagActionStore.addDagAction(flowGroup, flowName, flowExecutionId, DagActionStore.DagActionValue.KILL);
    dagActionStore.addDagAction(flowGroup, flowName, flowExecutionId_2, DagActionStore.DagActionValue.RESUME);
    dagManager = new MockedDagManager(ConfigUtils.propertiesToConfig(props), false);
    dagManager.dagActionStore = Optional.of(dagActionStore);
    dagManager.setActive(true);
    this.dagNumThreads = dagManager.getNumThreads();
    Thread.sleep(10000);
    // On active, should proceed request and delete action entry
    Assert.assertEquals(dagActionStore.getDagActions().size(), 0);
  }

  @Test
  void testAddDeleteSpec() throws Exception {
    long flowExecutionId1 = System.currentTimeMillis();
    long flowExecutionId2 = flowExecutionId1 + 1;
    long flowExecutionId3 = flowExecutionId1 + 2;

    Dag<JobExecutionPlan> dag1 = DagManagerTest.buildDag("0", flowExecutionId1, "FINISH_RUNNING", 1);
    Dag<JobExecutionPlan> dag2 = DagManagerTest.buildDag("1", flowExecutionId2, "FINISH_RUNNING", 1);
    Dag<JobExecutionPlan> dag3 = DagManagerTest.buildDag("2", flowExecutionId3, "FINISH_RUNNING", 1);

    String dagId1 = DagManagerUtils.generateDagId(dag1).toString();
    String dagId2 = DagManagerUtils.generateDagId(dag2).toString();
    String dagId3 = DagManagerUtils.generateDagId(dag3).toString();

    int queue1 = DagManagerUtils.getDagQueueId(dag1, dagNumThreads);
    int queue2 = DagManagerUtils.getDagQueueId(dag2, dagNumThreads);
    int queue3 = DagManagerUtils.getDagQueueId(dag3, dagNumThreads);

    when(this.dagManager.getJobStatusRetriever().getLatestExecutionIdsForFlow(eq("flow0"), eq("group0"), anyInt()))
        .thenReturn(Collections.singletonList(flowExecutionId1));
    when(this.dagManager.getJobStatusRetriever().getLatestExecutionIdsForFlow(eq("flow1"), eq("group1"), anyInt()))
        .thenReturn(Collections.singletonList(flowExecutionId2));
    when(this.dagManager.getJobStatusRetriever().getLatestExecutionIdsForFlow(eq("flow2"), eq("group2"), anyInt()))
        .thenReturn(Collections.singletonList(flowExecutionId3));

    // mock add spec
    dagManager.addDag(dag1, true, true);
    dagManager.addDag(dag2, true, true);
    dagManager.addDag(dag3, true, true);

    // check existence of dag in dagToJobs map
    AssertWithBackoff.create().maxSleepMs(5000).backoffFactor(1).
        assertTrue(input -> dagManager.dagManagerThreads[queue1].dagToJobs.containsKey(dagId1), ERROR_MESSAGE);
    AssertWithBackoff.create().maxSleepMs(1000).backoffFactor(1).
        assertTrue(input -> dagManager.dagManagerThreads[queue2].dagToJobs.containsKey(dagId2), ERROR_MESSAGE);
    AssertWithBackoff.create().maxSleepMs(1000).backoffFactor(1).
        assertTrue(input -> dagManager.dagManagerThreads[queue3].dagToJobs.containsKey(dagId3), ERROR_MESSAGE);

    // mock cancel job
    dagManager.stopDag(FlowSpec.Utils.createFlowSpecUri(new FlowId().setFlowGroup("group0").setFlowName("flow0")));
    dagManager.stopDag(FlowSpec.Utils.createFlowSpecUri(new FlowId().setFlowGroup("group1").setFlowName("flow1")));
    dagManager.stopDag(FlowSpec.Utils.createFlowSpecUri(new FlowId().setFlowGroup("group2").setFlowName("flow2")));

    // verify cancelJob() of specProducer is called once
    AssertWithBackoff.create().maxSleepMs(5000).backoffFactor(1).assertTrue(new CancelPredicate(dag1), ERROR_MESSAGE);
    AssertWithBackoff.create().maxSleepMs(1000).backoffFactor(1).assertTrue(new CancelPredicate(dag2), ERROR_MESSAGE);
    AssertWithBackoff.create().maxSleepMs(1000).backoffFactor(1).assertTrue(new CancelPredicate(dag3), ERROR_MESSAGE);

    // mock flow cancellation tracking event
    Mockito.doReturn(DagManagerTest.getMockJobStatus("flow0", "group0", flowExecutionId1,
        "group0", "job0", String.valueOf(ExecutionStatus.CANCELLED)))
        .when(dagManager.getJobStatusRetriever()).getJobStatusesForFlowExecution("flow0", "group0",
        flowExecutionId1, "job0", "group0");

    Mockito.doReturn(DagManagerTest.getMockJobStatus("flow1", "group1", flowExecutionId2,
        "group1", "job0", String.valueOf(ExecutionStatus.CANCELLED)))
        .when(dagManager.getJobStatusRetriever()).getJobStatusesForFlowExecution("flow1", "group1",
        flowExecutionId2, "job0", "group1");

    Mockito.doReturn(DagManagerTest.getMockJobStatus("flow2", "group2", flowExecutionId3,
        "group2", "job0", String.valueOf(ExecutionStatus.CANCELLED)))
        .when(dagManager.getJobStatusRetriever()).getJobStatusesForFlowExecution("flow2", "group2",
        flowExecutionId3, "job0", "group2");

    // check removal of dag in dagToJobs map
    AssertWithBackoff.create().maxSleepMs(5000).backoffFactor(1).
        assertTrue(input -> !dagManager.dagManagerThreads[queue1].dagToJobs.containsKey(dagId1), ERROR_MESSAGE);
    AssertWithBackoff.create().maxSleepMs(1000).backoffFactor(1).
        assertTrue(input -> !dagManager.dagManagerThreads[queue2].dagToJobs.containsKey(dagId2), ERROR_MESSAGE);
    AssertWithBackoff.create().maxSleepMs(1000).backoffFactor(1).
        assertTrue(input -> !dagManager.dagManagerThreads[queue3].dagToJobs.containsKey(dagId3), ERROR_MESSAGE);
  }

  @Test
  void testFlowSlaWithoutConfig() throws Exception {
    long flowExecutionId = System.currentTimeMillis();
    Dag<JobExecutionPlan> dag = DagManagerTest.buildDag("3", flowExecutionId, "FINISH_RUNNING", 1);
    String dagId = DagManagerUtils.generateDagId(dag).toString();
    int queue = DagManagerUtils.getDagQueueId(dag, dagNumThreads);

    when(this.dagManager.getJobStatusRetriever().getLatestExecutionIdsForFlow(eq("flow3"), eq("group3"), anyInt()))
        .thenReturn(Collections.singletonList(flowExecutionId));

    // mock add spec
    dagManager.addDag(dag, true, true);

    // check existence of dag in dagToJobs map
    AssertWithBackoff.create().maxSleepMs(5000).backoffFactor(1).
        assertTrue(input -> dagManager.dagManagerThreads[queue].dagToJobs.containsKey(dagId), ERROR_MESSAGE);

    // check existence of dag in dagToSLA map
    AssertWithBackoff.create().maxSleepMs(5000).backoffFactor(1).
        assertTrue(input -> dagManager.dagManagerThreads[queue].dagToSLA.containsKey(dagId), ERROR_MESSAGE);

    // check the SLA value
    Assert.assertEquals(dagManager.dagManagerThreads[queue].dagToSLA.get(dagId).longValue(), DagManagerUtils.DEFAULT_FLOW_SLA_MILLIS);

    // verify cancelJob() of the specProducer is not called once
    // which means job cancellation was triggered
    try {
      AssertWithBackoff.create().maxSleepMs(5000).backoffFactor(1).assertTrue(new CancelPredicate(dag), ERROR_MESSAGE);
    } catch (TimeoutException e) {
      AssertWithBackoff.create().maxSleepMs(5000).backoffFactor(1).
          assertTrue(input -> dagManager.dagManagerThreads[queue].dagToJobs.containsKey(dagId), ERROR_MESSAGE);
      return;
    }

    Assert.fail("Job cancellation was not triggered.");
  }

  @Test()
  void testFlowSlaWithConfig() throws Exception {
    long flowExecutionId = System.currentTimeMillis();
    Dag<JobExecutionPlan> dag = DagManagerTest.buildDag("4", flowExecutionId, "FINISH_RUNNING", 1);
    String dagId = DagManagerUtils.generateDagId(dag).toString();
    int queue = DagManagerUtils.getDagQueueId(dag, dagNumThreads);

    when(this.dagManager.getJobStatusRetriever().getLatestExecutionIdsForFlow(eq("flow4"), eq("group4"), anyInt()))
        .thenReturn(Collections.singletonList(flowExecutionId));

    // change config to set a small sla
    Config jobConfig = dag.getStartNodes().get(0).getValue().getJobSpec().getConfig();
    jobConfig = jobConfig
        .withValue(ConfigurationKeys.GOBBLIN_FLOW_SLA_TIME, ConfigValueFactory.fromAnyRef("7"))
        .withValue(ConfigurationKeys.GOBBLIN_FLOW_SLA_TIME_UNIT, ConfigValueFactory.fromAnyRef(TimeUnit.SECONDS.name()));
    dag.getStartNodes().get(0).getValue().getJobSpec().setConfig(jobConfig);

    // mock add spec
    dagManager.addDag(dag, true, true);

    // check existence of dag in dagToSLA map
    AssertWithBackoff.create().maxSleepMs(5000).backoffFactor(1).
        assertTrue(input -> dagManager.dagManagerThreads[queue].dagToSLA.containsKey(dagId), ERROR_MESSAGE);

    // check the SLA value
    Assert.assertEquals(dagManager.dagManagerThreads[queue].dagToSLA.get(dagId).longValue(), TimeUnit.SECONDS.toMillis(7L));

    // check existence of dag in dagToJobs map
    AssertWithBackoff.create().maxSleepMs(5000).backoffFactor(1).
        assertTrue(input -> dagManager.dagManagerThreads[queue].dagToJobs.containsKey(dagId), ERROR_MESSAGE);

    // verify cancelJob() of specProducer is called once
    // which means job cancellation was triggered
    AssertWithBackoff.create().maxSleepMs(5000).backoffFactor(1).assertTrue(new CancelPredicate(dag), ERROR_MESSAGE);

    // check removal of dag from dagToSLA map
    AssertWithBackoff.create().maxSleepMs(5000).backoffFactor(1).
        assertTrue(input -> !dagManager.dagManagerThreads[queue].dagToSLA.containsKey(dagId), ERROR_MESSAGE);
  }

  @Test()
  void testOrphanFlowKill() throws Exception {
    Long flowExecutionId = System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(10);
    Dag<JobExecutionPlan> dag = DagManagerTest.buildDag("6", flowExecutionId, "FINISH_RUNNING", 1);
    String dagId = DagManagerUtils.generateDagId(dag).toString();
    int queue = DagManagerUtils.getDagQueueId(dag, dagNumThreads);

    // change config to set a small sla
    Config jobConfig = dag.getStartNodes().get(0).getValue().getJobSpec().getConfig();
    jobConfig = jobConfig
        .withValue(ConfigurationKeys.GOBBLIN_JOB_START_SLA_TIME, ConfigValueFactory.fromAnyRef("7"))
        .withValue(ConfigurationKeys.GOBBLIN_JOB_START_SLA_TIME_UNIT, ConfigValueFactory.fromAnyRef(TimeUnit.SECONDS.name()));
    dag.getStartNodes().get(0).getValue().getJobSpec().setConfig(jobConfig);

    // mock add spec
    dagManager.addDag(dag, true, true);

    // check existence of dag in dagToSLA map
    AssertWithBackoff.create().maxSleepMs(5000).backoffFactor(1).
        assertTrue(input -> dagManager.dagManagerThreads[queue].dagToSLA.containsKey(dagId), ERROR_MESSAGE);

    Mockito.doReturn(DagManagerTest.getMockJobStatus("flow6", "group6", flowExecutionId,
        "group6", "job0", String.valueOf(ExecutionStatus.ORCHESTRATED)))
        .when(dagManager.getJobStatusRetriever()).getJobStatusesForFlowExecution("flow6", "group6",
        flowExecutionId, "job0", "group6");

    // check existence of dag in dagToJobs map
    AssertWithBackoff.create().maxSleepMs(5000).backoffFactor(1).
        assertTrue(input -> dagManager.dagManagerThreads[queue].dagToJobs.containsKey(dagId), ERROR_MESSAGE);

    // verify cancelJob() of specProducer is called once
    // which means job cancellation was triggered
    AssertWithBackoff.create().maxSleepMs(5000).backoffFactor(1).assertTrue(new CancelPredicate(dag), ERROR_MESSAGE);

    // check removal of dag from dagToSLA map
    AssertWithBackoff.create().maxSleepMs(5000).backoffFactor(1).
        assertTrue(input -> !dagManager.dagManagerThreads[queue].dagToSLA.containsKey(dagId), ERROR_MESSAGE);
  }

  @Test
  void slaConfigCheck() throws Exception {
    Dag<JobExecutionPlan> dag = DagManagerTest.buildDag("5", 123456783L, "FINISH_RUNNING", 1);
    Assert.assertEquals(DagManagerUtils.getFlowSLA(dag.getStartNodes().get(0)), DagManagerUtils.DEFAULT_FLOW_SLA_MILLIS);

    Config jobConfig = dag.getStartNodes().get(0).getValue().getJobSpec().getConfig();
    jobConfig = jobConfig
        .withValue(ConfigurationKeys.GOBBLIN_FLOW_SLA_TIME, ConfigValueFactory.fromAnyRef("7"))
        .withValue(ConfigurationKeys.GOBBLIN_FLOW_SLA_TIME_UNIT, ConfigValueFactory.fromAnyRef(TimeUnit.SECONDS.name()));
    dag.getStartNodes().get(0).getValue().getJobSpec().setConfig(jobConfig);
    Assert.assertEquals(DagManagerUtils.getFlowSLA(dag.getStartNodes().get(0)), TimeUnit.SECONDS.toMillis(7L));

    jobConfig = jobConfig
        .withValue(ConfigurationKeys.GOBBLIN_FLOW_SLA_TIME, ConfigValueFactory.fromAnyRef("8"))
        .withValue(ConfigurationKeys.GOBBLIN_FLOW_SLA_TIME_UNIT, ConfigValueFactory.fromAnyRef(TimeUnit.MINUTES.name()));
    dag.getStartNodes().get(0).getValue().getJobSpec().setConfig(jobConfig);
    Assert.assertEquals(DagManagerUtils.getFlowSLA(dag.getStartNodes().get(0)), TimeUnit.MINUTES.toMillis(8L));
  }
}

class CancelPredicate implements Predicate<Void> {
  private final Dag<JobExecutionPlan> dag;
  public CancelPredicate(Dag<JobExecutionPlan> dag) {
    this.dag = dag;
  }

  @Override
  public boolean apply(@Nullable Void input) {
    try {
      verify(dag.getNodes().get(0).getValue().getSpecExecutor().getProducer().get()).cancelJob(any(), any());
    } catch (Throwable e) {
      return false;
    }
    return true;
  }
}

class MockedDagManager extends DagManager {

  public MockedDagManager(Config config, boolean instrumentationEnabled) {
    super(config, createJobStatusRetriever(), instrumentationEnabled);
  }

  private static JobStatusRetriever createJobStatusRetriever() {
    JobStatusRetriever mockedJbStatusRetriever = Mockito.mock(JobStatusRetriever.class);
    Mockito.doReturn(Collections.emptyIterator()).when(mockedJbStatusRetriever).
        getJobStatusesForFlowExecution(anyString(), anyString(), anyLong(), anyString(), anyString());
    return mockedJbStatusRetriever;
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
