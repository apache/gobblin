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

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.flowgraph.DagNodeId;
import org.apache.gobblin.service.modules.orchestration.proc.LaunchDagProcTest;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.JobStatus;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;
import org.apache.gobblin.util.CompletedFuture;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;


/**
 * Mainly testing functionalities related to DagStateStore but not Mysql-related components.
 */
public class MySqlDagManagementStateStoreTest {

  private ITestMetastoreDatabase testDb;
  private MySqlDagManagementStateStore dagManagementStateStore;
  private static final String TEST_USER = "testUser";
  public static final String TEST_PASSWORD = "testPassword";
  private static final String TEST_TABLE = "table";
  public static String TEST_SPEC_EXECUTOR_URI = "mySpecExecutor";

  @BeforeClass
  public void setUp() throws Exception {
    // Setting up mock DB
    this.testDb = TestMetastoreDatabaseFactory.get();
    this.dagManagementStateStore = getDummyDMSS(this.testDb);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() throws Exception {
    if (this.testDb != null) {
      // `.close()` to avoid (in the aggregate, across multiple suites) - java.sql.SQLNonTransientConnectionException: Too many connections
      this.testDb.close();
    }
  }

  public static <T> boolean compareLists(List<T> list1, List<T> list2) {
    if (list1.size() != list2.size()) {
      return false;
    }
    for (T item : list1) {
      if (Collections.frequency(list1, item) != Collections.frequency(list2, item)) {
        return false;
      }
    }
    return true;
  }

  @Test
  public void testAddDag() throws Exception {
    Dag<JobExecutionPlan> dag = DagTestUtils.buildDag("test", 12345L);
    Dag<JobExecutionPlan> dag2 = DagTestUtils.buildDag("test2", 123456L);
    Dag.DagNode<JobExecutionPlan> dagNode = dag.getNodes().get(0);
    Dag.DagNode<JobExecutionPlan> dagNode2 = dag.getNodes().get(1);
    Dag.DagNode<JobExecutionPlan> dagNode3 = dag2.getNodes().get(0);
    Dag.DagId dagId = DagUtils.generateDagId(dag);
    DagNodeId dagNodeId = DagUtils.calcJobId(dagNode.getValue().getJobSpec().getConfig());

    this.dagManagementStateStore.addDag(dag);
    this.dagManagementStateStore.addDag(dag2);
    this.dagManagementStateStore.updateDagNode(dagNode);
    this.dagManagementStateStore.updateDagNode(dagNode2);
    this.dagManagementStateStore.updateDagNode(dagNode3);

    Assert.assertTrue(compareLists(dag.getNodes(), this.dagManagementStateStore.getDag(dagId).get().getNodes()));
    Assert.assertEquals(dagNode, this.dagManagementStateStore.getDagNodeWithJobStatus(dagNodeId).getLeft().get());

    Set<Dag.DagNode<JobExecutionPlan>> dagNodes = this.dagManagementStateStore.getDagNodes(dagId);
    Assert.assertEquals(2, dagNodes.size());
    Assert.assertTrue(dagNodes.contains(dagNode));
    Assert.assertTrue(dagNodes.contains(dagNode2));

    // test to verify that adding a new dag node with the same dag node id (defined by the jobSpec) replaces the existing one
    JobExecutionPlan jobExecutionPlan = new JobExecutionPlan(dagNode2.getValue().getJobSpec(),
        DagTestUtils.buildNaiveTopologySpec("mySpecExecutor").getSpecExecutor());
    jobExecutionPlan.setExecutionStatus(ExecutionStatus.RUNNING);
    // Future of type CompletedFuture is used because in tests InMemorySpecProducer is used and that responds with CompletedFuture
    CompletedFuture<Boolean> future = new CompletedFuture<>(Boolean.TRUE, null);
    jobExecutionPlan.setJobFuture(Optional.of(future));

    Dag.DagNode<JobExecutionPlan> duplicateDagNode = new Dag.DagNode<>(jobExecutionPlan);
    this.dagManagementStateStore.updateDagNode(duplicateDagNode);
    Assert.assertEquals(this.dagManagementStateStore.getDagNodes(dagId).size(), 2);
  }

  public static MySqlDagManagementStateStore getDummyDMSS(ITestMetastoreDatabase testMetastoreDatabase) throws Exception {
    ConfigBuilder configBuilder = ConfigBuilder.create();
    configBuilder.addPrimitive(MySqlDagManagementStateStore.DAG_STATESTORE_CLASS_KEY, MysqlDagStateStoreWithDagNodes.class.getName())
        .addPrimitive(ConfigurationKeys.STATE_STORE_DB_URL_KEY, testMetastoreDatabase.getJdbcUrl())
        .addPrimitive(ConfigurationKeys.STATE_STORE_DB_TABLE_KEY, "dag" + 1)
        .addPrimitive(ConfigurationKeys.STATE_STORE_DB_USER_KEY, TEST_USER)
        .addPrimitive(MySqlDagManagementStateStore.FAILED_DAG_STATESTORE_PREFIX
            + "." + ConfigurationKeys.STATE_STORE_DB_TABLE_KEY, TEST_TABLE + 2);
    Config config = configBuilder.build();
    JobStatusRetriever jobStatusRetriever = mock(JobStatusRetriever.class);
    JobStatus dummyJobStatus = JobStatus.builder().flowName("fn").flowGroup("fg").jobGroup("fg").jobName("job0")
        .flowExecutionId(12345L).message("Test message").eventName(ExecutionStatus.COMPLETE.name()).build();
    doReturn(Lists.newArrayList(dummyJobStatus).iterator()).when(jobStatusRetriever)
        .getJobStatusesForFlowExecution(anyString(), anyString(), anyLong(), anyString(), anyString());

    // Constructing TopologySpecMap.
    Map<URI, TopologySpec> topologySpecMap = new HashMap<>();

    TopologySpec topologySpec = LaunchDagProcTest.buildNaiveTopologySpec(TEST_SPEC_EXECUTOR_URI);
    URI specExecURI = new URI(TEST_SPEC_EXECUTOR_URI);
    topologySpecMap.put(specExecURI, topologySpec);
    MySqlDagManagementStateStore dagManagementStateStore =
        new MySqlDagManagementStateStore(config, null, null, jobStatusRetriever,
            MysqlDagActionStoreTest.getTestDagActionStore(testMetastoreDatabase));
    dagManagementStateStore.setTopologySpecMap(topologySpecMap);
    return dagManagementStateStore;
  }
}