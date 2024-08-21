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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.typesafe.config.Config;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.metastore.MysqlDataSourceFactory;
import org.apache.gobblin.util.DBStatementExecutor;

/**
 * Mainly testing functionalities related to DagStateStore but not Mysql-related components.
 */
@Slf4j
public class MysqlDagStateStoreWithDagNodesTest {

  private DagStateStore dagStateStore;
  private static final String TEST_USER = "testUser";
  private static ITestMetastoreDatabase testDb;
  private DBStatementExecutor dbStatementExecutor;
  private static final String GET_DAG_NODES_STATEMENT = "SELECT dag_node, is_failed_dag FROM %s WHERE parent_dag_id = ?";
  private static final String tableName = "dag_node_state_store";

  @BeforeClass
  public void setUp() throws Exception {
    testDb = TestMetastoreDatabaseFactory.get();
    ConfigBuilder configBuilder = ConfigBuilder.create()
        .addPrimitive(ConfigurationKeys.STATE_STORE_DB_USER_KEY, TEST_USER)
        .addPrimitive(ConfigurationKeys.STATE_STORE_DB_URL_KEY, testDb.getJdbcUrl())
        .addPrimitive(ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, MySqlDagManagementStateStoreTest.TEST_PASSWORD);

    // Constructing TopologySpecMap.
    Map<URI, TopologySpec> topologySpecMap = new HashMap<>();
    String specExecInstance = "mySpecExecutor";
    TopologySpec topologySpec = DagTestUtils.buildNaiveTopologySpec(specExecInstance);
    URI specExecURI = new URI(specExecInstance);
    topologySpecMap.put(specExecURI, topologySpec);
    this.dagStateStore = new MysqlDagStateStoreWithDagNodes(configBuilder.build(), topologySpecMap);
    dbStatementExecutor = new DBStatementExecutor(
        MysqlDataSourceFactory.get(configBuilder.build(), SharedResourcesBrokerFactory.getImplicitBroker()), log);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() throws Exception {
    if (testDb != null) {
      // `.close()` to avoid (in the aggregate, across multiple suites) - java.sql.SQLNonTransientConnectionException: Too many connections
      testDb.close();
    }
  }

  @Test
  public void testAddGetAndDeleteDag() throws Exception {
    Dag<JobExecutionPlan> originalDag1 = DagTestUtils.buildDag("random_1", 123L);
    Dag<JobExecutionPlan> originalDag2 = DagTestUtils.buildDag("random_2", 456L);
    DagManager.DagId dagId1 = DagManagerUtils.generateDagId(originalDag1);
    DagManager.DagId dagId2 = DagManagerUtils.generateDagId(originalDag2);
    originalDag1.getStartNodes().get(0).getValue().setCurrentGeneration(2);
    originalDag1.getStartNodes().get(0).getValue().setCurrentAttempts(3);
    this.dagStateStore.writeCheckpoint(originalDag1);
    this.dagStateStore.writeCheckpoint(originalDag2);

    // Verify get one dag
    Dag<JobExecutionPlan> dag1 = this.dagStateStore.getDag(dagId1);
    Dag<JobExecutionPlan> dag2 = this.dagStateStore.getDag(dagId2);
    Assert.assertTrue(MySqlDagManagementStateStoreTest.compareLists(dag1.getNodes(), originalDag1.getNodes()));
    Assert.assertTrue(MySqlDagManagementStateStoreTest.compareLists(dag2.getNodes(), originalDag2.getNodes()));

    // Verify dag contents
    Dag<JobExecutionPlan> dagDeserialized = dag1;
    Assert.assertEquals(dagDeserialized.getNodes().size(), 2);
    Assert.assertEquals(dagDeserialized.getStartNodes().size(), 1);
    Assert.assertEquals(dagDeserialized.getEndNodes().size(), 1);
    Dag.DagNode<JobExecutionPlan> child = dagDeserialized.getEndNodes().get(0);
    Dag.DagNode<JobExecutionPlan> parent = dagDeserialized.getStartNodes().get(0);
    Assert.assertEquals(dagDeserialized.getParentChildMap().size(), 1);
    Assert.assertTrue(dagDeserialized.getParentChildMap().get(parent).contains(child));
    Assert.assertEquals(dagDeserialized.getStartNodes().get(0).getValue().getCurrentGeneration(), 2);
    Assert.assertEquals(dagDeserialized.getStartNodes().get(0).getValue().getCurrentAttempts(), 3);

    for (int i = 0; i < 2; i++) {
      JobExecutionPlan plan = dagDeserialized.getNodes().get(i).getValue();
      Config jobConfig = plan.getJobSpec().getConfig();
      Assert.assertEquals(jobConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY), "group" + "random_1");
      Assert.assertEquals(jobConfig.getString(ConfigurationKeys.FLOW_NAME_KEY), "flow" + "random_1");
      Assert.assertEquals(jobConfig.getLong(ConfigurationKeys.FLOW_EXECUTION_ID_KEY), 123L);
      Assert.assertEquals(plan.getExecutionStatus(), ExecutionStatus.RUNNING);
      Assert.assertTrue(Boolean.parseBoolean(plan.getJobFuture().get().get().toString()));
      Assert.assertTrue(Boolean.parseBoolean(plan.getJobFuture().get().get().toString()));
    }

    dagDeserialized = dag2;
    Assert.assertEquals(dagDeserialized.getNodes().size(), 2);
    Assert.assertEquals(dagDeserialized.getStartNodes().size(), 1);
    Assert.assertEquals(dagDeserialized.getEndNodes().size(), 1);
    child = dagDeserialized.getEndNodes().get(0);
    parent = dagDeserialized.getStartNodes().get(0);
    Assert.assertEquals(dagDeserialized.getParentChildMap().size(), 1);
    Assert.assertTrue(dagDeserialized.getParentChildMap().get(parent).contains(child));

    for (int i = 0; i < 2; i++) {
      JobExecutionPlan plan = dagDeserialized.getNodes().get(i).getValue();
      Config jobConfig = plan.getJobSpec().getConfig();
      Assert.assertEquals(jobConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY), "group" + "random_2");
      Assert.assertEquals(jobConfig.getString(ConfigurationKeys.FLOW_NAME_KEY), "flow" + "random_2");
      Assert.assertEquals(jobConfig.getLong(ConfigurationKeys.FLOW_EXECUTION_ID_KEY), 456L);
      Assert.assertEquals(plan.getExecutionStatus(), ExecutionStatus.RUNNING);
    }

    dagStateStore.cleanUp(dagId1);
    dagStateStore.cleanUp(dagId2);

    Assert.assertNull(this.dagStateStore.getDag(dagId1));
    Assert.assertNull(this.dagStateStore.getDag(dagId2));
  }

  @Test
  public void testMarkDagAsFailed() throws Exception {
    // Set up initial conditions
    Dag<JobExecutionPlan> dag = DagTestUtils.buildDag("test_dag", 789L);
    DagManager.DagId dagId = DagManagerUtils.generateDagId(dag);

    this.dagStateStore.writeCheckpoint(dag);

    // Fetch all initial states into a list
    List<Boolean> initialStates = fetchDagNodeStates(dagId.toString());

    // Check Initial State
    for (Boolean state : initialStates) {
      Assert.assertFalse(state);
    }
    // Set the DAG as failed
    dag.setFailedDag(true);
    this.dagStateStore.writeCheckpoint(dag);

    // Fetch all states after marking the DAG as failed
    List<Boolean> failedStates = fetchDagNodeStates(dagId.toString());

    // Check if all states are now true (indicating failure)
    for (Boolean state : failedStates) {
      Assert.assertTrue(state);
    }
    dagStateStore.cleanUp(dagId);
    Assert.assertNull(this.dagStateStore.getDag(dagId));
  }

  private List<Boolean> fetchDagNodeStates(String dagId) throws IOException {
    List<Boolean> states = new ArrayList<>();

    dbStatementExecutor.withPreparedStatement(String.format(GET_DAG_NODES_STATEMENT, tableName), getStatement -> {

      getStatement.setString(1, dagId.toString());

      HashSet<Dag.DagNode<JobExecutionPlan>> dagNodes = new HashSet<>();

      try (ResultSet rs = getStatement.executeQuery()) {
        while (rs.next()) {
          states.add(rs.getBoolean(2));
        }
        return dagNodes;
      } catch (SQLException e) {
        throw new IOException(String.format("Failure get dag nodes for dag %s", dagId), e);
      }
    }, true);

    return states;
  }
}