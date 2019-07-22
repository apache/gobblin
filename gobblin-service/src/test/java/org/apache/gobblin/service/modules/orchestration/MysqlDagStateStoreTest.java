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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbcp.BasicDataSource;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.typesafe.config.Config;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.MysqlStateStore;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;


/**
 * Mainly testing functionalities related to DagStateStore but not Mysql-related components.
 */
public class MysqlDagStateStoreTest {

  private DagStateStore _dagStateStore;
  private Map<URI, TopologySpec> topologySpecMap;

  private static final String TEST_USER = "testUser";
  private static final String TEST_PASSWORD = "testPassword";
  private static final String TEST_DAG_STATE_STORE = "TestDagStateStore";

  @BeforeClass
  public void setUp() throws Exception {


    ConfigBuilder configBuilder = ConfigBuilder.create();

    // Constructing TopologySpecMap.
    this.topologySpecMap = new HashMap<>();
    String specExecInstance = "mySpecExecutor";
    TopologySpec topologySpec = DagTestUtils.buildNaiveTopologySpec(specExecInstance);
    URI specExecURI = new URI(specExecInstance);
    this.topologySpecMap.put(specExecURI, topologySpec);

    this._dagStateStore = new TestMysqlDagStateStore(configBuilder.build(), this.topologySpecMap);
  }


  @Test
  public void testWriteCheckpointAndGetAll() throws Exception{
    Dag<JobExecutionPlan> dag_0 = DagTestUtils.buildDag("random_0", 123L);
    Dag<JobExecutionPlan> dag_1 = DagTestUtils.buildDag("random_1", 456L);
    _dagStateStore.writeCheckpoint(dag_0);
    _dagStateStore.writeCheckpoint(dag_1);

    List<Dag<JobExecutionPlan>> dags = _dagStateStore.getDags();
    Assert.assertEquals(dags.size(), 2);

    // Verify dag contents
    Dag<JobExecutionPlan> dagDeserialized = dags.get(0);
    Assert.assertEquals(dagDeserialized.getNodes().size(), 2);
    Assert.assertEquals(dagDeserialized.getStartNodes().size(), 1);
    Assert.assertEquals(dagDeserialized.getEndNodes().size(), 1);
    Dag.DagNode<JobExecutionPlan> child = dagDeserialized.getEndNodes().get(0);
    Dag.DagNode<JobExecutionPlan> parent = dagDeserialized.getStartNodes().get(0);
    Assert.assertEquals(dagDeserialized.getParentChildMap().size(), 1);
    Assert.assertTrue(dagDeserialized.getParentChildMap().get(parent).contains(child));

    for (int i = 0; i < 2; i++) {
      JobExecutionPlan plan = dagDeserialized.getNodes().get(i).getValue();
      Config jobConfig = plan.getJobSpec().getConfig();
      Assert.assertEquals(jobConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY), "group" + "random_0");
      Assert.assertEquals(jobConfig.getString(ConfigurationKeys.FLOW_NAME_KEY), "flow" + "random_0");
      Assert.assertEquals(jobConfig.getLong(ConfigurationKeys.FLOW_EXECUTION_ID_KEY), 123L);
      Assert.assertEquals(plan.getExecutionStatus(), ExecutionStatus.RUNNING);
      Assert.assertTrue(Boolean.parseBoolean(plan.getJobFuture().get().get().toString()));
      Assert.assertTrue(Boolean.parseBoolean(plan.getJobFuture().get().get().toString()));
    }

    dagDeserialized = dags.get(1);
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
      Assert.assertEquals(jobConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY), "group" + "random_1");
      Assert.assertEquals(jobConfig.getString(ConfigurationKeys.FLOW_NAME_KEY), "flow" + "random_1");
      Assert.assertEquals(jobConfig.getLong(ConfigurationKeys.FLOW_EXECUTION_ID_KEY), 456L);
      Assert.assertEquals(plan.getExecutionStatus(), ExecutionStatus.RUNNING);
    }
  }

  @Test (dependsOnMethods = "testWriteCheckpointAndGetAll")
  public void testCleanUp() throws Exception {
    Dag<JobExecutionPlan> dag_0 = DagTestUtils.buildDag("random_0", 123L);
    Dag<JobExecutionPlan> dag_1 = DagTestUtils.buildDag("random_1", 456L);
    _dagStateStore.writeCheckpoint(dag_0);
    _dagStateStore.writeCheckpoint(dag_1);

    List<Dag<JobExecutionPlan>> dags = _dagStateStore.getDags();
    Assert.assertEquals(dags.size(), 2);

    _dagStateStore.cleanUp(dags.get(0));
    _dagStateStore.cleanUp(dags.get(1));

    dags = _dagStateStore.getDags();
    Assert.assertEquals(dags.size(), 0);
  }

  /**
   * Only overwrite {@link #createStateStore(Config)} method to directly return a mysqlStateStore
   * backed by mocked db.
   */
  public class TestMysqlDagStateStore extends MysqlDagStateStore {
    public TestMysqlDagStateStore(Config config, Map<URI, TopologySpec> topologySpecMap) {
      super(config, topologySpecMap);
    }

    @Override
    protected StateStore<State> createStateStore(Config config) {
      try {
        // Setting up mock DB
        ITestMetastoreDatabase testMetastoreDatabase = TestMetastoreDatabaseFactory.get();
        String jdbcUrl = testMetastoreDatabase.getJdbcUrl();
        BasicDataSource mySqlDs = new BasicDataSource();

        mySqlDs.setDriverClassName(ConfigurationKeys.DEFAULT_STATE_STORE_DB_JDBC_DRIVER);
        mySqlDs.setDefaultAutoCommit(false);
        mySqlDs.setUrl(jdbcUrl);
        mySqlDs.setUsername(TEST_USER);
        mySqlDs.setPassword(TEST_PASSWORD);

        return new MysqlStateStore<>(mySqlDs, TEST_DAG_STATE_STORE, false, State.class);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}