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
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.MysqlStateStore;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.flowgraph.DagNodeId;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;


/**
 * Mainly testing functionalities related to DagStateStore but not Mysql-related components.
 */
public class MostlyMySqlDagManagementStateStoreTest {

  private DagManagementStateStore dagManagementStateStore;
  private static final String TEST_USER = "testUser";
  private static final String TEST_PASSWORD = "testPassword";
  private static final String TEST_DAG_STATE_STORE = "TestDagStateStore";
  private static final String TEST_TABLE = "quotas";
  static ITestMetastoreDatabase testMetastoreDatabase;


  @BeforeClass
  public void setUp() throws Exception {
    // Setting up mock DB
    testMetastoreDatabase = TestMetastoreDatabaseFactory.get();

    Config config;
    ConfigBuilder configBuilder = ConfigBuilder.create();
    configBuilder.addPrimitive(MostlyMySqlDagManagementStateStore.DAG_STATESTORE_CLASS_KEY, TestMysqlDagStateStore.class.getName())
        .addPrimitive(MysqlUserQuotaManager.qualify(ConfigurationKeys.STATE_STORE_DB_URL_KEY), testMetastoreDatabase.getJdbcUrl())
        .addPrimitive(MysqlUserQuotaManager.qualify(ConfigurationKeys.STATE_STORE_DB_USER_KEY), TEST_USER)
        .addPrimitive(MysqlUserQuotaManager.qualify(ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY), TEST_PASSWORD)
        .addPrimitive(MysqlUserQuotaManager.qualify(ConfigurationKeys.STATE_STORE_DB_TABLE_KEY), TEST_TABLE);
    config = configBuilder.build();

    this.dagManagementStateStore = new MostlyMySqlDagManagementStateStore(config);
  }

  @Test
  public void testAddDag() throws Exception {
    Dag<JobExecutionPlan> dag = DagTestUtils.buildDag("test", 12345L);
    Dag<JobExecutionPlan> dag2 = DagTestUtils.buildDag("test2", 123456L);
    Dag.DagNode<JobExecutionPlan> dagNode = dag.getNodes().get(0);
    Dag.DagNode<JobExecutionPlan> dagNode2 = dag.getNodes().get(1);
    Dag.DagNode<JobExecutionPlan> dagNode3 = dag2.getNodes().get(0);
    DagManager.DagId dagId = DagManagerUtils.generateDagId(dag);
    DagManager.DagId dagId2 = DagManagerUtils.generateDagId(dag2);
    DagNodeId dagNodeId = DagManagerUtils.calcJobId(dagNode.getValue().getJobSpec().getConfig());

    this.dagManagementStateStore.checkpointDag(dag);
    this.dagManagementStateStore.checkpointDag(dag2);
    this.dagManagementStateStore.addDagNodeState(dagNode, dagId);
    this.dagManagementStateStore.addDagNodeState(dagNode2, dagId);
    this.dagManagementStateStore.addDagNodeState(dagNode3, dagId2);

    Assert.assertTrue(this.dagManagementStateStore.containsDag(dagId));
    Assert.assertEquals(dag.toString(), this.dagManagementStateStore.getDag(dagId).get().toString());
    Assert.assertEquals(dagNode, this.dagManagementStateStore.getDagNode(dagNodeId).get());
    Assert.assertEquals(dag.toString(), this.dagManagementStateStore.getParentDag(dagNode).get().toString());

    List<Dag.DagNode<JobExecutionPlan>> dagNodes = this.dagManagementStateStore.getDagNodes(dagId);
    Assert.assertEquals(2, dagNodes.size());
    Assert.assertEquals(dagNode, dagNodes.get(0));
    Assert.assertEquals(dagNode2, dagNodes.get(1));

    dagNodes = this.dagManagementStateStore.getDagNodes(dagId);
    Assert.assertEquals(2, dagNodes.size());
    Assert.assertTrue(dagNodes.contains(dagNode));
    Assert.assertTrue(dagNodes.contains(dagNode2));

    this.dagManagementStateStore.deleteDagNodeState(dagId, dagNode);
    Assert.assertFalse(this.dagManagementStateStore.getDagNodes(dagId).contains(dagNode));
    Assert.assertTrue(this.dagManagementStateStore.getDagNodes(dagId).contains(dagNode2));
    Assert.assertTrue(this.dagManagementStateStore.getDagNodes(dagId2).contains(dagNode3));
  }

  /**
   * Only overwrite {@link #createStateStore(Config)} method to directly return a mysqlStateStore
   * backed by mocked db.
   */
  public static class TestMysqlDagStateStore extends MysqlDagStateStore {
    public TestMysqlDagStateStore(Config config, Map<URI, TopologySpec> topologySpecMap) {
      super(config, topologySpecMap);
    }

    @Override
    protected StateStore<State> createStateStore(Config config) {
      try {

        String jdbcUrl = MostlyMySqlDagManagementStateStoreTest.testMetastoreDatabase.getJdbcUrl();
        HikariDataSource dataSource = new HikariDataSource();

        dataSource.setDriverClassName(ConfigurationKeys.DEFAULT_STATE_STORE_DB_JDBC_DRIVER);
        dataSource.setAutoCommit(false);
        dataSource.setJdbcUrl(jdbcUrl);
        dataSource.setUsername(TEST_USER);
        dataSource.setPassword(TEST_PASSWORD);

        return new MysqlStateStore<>(dataSource, TEST_DAG_STATE_STORE, false, State.class);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}