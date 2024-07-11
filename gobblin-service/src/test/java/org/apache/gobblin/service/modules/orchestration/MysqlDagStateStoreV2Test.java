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
import java.util.Map;

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


/**
 * Mainly testing functionalities related to DagStateStore but not Mysql-related components.
 */
public class MysqlDagStateStoreV2Test {

  private DagStateStore dagStateStore;

  private static final String TEST_USER = "testUser";
  private static ITestMetastoreDatabase testDb;

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
    this.dagStateStore = new MysqlDagStateStoreV2(configBuilder.build(), topologySpecMap);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() throws Exception {
    if (testDb != null) {
      // `.close()` to avoid (in the aggregate, across multiple suites) - java.sql.SQLNonTransientConnectionException: Too many connections
      testDb.close();
    }
  }

  @Test
  public void testWriteGetAndDeleteDag() throws Exception{
    Dag<JobExecutionPlan> dag_0 = DagTestUtils.buildDag("random_0", 123L);
    Dag<JobExecutionPlan> dag_1 = DagTestUtils.buildDag("random_1", 456L);
    DagManager.DagId dagId0 = DagManagerUtils.generateDagId(dag_0);
    DagManager.DagId dagId1 = DagManagerUtils.generateDagId(dag_1);
    this.dagStateStore.writeCheckpoint(dag_0);
    this.dagStateStore.writeCheckpoint(dag_1);

    // Verify get one dag
    Dag<JobExecutionPlan> dag0 = this.dagStateStore.getDag(dagId0);
    Dag<JobExecutionPlan> dag1 = this.dagStateStore.getDag(dagId1);
    Assert.assertTrue(MySqlDagManagementStateStoreTest.compareLists(dag0.getNodes(), dag_0.getNodes()));
    Assert.assertTrue(MySqlDagManagementStateStoreTest.compareLists(dag1.getNodes(), dag_1.getNodes()));

    // Verify dag contents
    Dag<JobExecutionPlan> dagDeserialized = dag0;
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

    dagDeserialized = dag1;
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

    dagStateStore.cleanUp(dagId0);
    dagStateStore.cleanUp(dagId1);

    Assert.assertNull(this.dagStateStore.getDag(dagId0));
    Assert.assertNull(this.dagStateStore.getDag(dagId1));
  }
}