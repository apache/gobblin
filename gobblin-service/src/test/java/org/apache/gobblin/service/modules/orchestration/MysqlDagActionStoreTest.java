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
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashSet;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.typesafe.config.Config;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.service.modules.orchestration.task.DagProcessingEngineMetrics;

public class MysqlDagActionStoreTest {
  private static final String USER = "testUser";
  private static final String PASSWORD = "testPassword";
  private static final String TABLE = "dag_action_store";
  private static final String flowGroup = "testFlowGroup";
  private static final String flowName = "testFlowName";
  private static final String jobName = "testJobName";
  private static final String jobName_2 = "testJobName2";
  private static final long flowExecutionId = 12345677L;
  private static final long flowExecutionId_2 = 12345678L;
  private static final long flowExecutionId_3 = 12345679L;
  private ITestMetastoreDatabase testDb;
  private DagActionStore mysqlDagActionStore;

  @BeforeClass
  public void setUp() throws Exception {
    this.testDb = TestMetastoreDatabaseFactory.get();
    this.mysqlDagActionStore = getTestDagActionStore(this.testDb);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() throws IOException {
    // `.close()` to avoid (in the aggregate, across multiple suites) - java.sql.SQLNonTransientConnectionException: Too many connections
    this.testDb.close();
  }

  public static DagActionStore getTestDagActionStore(ITestMetastoreDatabase testDb) throws Exception {
    return new MysqlDagActionStore(getDagActionStoreTestConfigs(testDb), Mockito.mock(DagProcessingEngineMetrics.class));
  }

  public static Config getDagActionStoreTestConfigs(ITestMetastoreDatabase testDb) throws URISyntaxException {
    return ConfigBuilder.create()
        .addPrimitive(MysqlDagActionStore.CONFIG_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_URL_KEY, testDb.getJdbcUrl())
        .addPrimitive(MysqlDagActionStore.CONFIG_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_USER_KEY, USER)
        .addPrimitive(MysqlDagActionStore.CONFIG_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, PASSWORD)
        .addPrimitive(MysqlDagActionStore.CONFIG_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_TABLE_KEY, TABLE)
        .build();
  }

  @Test
  public void testAddAction() throws Exception {
    this.mysqlDagActionStore.addJobDagAction(flowGroup, flowName, flowExecutionId, jobName, DagActionStore.DagActionType.KILL);
    //Should not be able to add KILL again when previous one exist
    Assert.expectThrows(IOException.class,
        () -> this.mysqlDagActionStore.addJobDagAction(flowGroup, flowName, flowExecutionId, jobName, DagActionStore.DagActionType.KILL));
    //Should be able to add a RESUME action for same execution as well as KILL for another execution of the flow
    this.mysqlDagActionStore.addJobDagAction(flowGroup, flowName, flowExecutionId, jobName, DagActionStore.DagActionType.RESUME);
    this.mysqlDagActionStore.addJobDagAction(flowGroup, flowName, flowExecutionId_2, jobName, DagActionStore.DagActionType.KILL);
  }

  @Test(dependsOnMethods = "testAddAction")
  public void testExists() throws Exception {
    Assert.assertTrue(this.mysqlDagActionStore.exists(flowGroup, flowName, flowExecutionId, jobName, DagActionStore.DagActionType.KILL));
    Assert.assertTrue(this.mysqlDagActionStore.exists(flowGroup, flowName, flowExecutionId, jobName, DagActionStore.DagActionType.RESUME));
    Assert.assertTrue(this.mysqlDagActionStore.exists(flowGroup, flowName, flowExecutionId_2, jobName, DagActionStore.DagActionType.KILL));
    Assert.assertFalse(this.mysqlDagActionStore.exists(flowGroup, flowName, flowExecutionId_2, jobName_2, DagActionStore.DagActionType.KILL));
    Assert.assertFalse(this.mysqlDagActionStore.exists(flowGroup, flowName, flowExecutionId_3, jobName, DagActionStore.DagActionType.RESUME));
    Assert.assertFalse(this.mysqlDagActionStore.exists(flowGroup, flowName, flowExecutionId_3, jobName, DagActionStore.DagActionType.KILL));
  }

  @Test(dependsOnMethods = "testExists")
  public void testGetActions() throws IOException {
    Collection<DagActionStore.DagAction> dagActions = this.mysqlDagActionStore.getDagActions();
    Assert.assertEquals(3, dagActions.size());
    HashSet<DagActionStore.DagAction> set = new HashSet<>();
    set.add(new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, jobName, DagActionStore.DagActionType.KILL));
    set.add(new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, jobName, DagActionStore.DagActionType.RESUME));
    set.add(new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId_2, jobName, DagActionStore.DagActionType.KILL));
    Assert.assertEquals(dagActions, set);
  }

  @Test(dependsOnMethods = "testGetActions")
  public void testDeleteAction() throws IOException {
     this.mysqlDagActionStore.deleteDagAction(
         new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, jobName, DagActionStore.DagActionType.KILL));
     Assert.assertEquals(this.mysqlDagActionStore.getDagActions().size(), 2);
     Assert.assertFalse(this.mysqlDagActionStore.exists(flowGroup, flowName, flowExecutionId, jobName, DagActionStore.DagActionType.KILL));
     Assert.assertTrue(this.mysqlDagActionStore.exists(flowGroup, flowName, flowExecutionId, jobName, DagActionStore.DagActionType.RESUME));
     Assert.assertTrue(this.mysqlDagActionStore.exists(flowGroup, flowName, flowExecutionId_2, jobName, DagActionStore.DagActionType.KILL));
  }
}