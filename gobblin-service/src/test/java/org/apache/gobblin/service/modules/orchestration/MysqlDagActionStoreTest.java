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
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.typesafe.config.Config;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;

public class MysqlDagActionStoreTest {
  private static final String USER = "testUser";
  private static final String PASSWORD = "testPassword";
  private static final String TABLE = "dag_action_store";
  private static final String flowGroup = "testFlowGroup";
  private static final String flowName = "testFlowName";
  private static final String jobName = "testJobName";
  private static final String jobName_2 = "testJobName2";
  private static final String flowExecutionId = "12345677";
  private static final String flowExecutionId_2 = "12345678";
  private static final String flowExecutionId_3 = "12345679";
  private MysqlDagActionStore mysqlDagActionStore;

  @BeforeClass
  public void setUp() throws Exception {
    ITestMetastoreDatabase testDb = TestMetastoreDatabaseFactory.get();

    Config config = ConfigBuilder.create()
        .addPrimitive("MysqlDagActionStore." + ConfigurationKeys.STATE_STORE_DB_URL_KEY, testDb.getJdbcUrl())
        .addPrimitive("MysqlDagActionStore." + ConfigurationKeys.STATE_STORE_DB_USER_KEY, USER)
        .addPrimitive("MysqlDagActionStore." + ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, PASSWORD)
        .addPrimitive("MysqlDagActionStore." + ConfigurationKeys.STATE_STORE_DB_TABLE_KEY, TABLE)
        .build();

    this.mysqlDagActionStore = new MysqlDagActionStore(config);
  }

  @Test
  public void testAddAction() throws Exception {
    this.mysqlDagActionStore.addDagAction(flowGroup, flowName, flowExecutionId, jobName, DagActionStore.DagActionType.KILL);
    //Should not be able to add KILL again when previous one exist
    Assert.expectThrows(IOException.class,
        () -> this.mysqlDagActionStore.addDagAction(flowGroup, flowName, flowExecutionId, jobName, DagActionStore.DagActionType.KILL));
    //Should be able to add a RESUME action for same execution as well as KILL for another execution of the flow
    this.mysqlDagActionStore.addDagAction(flowGroup, flowName, flowExecutionId, jobName, DagActionStore.DagActionType.RESUME);
    this.mysqlDagActionStore.addDagAction(flowGroup, flowName, flowExecutionId_2, jobName, DagActionStore.DagActionType.KILL);
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
  public void testDeleteAction() throws IOException, SQLException {
   this.mysqlDagActionStore.deleteDagAction(
       new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, jobName, DagActionStore.DagActionType.KILL));
   Assert.assertEquals(this.mysqlDagActionStore.getDagActions().size(), 2);
   Assert.assertFalse(this.mysqlDagActionStore.exists(flowGroup, flowName, flowExecutionId, jobName, DagActionStore.DagActionType.KILL));
    Assert.assertTrue(this.mysqlDagActionStore.exists(flowGroup, flowName, flowExecutionId, jobName, DagActionStore.DagActionType.RESUME));
   Assert.assertTrue(this.mysqlDagActionStore.exists(flowGroup, flowName, flowExecutionId_2, jobName, DagActionStore.DagActionType.KILL));
  }

}