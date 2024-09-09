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
import java.util.Collections;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;


public class InMemoryUserQuotaManagerTest {

  InMemoryUserQuotaManager _quotaManager;

  @BeforeClass
  public void setUp() {
    Config quotaConfig = ConfigFactory.empty()
        .withValue(AbstractUserQuotaManager.PER_USER_QUOTA, ConfigValueFactory.fromAnyRef("user:1,user2:1,user3:1,user6:1"))
        .withValue(AbstractUserQuotaManager.PER_FLOWGROUP_QUOTA, ConfigValueFactory.fromAnyRef("group1:1,group2:2"));
    this._quotaManager = new InMemoryUserQuotaManager(quotaConfig);
  }

  // Tests that if exceeding the quota on startup, do not throw an exception and do not decrement the counter
  @Test
  public void testExceedsQuotaOnStartup() throws Exception {
    List<Dag<JobExecutionPlan>> dags = DagTestUtils.buildDagList(2, "user", ConfigFactory.empty());
    // Ensure that the current attempt is 1, normally done by DagProcs
    dags.get(0).getNodes().get(0).getValue().setCurrentAttempts(1);
    dags.get(1).getNodes().get(0).getValue().setCurrentAttempts(1);

    // Should not be throwing the exception
    this._quotaManager.init(dags);
  }

  @Test
  public void testExceedsUserQuotaThrowsException() throws Exception {
    List<Dag<JobExecutionPlan>> dags = DagTestUtils.buildDagList(2, "user2", ConfigFactory.empty());

    // Ensure that the current attempt is 1, normally done by job is submitted by Launch and Reevaluate DagProcs
    dags.get(0).getNodes().get(0).getValue().setCurrentAttempts(1);
    dags.get(1).getNodes().get(0).getValue().setCurrentAttempts(1);

    this._quotaManager.checkQuota(Collections.singleton(dags.get(0).getNodes().get(0)));
    Assert.assertThrows(IOException.class, () -> {
      this._quotaManager.checkQuota(Collections.singleton(dags.get(1).getNodes().get(0)));
    });
  }

  @Test
  public void testMultipleRemoveQuotasIdempotent() throws Exception {
    // Test that multiple decrements cannot cause the number to decrease by more than 1
    List<Dag<JobExecutionPlan>> dags = DagTestUtils.buildDagList(2, "user3", ConfigFactory.empty());

    // Ensure that the current attempt is 1, normally done by DagProcs
    dags.get(0).getNodes().get(0).getValue().setCurrentAttempts(1);
    dags.get(1).getNodes().get(0).getValue().setCurrentAttempts(1);

    this._quotaManager.checkQuota(Collections.singleton(dags.get(0).getNodes().get(0)));
    Assert.assertTrue(this._quotaManager.releaseQuota(dags.get(0).getNodes().get(0)));
    Assert.assertFalse(this._quotaManager.releaseQuota(dags.get(0).getNodes().get(0)));
  }

  @Test
  public void testExceedsFlowGroupQuotaThrowsException() throws Exception {
    // Test flowgroup quotas
    List<Dag<JobExecutionPlan>> dags = DagTestUtils.buildDagList(2, "user4", ConfigFactory.empty().withValue(
        ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef("group1")));

    // Ensure that the current attempt is 1, normally done by DagProcs
    dags.get(0).getNodes().get(0).getValue().setCurrentAttempts(1);
    dags.get(1).getNodes().get(0).getValue().setCurrentAttempts(1);

    this._quotaManager.checkQuota(Collections.singleton(dags.get(0).getNodes().get(0)));
    Assert.assertThrows(IOException.class, () -> {
      this._quotaManager.checkQuota(Collections.singleton(dags.get(1).getNodes().get(0)));
    });
  }


  @Test
  public void testUserAndFlowGroupQuotaMultipleUsersAdd() throws Exception {
    // Test that user quota and group quotas can both be exceeded, and that decrementing one flow will change both quotas
    Dag<JobExecutionPlan> dag1 = DagTestUtils.buildDag("1", System.currentTimeMillis(), DagProcessingEngine.FailureOption.FINISH_ALL_POSSIBLE.name(),
        1, "user5", ConfigFactory.empty().withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef("group2")));
    Dag<JobExecutionPlan> dag2 = DagTestUtils.buildDag("2", System.currentTimeMillis(), DagProcessingEngine.FailureOption.FINISH_ALL_POSSIBLE.name(),
        1, "user6", ConfigFactory.empty().withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef("group2")));
    Dag<JobExecutionPlan> dag3 = DagTestUtils.buildDag("3", System.currentTimeMillis(), DagProcessingEngine.FailureOption.FINISH_ALL_POSSIBLE.name(),
        1, "user6", ConfigFactory.empty().withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef("group3")));
    Dag<JobExecutionPlan> dag4 = DagTestUtils.buildDag("4", System.currentTimeMillis(), DagProcessingEngine.FailureOption.FINISH_ALL_POSSIBLE.name(),
        1, "user5", ConfigFactory.empty().withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef("group2")));
    // Ensure that the current attempt is 1, normally done by DagProcs
    dag1.getNodes().get(0).getValue().setCurrentAttempts(1);
    dag2.getNodes().get(0).getValue().setCurrentAttempts(1);
    dag3.getNodes().get(0).getValue().setCurrentAttempts(1);
    dag4.getNodes().get(0).getValue().setCurrentAttempts(1);

    this._quotaManager.checkQuota(Collections.singleton(dag1.getNodes().get(0)));
    this._quotaManager.checkQuota(Collections.singleton(dag2.getNodes().get(0)));

    // Should fail due to user quota
    Assert.assertThrows(IOException.class, () -> {
      this._quotaManager.checkQuota(Collections.singleton(dag3.getNodes().get(0)));
    });
    // Should fail due to flowgroup quota
    Assert.assertThrows(IOException.class, () -> {
      this._quotaManager.checkQuota(Collections.singleton(dag4.getNodes().get(0)));
    });
    // should pass due to quota being released
    this._quotaManager.releaseQuota(dag2.getNodes().get(0));
    this._quotaManager.checkQuota(Collections.singleton(dag3.getNodes().get(0)));
    this._quotaManager.checkQuota(Collections.singleton(dag4.getNodes().get(0)));
  }
}
