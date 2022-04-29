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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.io.IOException;
import java.util.List;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class UserQuotaManagerTest {

  UserQuotaManager _quotaManager;

  @BeforeClass
  public void setUp() {
    Config quotaConfig = ConfigFactory.empty()
        .withValue(UserQuotaManager.PER_USER_QUOTA, ConfigValueFactory.fromAnyRef("user:1,user2:1,user3:1"));
    this._quotaManager = new UserQuotaManager(quotaConfig);
  }

  // Tests that if exceeding the quota on startup, do not throw an exception and do not decrement the counter
  @Test
  public void testExceedsQuotaOnStartup() throws Exception {
    List<Dag<JobExecutionPlan>> dags = DagManagerTest.buildDagList(2, "user", ConfigFactory.empty());
    // Ensure that the current attempt is 1, normally done by DagManager
    dags.get(0).getNodes().get(0).getValue().setCurrentAttempts(1);
    dags.get(1).getNodes().get(0).getValue().setCurrentAttempts(1);

    this._quotaManager.checkQuota(dags.get(0).getNodes().get(0), true);
    // Should not be throwing the exception
    this._quotaManager.checkQuota(dags.get(1).getNodes().get(0), true);

    // TODO: add verification when adding a public method for getting the current count and quota per user
  }

  @Test
  public void testExceedsQuotaThrowsException() throws Exception {
    List<Dag<JobExecutionPlan>> dags = DagManagerTest.buildDagList(2, "user2", ConfigFactory.empty());

    // Ensure that the current attempt is 1, normally done by DagManager
    dags.get(0).getNodes().get(0).getValue().setCurrentAttempts(1);
    dags.get(1).getNodes().get(0).getValue().setCurrentAttempts(1);

    this._quotaManager.checkQuota(dags.get(0).getNodes().get(0), false);
    Assert.assertThrows(IOException.class, () -> {
      this._quotaManager.checkQuota(dags.get(1).getNodes().get(0), false);
    });
  }

  @Test
  public void testMultipleRemoveQuotasNegative() throws Exception {
    List<Dag<JobExecutionPlan>> dags = DagManagerTest.buildDagList(2, "user3", ConfigFactory.empty());

    // Ensure that the current attempt is 1, normally done by DagManager
    dags.get(0).getNodes().get(0).getValue().setCurrentAttempts(1);
    dags.get(1).getNodes().get(0).getValue().setCurrentAttempts(1);

    this._quotaManager.checkQuota(dags.get(0).getNodes().get(0), false);
    Assert.assertTrue(this._quotaManager.releaseQuota(dags.get(0).getNodes().get(0)));
    Assert.assertFalse(this._quotaManager.releaseQuota(dags.get(0).getNodes().get(0)));
  }
}
