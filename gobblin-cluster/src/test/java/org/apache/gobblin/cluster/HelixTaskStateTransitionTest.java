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

package org.apache.gobblin.cluster;

import java.io.IOException;

import org.apache.gobblin.testing.AssertWithBackoff;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.typesafe.config.Config;


/**
 * This test examines cases in task-state transition when task failed due to external(e.g. ZK related),
 * or internal failure (Gobblin Task failure due to mis-configuration, for example).
 *
 * For example, we expect when a Helix Task failed internally in a participants, it should get reassigned to another instances(or the same) to be executed
 * and we could expect the task to be finished gracefully.
 */
public class HelixTaskStateTransitionTest {

  private HelixAssignedParticipantCheckTest.IntegrationJobSuite suite;
  private HelixManager helixManager;
  private Config helixConfig;

  @BeforeClass
  public void setUp()
      throws Exception {
    //Set up a Gobblin Helix cluster with more than 1 workers available to accept Helix participants.
    suite = new HelixAssignedParticipantCheckTest.IntegrationJobSuite(2);

    helixConfig = suite.getManagerConfig();
    String clusterName = helixConfig.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY);
    String zkConnectString = helixConfig.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    helixManager =
        HelixManagerFactory.getZKHelixManager(clusterName, "TestManager", InstanceType.ADMINISTRATOR, zkConnectString);
  }

  @AfterClass
  public void tearDown()
      throws IOException, InterruptedException {
    //Shutdown cluster
    suite.shutdownCluster();
    if (helixManager.isConnected()) {
      helixManager.disconnect();
    }
  }

  @Test(groups = {"disabledOnTravis"})
  //Test disabled on Travis because cluster integration tests are generally flaky on Travis.
  public void testZKDisconnect()
      throws Exception {
    suite.startCluster();

    //Connect to the previously started Helix cluster
    helixManager.connect();

    //Ensure that Helix has created a workflow
    AssertWithBackoff.create().maxSleepMs(1000).backoffFactor(1).
        assertTrue(ClusterIntegrationTest
                .isWorkflowStarted(helixManager, HelixAssignedParticipantCheckTest.IntegrationJobSuite.JOB_ID),
            "Waiting for the job to start...");

    //Ensure that all the GobblinTaskRunner is spun up.
    for (int i = 0; i < suite.getWorkerConfigs(suite.getNumOfWorkers()).size(); i++) {
      AssertWithBackoff.create().maxSleepMs(100).timeoutMs(2000).backoffFactor(1).
          assertTrue(ClusterIntegrationTest
                  .isTaskRunning(HelixAssignedParticipantCheckTest.IntegrationJobSuite.TASK_STATE_FILE + "_" + i),
              "Waiting for the task to enter running state");
    }
  }
}
