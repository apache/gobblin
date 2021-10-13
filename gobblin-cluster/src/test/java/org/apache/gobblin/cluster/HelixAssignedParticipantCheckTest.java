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

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.cluster.suite.IntegrationBasicSuite;
import org.apache.gobblin.commit.CommitStepException;
import org.apache.gobblin.testing.AssertWithBackoff;


public class HelixAssignedParticipantCheckTest {
  private static final String JOB_ID = "job_testJob_345";
  private static final String TASK_STATE_FILE = "/tmp/" + HelixAssignedParticipantCheckTest.class.getSimpleName() + "/taskState/_RUNNING";

  private IntegrationBasicSuite suite;
  private HelixManager helixManager;
  private Config helixConfig;

  @BeforeClass
  public void setUp()
      throws Exception {
    Config jobConfigOverrides = ClusterIntegrationTestUtils.buildSleepingJob(JOB_ID, TASK_STATE_FILE);
    //Set up a Gobblin Helix cluster integration job
    suite = new IntegrationBasicSuite(jobConfigOverrides);

    helixConfig = suite.getManagerConfig();
  }

  @Test (groups = {"disabledOnCI"})
  //Test disabled on Travis because cluster integration tests are generally flaky on Travis.
  public void testExecute() throws Exception {
    suite.startCluster();
    String clusterName = helixConfig.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY);
    String zkConnectString = helixConfig.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    helixManager = HelixManagerFactory.getZKHelixManager(clusterName, "TestManager",
        InstanceType.SPECTATOR, zkConnectString);

    //Connect to the previously started Helix cluster
    helixManager.connect();

    //Ensure that Helix has created a workflow
    AssertWithBackoff.create().maxSleepMs(1000).backoffFactor(1).
        assertTrue(ClusterIntegrationTest.isTaskStarted(helixManager, JOB_ID), "Waiting for the job to start...");

    //Instantiate config for HelixAssignedParticipantCheck
    String helixJobId = Joiner.on("_").join(JOB_ID, JOB_ID);
    helixConfig = helixConfig.withValue(GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_KEY,
        ConfigValueFactory.fromAnyRef(IntegrationBasicSuite.WORKER_INSTANCE_0))
        .withValue(GobblinClusterConfigurationKeys.HELIX_JOB_ID_KEY, ConfigValueFactory.fromAnyRef(helixJobId))
        .withValue(GobblinClusterConfigurationKeys.HELIX_PARTITION_ID_KEY, ConfigValueFactory.fromAnyRef(0));
    HelixAssignedParticipantCheck check = new HelixAssignedParticipantCheck(helixConfig);

    //Ensure that the SleepingTask is running
    AssertWithBackoff.create().maxSleepMs(100).timeoutMs(2000).backoffFactor(1).
        assertTrue(ClusterIntegrationTest.isTaskRunning(TASK_STATE_FILE),"Waiting for the task to enter running state");

    //Run the check. Ensure that the configured Helix instance is indeed the assigned participant
    // (i.e. no exceptions thrown).
    check.execute();

    //Disconnect the helixmanager used to check the assigned participant to force an Exception on the first attempt.
    //The test should succeed on the following attempt.
    HelixManager helixManagerOriginal = HelixAssignedParticipantCheck.getHelixManager();
    helixManagerOriginal.disconnect();
    check.execute();
    //Ensure that a new HelixManager instance is created.
    Assert.assertTrue(HelixAssignedParticipantCheck.getHelixManager() != helixManagerOriginal);

    //Create Helix config with invalid partition num. Ensure HelixAssignedParticipantCheck fails.
    helixConfig = helixConfig.withValue(GobblinClusterConfigurationKeys.HELIX_PARTITION_ID_KEY, ConfigValueFactory.fromAnyRef(1));
    check = new HelixAssignedParticipantCheck(helixConfig);

    try {
      check.execute();
      Assert.fail("Expected to throw CommitStepException");
    } catch (CommitStepException e) {
      //Expected to throw CommitStepException
      Assert.assertTrue(e.getClass().equals(CommitStepException.class));
    }
  }

  public void tearDown() throws IOException, InterruptedException {
    //Shutdown cluster
    suite.shutdownCluster();
    if (helixManager.isConnected()) {
      helixManager.disconnect();
    }
  }
}