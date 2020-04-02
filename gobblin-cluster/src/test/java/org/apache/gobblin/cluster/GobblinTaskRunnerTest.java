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
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.manager.zk.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.cluster.suite.IntegrationBasicSuite;
import org.apache.gobblin.testing.AssertWithBackoff;


/**
 * Unit tests for {@link GobblinTaskRunner}.
 *
 * <p>
 *   This class uses a {@link TestingServer} as an embedded ZooKeeper server for testing. A
 *   {@link GobblinClusterManager} instance is used to send the test shutdown request message.
 * </p>
 *
 * @author Yinan Li
 */
@Test(groups = { "gobblin.cluster" })
public class GobblinTaskRunnerTest {
  public final static Logger LOG = LoggerFactory.getLogger(GobblinTaskRunnerTest.class);

  private static final String JOB_ID = "job_taskRunnerTestJob_" + System.currentTimeMillis();
  private static final String TASK_STATE_FILE = "/tmp/" + GobblinTaskRunnerTest.class.getSimpleName() + "/taskState/_RUNNING";

  public static final String HADOOP_OVERRIDE_PROPERTY_NAME = "prop";

  private TestingServer testingZKServer;

  private GobblinTaskRunner gobblinTaskRunner;

  private GobblinClusterManager gobblinClusterManager;
  private GobblinTaskRunner corruptGobblinTaskRunner;
  private String clusterName;
  private String corruptHelixInstance;
  private TaskAssignmentAfterConnectionRetry suite;

  @BeforeClass
  public void setUp() throws Exception {
    this.testingZKServer = new TestingServer(-1);
    LOG.info("Testing ZK Server listening on: " + testingZKServer.getConnectString());

    URL url = GobblinTaskRunnerTest.class.getClassLoader().getResource(
        GobblinTaskRunnerTest.class.getSimpleName() + ".conf");
    Assert.assertNotNull(url, "Could not find resource " + url);

    Config config = ConfigFactory.parseURL(url)
        .withValue("gobblin.cluster.zk.connection.string",
                   ConfigValueFactory.fromAnyRef(testingZKServer.getConnectString()))
        .withValue(GobblinClusterConfigurationKeys.HADOOP_CONFIG_OVERRIDES_PREFIX + "." + HADOOP_OVERRIDE_PROPERTY_NAME,
            ConfigValueFactory.fromAnyRef("value"))
        .withValue(GobblinClusterConfigurationKeys.HADOOP_CONFIG_OVERRIDES_PREFIX + "." + "fs.file.impl.disable.cache",
            ConfigValueFactory.fromAnyRef("true"))
        .resolve();

    String zkConnectionString = config.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    this.clusterName = config.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY);
    HelixUtils.createGobblinHelixCluster(zkConnectionString, this.clusterName);

    // Participant
    this.gobblinTaskRunner =
        new GobblinTaskRunner(TestHelper.TEST_APPLICATION_NAME, TestHelper.TEST_HELIX_INSTANCE_NAME,
            TestHelper.TEST_APPLICATION_ID, TestHelper.TEST_TASK_RUNNER_ID, config, Optional.<Path>absent());
    this.gobblinTaskRunner.connectHelixManager();

    // Participant with a partial Instance set up on Helix/ZK
    this.corruptHelixInstance = HelixUtils.getHelixInstanceName("CorruptHelixInstance", 0);
    this.corruptGobblinTaskRunner =
        new GobblinTaskRunner(TestHelper.TEST_APPLICATION_NAME, corruptHelixInstance,
            TestHelper.TEST_APPLICATION_ID, TestHelper.TEST_TASK_RUNNER_ID, config, Optional.<Path>absent());

    // Controller
    this.gobblinClusterManager =
        new GobblinClusterManager(TestHelper.TEST_APPLICATION_NAME, TestHelper.TEST_APPLICATION_ID, config,
            Optional.<Path>absent());
    this.gobblinClusterManager.connectHelixManager();
  }

  @Test
  public void testSendReceiveShutdownMessage() throws Exception {
    ExecutorService service = Executors.newSingleThreadExecutor();
    service.submit(() -> GobblinTaskRunnerTest.this.gobblinTaskRunner.start());

    Logger log = LoggerFactory.getLogger("testSendReceiveShutdownMessage");
    this.gobblinClusterManager.sendShutdownRequest();

    // Give Helix some time to handle the message
    AssertWithBackoff.create().logger(log).timeoutMs(20000)
      .assertTrue(new Predicate<Void>() {
        @Override public boolean apply(Void input) {
          return GobblinTaskRunnerTest.this.gobblinTaskRunner.isStopped();
        }
      }, "gobblinTaskRunner stopped");
  }

  @Test
  public void testBuildFileSystemConfig() {
    FileSystem fileSystem = this.gobblinTaskRunner.getFs();
    Assert.assertEquals(fileSystem.getConf().get(HADOOP_OVERRIDE_PROPERTY_NAME), "value");
  }

  /**
   * A helper method that creates a partial instance structure in ZK.
   */
  private static void createPartialInstanceStructure(HelixManager helixManager, String zkConnectString) {
    //Connect and disconnect the helixManager to create a Helix Instance set up.
    try {
      helixManager.connect();
      helixManager.disconnect();
    } catch (Exception e) {
      Assert.fail("Failed to connect to ZK");
    }

    //Delete ERRORS/HISTORY/STATUSUPDATES znodes under INSTANCES to simulate partial instance set up.
    ZkClient zkClient = new ZkClient(zkConnectString);
    zkClient.delete(PropertyPathBuilder.instanceError(helixManager.getClusterName(), helixManager.getInstanceName()));
    zkClient.delete(PropertyPathBuilder.instanceHistory(helixManager.getClusterName(), helixManager.getInstanceName()));
    zkClient.delete(PropertyPathBuilder.instanceStatusUpdate(helixManager.getClusterName(), helixManager.getInstanceName()));
  }

  @Test
  public void testConnectHelixManagerWithRetry() {
    HelixManager instanceManager = HelixManagerFactory.getZKHelixManager(
        clusterName, corruptHelixInstance, InstanceType.PARTICIPANT, testingZKServer.getConnectString());

    createPartialInstanceStructure(instanceManager, testingZKServer.getConnectString());

    //Ensure that the connecting to Helix without retry will throw a HelixException
    try {
      corruptGobblinTaskRunner.connectHelixManager();
      Assert.fail("Unexpected success in connecting to HelixManager");
    } catch (Exception e) {
      //Assert that a HelixException is thrown.
      Assert.assertTrue(e.getClass().equals(HelixException.class));
    }

    //Ensure that connect with retry succeeds
    corruptGobblinTaskRunner.connectHelixManagerWithRetry();
    Assert.assertTrue(true);
  }

  @Test (groups = {"disabledOnTravis"})
  public void testTaskAssignmentAfterHelixConnectionRetry()
      throws Exception {
    Config jobConfigOverrides = ClusterIntegrationTestUtils.buildSleepingJob(JOB_ID, TASK_STATE_FILE);
    this.suite = new TaskAssignmentAfterConnectionRetry(jobConfigOverrides);

    String zkConnectString = suite.getManagerConfig().getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    String clusterName = suite.getManagerConfig().getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY);
    //A test manager instance for observing the state of the cluster
    HelixManager helixManager = HelixManagerFactory.getZKHelixManager(clusterName, "TestManager", InstanceType.SPECTATOR, zkConnectString);

    suite.startCluster();

    helixManager.connect();

    //Ensure that Helix has created a workflow
    AssertWithBackoff.create().maxSleepMs(1000).backoffFactor(1).
        assertTrue(ClusterIntegrationTest.isTaskStarted(helixManager, JOB_ID), "Waiting for the job to start...");

    //Ensure that the SleepingTask is running
    AssertWithBackoff.create().maxSleepMs(100).timeoutMs(2000).backoffFactor(1).
        assertTrue(ClusterIntegrationTest.isTaskRunning(TASK_STATE_FILE),"Waiting for the task to enter running state");

    helixManager.disconnect();
  }


  public static class TaskAssignmentAfterConnectionRetry extends IntegrationBasicSuite {
    TaskAssignmentAfterConnectionRetry(Config jobConfigOverrides) {
      super(jobConfigOverrides);
    }

    @Override
    protected void createHelixCluster() throws Exception {
      super.createHelixCluster();
      String clusterName = super.getManagerConfig().getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY);
      String zkConnectString = super.getManagerConfig().getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
      HelixManager helixManager = HelixManagerFactory
          .getZKHelixManager(clusterName, IntegrationBasicSuite.WORKER_INSTANCE_0, InstanceType.PARTICIPANT, zkConnectString);

      //Create a partial instance setup
      GobblinTaskRunnerTest.createPartialInstanceStructure(helixManager, zkConnectString);
    }
  }


  @AfterClass
  public void tearDown()
      throws IOException, InterruptedException {
    try {
      this.gobblinClusterManager.disconnectHelixManager();
      this.gobblinTaskRunner.disconnectHelixManager();
      this.corruptGobblinTaskRunner.disconnectHelixManager();
      if (this.suite != null) {
        this.suite.shutdownCluster();
      }
    } finally {
      this.testingZKServer.close();
    }
  }
}
