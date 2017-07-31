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

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.fs.Path;
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

  private TestingServer testingZKServer;

  private GobblinTaskRunner gobblinTaskRunner;

  private GobblinClusterManager gobblinClusterManager;

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
        .resolve();

    String zkConnectionString = config.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    HelixUtils.createGobblinHelixCluster(zkConnectionString,
        config.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY));

    // Participant
    this.gobblinTaskRunner =
        new GobblinTaskRunner(TestHelper.TEST_APPLICATION_NAME, TestHelper.TEST_HELIX_INSTANCE_NAME,
            TestHelper.TEST_APPLICATION_ID, TestHelper.TEST_TASK_RUNNER_ID, config, Optional.<Path>absent());
    this.gobblinTaskRunner.connectHelixManager();

    // Controller
    this.gobblinClusterManager =
        new GobblinClusterManager(TestHelper.TEST_APPLICATION_NAME, TestHelper.TEST_APPLICATION_ID, config,
            Optional.<Path>absent());
    this.gobblinClusterManager.connectHelixManager();

  }

  @Test
  public void testSendReceiveShutdownMessage() throws Exception {
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

  @AfterClass
  public void tearDown() throws IOException {
    try {
      this.gobblinClusterManager.disconnectHelixManager();
      this.gobblinTaskRunner.disconnectHelixManager();
    } finally {
      this.testingZKServer.close();
    }
  }
}
