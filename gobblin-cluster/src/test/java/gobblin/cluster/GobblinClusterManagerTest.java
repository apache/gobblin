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

package gobblin.cluster;

import java.net.URL;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.fs.Path;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.io.Closer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import gobblin.cluster.event.ClusterManagerShutdownRequest;
import gobblin.testing.AssertWithBackoff;


/**
 * Unit tests for {@link GobblinClusterManager}.
 *
 * <p>
 *   This class uses a {@link TestingServer} as an embedded ZooKeeper server for testing. The Curator
 *   framework is used to provide a ZooKeeper client. This class also uses the {@link HelixManager} to
 *   act as a testing Helix participant to receive the container (running the {@link GobblinTaskRunner})
 *   shutdown request message.
 * </p>
 *
 * @author Yinan Li
 */
@Test(groups = { "gobblin.cluster" })
public class GobblinClusterManagerTest implements HelixMessageTestBase {
  public final static Logger LOG = LoggerFactory.getLogger(GobblinClusterManagerTest.class);

  private TestingServer testingZKServer;

  private HelixManager helixManager;

  private GobblinClusterManager gobblinClusterManager;

  @BeforeClass
  public void setUp() throws Exception {
    // Use a random ZK port
    this.testingZKServer = new TestingServer(-1);
    LOG.info("Testing ZK Server listening on: " + testingZKServer.getConnectString());

    URL url = GobblinClusterManagerTest.class.getClassLoader().getResource(
        GobblinClusterManager.class.getSimpleName() + ".conf");
    Assert.assertNotNull(url, "Could not find resource " + url);

    Config config = ConfigFactory.parseURL(url)
        .withValue("gobblin.cluster.zk.connection.string",
                   ConfigValueFactory.fromAnyRef(testingZKServer.getConnectString()))
        .resolve();

    String zkConnectionString = config.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    HelixUtils.createGobblinHelixCluster(zkConnectionString,
        config.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY));

    this.helixManager = HelixManagerFactory
        .getZKHelixManager(config.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY),
            TestHelper.TEST_HELIX_INSTANCE_NAME, InstanceType.PARTICIPANT, zkConnectionString);
    this.helixManager.connect();
    this.helixManager.getMessagingService().registerMessageHandlerFactory(GobblinHelixConstants.SHUTDOWN_MESSAGE_TYPE,
        new TestShutdownMessageHandlerFactory(this));

    this.gobblinClusterManager =
        new GobblinClusterManager(TestHelper.TEST_APPLICATION_NAME, TestHelper.TEST_APPLICATION_ID, config,
            Optional.<Path>absent());
    this.gobblinClusterManager.getEventBus().register(this.gobblinClusterManager);
    this.gobblinClusterManager.connectHelixManager();
  }

  static class GetInstanceMessageNumFunc implements Function<Void, Integer> {

    private final CuratorFramework curatorFramework;
    private final String testName;

    public GetInstanceMessageNumFunc(String testName, CuratorFramework curatorFramework) {
      this.curatorFramework = curatorFramework;
      this.testName = testName;
    }

    @Override
    public Integer apply(Void input) {
      try {
        return this.curatorFramework.getChildren().forPath(String
            .format("/%s/INSTANCES/%s/MESSAGES", this.testName,
                TestHelper.TEST_HELIX_INSTANCE_NAME)).size();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void testSendShutdownRequest() throws Exception {
    Logger log = LoggerFactory.getLogger("testSendShutdownRequest");
    Closer closer = Closer.create();
    try {
      CuratorFramework curatorFramework = TestHelper.createZkClient(this.testingZKServer, closer);
      final GetInstanceMessageNumFunc getMessageNumFunc =
          new GetInstanceMessageNumFunc(GobblinClusterManagerTest.class.getSimpleName(),
              curatorFramework);
      AssertWithBackoff assertWithBackoff =
          AssertWithBackoff.create().logger(log).timeoutMs(30000);

      this.gobblinClusterManager.sendShutdownRequest();

      Assert.assertEquals(curatorFramework.checkExists().forPath(String
          .format("/%s/INSTANCES/%s/MESSAGES", GobblinClusterManagerTest.class.getSimpleName(),
              TestHelper.TEST_HELIX_INSTANCE_NAME)).getVersion(), 0);

      assertWithBackoff.assertEquals(getMessageNumFunc, 1, "1 message queued");

      // Give Helix sometime to handle the message
      assertWithBackoff.assertEquals(getMessageNumFunc, 0, "all messages processed");
    } finally {
      closer.close();
    }
  }

  @Test(dependsOnMethods = "testSendShutdownRequest")
  public void testHandleClusterManagerShutdownRequest() throws Exception {
    Logger log = LoggerFactory.getLogger("testHandleClusterManagerShutdownRequest");
    this.gobblinClusterManager.getEventBus().post(new ClusterManagerShutdownRequest());
    AssertWithBackoff.create().logger(log).timeoutMs(20000)
        .assertTrue(new Predicate<Void>() {
          @Override public boolean apply(Void input) {
            return !GobblinClusterManagerTest.this.gobblinClusterManager.isHelixManagerConnected();
          }
        }, "Cluster Manager shutdown");
  }

  @AfterClass
  public void tearDown() throws Exception {
    try {
      if (this.helixManager.isConnected()) {
        this.helixManager.disconnect();
      }
      this.gobblinClusterManager.disconnectHelixManager();
    } catch (Throwable t) {
      Assert.fail();
    } finally {
      this.testingZKServer.close();
    }
  }

  @Test(enabled = false)
  @Override
  public void assertMessageReception(Message message) {
    Assert.assertEquals(message.getMsgType(), GobblinHelixConstants.SHUTDOWN_MESSAGE_TYPE);
    Assert.assertEquals(message.getMsgSubType(), HelixMessageSubTypes.WORK_UNIT_RUNNER_SHUTDOWN.toString());
  }
}
