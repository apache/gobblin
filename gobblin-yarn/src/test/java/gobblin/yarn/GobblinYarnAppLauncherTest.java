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

package gobblin.yarn;

import com.google.common.base.Predicate;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
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

import com.google.common.io.Closer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import gobblin.cluster.GobblinClusterConfigurationKeys;
import gobblin.cluster.GobblinHelixConstants;
import gobblin.cluster.HelixMessageTestBase;
import gobblin.cluster.HelixUtils;
import gobblin.cluster.TestHelper;
import gobblin.cluster.TestShutdownMessageHandlerFactory;
import gobblin.testing.AssertWithBackoff;


/**
 * Unit tests for {@link GobblinYarnAppLauncher}.
 *
 * <p>
 *   This class uses a {@link TestingServer} as an embedded ZooKeeper server for testing. The Curator
 *   framework is used to provide a ZooKeeper client. This class also uses the {@link HelixManager} to
 *   act as a testing Helix controller to receive the ApplicationMaster shutdown request message. It
 *   also starts a {@link MiniYARNCluster} so submission of a Gobblin Yarn application can be tested.
 *   A {@link YarnClient} is used to work with the {@link MiniYARNCluster}.
 * </p>
 *
 * @author Yinan Li
 */
@Test(groups = { "gobblin.yarn" }, singleThreaded=true)
public class GobblinYarnAppLauncherTest implements HelixMessageTestBase {
  final Logger LOG = LoggerFactory.getLogger(GobblinYarnAppLauncherTest.class);

  private YarnClient yarnClient;

  private CuratorFramework curatorFramework;

  private Config config;

  private HelixManager helixManager;

  private GobblinYarnAppLauncher gobblinYarnAppLauncher;
  private ApplicationId applicationId;

  private final Closer closer = Closer.create();

  private static void setEnv(String key, String value) {
    try {
      Map<String, String> env = System.getenv();
      Class<?> cl = env.getClass();
      Field field = cl.getDeclaredField("m");
      field.setAccessible(true);
      Map<String, String> writableEnv = (Map<String, String>) field.get(env);
      writableEnv.put(key, value);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to set environment variable", e);
    }
  }

  @BeforeClass
  public void setUp() throws Exception {
    // Set java home in environment since it isn't set on some systems
    String javaHome = System.getProperty("java.home");
    setEnv("JAVA_HOME", javaHome);

    final YarnConfiguration clusterConf = new YarnConfiguration();

    MiniYARNCluster miniYARNCluster = this.closer.register(new MiniYARNCluster("TestCluster", 1, 1, 1));
    miniYARNCluster.init(clusterConf);
    miniYARNCluster.start();

    // YARN client should not be started before the Resource Manager is up
    AssertWithBackoff.create().logger(LOG).timeoutMs(10000)
        .assertTrue(new Predicate<Void>() {
          @Override public boolean apply(Void input) {
            return !clusterConf.get(YarnConfiguration.RM_ADDRESS).contains(":0");
          }
        }, "Waiting for RM");

    this.yarnClient = this.closer.register(YarnClient.createYarnClient());
    this.yarnClient.init(clusterConf);
    this.yarnClient.start();

    // Use a random ZK port
    TestingServer testingZKServer = this.closer.register(new TestingServer(-1));
    LOG.info("Testing ZK Server listening on: " + testingZKServer.getConnectString());

    // the zk port is dynamically configured
    try (PrintWriter pw = new PrintWriter("dynamic.conf")) {
      File dir = new File("target/dummydir");

      // dummy directory specified in configuration
      dir.mkdir();

      pw.println("gobblin.cluster.zk.connection.string=\"" + testingZKServer.getConnectString() + "\"");
      pw.println("jobconf.fullyQualifiedPath=\"" + dir.getAbsolutePath() + "\"");
    }

    // YARN config is dynamic and needs to be passed to other processes
    try (OutputStream os = new FileOutputStream(new File("yarn-site.xml"))) {
      clusterConf.writeXml(os);
    }

    this.curatorFramework = TestHelper.createZkClient(testingZKServer, this.closer);

    URL url = GobblinYarnAppLauncherTest.class.getClassLoader()
        .getResource(GobblinYarnAppLauncherTest.class.getSimpleName() + ".conf");
    Assert.assertNotNull(url, "Could not find resource " + url);

    this.config = ConfigFactory.parseURL(url)
        .withValue("gobblin.cluster.zk.connection.string",
                   ConfigValueFactory.fromAnyRef(testingZKServer.getConnectString()))
        .resolve();

    String zkConnectionString = this.config.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    this.helixManager = HelixManagerFactory.getZKHelixManager(
        this.config.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY), TestHelper.TEST_HELIX_INSTANCE_NAME,
        InstanceType.CONTROLLER, zkConnectionString);

    this.gobblinYarnAppLauncher = new GobblinYarnAppLauncher(this.config, clusterConf);
  }

  @Test
  public void testCreateHelixCluster() throws Exception {
    // This is tested here instead of in HelixUtilsTest to avoid setting up yet another testing ZooKeeper server.
    HelixUtils.createGobblinHelixCluster(
        this.config.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY),
        this.config.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY));

    Assert.assertEquals(this.curatorFramework.checkExists()
        .forPath(String.format("/%s", GobblinYarnAppLauncherTest.class.getSimpleName())).getVersion(), 0);
    Assert.assertEquals(
        this.curatorFramework.checkExists()
            .forPath(String.format("/%s/CONTROLLER", GobblinYarnAppLauncherTest.class.getSimpleName())).getVersion(),
        0);
  }

  /**
   * For some yet unknown reason, hostname resolution for the ResourceManager in {@link MiniYARNCluster}
   * has some issue that causes the {@link YarnClient} not be able to connect and submit the Gobblin Yarn
   * application successfully. This works fine on local machine though. So disabling this and the test
   * below that depends on it on Travis-CI.
   */
  @Test(groups = { "disabledOnTravis" }, dependsOnMethods = "testCreateHelixCluster")
  public void testSetupAndSubmitApplication() throws Exception {
    this.gobblinYarnAppLauncher.startYarnClient();
    this.applicationId = this.gobblinYarnAppLauncher.setupAndSubmitApplication();

    int i;

    // wait for application to come up
    for (i = 0; i < 120; i++) {
      if (yarnClient.getApplicationReport(applicationId).getYarnApplicationState() ==
          YarnApplicationState.RUNNING) {
        break;
      }
      Thread.sleep(1000);
    }

    Assert.assertTrue(i < 120, "timed out waiting for RUNNING state");

    // wait another 10 seconds and check state again to make sure that the application stays up
    Thread.sleep(10000);

    Assert.assertEquals(yarnClient.getApplicationReport(applicationId).getYarnApplicationState(),
        YarnApplicationState.RUNNING, "Application may have aborted");
  }

  @Test(groups = { "disabledOnTravis" }, dependsOnMethods = "testSetupAndSubmitApplication")
  public void testGetReconnectableApplicationId() throws Exception {
    Assert.assertEquals(this.gobblinYarnAppLauncher.getReconnectableApplicationId().get(), this.applicationId);
    this.yarnClient.killApplication(this.applicationId);

    Assert.assertEquals(yarnClient.getApplicationReport(applicationId).getYarnApplicationState(),
        YarnApplicationState.KILLED, "Application not killed");

    // takes some time for kill to take effect and app master to go down
    Thread.sleep(5000);
  }

  @Test(dependsOnMethods = "testCreateHelixCluster")
  public void testSendShutdownRequest() throws Exception {
    this.helixManager.connect();
    this.helixManager.getMessagingService().registerMessageHandlerFactory(GobblinHelixConstants.SHUTDOWN_MESSAGE_TYPE,
        new TestShutdownMessageHandlerFactory(this));

    this.gobblinYarnAppLauncher.connectHelixManager();
    this.gobblinYarnAppLauncher.sendShutdownRequest();

    Assert.assertEquals(this.curatorFramework.checkExists()
        .forPath(String.format("/%s/CONTROLLER/MESSAGES", GobblinYarnAppLauncherTest.class.getSimpleName()))
        .getVersion(), 0);
    YarnSecurityManagerTest.GetControllerMessageNumFunc getCtrlMessageNum =
        new YarnSecurityManagerTest.GetControllerMessageNumFunc(GobblinYarnAppLauncherTest.class.getSimpleName(),
            this.curatorFramework);
    AssertWithBackoff assertWithBackoff =
        AssertWithBackoff.create().logger(LoggerFactory.getLogger("testSendShutdownRequest")).timeoutMs(20000);
    assertWithBackoff.assertEquals(getCtrlMessageNum, 1, "1 controller message queued");

    // Give Helix sometime to handle the message
    assertWithBackoff.assertEquals(getCtrlMessageNum, 0, "all controller messages processed");
  }

  @AfterClass
  public void tearDown() throws IOException, TimeoutException {
    try {
      this.gobblinYarnAppLauncher.stopYarnClient();

      if (this.helixManager.isConnected()) {
        this.helixManager.disconnect();
      }

      this.gobblinYarnAppLauncher.disconnectHelixManager();

      if (applicationId != null) {
        this.gobblinYarnAppLauncher.cleanUpAppWorkDirectory(applicationId);
      }
    } finally {
      this.closer.close();
    }
  }

  @Test(enabled = false)
  @Override
  public void assertMessageReception(Message message) {
    Assert.assertEquals(message.getMsgType(), GobblinHelixConstants.SHUTDOWN_MESSAGE_TYPE);
    Assert.assertEquals(message.getMsgSubType(), HelixMessageSubTypes.APPLICATION_MASTER_SHUTDOWN.toString());
  }
}
