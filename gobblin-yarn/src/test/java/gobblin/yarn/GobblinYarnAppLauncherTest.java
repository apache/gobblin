/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.yarn;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.TimeoutException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.Message;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.io.Closer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

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
@Test(groups = { "gobblin.yarn" })
public class GobblinYarnAppLauncherTest implements HelixMessageTestBase {

  private static final int TEST_ZK_PORT = 3181;

  private YarnClient yarnClient;

  private CuratorFramework curatorFramework;

  private Config config;

  private HelixManager helixManager;

  private GobblinYarnAppLauncher gobblinYarnAppLauncher;
  private ApplicationId applicationId;

  private final Closer closer = Closer.create();

  @BeforeClass
  public void setUp() throws Exception {
    YarnConfiguration clusterConf = new YarnConfiguration();

    MiniYARNCluster miniYARNCluster = this.closer.register(new MiniYARNCluster("TestCluster", 1, 1, 1));
    miniYARNCluster.init(clusterConf);
    miniYARNCluster.start();

    this.yarnClient = this.closer.register(YarnClient.createYarnClient());
    this.yarnClient.init(clusterConf);
    this.yarnClient.start();

    TestingServer testingZKServer = this.closer.register(new TestingServer(TEST_ZK_PORT));

    this.curatorFramework = TestHelper.createZkClient(testingZKServer, this.closer);

    URL url = GobblinYarnAppLauncherTest.class.getClassLoader()
        .getResource(GobblinYarnAppLauncherTest.class.getSimpleName() + ".conf");
    Assert.assertNotNull(url, "Could not find resource " + url);

    this.config = ConfigFactory.parseURL(url).resolve();

    String zkConnectionString = this.config.getString(GobblinYarnConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    this.helixManager = HelixManagerFactory.getZKHelixManager(
        this.config.getString(GobblinYarnConfigurationKeys.HELIX_CLUSTER_NAME_KEY), TestHelper.TEST_HELIX_INSTANCE_NAME,
        InstanceType.CONTROLLER, zkConnectionString);

    this.gobblinYarnAppLauncher = new GobblinYarnAppLauncher(this.config, clusterConf);
  }

  @Test
  public void testCreateHelixCluster() throws Exception {
    // This is tested here instead of in YarnHelixUtilsTest to avoid setting up yet another testing ZooKeeper server.
    YarnHelixUtils.createGobblinYarnHelixCluster(
        this.config.getString(GobblinYarnConfigurationKeys.ZK_CONNECTION_STRING_KEY),
        this.config.getString(GobblinYarnConfigurationKeys.HELIX_CLUSTER_NAME_KEY));

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
  }

  @Test(groups = { "disabledOnTravis" }, dependsOnMethods = "testSetupAndSubmitApplication")
  public void testGetReconnectableApplicationId() throws Exception {
    Assert.assertEquals(this.gobblinYarnAppLauncher.getReconnectableApplicationId().get(), this.applicationId);
    this.yarnClient.killApplication(this.applicationId);
  }

  @Test(dependsOnMethods = "testCreateHelixCluster")
  public void testSendShutdownRequest() throws Exception {
    this.helixManager.connect();
    this.helixManager.getMessagingService().registerMessageHandlerFactory(Message.MessageType.SHUTDOWN.toString(),
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
    } finally {
      this.closer.close();
    }
  }

  @Test(enabled = false)
  @Override
  public void assertMessageReception(Message message) {
    Assert.assertEquals(message.getMsgType(), Message.MessageType.SHUTDOWN.toString());
    Assert.assertEquals(message.getMsgSubType(), HelixMessageSubTypes.APPLICATION_MASTER_SHUTDOWN.toString());
  }
}
