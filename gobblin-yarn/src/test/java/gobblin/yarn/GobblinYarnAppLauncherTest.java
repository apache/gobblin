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
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.yarn.api.records.ApplicationId;
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

import com.google.common.base.Predicate;
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
  static final Logger LOG = LoggerFactory.getLogger(GobblinYarnAppLauncher.class);

  private static final int TEST_ZK_PORT = 3081;

  private YarnClient yarnClient;

  private CuratorFramework curatorFramework;

  private Config config;

  private HelixManager helixManager;

  private GobblinYarnAppLauncher gobblinYarnAppLauncher;
  private ApplicationId applicationId;
  private MiniYARNCluster miniYARNCluster;

  private final Closer closer = Closer.create();

  @BeforeClass
  public void setUp() throws Exception {
    LOG.info("Setting up");
    YarnConfiguration clusterConf = new YarnConfiguration();
    clusterConf.set("yarn.resourcemanager.hostname", "localhost");

    LOG.info("Setting up YARN cluster");
    this.miniYARNCluster = this.closer.register(new MiniYARNCluster("TestCluster", 1, 1, 1));
    this.miniYARNCluster.init(clusterConf);
    this.miniYARNCluster.start();
    AssertWithBackoff.assertTrue(new Predicate<Void>() {
      @Override public boolean apply(Void input) {
        return miniYARNCluster.getResourceManager().isInState(STATE.STARTED);
      }
    }, 1000, "Waiting for resource manager to start", LOG, 0.5, 100);

    LOG.info("yarn.resourcemanager.address=" + clusterConf.get("yarn.resourcemanager.address"));
    LOG.info("yarn.resourcemanager.hostname=" + clusterConf.get("yarn.resourcemanager.hostname"));

    LOG.info("Starting YARN client");
    this.yarnClient = this.closer.register(YarnClient.createYarnClient());
    this.yarnClient.init(clusterConf);
    this.yarnClient.start();

    LOG.info("Starting test ZK server");
    TestingServer testingZKServer = this.closer.register(new TestingServer(TEST_ZK_PORT));

    LOG.info("Starting test ZK client");
    this.curatorFramework = TestHelper.createZkClient(testingZKServer, this.closer);

    URL url = GobblinYarnAppLauncherTest.class.getClassLoader()
        .getResource(GobblinYarnAppLauncherTest.class.getSimpleName() + ".conf");
    Assert.assertNotNull(url, "Could not find resource " + url);

    this.config = ConfigFactory.parseURL(url).resolve();

    String zkConnectionString = this.config.getString(GobblinYarnConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    LOG.info("Creating Helix manager");
    this.helixManager = HelixManagerFactory.getZKHelixManager(
        this.config.getString(GobblinYarnConfigurationKeys.HELIX_CLUSTER_NAME_KEY), TestHelper.TEST_HELIX_INSTANCE_NAME,
        InstanceType.CONTROLLER, zkConnectionString);

    LOG.info("Creating Gobblin YARN application launcher");
    this.gobblinYarnAppLauncher = new GobblinYarnAppLauncher(this.config, clusterConf);
    LOG.info("Set-up done.");
  }

  @Test
  public void testCreateHelixCluster() throws Exception {
    final Logger log = LoggerFactory.getLogger(GobblinYarnAppLauncher.class.getCanonicalName() +
                                         ".testCreateHelixCluster");
    log.info("Creating Gobblin YARN Helix cluster");
    // This is tested here instead of in YarnHelixUtilsTest to avoid setting up yet another testing ZooKeeper server.
    YarnHelixUtils.createGobblinYarnHelixCluster(
        this.config.getString(GobblinYarnConfigurationKeys.ZK_CONNECTION_STRING_KEY),
        this.config.getString(GobblinYarnConfigurationKeys.HELIX_CLUSTER_NAME_KEY));
    log.info("Done creating Gobblin YARN Helix cluster");

    Assert.assertEquals(this.curatorFramework.checkExists()
        .forPath(String.format("/%s", GobblinYarnAppLauncherTest.class.getSimpleName())).getVersion(), 0);
    Assert.assertEquals(
        this.curatorFramework.checkExists()
            .forPath(String.format("/%s/CONTROLLER", GobblinYarnAppLauncherTest.class.getSimpleName())).getVersion(),
        0);
    log.info("done");
  }

  /**
   * For some yet unknown reason, hostname resolution for the ResourceManager in {@link MiniYARNCluster}
   * has some issue that causes the {@link YarnClient} not be able to connect and submit the Gobblin Yarn
   * application successfully. This works fine on local machine though. So disabling this and the test
   * below that depends on it on Travis-CI.
   */
  //@Test(groups = { "disabledOnTravis" }, dependsOnMethods = "testCreateHelixCluster")
  @Test(dependsOnMethods = "testCreateHelixCluster")
  public void testSetupAndSubmitApplication() throws Exception {
    final Logger log = LoggerFactory.getLogger(GobblinYarnAppLauncher.class.getCanonicalName() +
        ".testSetupAndSubmitApplication");
    log.info("Creating YARN client");
    this.gobblinYarnAppLauncher.startYarnClient();
    this.applicationId = this.gobblinYarnAppLauncher.setupAndSubmitApplication();
    log.info("done");
  }

  //@Test(groups = { "disabledOnTravis" }, dependsOnMethods = "testSetupAndSubmitApplication")
  @Test(dependsOnMethods = "testSetupAndSubmitApplication")
  public void testGetReconnectableApplicationId() throws Exception {
    final Logger log = LoggerFactory.getLogger(GobblinYarnAppLauncher.class.getCanonicalName() +
        ".testGetReconnectableApplicationId");
    Assert.assertEquals(this.gobblinYarnAppLauncher.getReconnectableApplicationId().get(), this.applicationId);
    log.info("Killing application");
    this.yarnClient.killApplication(this.applicationId);
    log.info("done");
  }

  @Test(dependsOnMethods = "testGetReconnectableApplicationId")
  public void testSendShutdownRequest() throws Exception {
    final Logger log = LoggerFactory.getLogger(GobblinYarnAppLauncher.class.getCanonicalName() +
        ".testSendShutdownRequest");
    log.info("Creating shutdown request");
    this.helixManager.connect();
    this.helixManager.getMessagingService().registerMessageHandlerFactory(Message.MessageType.SHUTDOWN.toString(),
        new TestShutdownMessageHandlerFactory(this));

    log.info("Sending shutdown request");
    this.gobblinYarnAppLauncher.connectHelixManager();
    this.gobblinYarnAppLauncher.sendShutdownRequest();

    log.info("Waiting for message");
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
    log.info("done.");
  }

  @AfterClass
  public void tearDown() throws IOException, TimeoutException {
    LOG.info("Tearing done.");
    try {
      LOG.info("Stopping YARN client");
      this.gobblinYarnAppLauncher.stopYarnClient();

      LOG.info("Disconnecting Helix manager");
      if (this.helixManager.isConnected()) {
        this.helixManager.disconnect();
      }

      LOG.info("Disconnecting from Helix manager");
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
