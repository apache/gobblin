/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
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

import java.net.URL;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
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
import com.google.common.base.Predicate;
import com.google.common.io.Closer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.testing.AssertWithBackoff;
import gobblin.yarn.event.ApplicationMasterShutdownRequest;


/**
 * Unit tests for {@link GobblinApplicationMaster}.
 *
 * <p>
 *   This class uses a {@link TestingServer} as an embedded ZooKeeper server for testing. The Curator
 *   framework is used to provide a ZooKeeper client. This class also uses the {@link HelixManager} to
 *   act as a testing Helix participant to receive the container (running the {@link GobblinWorkUnitRunner})
 *   shutdown request message.
 * </p>
 *
 * @author Yinan Li
 */
@Test(groups = { "gobblin.yarn" })
public class GobblinApplicationMasterTest implements HelixMessageTestBase {

  private static final int TEST_ZK_PORT = 3182;

  private TestingServer testingZKServer;

  private HelixManager helixManager;

  private GobblinApplicationMaster gobblinApplicationMaster;

  @BeforeClass
  public void setUp() throws Exception {
    this.testingZKServer = new TestingServer(TEST_ZK_PORT);

    URL url = GobblinApplicationMasterTest.class.getClassLoader().getResource(
        GobblinApplicationMasterTest.class.getSimpleName() + ".conf");
    Assert.assertNotNull(url, "Could not find resource " + url);

    Config config = ConfigFactory.parseURL(url).resolve();

    YarnConfiguration yarnConfiguration = new YarnConfiguration();
    yarnConfiguration.set(YarnConfiguration.NM_CLIENT_MAX_NM_PROXIES, "1");

    String zkConnectionString = config.getString(GobblinYarnConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    YarnHelixUtils.createGobblinYarnHelixCluster(zkConnectionString,
        config.getString(GobblinYarnConfigurationKeys.HELIX_CLUSTER_NAME_KEY));

    this.helixManager = HelixManagerFactory
        .getZKHelixManager(config.getString(GobblinYarnConfigurationKeys.HELIX_CLUSTER_NAME_KEY),
            TestHelper.TEST_HELIX_INSTANCE_NAME, InstanceType.PARTICIPANT, zkConnectionString);
    this.helixManager.connect();
    this.helixManager.getMessagingService().registerMessageHandlerFactory(Message.MessageType.SHUTDOWN.toString(),
        new TestShutdownMessageHandlerFactory(this));

    this.gobblinApplicationMaster = new GobblinApplicationMaster(TestHelper.TEST_APPLICATION_NAME,
        ConverterUtils.toContainerId(TestHelper.TEST_CONTROLLER_CONTAINER_ID), config, yarnConfiguration);
    this.gobblinApplicationMaster.getEventBus().register(this.gobblinApplicationMaster);
    this.gobblinApplicationMaster.connectHelixManager();
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
          new GetInstanceMessageNumFunc(GobblinApplicationMasterTest.class.getSimpleName(),
              curatorFramework);
      AssertWithBackoff assertWithBackoff =
          AssertWithBackoff.create().logger(log).timeoutMs(30000);

      this.gobblinApplicationMaster.sendShutdownRequest();

      Assert.assertEquals(curatorFramework.checkExists().forPath(String
          .format("/%s/INSTANCES/%s/MESSAGES", GobblinApplicationMasterTest.class.getSimpleName(),
              TestHelper.TEST_HELIX_INSTANCE_NAME)).getVersion(), 0);

      assertWithBackoff.assertEquals(getMessageNumFunc, 1, "1 message queued");

      // Give Helix sometime to handle the message
      assertWithBackoff.assertEquals(getMessageNumFunc, 0, "all messages processed");
    } finally {
      closer.close();
    }
  }

  @Test(dependsOnMethods = "testSendShutdownRequest")
  public void testHandleApplicationMasterShutdownRequest() throws Exception {
    Logger log = LoggerFactory.getLogger("testHandleApplicationMasterShutdownRequest");
    this.gobblinApplicationMaster.getEventBus().post(new ApplicationMasterShutdownRequest());
    AssertWithBackoff.create().logger(log).timeoutMs(20000)
      .assertTrue(new Predicate<Void>() {
        @Override public boolean apply(Void input) {
          return !GobblinApplicationMasterTest.this.gobblinApplicationMaster.isHelixManagerConnected();
        }
      }, "ApplicationMaster shutdown");
  }

  @AfterClass
  public void tearDown() throws Exception {
    try {
      if (this.helixManager.isConnected()) {
        this.helixManager.disconnect();
      }
      this.gobblinApplicationMaster.disconnectHelixManager();
    } catch (Throwable t) {
      Assert.fail();
    } finally {
      this.testingZKServer.close();
    }
  }

  @Test(enabled = false)
  @Override
  public void assertMessageReception(Message message) {
    Assert.assertEquals(message.getMsgType(), Message.MessageType.SHUTDOWN.toString());
    Assert.assertEquals(message.getMsgSubType(), HelixMessageSubTypes.WORK_UNIT_RUNNER_SHUTDOWN.toString());
  }
}
