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

import java.io.IOException;
import java.net.URL;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


/**
 * Unit tests for {@link GobblinWorkUnitRunner}.
 *
 * @author ynli
 */
@Test(groups = { "gobblin.yarn" })
public class GobblinWorkUnitRunnerTest {

  private static final int TEST_ZK_PORT = 3183;

  private TestingServer testingZKServer;

  private GobblinWorkUnitRunner gobblinWorkUnitRunner;

  private GobblinApplicationMaster gobblinApplicationMaster;

  @BeforeClass
  public void setUp() throws Exception {
    this.testingZKServer = new TestingServer(TEST_ZK_PORT);

    URL url = GobblinWorkUnitRunnerTest.class.getClassLoader().getResource(
        GobblinWorkUnitRunnerTest.class.getSimpleName() + ".conf");
    Assert.assertNotNull(url, "Could not find resource " + url);

    Config config = ConfigFactory.parseURL(url).resolve();

    String zkConnectionString = config.getString(GobblinYarnConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    YarnHelixUtils.createGobblinYarnHelixCluster(zkConnectionString,
        config.getString(GobblinYarnConfigurationKeys.HELIX_CLUSTER_NAME_KEY));

    // Participant
    this.gobblinWorkUnitRunner = new GobblinWorkUnitRunner(TestHelper.TEST_APPLICATION_NAME,
        ConverterUtils.toContainerId(TestHelper.TEST_PARTICIPANT_CONTAINER_ID), config, Optional.<Path>absent());
    this.gobblinWorkUnitRunner.connectHelixManager();

    // Controller
    this.gobblinApplicationMaster = new GobblinApplicationMaster(TestHelper.TEST_APPLICATION_NAME,
        ConverterUtils.toContainerId(TestHelper.TEST_CONTROLLER_CONTAINER_ID), config, new YarnConfiguration());
    this.gobblinApplicationMaster.connectHelixManager();

  }

  @Test
  public void testSendReceiveShutdownMessage() throws Exception {
    this.gobblinApplicationMaster.sendShutdownRequest();

    // Give Helix some time to handle the message
    Thread.sleep(2000);

    Assert.assertTrue(this.gobblinWorkUnitRunner.isStopped());
  }

  @AfterClass
  public void tearDown() throws IOException {
    try {
      this.gobblinApplicationMaster.disconnectHelixManager();
      this.gobblinWorkUnitRunner.disconnectHelixManager();
    } finally {
      this.testingZKServer.close();
    }
  }
}
