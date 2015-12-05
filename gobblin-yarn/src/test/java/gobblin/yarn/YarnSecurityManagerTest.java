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
import java.util.Collection;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.eventbus.EventBus;
import com.google.common.io.Closer;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


/**
 * Unit tests for {@link YarnAppSecurityManager} and {@link YarnContainerSecurityManager}.
 *
 * <p>
 *   This class tests {@link YarnAppSecurityManager} and {@link YarnContainerSecurityManager} together
 *   as it is more convenient to test both here where all dependencies are setup between the two.
 * </p>
 *
 * @author ynli
 */
@Test(groups = { "gobblin.yarn" })
public class YarnSecurityManagerTest {

  private static final int TEST_ZK_PORT = 3185;

  private CuratorFramework curatorFramework;

  private HelixManager helixManager;

  private MiniDFSCluster miniDFSCluster;

  private Configuration dfsConfig;
  private FileSystem fs;
  private Path miniHDFSBaseDir;
  private Path tokenFilePath;
  private Token<?> token;

  private YarnAppSecurityManager yarnAppSecurityManager;
  private YarnContainerSecurityManager yarnContainerSecurityManager;

  private final Closer closer = Closer.create();

  @BeforeClass
  public void setUp() throws Exception {
    TestingServer testingZKServer = this.closer.register(new TestingServer(TEST_ZK_PORT));

    this.curatorFramework = this.closer.register(
        CuratorFrameworkFactory.newClient(testingZKServer.getConnectString(), new RetryOneTime(2000)));
    this.curatorFramework.start();

    URL url = YarnSecurityManagerTest.class.getClassLoader().getResource(
        YarnSecurityManagerTest.class.getSimpleName() + ".conf");
    Assert.assertNotNull(url, "Could not find resource " + url);

    Config config = ConfigFactory.parseURL(url).resolve();

    String zkConnectingString = config.getString(GobblinYarnConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    String helixClusterName = config.getString(GobblinYarnConfigurationKeys.HELIX_CLUSTER_NAME_KEY);

    YarnHelixUtils.createGobblinYarnHelixCluster(zkConnectingString, helixClusterName);

    this.helixManager = HelixManagerFactory.getZKHelixManager(
        helixClusterName, TestHelper.TEST_HELIX_INSTANCE_NAME, InstanceType.SPECTATOR, zkConnectingString);
    this.helixManager.connect();

    this.miniHDFSBaseDir = new Path(YarnSecurityManagerTest.class.getSimpleName());
    this.dfsConfig = getTestConfiguration(this.miniHDFSBaseDir);
    dfsConfig.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, this.miniHDFSBaseDir.toUri().toString());
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(dfsConfig);
    this.miniDFSCluster = builder.numDataNodes(1).build();

    this.fs = Mockito.spy(this.closer.register(this.miniDFSCluster.getFileSystem()));

    this.token = new Token<>();
    this.token.setKind(new Text("test"));
    this.token.setService(new Text("test"));
    Mockito.<Token<?>>when(this.fs.getDelegationToken(UserGroupInformation.getLoginUser().getShortUserName()))
        .thenReturn(this.token);

    this.tokenFilePath = new Path(this.fs.getHomeDirectory(), GobblinYarnConfigurationKeys.TOKEN_FILE_NAME);
    this.yarnAppSecurityManager = new YarnAppSecurityManager(config, this.helixManager, this.fs, this.tokenFilePath);
    this.yarnContainerSecurityManager = new YarnContainerSecurityManager(config, this.fs, new EventBus());
  }

  @Test
  public void testGetNewDelegationTokenForLoginUser() throws IOException {
    this.yarnAppSecurityManager.getNewDelegationTokenForLoginUser();
  }

  @Test(dependsOnMethods = "testGetNewDelegationTokenForLoginUser")
  public void testWriteDelegationTokenToFile() throws IOException {
    this.yarnAppSecurityManager.writeDelegationTokenToFile();
    Assert.assertTrue(this.fs.exists(this.tokenFilePath));
    assertToken(YarnHelixUtils.readTokensFromFile(this.tokenFilePath, this.dfsConfig));
  }

  @Test
  public void testSendTokenFileUpdatedMessage() throws Exception {
    this.yarnAppSecurityManager.sendTokenFileUpdatedMessage(InstanceType.CONTROLLER);
    Assert.assertEquals(this.curatorFramework.checkExists().forPath(
        String.format("/%s/CONTROLLER/MESSAGES", YarnSecurityManagerTest.class.getSimpleName())).getVersion(), 0);
    Thread.sleep(500);
    Assert.assertEquals(this.curatorFramework.getChildren().forPath(String.format("/%s/CONTROLLER/MESSAGES",
        YarnSecurityManagerTest.class.getSimpleName())).size(), 1);
  }

  @Test(dependsOnMethods = "testWriteDelegationTokenToFile")
  public void testYarnContainerSecurityManager() throws IOException {
    Collection<Token<?>> tokens = this.yarnContainerSecurityManager.readDelegationTokens(this.tokenFilePath);
    assertToken(tokens);
    this.yarnContainerSecurityManager.addDelegationTokens(tokens);
    assertToken(UserGroupInformation.getCurrentUser().getTokens());
  }

  @AfterClass
  public void tearDown() throws IOException {
    try {
      if (this.helixManager.isConnected()) {
        this.helixManager.disconnect();
      }
      try {
        FileSystem.getLocal(this.dfsConfig).delete(this.miniHDFSBaseDir, true);
      } finally {
        this.miniDFSCluster.shutdown(true);
      }
    } catch (Throwable t) {
      throw this.closer.rethrow(t);
    } finally {
      this.closer.close();
    }
  }

  private void assertToken(Collection<Token<?>> tokens) {
    Assert.assertEquals(tokens.size(), 1);
    Token<?> token = tokens.iterator().next();
    Assert.assertEquals(token, this.token);
  }

  private HdfsConfiguration getTestConfiguration(Path miniHDFSBaseDir) {
    HdfsConfiguration hdfsConfiguration = new HdfsConfiguration(false);

    hdfsConfiguration.set("dfs.namenode.name.dir", miniHDFSBaseDir.toString());
    hdfsConfiguration.set("dfs.namenode.edits.dir", miniHDFSBaseDir.toString());
    hdfsConfiguration.setLong("dfs.namenode.fs-limits.min-block-size", 0L);

    return hdfsConfiguration;
  }
}
