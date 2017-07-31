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

package org.apache.gobblin.yarn;

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Closer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.HelixUtils;
import org.apache.gobblin.cluster.TestHelper;
import org.apache.gobblin.testing.AssertWithBackoff;


/**
 * Unit tests for {@link YarnAppSecurityManager} and {@link YarnContainerSecurityManager}.
 *
 * <p>
 *   This class tests {@link YarnAppSecurityManager} and {@link YarnContainerSecurityManager} together
 *   as it is more convenient to test both here where all dependencies are setup between the two.
 * </p>
 *
 * <p>
 *   This class uses a {@link TestingServer} as an embedded ZooKeeper server for testing. The Curator
 *   framework is used to provide a ZooKeeper client. This class also uses a {@link HelixManager} as
 *   being required by {@link YarnAppSecurityManager}. The local file system as returned by
 *   {@link FileSystem#getLocal(Configuration)} is used for writing the testing delegation token, which
 *   is acquired by mocking the method {@link FileSystem#getDelegationToken(String)} on the local
 *   {@link FileSystem} instance.
 * </p>
 * @author Yinan Li
 */
@Test(groups = { "gobblin.yarn" })
public class YarnSecurityManagerTest {
  final Logger LOG = LoggerFactory.getLogger(YarnSecurityManagerTest.class);

  private CuratorFramework curatorFramework;

  private HelixManager helixManager;

  private Configuration configuration;
  private FileSystem localFs;
  private Path baseDir;
  private Path tokenFilePath;
  private Token<?> token;

  private YarnAppSecurityManager yarnAppSecurityManager;
  private YarnContainerSecurityManager yarnContainerSecurityManager;

  private final Closer closer = Closer.create();

  @BeforeClass
  public void setUp() throws Exception {
    // Use a random ZK port
    TestingServer testingZKServer = this.closer.register(new TestingServer(-1));
    LOG.info("Testing ZK Server listening on: " + testingZKServer.getConnectString());

    this.curatorFramework = this.closer.register(
        CuratorFrameworkFactory.newClient(testingZKServer.getConnectString(), new RetryOneTime(2000)));
    this.curatorFramework.start();

    URL url = YarnSecurityManagerTest.class.getClassLoader().getResource(
        YarnSecurityManagerTest.class.getSimpleName() + ".conf");
    Assert.assertNotNull(url, "Could not find resource " + url);

    Config config = ConfigFactory.parseURL(url)
        .withValue("gobblin.cluster.zk.connection.string",
                   ConfigValueFactory.fromAnyRef(testingZKServer.getConnectString()))
        .resolve();

    String zkConnectingString = config.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    String helixClusterName = config.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY);

    HelixUtils.createGobblinHelixCluster(zkConnectingString, helixClusterName);

    this.helixManager = HelixManagerFactory.getZKHelixManager(
        helixClusterName, TestHelper.TEST_HELIX_INSTANCE_NAME, InstanceType.SPECTATOR, zkConnectingString);
    this.helixManager.connect();

    this.configuration = new Configuration();
    this.localFs = Mockito.spy(FileSystem.getLocal(this.configuration));

    this.token = new Token<>();
    this.token.setKind(new Text("test"));
    this.token.setService(new Text("test"));
    Mockito.<Token<?>>when(this.localFs.getDelegationToken(UserGroupInformation.getLoginUser().getShortUserName()))
        .thenReturn(this.token);

    this.baseDir = new Path(YarnSecurityManagerTest.class.getSimpleName());
    this.tokenFilePath = new Path(this.baseDir, GobblinYarnConfigurationKeys.TOKEN_FILE_NAME);
    this.yarnAppSecurityManager =
        new YarnAppSecurityManager(config, this.helixManager, this.localFs, this.tokenFilePath);
    this.yarnContainerSecurityManager = new YarnContainerSecurityManager(config, this.localFs, new EventBus());
  }

  @Test
  public void testGetNewDelegationTokenForLoginUser() throws IOException {
    this.yarnAppSecurityManager.getNewDelegationTokenForLoginUser();
  }

  @Test(dependsOnMethods = "testGetNewDelegationTokenForLoginUser")
  public void testWriteDelegationTokenToFile() throws IOException {
    this.yarnAppSecurityManager.writeDelegationTokenToFile();
    Assert.assertTrue(this.localFs.exists(this.tokenFilePath));
    assertToken(YarnHelixUtils.readTokensFromFile(this.tokenFilePath, this.configuration));
  }


  static class GetControllerMessageNumFunc implements Function<Void, Integer> {
    private final CuratorFramework curatorFramework;
    private final String testName;

    public GetControllerMessageNumFunc(String testName, CuratorFramework curatorFramework) {
      this.curatorFramework = curatorFramework;
      this.testName = testName;
    }

    @Override
    public Integer apply(Void input) {
      try {
        return this.curatorFramework.getChildren().forPath(String.format("/%s/CONTROLLER/MESSAGES",
            this.testName)).size();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

  }

  @Test
  public void testSendTokenFileUpdatedMessage() throws Exception {
    Logger log = LoggerFactory.getLogger("testSendTokenFileUpdatedMessage");
    this.yarnAppSecurityManager.sendTokenFileUpdatedMessage(InstanceType.CONTROLLER);
    Assert.assertEquals(this.curatorFramework.checkExists().forPath(
        String.format("/%s/CONTROLLER/MESSAGES", YarnSecurityManagerTest.class.getSimpleName())).getVersion(), 0);
    AssertWithBackoff.create().logger(log).timeoutMs(20000)
      .assertEquals(new GetControllerMessageNumFunc(YarnSecurityManagerTest.class.getSimpleName(),
          this.curatorFramework), 1, "1 controller message queued");
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
      this.localFs.delete(this.baseDir, true);
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
}
