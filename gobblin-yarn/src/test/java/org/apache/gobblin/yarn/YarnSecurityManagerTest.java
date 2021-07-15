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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
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

import static org.mockito.Matchers.any;


/**
 * Unit tests for {@link YarnAppSecurityManagerWithKeytabs} and {@link YarnContainerSecurityManager}.
 *
 * <p>
 *   This class tests {@link YarnAppSecurityManagerWithKeytabs} and {@link YarnContainerSecurityManager} together
 *   as it is more convenient to test both here where all dependencies are setup between the two.
 * </p>
 *
 * <p>
 *   This class uses a {@link TestingServer} as an embedded ZooKeeper server for testing. The Curator
 *   framework is used to provide a ZooKeeper client. This class also uses a {@link HelixManager} as
 *   being required by {@link YarnAppSecurityManagerWithKeytabs}. The local file system as returned by
 *   {@link FileSystem#getLocal(Configuration)} is used for writing the testing delegation token, which
 *   is acquired by mocking the method {@link FileSystem#getDelegationToken(String)} on the local
 *   {@link FileSystem} instance.
 * </p>
 * @author Yinan Li
 */
@Test(groups = { "gobblin.yarn" })
public class YarnSecurityManagerTest {
  final Logger LOG = LoggerFactory.getLogger(YarnSecurityManagerTest.class);
  private static final String HELIX_TEST_INSTANCE_PARTICIPANT = HelixUtils.getHelixInstanceName("TestInstance", 1);

  private CuratorFramework curatorFramework;

  private HelixManager helixManager;
  private HelixManager helixManagerParticipant;

  private Configuration configuration;
  private FileSystem localFs;
  private Path baseDir;
  private Path tokenFilePath;
  private Token<?> fsToken;
  private List<Token<?>> allTokens;

  private YarnAppSecurityManagerWithKeytabs _yarnAppYarnAppSecurityManagerWithKeytabs;
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

    this.helixManagerParticipant = HelixManagerFactory.getZKHelixManager(
        helixClusterName, HELIX_TEST_INSTANCE_PARTICIPANT, InstanceType.PARTICIPANT, zkConnectingString);
    this.helixManagerParticipant.connect();

    this.configuration = new Configuration();
    this.localFs = Mockito.spy(FileSystem.getLocal(this.configuration));

    this.fsToken = new Token<>();
    this.fsToken.setKind(new Text("HDFS_DELEGATION_TOKEN"));
    this.fsToken.setService(new Text("HDFS"));
    this.allTokens = new ArrayList<>();
    allTokens.add(fsToken);
    Token<?>[] allTokenArray = new Token<?>[2];
    allTokenArray[0]= fsToken;

    Mockito.<Token<?>[]>when(localFs.addDelegationTokens(any(String.class), any(Credentials.class)))
            .thenReturn(allTokenArray);

    this.baseDir = new Path(YarnSecurityManagerTest.class.getSimpleName());
    this.tokenFilePath = new Path(this.baseDir, GobblinYarnConfigurationKeys.TOKEN_FILE_NAME);
    this._yarnAppYarnAppSecurityManagerWithKeytabs = Mockito.spy(new YarnAppSecurityManagerWithKeytabs(config, this.helixManager, this.localFs, this.tokenFilePath));
    this.yarnContainerSecurityManager = new YarnContainerSecurityManager(config, this.localFs, new EventBus());

    Mockito.doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) throws Throwable {
        _yarnAppYarnAppSecurityManagerWithKeytabs.credentials.addToken(new Text("HDFS_DELEGATION_TOKEN"), fsToken);
        return null;
      }
    }).when(_yarnAppYarnAppSecurityManagerWithKeytabs).getNewDelegationTokenForLoginUser();
  }

  @Test
  public void testGetNewDelegationTokenForLoginUser() throws IOException, InterruptedException {
    this._yarnAppYarnAppSecurityManagerWithKeytabs.getNewDelegationTokenForLoginUser();
  }

  @Test(dependsOnMethods = "testGetNewDelegationTokenForLoginUser")
  public void testWriteDelegationTokenToFile() throws IOException {
    this._yarnAppYarnAppSecurityManagerWithKeytabs.writeDelegationTokenToFile();
    Assert.assertTrue(this.localFs.exists(this.tokenFilePath));
    assertToken(YarnHelixUtils.readTokensFromFile(this.tokenFilePath, this.configuration));
  }


  static class GetHelixMessageNumFunc implements Function<Void, Integer> {
    private final CuratorFramework curatorFramework;
    private final String testName;
    private final String instanceName;
    private final InstanceType instanceType;
    private final String path;

    public GetHelixMessageNumFunc(String testName, InstanceType instanceType, String instanceName, CuratorFramework curatorFramework) {
      this.curatorFramework = curatorFramework;
      this.testName = testName;
      this.instanceType = instanceType;
      this.instanceName = instanceName;
      switch (instanceType) {
        case CONTROLLER:
          this.path = String.format("/%s/CONTROLLER/MESSAGES", this.testName);
          break;
        case PARTICIPANT:
          this.path = String.format("/%s/INSTANCES/%s/MESSAGES", this.testName, this.instanceName);
          break;
        default:
          throw new RuntimeException("Invalid instance type " + instanceType.name());
      }
    }

    @Override
    public Integer apply(Void input) {
      try {
        return this.curatorFramework.getChildren().forPath(this.path).size();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

  }

  @Test
  public void testSendTokenFileUpdatedMessage() throws Exception {
    Logger log = LoggerFactory.getLogger("testSendTokenFileUpdatedMessage");
    this._yarnAppYarnAppSecurityManagerWithKeytabs.sendTokenFileUpdatedMessage(InstanceType.CONTROLLER);
    Assert.assertEquals(this.curatorFramework.checkExists().forPath(
        String.format("/%s/CONTROLLER/MESSAGES", YarnSecurityManagerTest.class.getSimpleName())).getVersion(), 0);
    AssertWithBackoff.create().logger(log).timeoutMs(20000)
      .assertEquals(new GetHelixMessageNumFunc(YarnSecurityManagerTest.class.getSimpleName(), InstanceType.CONTROLLER, "",
          this.curatorFramework), 1, "1 controller message queued");

    this._yarnAppYarnAppSecurityManagerWithKeytabs.sendTokenFileUpdatedMessage(InstanceType.PARTICIPANT, HELIX_TEST_INSTANCE_PARTICIPANT);
    Assert.assertEquals(this.curatorFramework.checkExists().forPath(
        String.format("/%s/INSTANCES/%s/MESSAGES", YarnSecurityManagerTest.class.getSimpleName(), HELIX_TEST_INSTANCE_PARTICIPANT)).getVersion(), 0);
    AssertWithBackoff.create().logger(log).timeoutMs(20000)
        .assertEquals(new GetHelixMessageNumFunc(YarnSecurityManagerTest.class.getSimpleName(), InstanceType.PARTICIPANT, HELIX_TEST_INSTANCE_PARTICIPANT,
            this.curatorFramework), 1, "1 controller message queued");
  }

  @Test(dependsOnMethods = "testWriteDelegationTokenToFile")
  public void testYarnContainerSecurityManager() throws IOException {
    Credentials credentials = this.yarnContainerSecurityManager.readCredentials(this.tokenFilePath);
    assertToken(credentials.getAllTokens());
    this.yarnContainerSecurityManager.addCredentials(credentials);
    assertToken(UserGroupInformation.getCurrentUser().getTokens());
  }

  @AfterClass
  public void tearDown() throws IOException {
    try {
      if (this.helixManager.isConnected()) {
        this.helixManager.disconnect();
      }
      if (this.helixManagerParticipant.isConnected()) {
        this.helixManagerParticipant.disconnect();
      }
      this.localFs.delete(this.baseDir, true);
    } catch (Throwable t) {
      throw this.closer.rethrow(t);
    } finally {
      this.closer.close();
    }
  }

  private void assertToken(Collection<Token<?>> tokens) {
    tokens.forEach( token -> org.junit.Assert.assertTrue(allTokens.contains(token)));
  }
}
