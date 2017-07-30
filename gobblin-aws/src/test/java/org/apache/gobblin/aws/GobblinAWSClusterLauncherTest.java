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

package gobblin.aws;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.Message;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.services.autoscaling.model.AutoScalingGroup;
import com.amazonaws.services.autoscaling.model.Tag;
import com.amazonaws.services.autoscaling.model.TagDescription;
import com.amazonaws.services.ec2.model.AvailabilityZone;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import gobblin.cluster.GobblinClusterConfigurationKeys;
import gobblin.cluster.GobblinHelixConstants;
import gobblin.cluster.HelixMessageSubTypes;
import gobblin.cluster.HelixMessageTestBase;
import gobblin.cluster.HelixUtils;
import gobblin.cluster.TestHelper;
import gobblin.cluster.TestShutdownMessageHandlerFactory;
import gobblin.testing.AssertWithBackoff;


/**
 * Unit tests for {@link GobblinAWSClusterLauncher}.
 *
 * @author Abhishek Tiwari
 */
@Test(groups = { "gobblin.aws" })
@PrepareForTest({ AWSSdkClient.class, GobblinAWSClusterLauncher.class})
@PowerMockIgnore({"javax.*", "org.apache.*", "org.w3c.*", "org.xml.*"})
public class GobblinAWSClusterLauncherTest extends PowerMockTestCase implements HelixMessageTestBase  {
  public final static Logger LOG = LoggerFactory.getLogger(GobblinAWSClusterLauncherTest.class);

  private CuratorFramework curatorFramework;
  private Config config;

  private GobblinAWSClusterLauncher gobblinAwsClusterLauncher;
  private HelixManager helixManager;

  private String gobblinClusterName = "testCluster";
  private String helixClusterName;
  private String clusterId;

  private TagDescription clusterNameTag = new TagDescription()
      .withKey(GobblinAWSClusterLauncher.CLUSTER_NAME_ASG_TAG).withValue(gobblinClusterName);
  private TagDescription clusterIdTag = new TagDescription()
      .withKey(GobblinAWSClusterLauncher.CLUSTER_ID_ASG_TAG).withValue("dummy");
  private TagDescription masterTypeTag = new TagDescription()
      .withKey(GobblinAWSClusterLauncher.ASG_TYPE_ASG_TAG).withValue(GobblinAWSClusterLauncher.ASG_TYPE_MASTER);
  private TagDescription workerTypeTag = new TagDescription()
      .withKey(GobblinAWSClusterLauncher.ASG_TYPE_ASG_TAG).withValue(GobblinAWSClusterLauncher.ASG_TYPE_WORKERS);
  private AutoScalingGroup masterASG = new AutoScalingGroup()
      .withAutoScalingGroupName("AutoScalingGroup_master")
      .withLaunchConfigurationName("LaunchConfiguration_master")
      .withTags(clusterNameTag, clusterIdTag, masterTypeTag);
  private AutoScalingGroup workerASG = new AutoScalingGroup()
      .withAutoScalingGroupName("AutoScalingGroup_worker")
      .withLaunchConfigurationName("LaunchConfiguration_worker")
      .withTags(clusterNameTag, clusterIdTag, workerTypeTag);
  private AvailabilityZone availabilityZone = new AvailabilityZone().withZoneName("A");
  private Instance instance = new Instance().withPublicIpAddress("0.0.0.0");

  private final Closer closer = Closer.create();

  @Mock
  private AWSSdkClient awsSdkClient;

  @BeforeClass
  public void setUp() throws Exception {

    // Mock AWS SDK calls
    MockitoAnnotations.initMocks(this);

    PowerMockito.whenNew(AWSSdkClient.class).withAnyArguments().thenReturn(awsSdkClient);

    Mockito.doNothing()
        .when(awsSdkClient)
        .createSecurityGroup(Mockito.anyString(), Mockito.anyString());
    Mockito.doReturn(Lists.<AvailabilityZone>newArrayList(availabilityZone))
        .when(awsSdkClient)
        .getAvailabilityZones();
    Mockito.doReturn("dummy")
        .when(awsSdkClient)
        .createKeyValuePair(Mockito.anyString());
    Mockito.doReturn(Lists.<AutoScalingGroup>newArrayList(masterASG, workerASG))
        .when(awsSdkClient)
        .getAutoScalingGroupsWithTag(Mockito.any(Tag.class));
    Mockito.doReturn(Lists.<Instance>newArrayList(instance))
        .when(awsSdkClient)
        .getInstancesForGroup(Mockito.anyString(), Mockito.anyString());
    Mockito.doReturn(Lists.<S3ObjectSummary>newArrayList())
        .when(awsSdkClient)
        .listS3Bucket(Mockito.anyString(), Mockito.anyString());
    Mockito.doNothing()
        .when(awsSdkClient)
        .addPermissionsToSecurityGroup(Mockito.any(String.class), Mockito.any(String.class), Mockito.any(String.class),
            Mockito.any(Integer.class), Mockito.any(Integer.class));
    Mockito.doNothing()
        .when(awsSdkClient)
        .createAutoScalingGroup(Mockito.any(String.class), Mockito.any(String.class), Mockito.any(Integer.class),
            Mockito.any(Integer.class), Mockito.any(Integer.class), Mockito.any(Optional.class),
            Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class),
            Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(List.class));
    Mockito.doNothing()
        .when(awsSdkClient)
        .createLaunchConfig(Mockito.any(String.class), Mockito.any(String.class), Mockito.any(String.class),
            Mockito.any(String.class), Mockito.any(String.class), Mockito.any(Optional.class),
            Mockito.any(Optional.class), Mockito.any(Optional.class), Mockito.any(Optional.class),
            Mockito.any(Optional.class), Mockito.any(String.class));
    Mockito
        .doNothing()
        .when(awsSdkClient)
        .deleteAutoScalingGroup(Mockito.any(String.class), Mockito.any(boolean.class));
    Mockito
        .doNothing()
        .when(awsSdkClient)
        .deleteLaunchConfiguration(Mockito.any(String.class));
    Mockito.doNothing()
        .when(awsSdkClient)
        .addPermissionsToSecurityGroup(Mockito.any(String.class), Mockito.any(String.class), Mockito.any(String.class),
            Mockito.any(Integer.class), Mockito.any(Integer.class));

    // Local test Zookeeper
    final TestingServer testingZKServer = this.closer.register(new TestingServer(-1));
    LOG.info("Testing ZK Server listening on: " + testingZKServer.getConnectString());
    this.curatorFramework = TestHelper.createZkClient(testingZKServer, this.closer);

    // Load configuration
    final URL url = GobblinAWSClusterLauncherTest.class.getClassLoader().getResource(
        GobblinAWSClusterLauncherTest.class.getSimpleName() + ".conf");
    Assert.assertNotNull(url, "Could not find resource " + url);
    this.config = ConfigFactory.parseURL(url)
        .withValue("gobblin.cluster.zk.connection.string",
                   ConfigValueFactory.fromAnyRef(testingZKServer.getConnectString()))
        .resolve();
    this.helixClusterName = this.config.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY);

    final String zkConnectionString = this.config.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    this.helixManager = HelixManagerFactory
        .getZKHelixManager(this.config.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY),
            TestHelper.TEST_HELIX_INSTANCE_NAME, InstanceType.CONTROLLER, zkConnectionString);

    // Gobblin AWS Cluster Launcher to test
    this.gobblinAwsClusterLauncher = new GobblinAWSClusterLauncher(this.config);
  }

  @Test
  public void testCreateHelixCluster() throws Exception {
    // This is tested here instead of in HelixUtilsTest to avoid setting up yet another testing ZooKeeper server.
    HelixUtils
        .createGobblinHelixCluster(this.config.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY),
            this.config.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY));

    // Assert to check if there is no pre-existing cluster
    Assert.assertEquals(this.curatorFramework.checkExists().forPath(String.format("/%s",
        this.helixClusterName)).getVersion(), 0);
    Assert.assertEquals(this.curatorFramework.checkExists().forPath(String.format("/%s/CONTROLLER",
        this.helixClusterName)).getVersion(), 0);
  }

  @Test(dependsOnMethods = "testCreateHelixCluster")
  public void testSetupAndSubmitApplication() throws Exception {
    // Setup new cluster
    this.clusterId = this.gobblinAwsClusterLauncher.setupGobblinCluster();
    this.clusterIdTag.setValue(this.clusterId);
  }

  @Test(dependsOnMethods = "testSetupAndSubmitApplication")
  public void testGetReconnectableApplicationId() throws Exception {
    // Assert to check if cluster was created correctly by trying to reconnect to it
    Assert.assertEquals(this.gobblinAwsClusterLauncher.getReconnectableClusterId().get(), this.clusterId);
  }

  @Test(dependsOnMethods = "testGetReconnectableApplicationId")
  public void testSendShutdownRequest() throws Exception {
    // Connect to Helix as Controller and register a shutdown request handler
    this.helixManager.connect();
    this.helixManager.getMessagingService().registerMessageHandlerFactory(GobblinHelixConstants.SHUTDOWN_MESSAGE_TYPE,
        new TestShutdownMessageHandlerFactory(this));

    // Make Gobblin AWS Cluster launcher start a shutdown
    this.gobblinAwsClusterLauncher.connectHelixManager();
    this.gobblinAwsClusterLauncher.sendShutdownRequest();

    Assert.assertEquals(this.curatorFramework.checkExists()
        .forPath(String.format("/%s/CONTROLLER/MESSAGES", this.helixClusterName)).getVersion(), 0);
    GetControllerMessageNumFunc getCtrlMessageNum =
        new GetControllerMessageNumFunc(this.helixClusterName, this.curatorFramework);

    // Assert to check if shutdown message was issued
    AssertWithBackoff assertWithBackoff =
        AssertWithBackoff.create().logger(LoggerFactory.getLogger("testSendShutdownRequest")).timeoutMs(20000);
    assertWithBackoff.assertEquals(getCtrlMessageNum, 1, "1 controller message queued");

    // Assert to check if shutdown message was processed
    // Give Helix sometime to handle the message
    assertWithBackoff.assertEquals(getCtrlMessageNum, 0, "all controller messages processed");
  }

  @AfterClass
  public void tearDown() throws IOException, TimeoutException {
    try {
      this.gobblinAwsClusterLauncher.stop();

      if (this.helixManager.isConnected()) {
        this.helixManager.disconnect();
      }

      this.gobblinAwsClusterLauncher.disconnectHelixManager();
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
}
