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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.Message;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Service;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.GobblinHelixConstants;
import org.apache.gobblin.cluster.GobblinHelixMultiManager;
import org.apache.gobblin.cluster.HelixMessageTestBase;
import org.apache.gobblin.cluster.HelixUtils;
import org.apache.gobblin.cluster.TestHelper;
import org.apache.gobblin.cluster.TestShutdownMessageHandlerFactory;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.DynamicConfigGenerator;
import org.apache.gobblin.metrics.kafka.KafkaAvroSchemaRegistry;
import org.apache.gobblin.metrics.kafka.KafkaSchemaRegistry;
import org.apache.gobblin.runtime.app.ServiceBasedAppLauncher;
import org.apache.gobblin.testing.AssertWithBackoff;

import static org.mockito.Mockito.times;


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
  private static final Object MANAGED_HELIX_CLUSTER_NAME = "GobblinYarnAppLauncherTestManagedHelix";
  public static final String TEST_HELIX_INSTANCE_NAME_MANAGED = HelixUtils.getHelixInstanceName("TestInstance", 1);

  public static final String DYNAMIC_CONF_PATH = "dynamic.conf";
  public static final String YARN_SITE_XML_PATH = "yarn-site.xml";
  final Logger LOG = LoggerFactory.getLogger(GobblinYarnAppLauncherTest.class);

  private YarnClient yarnClient;

  private CuratorFramework curatorFramework;

  private Config config;
  private Config configManagedHelix;

  private HelixManager helixManager;
  private HelixManager helixManagerManagedHelix;

  private GobblinYarnAppLauncher gobblinYarnAppLauncher;
  private GobblinYarnAppLauncher gobblinYarnAppLauncherManagedHelix;
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
    clusterConf.set("yarn.resourcemanager.connect.max-wait.ms", "10000");

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
    try (PrintWriter pw = new PrintWriter(DYNAMIC_CONF_PATH)) {
      File dir = new File("target/dummydir");

      // dummy directory specified in configuration
      dir.mkdir();

      pw.println("gobblin.cluster.zk.connection.string=\"" + testingZKServer.getConnectString() + "\"");
      pw.println("jobconf.fullyQualifiedPath=\"" + dir.getAbsolutePath() + "\"");
    }

    // YARN config is dynamic and needs to be passed to other processes
    try (OutputStream os = new FileOutputStream(new File(YARN_SITE_XML_PATH))) {
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

    this.configManagedHelix = ConfigFactory.parseURL(url)
        .withValue("gobblin.cluster.zk.connection.string",
            ConfigValueFactory.fromAnyRef(testingZKServer.getConnectString()))
        .withValue(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY, ConfigValueFactory.fromAnyRef(MANAGED_HELIX_CLUSTER_NAME))
        .withValue(GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_KEY, ConfigValueFactory.fromAnyRef(TEST_HELIX_INSTANCE_NAME_MANAGED))
        .withValue(GobblinClusterConfigurationKeys.IS_HELIX_CLUSTER_MANAGED, ConfigValueFactory.fromAnyRef("true"))
        .resolve();

    this.helixManagerManagedHelix = HelixManagerFactory.getZKHelixManager(
        this.configManagedHelix.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY), TEST_HELIX_INSTANCE_NAME_MANAGED,
        InstanceType.PARTICIPANT, zkConnectionString);

    this.gobblinYarnAppLauncherManagedHelix = new GobblinYarnAppLauncher(this.configManagedHelix, clusterConf);
  }

  @Test
  public void testBuildApplicationMasterCommand() {
    String command = this.gobblinYarnAppLauncher.buildApplicationMasterCommand("application_1234_3456", 64);

    // 41 is from 64 * 0.8 - 10
    Assert.assertTrue(command.contains("-Xmx41"));
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

    //Create managed Helix cluster and test it is created successfully
    HelixUtils.createGobblinHelixCluster(
        this.configManagedHelix.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY),
        this.configManagedHelix.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY));

    Assert.assertEquals(this.curatorFramework.checkExists()
        .forPath(String.format("/%s/CONTROLLER", MANAGED_HELIX_CLUSTER_NAME)).getVersion(), 0);
    Assert.assertEquals(
        this.curatorFramework.checkExists()
            .forPath(String.format("/%s/CONTROLLER", MANAGED_HELIX_CLUSTER_NAME)).getVersion(),
        0);
  }

  /**
   * For some yet unknown reason, hostname resolution for the ResourceManager in {@link MiniYARNCluster}
   * has some issue that causes the {@link YarnClient} not be able to connect and submit the Gobblin Yarn
   * application successfully. This works fine on local machine though. So disabling this and the test
   * below that depends on it on Travis-CI.
   */
  @Test(enabled=false, groups = { "disabledOnCI" }, dependsOnMethods = "testCreateHelixCluster")
  public void testSetupAndSubmitApplication() throws Exception {
    HelixUtils.createGobblinHelixCluster(
        this.config.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY),
        this.config.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY));

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

  @Test(enabled=false, groups = { "disabledOnCI" }, dependsOnMethods = "testSetupAndSubmitApplication")
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
    YarnSecurityManagerTest.GetHelixMessageNumFunc getCtrlMessageNum =
        new YarnSecurityManagerTest.GetHelixMessageNumFunc(GobblinYarnAppLauncherTest.class.getSimpleName(), InstanceType.CONTROLLER, "",
            this.curatorFramework);
    AssertWithBackoff assertWithBackoff =
        AssertWithBackoff.create().logger(LoggerFactory.getLogger("testSendShutdownRequest")).timeoutMs(20000);
    assertWithBackoff.assertEquals(getCtrlMessageNum, 1, "1 controller message queued");

    // Give Helix sometime to handle the message
    assertWithBackoff.assertEquals(getCtrlMessageNum, 0, "all controller messages processed");

    this.helixManagerManagedHelix.connect();
    this.helixManagerManagedHelix.getMessagingService().registerMessageHandlerFactory(GobblinHelixConstants.SHUTDOWN_MESSAGE_TYPE,
        new TestShutdownMessageHandlerFactory(this));

    this.gobblinYarnAppLauncherManagedHelix.connectHelixManager();
    this.gobblinYarnAppLauncherManagedHelix.sendShutdownRequest();

    Assert.assertEquals(this.curatorFramework.checkExists()
        .forPath(String.format("/%s/INSTANCES/%s/MESSAGES", this.configManagedHelix.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY), TEST_HELIX_INSTANCE_NAME_MANAGED))
        .getVersion(), 0);
    YarnSecurityManagerTest.GetHelixMessageNumFunc getInstanceMessageNum =
        new YarnSecurityManagerTest.GetHelixMessageNumFunc(this.configManagedHelix.getString(
            GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY),
            InstanceType.PARTICIPANT, TEST_HELIX_INSTANCE_NAME_MANAGED, this.curatorFramework);
    assertWithBackoff =
        AssertWithBackoff.create().logger(LoggerFactory.getLogger("testSendShutdownRequest")).timeoutMs(20000);
    assertWithBackoff.assertEquals(getInstanceMessageNum, 1, "1 controller message queued");

    // Give Helix sometime to handle the message
    assertWithBackoff.assertEquals(getInstanceMessageNum, 0, "all controller messages processed");
  }

  @AfterClass
  public void tearDown() throws IOException, TimeoutException {
    try {
      Files.deleteIfExists(Paths.get(DYNAMIC_CONF_PATH));
      Files.deleteIfExists(Paths.get(YARN_SITE_XML_PATH));

      this.gobblinYarnAppLauncher.stopYarnClient();

      if (this.helixManager.isConnected()) {
        this.helixManager.disconnect();
      }

      if (this.helixManagerManagedHelix.isConnected()) {
        this.helixManagerManagedHelix.disconnect();
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

  /**
   * Test that the dynamic config is added to the config specified when the {@link GobblinApplicationMaster}
   * is instantiated.
   */
  @Test
  public void testDynamicConfig() throws Exception {
    Config config = this.config.withFallback(
        ConfigFactory.parseMap(
        ImmutableMap.of(ConfigurationKeys.DYNAMIC_CONFIG_GENERATOR_CLASS_KEY,
            TestDynamicConfigGenerator.class.getName())));

    ContainerId containerId = ContainerId.newInstance(
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 0), 0), 0);
    TestApplicationMaster
        appMaster = new TestApplicationMaster("testApp", containerId, config,
        new YarnConfiguration());

    Assert.assertEquals(appMaster.getConfig().getString("dynamicKey1"), "dynamicValue1");
    Assert.assertEquals(appMaster.getConfig().getString(ConfigurationKeys.DYNAMIC_CONFIG_GENERATOR_CLASS_KEY),
        TestDynamicConfigGenerator.class.getName());

    ServiceBasedAppLauncher appLauncher = appMaster.getAppLauncher();
    Field servicesField = ServiceBasedAppLauncher.class.getDeclaredField("services");
    servicesField.setAccessible(true);

    List<Service> services = (List<Service>) servicesField.get(appLauncher);

    Optional<Service> yarnServiceOptional = services.stream().filter(e -> e instanceof YarnService).findFirst();

    Assert.assertTrue(yarnServiceOptional.isPresent());

    YarnService yarnService = (YarnService) yarnServiceOptional.get();
    Field configField = YarnService.class.getDeclaredField("config");
    configField.setAccessible(true);
    Config yarnServiceConfig = (Config) configField.get(yarnService);

    Assert.assertEquals(yarnServiceConfig.getString("dynamicKey1"), "dynamicValue1");
    Assert.assertEquals(yarnServiceConfig.getString(ConfigurationKeys.DYNAMIC_CONFIG_GENERATOR_CLASS_KEY),
        TestDynamicConfigGenerator.class.getName());
  }

  /**
   * Test that the job cleanup call is called
   */
  @Test
  public void testJobCleanup() throws Exception {
    ContainerId containerId = ContainerId.newInstance(
        ApplicationAttemptId.newInstance(ApplicationId.newInstance(0, 0), 0), 0);
    TestApplicationMaster
        appMaster = Mockito.spy(new TestApplicationMaster("testApp", containerId, config,
        new YarnConfiguration()));

    GobblinHelixMultiManager mockMultiManager = Mockito.mock(GobblinHelixMultiManager.class);

    appMaster.setMultiManager(mockMultiManager);
    appMaster.start();

    Mockito.verify(mockMultiManager, times(1)).cleanUpJobs();
  }

  @Test
  public void testOutputConfig() throws IOException {
    File tmpTestDir = com.google.common.io.Files.createTempDir();

    try {
      Path outputPath = Paths.get(tmpTestDir.toString(), "application.conf");
      Config config = ConfigFactory.empty()
          .withValue(ConfigurationKeys.FS_URI_KEY, ConfigValueFactory.fromAnyRef("file:///"))
          .withValue(GobblinYarnAppLauncher.GOBBLIN_YARN_CONFIG_OUTPUT_PATH,
              ConfigValueFactory.fromAnyRef(outputPath.toString()));

      GobblinYarnAppLauncher.outputConfigToFile(config);

      String configString = com.google.common.io.Files.toString(outputPath.toFile(), Charsets.UTF_8);
      Assert.assertTrue(configString.contains("fs"));
    } finally {
      FileUtils.deleteDirectory(tmpTestDir);
    }
  }

  @Test
  public void testAddMetricReportingDynamicConfig()
      throws IOException {
    KafkaAvroSchemaRegistry schemaRegistry = Mockito.mock(KafkaAvroSchemaRegistry.class);
    Mockito.when(schemaRegistry.register(Mockito.any(Schema.class), Mockito.anyString())).thenAnswer(new Answer<String>() {
      @Override
      public String answer(InvocationOnMock invocation) {
        return "testId";
      }
    });
    Config config = ConfigFactory.empty().withValue(ConfigurationKeys.METRICS_KAFKA_TOPIC_EVENTS, ConfigValueFactory.fromAnyRef("topic"))
        .withValue(ConfigurationKeys.METRICS_REPORTING_KAFKA_ENABLED_KEY, ConfigValueFactory.fromAnyRef(true))
        .withValue(ConfigurationKeys.METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY, ConfigValueFactory.fromAnyRef(true))
        .withValue(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_URL, ConfigValueFactory.fromAnyRef("http://testSchemaReg:0000"));
    config = GobblinYarnAppLauncher.addMetricReportingDynamicConfig(config, schemaRegistry);
    Assert.assertEquals(config.getString(ConfigurationKeys.METRICS_REPORTING_EVENTS_KAFKA_AVRO_SCHEMA_ID), "testId");
    Assert.assertFalse(config.hasPath(ConfigurationKeys.METRICS_REPORTING_METRICS_KAFKA_AVRO_SCHEMA_ID));
  }

  /**
   * An application master for accessing protected fields in {@link GobblinApplicationMaster}
   * for testing.
   */
  private static class TestApplicationMaster extends GobblinApplicationMaster {
    public TestApplicationMaster(String applicationName, ContainerId containerId, Config config,
        YarnConfiguration yarnConfiguration)
        throws Exception {
      super(applicationName, containerId.getApplicationAttemptId().getApplicationId().toString(), containerId, config, yarnConfiguration);
    }

    @Override
    protected YarnService buildYarnService(Config config, String applicationName, String applicationId,
        YarnConfiguration yarnConfiguration, FileSystem fs) throws Exception {

      YarnService testYarnService = new TestYarnService(config, applicationName, applicationId, yarnConfiguration, fs,
          new EventBus("GobblinYarnAppLauncherTest"));

      return testYarnService;
    }

    public Config getConfig() {
      return this.config;
    }

    public ServiceBasedAppLauncher getAppLauncher() {
      return this.applicationLauncher;
    }

    public void setMultiManager(GobblinHelixMultiManager multiManager) {
      this.multiManager = multiManager;
    }
  }

  /**
   * Class for testing that dynamic config is injected
   */
  @VisibleForTesting
  public static class TestDynamicConfigGenerator implements DynamicConfigGenerator {
    public TestDynamicConfigGenerator() {
    }

    @Override
    public Config generateDynamicConfig(Config config) {
      return ConfigFactory.parseMap(ImmutableMap.of("dynamicKey1", "dynamicValue1"));
    }
  }

  /**
   * Test class for mocking out YarnService. Need to use this instead of Mockito because of final methods.
   */
  private static class TestYarnService extends YarnService {
    public TestYarnService(Config config, String applicationName, String applicationId, YarnConfiguration yarnConfiguration,
        FileSystem fs, EventBus eventBus) throws Exception {
      super(config, applicationName, applicationId, yarnConfiguration, fs, eventBus, null, null);
    }

    @Override
    protected void startUp()
        throws Exception {
      // do nothing
    }
  }
}
