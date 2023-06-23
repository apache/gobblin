package org.apache.gobblin.yarn;

import java.net.URL;
import java.util.Map;

import org.apache.commons.cli.Options;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.HelixProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ClusterConfig;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import junit.framework.TestCase;

import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.GobblinClusterManager;
import org.apache.gobblin.cluster.GobblinClusterManagerTest;
import org.apache.gobblin.cluster.GobblinHelixConstants;
import org.apache.gobblin.cluster.GobblinHelixMultiManager;
import org.apache.gobblin.cluster.HelixUtils;
import org.apache.gobblin.cluster.TestHelper;
import org.apache.gobblin.cluster.TestShutdownMessageHandlerFactory;
import org.apache.gobblin.util.JvmUtils;
import org.apache.gobblin.util.logs.Log4jConfigurationHelper;

import static org.mockito.Mockito.when;


public class GobblinApplicationMasterTest extends TestCase {

  GobblinHelixMultiManager mockMultiManager;

  public static final String HADOOP_OVERRIDE_PROPERTY_NAME = "prop";
  public final static Logger LOG = LoggerFactory.getLogger(GobblinClusterManagerTest.class);
  private TestingServer testingZKServer;


  @BeforeClass
  public void setUp() throws Exception {
    // Use a random ZK port
    this.testingZKServer = new TestingServer(-1);
    LOG.info("Testing ZK Server listening on: " + testingZKServer.getConnectString());
  }

  @Test
  public void testDisableTaskRunnersFromPreviousExecutions() throws Exception {
    this.mockMultiManager = Mockito.mock(GobblinHelixMultiManager.class);

    HelixManager mockHelixManager = Mockito.mock(HelixManager.class);
    when(mockMultiManager.getJobClusterHelixManager()).thenReturn(mockHelixManager);

    HelixAdmin mockHelixAdmin = Mockito.mock(HelixAdmin.class);
    when(mockHelixManager.getClusterManagmentTool()).thenReturn(mockHelixAdmin);
    when(mockHelixManager.getClusterName()).thenReturn("mockCluster");

    HelixDataAccessor mockAccessor = Mockito.mock(HelixDataAccessor.class);
    when(mockHelixManager.getHelixDataAccessor()).thenReturn(mockAccessor);

    PropertyKey.Builder mockBuilder = Mockito.mock(PropertyKey.Builder.class);
    when(mockAccessor.keyBuilder()).thenReturn(mockBuilder);

    PropertyKey mockLiveInstancesKey = Mockito.mock(PropertyKey.class);
    when(mockBuilder.liveInstances()).thenReturn(mockLiveInstancesKey);

    Map<String, HelixProperty> mockChildValuesMap = ImmutableMap.of(
        "GobblinYarnTaskRunner_TestInstance_0", Mockito.mock(HelixProperty.class),
        "GobblinYarnTaskRunner_TestInstance_1", Mockito.mock(HelixProperty.class),
        "GobblinYarnTaskRunner_TestInstance_2", Mockito.mock(HelixProperty.class)
    );
    when(mockAccessor.getChildValuesMap(mockLiveInstancesKey)).thenReturn(mockChildValuesMap);

    ConfigAccessor mockConfigAccessor = Mockito.mock(ConfigAccessor.class);
    when(mockHelixManager.getConfigAccessor()).thenReturn(mockConfigAccessor);

    ClusterConfig mockClusterConfig = Mockito.mock(ClusterConfig.class);
    when(mockConfigAccessor.getClusterConfig("GobblinApplicationMasterTest")).thenReturn(mockClusterConfig);

    URL url = GobblinApplicationMasterTest.class.getClassLoader().getResource(
        GobblinApplicationMasterTest.class.getSimpleName() + ".conf");
    Assert.assertNotNull(url, "Could not find resource " + url);

    Config config = ConfigFactory.parseURL(url)
        .withValue("gobblin.yarn.zk.connection.string",
            ConfigValueFactory.fromAnyRef(testingZKServer.getConnectString()))
        .withValue(GobblinClusterConfigurationKeys.HELIX_TASK_QUOTA_CONFIG_KEY,
            ConfigValueFactory.fromAnyRef("DEFAULT:1,OTHER:10"))
        .withValue(GobblinClusterConfigurationKeys.HADOOP_CONFIG_OVERRIDES_PREFIX + "." + HADOOP_OVERRIDE_PROPERTY_NAME,
            ConfigValueFactory.fromAnyRef("value"))
        .withValue(GobblinClusterConfigurationKeys.HADOOP_CONFIG_OVERRIDES_PREFIX + "." + "fs.file.impl.disable.cache",
            ConfigValueFactory.fromAnyRef("true"))
        .resolve();

    ContainerId mockContainerId = Mockito.mock(ContainerId.class);

    TestGobblinApplicationMaster testGobblinApplicationMaster = new TestGobblinApplicationMaster(
        GobblinApplicationMasterTest.class.getSimpleName(),
        TestHelper.TEST_APPLICATION_ID,
        mockContainerId,
        config, new YarnConfiguration());

    testGobblinApplicationMaster.start();

    for (String mockLiveInstance: mockChildValuesMap.keySet()) {
      Mockito.verify(mockHelixAdmin).enableInstance("mockCluster", mockLiveInstance, false);
    }
  }

  public class TestGobblinApplicationMaster extends GobblinApplicationMaster {

    public TestGobblinApplicationMaster(String applicationName, String applicationId, ContainerId containerId,
        Config config, YarnConfiguration yarnConfiguration)
        throws Exception {
      super(applicationName, applicationId, containerId, config, yarnConfiguration);
    }

    @Override
    public GobblinHelixMultiManager createMultiManager() {
      return mockMultiManager;
    }
  }

}