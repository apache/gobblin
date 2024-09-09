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
package org.apache.gobblin.service.modules.core;

import java.io.File;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;
import com.linkedin.data.template.StringMap;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.restli.client.RestLiResponseException;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.MysqlJobStatusStateStoreFactory;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.service.FlowConfig;
import org.apache.gobblin.service.FlowConfigV2Client;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.GobblinServiceManagerTest;
import org.apache.gobblin.service.Schedule;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.TestServiceDatabaseConfig;
import org.apache.gobblin.service.monitoring.FsJobStatusRetriever;


@Test
public class GobblinServiceHATest {

  private static final Logger logger = LoggerFactory.getLogger(GobblinServiceHATest.class);

  private static final String QUARTZ_INSTANCE_NAME = "org.quartz.scheduler.instanceName";
  private static final String QUARTZ_THREAD_POOL_COUNT = "org.quartz.threadPool.threadCount";

  private static final String COMMON_SPEC_STORE_PARENT_DIR = "/tmp/serviceCoreCommon/";

  private static final String NODE_1_SERVICE_WORK_DIR = "/tmp/serviceWorkDirNode1/";
  private static final String NODE_1_SPEC_STORE_PARENT_DIR = "/tmp/serviceCoreNode1/";
  private static final String NODE_1_TOPOLOGY_SPEC_STORE_DIR = "/tmp/serviceCoreNode1/topologyTestSpecStoreNode1";
  private static final String NODE_1_FLOW_SPEC_STORE_DIR = "/tmp/serviceCoreCommon/flowTestSpecStore";
  private static final String NODE_1_JOB_STATUS_STATE_STORE_DIR = "/tmp/serviceCoreNode1/fsJobStatusRetriever";

  private static final String NODE_2_SERVICE_WORK_DIR = "/tmp/serviceWorkDirNode2/";
  private static final String NODE_2_SPEC_STORE_PARENT_DIR = "/tmp/serviceCoreNode2/";
  private static final String NODE_2_TOPOLOGY_SPEC_STORE_DIR = "/tmp/serviceCoreNode2/topologyTestSpecStoreNode2";
  private static final String NODE_2_FLOW_SPEC_STORE_DIR = "/tmp/serviceCoreCommon/flowTestSpecStore";
  private static final String NODE_2_JOB_STATUS_STATE_STORE_DIR = "/tmp/serviceCoreNode2/fsJobStatusRetriever";

  private static final String TEST_GROUP_NAME_1 = "testGroup1";
  private static final String TEST_FLOW_NAME_1 = "testFlow1";
  private static final String TEST_SCHEDULE_1 = "0 1/0 * ? * *";
  private static final String TEST_TEMPLATE_URI_1 = "FS:///templates/test.template";
  private static final String TEST_DUMMY_GROUP_NAME_1 = "dummyGroup";
  private static final String TEST_DUMMY_FLOW_NAME_1 = "dummyFlow";

  private static final String TEST_GROUP_NAME_2 = "testGroup2";
  private static final String TEST_FLOW_NAME_2 = "testFlow2";
  private static final String TEST_SCHEDULE_2 = "0 1/0 * ? * *";
  private static final String TEST_TEMPLATE_URI_2 = "FS:///templates/test.template";

  private static final String TEST_GOBBLIN_EXECUTOR_NAME = "testGobblinExecutor";
  private static final String TEST_SOURCE_NAME = "testSource";
  private static final String TEST_SINK_NAME = "testSink";

  private GobblinServiceManager node1GobblinServiceManager;
  private FlowConfigV2Client node1FlowConfigClient;

  private GobblinServiceManager node2GobblinServiceManager;
  private FlowConfigV2Client node2FlowConfigClient;

  private TestingServer testingZKServer;

  private ITestMetastoreDatabase testMetastoreDatabase;
  private MySQLContainer<?> mysql;

  @BeforeClass
  public void setup() throws Exception {
    // Clean up common Flow Spec Dir
    cleanUpDir(COMMON_SPEC_STORE_PARENT_DIR);

    // Clean up work dir for Node 1
    cleanUpDir(NODE_1_SERVICE_WORK_DIR);
    cleanUpDir(NODE_1_SPEC_STORE_PARENT_DIR);

    // Clean up work dir for Node 2
    cleanUpDir(NODE_2_SERVICE_WORK_DIR);
    cleanUpDir(NODE_2_SPEC_STORE_PARENT_DIR);

    // Use a random ZK port
    this.testingZKServer = new TestingServer(-1);
    logger.info("Testing ZK Server listening on: " + testingZKServer.getConnectString());

    testMetastoreDatabase = TestMetastoreDatabaseFactory.get();

    Properties commonServiceCoreProperties = new Properties();

    mysql = new MySQLContainer<>("mysql:" + TestServiceDatabaseConfig.MysqlVersion);
    mysql.start();
    commonServiceCoreProperties.put(ServiceConfigKeys.SERVICE_DB_URL_KEY, mysql.getJdbcUrl());
    commonServiceCoreProperties.put(ServiceConfigKeys.SERVICE_DB_USERNAME, mysql.getUsername());
    commonServiceCoreProperties.put(ServiceConfigKeys.SERVICE_DB_PASSWORD, mysql.getPassword());

    commonServiceCoreProperties.put(ServiceConfigKeys.TOPOLOGY_FACTORY_TOPOLOGY_NAMES_KEY , TEST_GOBBLIN_EXECUTOR_NAME);
    commonServiceCoreProperties.put(ServiceConfigKeys.TOPOLOGY_FACTORY_PREFIX +  TEST_GOBBLIN_EXECUTOR_NAME + ".description",
        "StandaloneTestExecutor");
    commonServiceCoreProperties.put(ServiceConfigKeys.TOPOLOGY_FACTORY_PREFIX +  TEST_GOBBLIN_EXECUTOR_NAME + ".version",
        "1");
    commonServiceCoreProperties.put(ServiceConfigKeys.TOPOLOGY_FACTORY_PREFIX +  TEST_GOBBLIN_EXECUTOR_NAME + ".uri",
        "gobblinExecutor");
    commonServiceCoreProperties.put(ServiceConfigKeys.TOPOLOGY_FACTORY_PREFIX +  TEST_GOBBLIN_EXECUTOR_NAME + ".specExecutorInstance",
        "org.gobblin.service.InMemorySpecExecutor");
    commonServiceCoreProperties.put(ServiceConfigKeys.TOPOLOGY_FACTORY_PREFIX +  TEST_GOBBLIN_EXECUTOR_NAME + ".specExecInstance.capabilities",
        TEST_SOURCE_NAME + ":" + TEST_SINK_NAME);
    commonServiceCoreProperties.put(ConfigurationKeys.STATE_STORE_DB_USER_KEY, "testUser");
    commonServiceCoreProperties.put(ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, "testPassword");
    commonServiceCoreProperties.put(ConfigurationKeys.STATE_STORE_DB_URL_KEY, testMetastoreDatabase.getJdbcUrl());
    commonServiceCoreProperties.put("zookeeper.connect", testingZKServer.getConnectString());
    commonServiceCoreProperties.put(ConfigurationKeys.STATE_STORE_FACTORY_CLASS_KEY, MysqlJobStatusStateStoreFactory.class.getName());
    commonServiceCoreProperties.put(ServiceConfigKeys.GOBBLIN_SERVICE_JOB_STATUS_MONITOR_ENABLED_KEY, false);
    commonServiceCoreProperties.put(ServiceConfigKeys.GOBBLIN_SERVICE_GIT_CONFIG_MONITOR_ENABLED_KEY, false);

    Properties node1ServiceCoreProperties = new Properties();
    node1ServiceCoreProperties.putAll(commonServiceCoreProperties);
    node1ServiceCoreProperties.put(ConfigurationKeys.TOPOLOGYSPEC_STORE_DIR_KEY, NODE_1_TOPOLOGY_SPEC_STORE_DIR);
    node1ServiceCoreProperties.put(FlowCatalog.FLOWSPEC_STORE_DIR_KEY, NODE_1_FLOW_SPEC_STORE_DIR);
    node1ServiceCoreProperties.put(FsJobStatusRetriever.CONF_PREFIX + "." + ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, NODE_1_JOB_STATUS_STATE_STORE_DIR);
    node1ServiceCoreProperties.put(QUARTZ_INSTANCE_NAME, "QuartzScheduler1");
    node1ServiceCoreProperties.put(QUARTZ_THREAD_POOL_COUNT, 3);

    Properties node2ServiceCoreProperties = new Properties();
    node2ServiceCoreProperties.putAll(commonServiceCoreProperties);
    node2ServiceCoreProperties.put(ConfigurationKeys.TOPOLOGYSPEC_STORE_DIR_KEY, NODE_2_TOPOLOGY_SPEC_STORE_DIR);
    node2ServiceCoreProperties.put(FlowCatalog.FLOWSPEC_STORE_DIR_KEY, NODE_2_FLOW_SPEC_STORE_DIR);
    node2ServiceCoreProperties.put(FsJobStatusRetriever.CONF_PREFIX + "." + ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, NODE_2_JOB_STATUS_STATE_STORE_DIR);
    node2ServiceCoreProperties.put(QUARTZ_INSTANCE_NAME, "QuartzScheduler2");
    node2ServiceCoreProperties.put(QUARTZ_THREAD_POOL_COUNT, 3);

    // Start Node 1
    this.node1GobblinServiceManager = GobblinServiceManagerTest.createTestGobblinServiceManager(
        node1ServiceCoreProperties, "CoreService1", "1", NODE_1_SERVICE_WORK_DIR, testMetastoreDatabase);
    this.node1GobblinServiceManager.start();

    // Start Node 2
    this.node2GobblinServiceManager = GobblinServiceManagerTest.createTestGobblinServiceManager(
        node2ServiceCoreProperties, "CoreService2", "2", NODE_2_SERVICE_WORK_DIR, testMetastoreDatabase);
    this.node2GobblinServiceManager.start();

    // Initialize Node 1 Client
    Map<String, String> transportClientProperties = Maps.newHashMap();
    transportClientProperties.put(HttpClientFactory.HTTP_REQUEST_TIMEOUT, "10000");
    this.node1FlowConfigClient = new FlowConfigV2Client(String.format("http://localhost:%s/",
        this.node1GobblinServiceManager.restliServer.getPort()), transportClientProperties);

    // Initialize Node 2 Client
    this.node2FlowConfigClient = new FlowConfigV2Client(String.format("http://localhost:%s/",
        this.node2GobblinServiceManager.restliServer.getPort()), transportClientProperties);
  }

  private void cleanUpDir(String dir) throws Exception {
    File specStoreDir = new File(dir);
    if (specStoreDir.exists()) {
      FileUtils.deleteDirectory(specStoreDir);
    }
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() throws Exception {
    // Shutdown Node 1
    try {
      logger.info("+++++++++++++++++++ start shutdown noad1");
      this.node1GobblinServiceManager.stop();
    } catch (Exception e) {
      logger.warn("Could not cleanly stop Node 1 of Gobblin Service", e);
    }

    // Shutdown Node 2
    try {
      logger.info("+++++++++++++++++++ start shutdown noad2");
      this.node2GobblinServiceManager.stop();
    } catch (Exception e) {
      logger.warn("Could not cleanly stop Node 2 of Gobblin Service", e);
    }

    // Stop Zookeeper
    try {
      this.testingZKServer.close();
    } catch (Exception e) {
      logger.warn("Could not cleanly stop Testing Zookeeper", e);
    }

    // Cleanup Node 1
    try {
      cleanUpDir(NODE_1_SERVICE_WORK_DIR);
    } catch (Exception e) {
      logger.warn("Could not completely cleanup Node 1 Work Dir");
    }
    try {
      cleanUpDir(NODE_1_SPEC_STORE_PARENT_DIR);
    } catch (Exception e) {
      logger.warn("Could not completely cleanup Node 1 Spec Store Parent Dir");
    }

    // Cleanup Node 2
    try {
      cleanUpDir(NODE_2_SERVICE_WORK_DIR);
    } catch (Exception e) {
      logger.warn("Could not completely cleanup Node 2 Work Dir");
    }
    try {
      cleanUpDir(NODE_2_SPEC_STORE_PARENT_DIR);
    } catch (Exception e) {
      logger.warn("Could not completely cleanup Node 2 Spec Store Parent Dir");
    }

    cleanUpDir(COMMON_SPEC_STORE_PARENT_DIR);

    try {
      mysql.stop();
    } catch (Exception e) {
      logger.warn("Could not completely stop Mysql");
    }

    // `.close()` to avoid (in the aggregate, across multiple suites) - java.sql.SQLNonTransientConnectionException: Too many connections
    testMetastoreDatabase.close();
  }

  @Test
  public void testCreate() throws Exception {
    logger.info("+++++++++++++++++++ testCreate START");
    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1");
    flowProperties.put(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY, TEST_SOURCE_NAME);
    flowProperties.put(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, TEST_SINK_NAME);

    FlowConfig flowConfig1 = new FlowConfig()
        .setId(new FlowId().setFlowGroup(TEST_GROUP_NAME_1).setFlowName(TEST_FLOW_NAME_1))
        .setTemplateUris(TEST_TEMPLATE_URI_1).setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE_1).
            setRunImmediately(true))
        .setProperties(new StringMap(flowProperties));
    FlowConfig flowConfig2 = new FlowConfig()
        .setId(new FlowId().setFlowGroup(TEST_GROUP_NAME_2).setFlowName(TEST_FLOW_NAME_2))
        .setTemplateUris(TEST_TEMPLATE_URI_2).setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE_2).
            setRunImmediately(true))
        .setProperties(new StringMap(flowProperties));

    // Try create on both nodes
    this.node1FlowConfigClient.createFlowConfig(flowConfig1);
    this.node2FlowConfigClient.createFlowConfig(flowConfig2);

    Assert.assertEquals(2, this.node1GobblinServiceManager.getFlowCatalog().getSpecs().size());
    Assert.assertEquals(2, this.node2GobblinServiceManager.getFlowCatalog().getSpecs().size());
    logger.info("+++++++++++++++++++ testCreate END");
  }

  @Test (dependsOnMethods = "testCreate", expectedExceptions = RestLiResponseException.class)
  public void testCreateAgainFlow1() throws Exception {
    logger.info("+++++++++++++++++++ testCreateAgain START");
    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1");
    flowProperties.put(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY, TEST_SOURCE_NAME);
    flowProperties.put(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, TEST_SINK_NAME);

    FlowConfig flowConfig1 = new FlowConfig()
        .setId(new FlowId().setFlowGroup(TEST_GROUP_NAME_1).setFlowName(TEST_FLOW_NAME_1))
        .setTemplateUris(TEST_TEMPLATE_URI_1).setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE_1).
            setRunImmediately(true))
        .setProperties(new StringMap(flowProperties));

    // Try to create on both nodes
    try {
      this.node1FlowConfigClient.createFlowConfig(flowConfig1);
    } catch (RestLiResponseException e) {
      this.node2FlowConfigClient.createFlowConfig(flowConfig1);
    }

    Assert.fail("Create Again should fail without complaining that the spec already exists.");
  }

  @Test (dependsOnMethods = "testCreate", expectedExceptions = RestLiResponseException.class)
  public void testCreateAgainFlow2() throws Exception {
    logger.info("+++++++++++++++++++ testCreateAgain START");
    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1");
    flowProperties.put(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY, TEST_SOURCE_NAME);
    flowProperties.put(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, TEST_SINK_NAME);

    FlowConfig flowConfig2 = new FlowConfig()
        .setId(new FlowId().setFlowGroup(TEST_GROUP_NAME_2).setFlowName(TEST_FLOW_NAME_2))
        .setTemplateUris(TEST_TEMPLATE_URI_2).setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE_2).
            setRunImmediately(true))
        .setProperties(new StringMap(flowProperties));

    // Try to create on both nodes
    try {
      this.node1FlowConfigClient.createFlowConfig(flowConfig2);
    } catch (RestLiResponseException e) {
      this.node2FlowConfigClient.createFlowConfig(flowConfig2);
    }

    Assert.fail("Create Again should fail without complaining that the spec already exists.");
  }

  @Test (dependsOnMethods = {"testCreateAgainFlow1", "testCreateAgainFlow2"})
  public void testGet() throws Exception {
    logger.info("+++++++++++++++++++ testGet START");
    FlowId flowId1 = new FlowId().setFlowGroup(TEST_GROUP_NAME_1).setFlowName(TEST_FLOW_NAME_1);

    FlowConfig flowConfig1 = this.node1FlowConfigClient.getFlowConfig(flowId1);
    Assert.assertEquals(flowConfig1.getId().getFlowGroup(), TEST_GROUP_NAME_1);
    Assert.assertEquals(flowConfig1.getId().getFlowName(), TEST_FLOW_NAME_1);
    Assert.assertEquals(flowConfig1.getSchedule().getCronSchedule(), TEST_SCHEDULE_1);
    Assert.assertEquals(flowConfig1.getTemplateUris(), TEST_TEMPLATE_URI_1);
    Assert.assertTrue(flowConfig1.getSchedule().isRunImmediately());
    Assert.assertEquals(flowConfig1.getProperties().get("param1"), "value1");

    flowConfig1 = this.node2FlowConfigClient.getFlowConfig(flowId1);
    Assert.assertEquals(flowConfig1.getId().getFlowGroup(), TEST_GROUP_NAME_1);
    Assert.assertEquals(flowConfig1.getId().getFlowName(), TEST_FLOW_NAME_1);
    Assert.assertEquals(flowConfig1.getSchedule().getCronSchedule(), TEST_SCHEDULE_1);
    Assert.assertEquals(flowConfig1.getTemplateUris(), TEST_TEMPLATE_URI_1);
    Assert.assertTrue(flowConfig1.getSchedule().isRunImmediately());
    Assert.assertEquals(flowConfig1.getProperties().get("param1"), "value1");

    logger.info("+++++++++++++++++++ testGet END");
  }

  @Test (dependsOnMethods = "testGet")
  public void testUpdate() throws Exception {
    logger.info("+++++++++++++++++++ testUpdate START");
    // Update on one node and retrieve from another
    FlowId flowId = new FlowId().setFlowGroup(TEST_GROUP_NAME_1).setFlowName(TEST_FLOW_NAME_1);

    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1b");
    flowProperties.put("param2", "value2b");
    flowProperties.put(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY, TEST_SOURCE_NAME);
    flowProperties.put(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, TEST_SINK_NAME);

    FlowConfig flowConfig = new FlowConfig()
        .setId(new FlowId().setFlowGroup(TEST_GROUP_NAME_1).setFlowName(TEST_FLOW_NAME_1))
        .setTemplateUris(TEST_TEMPLATE_URI_1).setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE_1))
        .setProperties(new StringMap(flowProperties));

    this.node1FlowConfigClient.updateFlowConfig(flowConfig);

    FlowConfig retrievedFlowConfig = this.node2FlowConfigClient.getFlowConfig(flowId);

    Assert.assertEquals(retrievedFlowConfig.getId().getFlowGroup(), TEST_GROUP_NAME_1);
    Assert.assertEquals(retrievedFlowConfig.getId().getFlowName(), TEST_FLOW_NAME_1);
    Assert.assertEquals(Objects.requireNonNull(retrievedFlowConfig.getSchedule()).getCronSchedule(), TEST_SCHEDULE_1);
    Assert.assertEquals(retrievedFlowConfig.getTemplateUris(), TEST_TEMPLATE_URI_1);
    Assert.assertFalse(retrievedFlowConfig.getSchedule().isRunImmediately());
    Assert.assertEquals(retrievedFlowConfig.getProperties().get("param1"), "value1b");
    Assert.assertEquals(retrievedFlowConfig.getProperties().get("param2"), "value2b");

    logger.info("+++++++++++++++++++ testUpdate END");
  }

  @Test (dependsOnMethods = "testUpdate")
  public void testDelete() throws Exception {
    logger.info("+++++++++++++++++++ testDelete START");
    FlowId flowId = new FlowId().setFlowGroup(TEST_GROUP_NAME_1).setFlowName(TEST_FLOW_NAME_1);

    // make sure flow config exists
    FlowConfig flowConfig = this.node1FlowConfigClient.getFlowConfig(flowId);
    Assert.assertEquals(flowConfig.getId().getFlowGroup(), TEST_GROUP_NAME_1);
    Assert.assertEquals(flowConfig.getId().getFlowName(), TEST_FLOW_NAME_1);

    this.node1FlowConfigClient.deleteFlowConfig(flowId);

    // Check if deletion is reflected on both nodes
    try {
      this.node1FlowConfigClient.getFlowConfig(flowId);
      Assert.fail("Get should have gotten a 404 error");
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.NOT_FOUND_404);
    }

    try {
      this.node2FlowConfigClient.getFlowConfig(flowId);
      Assert.fail("Get should have gotten a 404 error");
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.NOT_FOUND_404);
    }

    logger.info("+++++++++++++++++++ testDelete END");
  }

  @Test (dependsOnMethods = "testDelete")
  public void testBadGet() throws Exception {
    logger.info("+++++++++++++++++++ testBadGet START");
    FlowId flowId = new FlowId().setFlowGroup(TEST_DUMMY_GROUP_NAME_1).setFlowName(TEST_DUMMY_FLOW_NAME_1);

    try {
      this.node1FlowConfigClient.getFlowConfig(flowId);
      Assert.fail("Get should have raised a 404 error");
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.NOT_FOUND_404);
    }

    try {
      this.node2FlowConfigClient.getFlowConfig(flowId);
      Assert.fail("Get should have raised a 404 error");
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.NOT_FOUND_404);
    }

    logger.info("+++++++++++++++++++ testBadGet END");
  }

  @Test (dependsOnMethods = "testBadGet")
  public void testBadDelete() throws Exception {
    logger.info("+++++++++++++++++++ testBadDelete START");
    FlowId flowId = new FlowId().setFlowGroup(TEST_DUMMY_GROUP_NAME_1).setFlowName(TEST_DUMMY_FLOW_NAME_1);

    try {
      this.node1FlowConfigClient.getFlowConfig(flowId);
      Assert.fail("Get should have raised a 404 error");
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.NOT_FOUND_404);
    }

    try {
      this.node2FlowConfigClient.getFlowConfig(flowId);
      Assert.fail("Get should have raised a 404 error");
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.NOT_FOUND_404);
    }

    logger.info("+++++++++++++++++++ testBadDelete END");
  }

  @Test (dependsOnMethods = "testBadDelete")
  public void testBadUpdate() throws Exception {
    logger.info("+++++++++++++++++++ testBadUpdate START");
    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1b");
    flowProperties.put("param2", "value2b");

    FlowConfig flowConfig = new FlowConfig()
        .setId(new FlowId().setFlowGroup(TEST_DUMMY_GROUP_NAME_1).setFlowName(TEST_DUMMY_FLOW_NAME_1))
        .setTemplateUris(TEST_TEMPLATE_URI_1).setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE_1))
        .setProperties(new StringMap(flowProperties));

    try {
      this.node1FlowConfigClient.updateFlowConfig(flowConfig);
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.NOT_FOUND_404);
    }

    try {
      this.node2FlowConfigClient.updateFlowConfig(flowConfig);
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.NOT_FOUND_404);
    }

    logger.info("+++++++++++++++++++ testBadUpdate END");
  }

  @Test (dependsOnMethods = "testBadUpdate")
  public void testKillNode() throws Exception {
    logger.info("+++++++++++++++++++ testKillNode START");

    int initialNode1Jobs = this.node1GobblinServiceManager.getFlowCatalog().getSpecs().size();
    Assert.assertEquals(initialNode1Jobs, 1, "We have deleted oen of the two jobs we created in the tests.");

    // Stop node1
    this.node1GobblinServiceManager.stop();

    int initialNode2Jobs = this.node2GobblinServiceManager.getFlowCatalog().getSpecs().size();
    Assert.assertEquals(initialNode2Jobs, 1, "We have deleted oen of the two jobs we created in the tests.");

    logger.info("+++++++++++++++++++ testKillNode END");
  }
}