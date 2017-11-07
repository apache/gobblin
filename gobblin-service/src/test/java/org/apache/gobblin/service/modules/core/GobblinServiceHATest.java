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

import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.Schedule;
import java.io.File;
import java.util.Map;
import java.util.Properties;

import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.fs.Path;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.linkedin.data.template.StringMap;
import com.linkedin.restli.client.RestLiResponseException;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.runtime.app.ServiceBasedAppLauncher;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.runtime.spec_catalog.TopologyCatalog;
import org.apache.gobblin.service.FlowConfig;
import org.apache.gobblin.service.FlowConfigClient;
import org.apache.gobblin.service.modules.utils.HelixUtils;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.orchestration.Orchestrator;
import org.apache.gobblin.util.ConfigUtils;


public class GobblinServiceHATest {

  private static final Logger logger = LoggerFactory.getLogger(GobblinServiceHATest.class);
  private static Gson gson = new GsonBuilder().setPrettyPrinting().create();

  private static final String COMMON_SPEC_STORE_PARENT_DIR = "/tmp/serviceCoreCommon/";

  private static final String NODE_1_SERVICE_WORK_DIR = "/tmp/serviceWorkDirNode1/";
  private static final String NODE_1_SPEC_STORE_PARENT_DIR = "/tmp/serviceCoreNode1/";
  private static final String NODE_1_TOPOLOGY_SPEC_STORE_DIR = "/tmp/serviceCoreNode1/topologyTestSpecStoreNode1";
  private static final String NODE_1_FLOW_SPEC_STORE_DIR = "/tmp/serviceCoreCommon/flowTestSpecStore";

  private static final String NODE_2_SERVICE_WORK_DIR = "/tmp/serviceWorkDirNode2/";
  private static final String NODE_2_SPEC_STORE_PARENT_DIR = "/tmp/serviceCoreNode2/";
  private static final String NODE_2_TOPOLOGY_SPEC_STORE_DIR = "/tmp/serviceCoreNode2/topologyTestSpecStoreNode2";
  private static final String NODE_2_FLOW_SPEC_STORE_DIR = "/tmp/serviceCoreCommon/flowTestSpecStore";

  private static final String TEST_HELIX_CLUSTER_NAME = "testGobblinServiceCluster";

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
  private static final String TEST_DUMMY_GROUP_NAME_2 = "dummyGroup";
  private static final String TEST_DUMMY_FLOW_NAME_2 = "dummyFlow";

  private static final String TEST_GOBBLIN_EXECUTOR_NAME = "testGobblinExecutor";
  private static final String TEST_SOURCE_NAME = "testSource";
  private static final String TEST_SINK_NAME = "testSink";

  private ServiceBasedAppLauncher serviceLauncher;
  private TopologyCatalog topologyCatalog;
  private TopologySpec topologySpec;

  private FlowCatalog flowCatalog;
  private FlowSpec flowSpec;

  private Orchestrator orchestrator;

  private GobblinServiceManager node1GobblinServiceManager;
  private FlowConfigClient node1FlowConfigClient;

  private GobblinServiceManager node2GobblinServiceManager;
  private FlowConfigClient node2FlowConfigClient;

  private TestingServer testingZKServer;

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
    HelixUtils.createGobblinHelixCluster(testingZKServer.getConnectString(), TEST_HELIX_CLUSTER_NAME);

    Properties commonServiceCoreProperties = new Properties();
    commonServiceCoreProperties.put(ServiceConfigKeys.ZK_CONNECTION_STRING_KEY, testingZKServer.getConnectString());
    commonServiceCoreProperties.put(ServiceConfigKeys.HELIX_CLUSTER_NAME_KEY, TEST_HELIX_CLUSTER_NAME);
    commonServiceCoreProperties.put(ServiceConfigKeys.HELIX_INSTANCE_NAME_KEY, "GaaS_" + UUID.randomUUID().toString());
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

    Properties node1ServiceCoreProperties = new Properties();
    node1ServiceCoreProperties.putAll(commonServiceCoreProperties);
    node1ServiceCoreProperties.put(ConfigurationKeys.TOPOLOGYSPEC_STORE_DIR_KEY, NODE_1_TOPOLOGY_SPEC_STORE_DIR);
    node1ServiceCoreProperties.put(ConfigurationKeys.FLOWSPEC_STORE_DIR_KEY, NODE_1_FLOW_SPEC_STORE_DIR);

    Properties node2ServiceCoreProperties = new Properties();
    node2ServiceCoreProperties.putAll(commonServiceCoreProperties);
    node2ServiceCoreProperties.put(ConfigurationKeys.TOPOLOGYSPEC_STORE_DIR_KEY, NODE_2_TOPOLOGY_SPEC_STORE_DIR);
    node2ServiceCoreProperties.put(ConfigurationKeys.FLOWSPEC_STORE_DIR_KEY, NODE_2_FLOW_SPEC_STORE_DIR);

    // Start Node 1
    this.node1GobblinServiceManager = new GobblinServiceManager("CoreService", "1",
        ConfigUtils.propertiesToConfig(node1ServiceCoreProperties), Optional.of(new Path(NODE_1_SERVICE_WORK_DIR)));
    this.node1GobblinServiceManager.start();

    // Start Node 2
    this.node2GobblinServiceManager = new GobblinServiceManager("CoreService", "1",
        ConfigUtils.propertiesToConfig(node2ServiceCoreProperties), Optional.of(new Path(NODE_2_SERVICE_WORK_DIR)));
    this.node2GobblinServiceManager.start();

    // Initialize Node 1 Client
    this.node1FlowConfigClient = new FlowConfigClient(String.format("http://localhost:%s/",
        this.node1GobblinServiceManager.restliServer.getPort()));

    // Initialize Node 2 Client
    this.node2FlowConfigClient = new FlowConfigClient(String.format("http://localhost:%s/",
        this.node2GobblinServiceManager.restliServer.getPort()));
  }

  private void cleanUpDir(String dir) throws Exception {
    File specStoreDir = new File(dir);
    if (specStoreDir.exists()) {
      FileUtils.deleteDirectory(specStoreDir);
    }
  }

  @AfterClass
  public void cleanUp() throws Exception {
    // Shutdown Node 1
    try {
      this.node1GobblinServiceManager.stop();
    } catch (Exception e) {
      logger.warn("Could not cleanly stop Node 1 of Gobblin Service", e);
    }

    // Shutdown Node 2
    try {
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
  }

  @Test
  public void testCreate() throws Exception {
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
    long schedulingStartTime = System.currentTimeMillis();
    this.node1FlowConfigClient.createFlowConfig(flowConfig1);
    this.node2FlowConfigClient.createFlowConfig(flowConfig2);

    // Check if created on master
    GobblinServiceManager master;
    if (this.node1GobblinServiceManager.isLeader()) {
      master = this.node1GobblinServiceManager;
    } else if (this.node2GobblinServiceManager.isLeader()) {
      master = this.node2GobblinServiceManager;
    } else {
      Assert.fail("No leader found in service cluster");
      return;
    }

    int attempt = 0;
    boolean assertSuccess = false;
    while (attempt < 800) {
      int masterJobs = master.flowCatalog.getSpecs().size();
      if (masterJobs == 2) {
        assertSuccess = true;
        break;
      }
      Thread.sleep(5);
      attempt ++;
    }
    long schedulingEndTime = System.currentTimeMillis();
    logger.info("Total scheduling time in ms: " + (schedulingEndTime - schedulingStartTime));

    Assert.assertTrue(assertSuccess, "Flow that was created is not reflecting in FlowCatalog");
  }

  @Test (dependsOnMethods = "testCreate")
  public void testCreateAgain() throws Exception {
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
    try {
      this.node1FlowConfigClient.createFlowConfig(flowConfig1);
    } catch (RestLiResponseException e) {
      Assert.fail("Create Again should pass without complaining that the spec already exists.");
    }

    try {
      this.node2FlowConfigClient.createFlowConfig(flowConfig2);
    } catch (RestLiResponseException e) {
      Assert.fail("Create Again should pass without complaining that the spec already exists.");
    }
  }

  @Test (dependsOnMethods = "testCreateAgain")
  public void testGet() throws Exception {
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
  }

  @Test (dependsOnMethods = "testGet")
  public void testUpdate() throws Exception {
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
    Assert.assertEquals(retrievedFlowConfig.getSchedule().getCronSchedule(), TEST_SCHEDULE_1);
    Assert.assertEquals(retrievedFlowConfig.getTemplateUris(), TEST_TEMPLATE_URI_1);
    Assert.assertFalse(retrievedFlowConfig.getSchedule().isRunImmediately());
    Assert.assertEquals(retrievedFlowConfig.getProperties().get("param1"), "value1b");
    Assert.assertEquals(retrievedFlowConfig.getProperties().get("param2"), "value2b");
  }

  @Test (dependsOnMethods = "testUpdate")
  public void testDelete() throws Exception {
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
  }

  @Test (dependsOnMethods = "testDelete")
  public void testBadGet() throws Exception {
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
  }

  @Test (dependsOnMethods = "testBadGet")
  public void testBadDelete() throws Exception {
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
  }

  @Test (dependsOnMethods = "testBadDelete")
  public void testBadUpdate() throws Exception {
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
      Assert.fail("Bad update should pass without complaining that the spec does not exists.");
    }

    try {
      this.node2FlowConfigClient.updateFlowConfig(flowConfig);
    } catch (RestLiResponseException e) {
      Assert.fail("Bad update should pass without complaining that the spec does not exists.");
    }
  }

  @Test (dependsOnMethods = "testBadUpdate")
  public void testKillNode() throws Exception {
    GobblinServiceManager master, secondary;
    if (this.node1GobblinServiceManager.isLeader()) {
      master = this.node1GobblinServiceManager;
      secondary = this.node2GobblinServiceManager;
    } else {
      master = this.node2GobblinServiceManager;
      secondary = this.node1GobblinServiceManager;
    }

    int initialMasterJobs = master.getScheduler().getScheduledFlowSpecs().size();
    int initialSecondaryJobs = secondary.getScheduler().getScheduledFlowSpecs().size();

    Assert.assertTrue(initialMasterJobs > 0, "Master initially should have a few jobs by now in test suite.");
    Assert.assertTrue(initialSecondaryJobs == 0, "Secondary node should not schedule any jobs initially.");

    // Stop current master
    long failOverStartTime = System.currentTimeMillis();
    master.stop();

    // Wait until secondary becomes master, max 4 seconds
    int attempt = 0;
    while (!secondary.isLeader()) {
      if (attempt > 800) {
        Assert.fail("Timeout waiting for Secondary to become master.");
      }
      Thread.sleep(5);
      attempt ++;
    }
    long failOverOwnerShipTransferTime = System.currentTimeMillis();

    attempt = 0;
    boolean assertSuccess = false;
    while (attempt < 800) {
      int newMasterJobs = secondary.getScheduler().getScheduledFlowSpecs().size();
      if (newMasterJobs == initialMasterJobs) {
        assertSuccess = true;
        break;
      }
      Thread.sleep(5);
      attempt ++;
    }
    long failOverEndTime = System.currentTimeMillis();
    logger.info("Total ownership transfer time in ms: " + (failOverOwnerShipTransferTime - failOverStartTime));
    logger.info("Total rescheduling time in ms: " + (failOverEndTime - failOverOwnerShipTransferTime));
    logger.info("Total failover time in ms: " + (failOverEndTime - failOverStartTime));

    Assert.assertTrue(assertSuccess, "New master should take over all old master jobs.");
  }
}