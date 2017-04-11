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

package gobblin.service.modules.core;

import java.io.File;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
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

import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.api.FlowSpec;
import gobblin.runtime.api.TopologySpec;
import gobblin.runtime.app.ServiceBasedAppLauncher;
import gobblin.runtime.spec_catalog.FlowCatalog;
import gobblin.runtime.spec_catalog.TopologyCatalog;
import gobblin.service.FlowConfig;
import gobblin.service.FlowConfigClient;
import gobblin.service.FlowConfigId;
import gobblin.service.modules.orchestration.Orchestrator;
import gobblin.util.ConfigUtils;


public class GobblinServiceManagerTest {

  private static final Logger logger = LoggerFactory.getLogger(TopologyCatalog.class);
  private static Gson gson = new GsonBuilder().setPrettyPrinting().create();

  private static final String SERVICE_WORK_DIR = "/tmp/serviceWorkDir/";
  private static final String SPEC_STORE_PARENT_DIR = "/tmp/serviceCore/";
  private static final String SPEC_DESCRIPTION = "Test ServiceCore";
  private static final String SPEC_VERSION = "1";
  private static final String TOPOLOGY_SPEC_STORE_DIR = "/tmp/serviceCore/topologyTestSpecStore";
  private static final String FLOW_SPEC_STORE_DIR = "/tmp/serviceCore/flowTestSpecStore";

  private static final String TEST_GROUP_NAME = "testGroup1";
  private static final String TEST_FLOW_NAME = "testFlow1";
  private static final String TEST_SCHEDULE = "";
  private static final String TEST_TEMPLATE_URI = "FS:///templates/test.template";
  private static final String TEST_DUMMY_GROUP_NAME = "dummyGroup";
  private static final String TEST_DUMMY_FLOW_NAME = "dummyFlow";

  private ServiceBasedAppLauncher serviceLauncher;
  private TopologyCatalog topologyCatalog;
  private TopologySpec topologySpec;

  private FlowCatalog flowCatalog;
  private FlowSpec flowSpec;

  private Orchestrator orchestrator;

  private GobblinServiceManager gobblinServiceManager;
  private FlowConfigClient flowConfigClient;

  @BeforeClass
  public void setup() throws Exception {
    cleanUpDir(SERVICE_WORK_DIR);
    cleanUpDir(SPEC_STORE_PARENT_DIR);

    Properties serviceCoreProperties = new Properties();
    serviceCoreProperties.put(ConfigurationKeys.TOPOLOGYSPEC_STORE_DIR_KEY, TOPOLOGY_SPEC_STORE_DIR);
    serviceCoreProperties.put(ConfigurationKeys.FLOWSPEC_STORE_DIR_KEY, FLOW_SPEC_STORE_DIR);

    this.gobblinServiceManager = new GobblinServiceManager("CoreService", "1",
        ConfigUtils.propertiesToConfig(serviceCoreProperties), Optional.of(new Path(SERVICE_WORK_DIR)));
    this.gobblinServiceManager.start();

    this.flowConfigClient = new FlowConfigClient(String.format("http://localhost:%s/",
        this.gobblinServiceManager.restliServer.getPort()));
  }

  private void cleanUpDir(String dir) throws Exception {
    File specStoreDir = new File(dir);
    if (specStoreDir.exists()) {
      FileUtils.deleteDirectory(specStoreDir);
    }
  }

  @AfterClass
  public void cleanUp() throws Exception {
    // Shutdown Service
    this.gobblinServiceManager.stop();

    cleanUpDir(SERVICE_WORK_DIR);
    cleanUpDir(SPEC_STORE_PARENT_DIR);
  }

  @Test
  public void testCreate() throws Exception {
    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1");

    FlowConfig flowConfig = new FlowConfig().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME)
        .setTemplateUris(TEST_TEMPLATE_URI).setSchedule(TEST_SCHEDULE).setRunImmediately(true)
        .setProperties(new StringMap(flowProperties));

    this.flowConfigClient.createFlowConfig(flowConfig);
    Assert.assertTrue(this.gobblinServiceManager.flowCatalog.getSpecs().size() == 1, "Flow that was created is not "
        + "reflecting in FlowCatalog");
  }

  @Test (dependsOnMethods = "testCreate")
  public void testCreateAgain() throws Exception {
    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1");

    FlowConfig flowConfig = new FlowConfig().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME)
        .setTemplateUris(TEST_TEMPLATE_URI).setSchedule(TEST_SCHEDULE).setProperties(new StringMap(flowProperties));

    try {
      this.flowConfigClient.createFlowConfig(flowConfig);
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.CONFLICT_409);
      return;
    }

    Assert.fail("Get should have gotten a 409 error");
  }

  @Test (dependsOnMethods = "testCreateAgain")
  public void testGet() throws Exception {
    FlowConfigId flowConfigId = new FlowConfigId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME);
    FlowConfig flowConfig = this.flowConfigClient.getFlowConfig(flowConfigId);

    Assert.assertEquals(flowConfig.getFlowGroup(), TEST_GROUP_NAME);
    Assert.assertEquals(flowConfig.getFlowName(), TEST_FLOW_NAME);
    Assert.assertEquals(flowConfig.getSchedule(), TEST_SCHEDULE );
    Assert.assertEquals(flowConfig.getTemplateUris(), TEST_TEMPLATE_URI);
    Assert.assertTrue(flowConfig.hasRunImmediately());
    Assert.assertTrue(flowConfig.isRunImmediately());
    // Add this asssert back when getFlowSpec() is changed to return the raw flow spec
    //Assert.assertEquals(flowConfig.getProperties().size(), 1);
    Assert.assertEquals(flowConfig.getProperties().get("param1"), "value1");
  }

  @Test (dependsOnMethods = "testGet")
  public void testUpdate() throws Exception {
    FlowConfigId flowConfigId = new FlowConfigId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME);

    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1b");
    flowProperties.put("param2", "value2b");

    FlowConfig flowConfig = new FlowConfig().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME)
        .setTemplateUris(TEST_TEMPLATE_URI).setSchedule(TEST_SCHEDULE).setProperties(new StringMap(flowProperties));

    this.flowConfigClient.updateFlowConfig(flowConfig);

    FlowConfig retrievedFlowConfig = this.flowConfigClient.getFlowConfig(flowConfigId);

    Assert.assertEquals(retrievedFlowConfig.getFlowGroup(), TEST_GROUP_NAME);
    Assert.assertEquals(retrievedFlowConfig.getFlowName(), TEST_FLOW_NAME);
    Assert.assertEquals(retrievedFlowConfig.getSchedule(), TEST_SCHEDULE );
    Assert.assertEquals(retrievedFlowConfig.getTemplateUris(), TEST_TEMPLATE_URI);
    Assert.assertFalse(retrievedFlowConfig.hasRunImmediately());
    // Add this asssert when getFlowSpec() is changed to return the raw flow spec
    //Assert.assertEquals(flowConfig.getProperties().size(), 2);
    Assert.assertEquals(retrievedFlowConfig.getProperties().get("param1"), "value1b");
    Assert.assertEquals(retrievedFlowConfig.getProperties().get("param2"), "value2b");
  }

  @Test (dependsOnMethods = "testUpdate")
  public void testDelete() throws Exception {
    FlowConfigId flowConfigId = new FlowConfigId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME);

    // make sure flow config exists
    FlowConfig flowConfig = this.flowConfigClient.getFlowConfig(flowConfigId);
    Assert.assertEquals(flowConfig.getFlowGroup(), TEST_GROUP_NAME);
    Assert.assertEquals(flowConfig.getFlowName(), TEST_FLOW_NAME);

    this.flowConfigClient.deleteFlowConfig(flowConfigId);

    try {
      this.flowConfigClient.getFlowConfig(flowConfigId);
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.NOT_FOUND_404);
      return;
    }

    Assert.fail("Get should have gotten a 404 error");
  }

  @Test
  public void testBadGet() throws Exception {
    FlowConfigId flowConfigId = new FlowConfigId().setFlowGroup(TEST_DUMMY_GROUP_NAME).setFlowName(TEST_DUMMY_FLOW_NAME);

    try {
      this.flowConfigClient.getFlowConfig(flowConfigId);
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.NOT_FOUND_404);
      return;
    }

    Assert.fail("Get should have raised a 404 error");
  }

  @Test
  public void testBadDelete() throws Exception {
    FlowConfigId flowConfigId = new FlowConfigId().setFlowGroup(TEST_DUMMY_GROUP_NAME).setFlowName(TEST_DUMMY_FLOW_NAME);

    try {
      this.flowConfigClient.getFlowConfig(flowConfigId);
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.NOT_FOUND_404);
      return;
    }

    Assert.fail("Get should have raised a 404 error");
  }

  @Test
  public void testBadUpdate() throws Exception {
    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1b");
    flowProperties.put("param2", "value2b");

    FlowConfig flowConfig = new FlowConfig().setFlowGroup(TEST_DUMMY_GROUP_NAME).setFlowName(TEST_DUMMY_FLOW_NAME)
        .setTemplateUris(TEST_TEMPLATE_URI).setSchedule(TEST_SCHEDULE).setProperties(new StringMap(flowProperties));

    try {
      this.flowConfigClient.updateFlowConfig(flowConfig);
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.NOT_FOUND_404);
      return;
    }

    Assert.fail("Get should have raised a 404 error");
  }
}
