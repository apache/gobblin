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

package org.apache.gobblin.service;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.gobblin.runtime.api.SpecCatalogListener;
import org.apache.gobblin.runtime.spec_catalog.AddSpecResponse;
import org.mortbay.jetty.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.StringMap;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.restli.client.RestLiResponseException;
import com.linkedin.restli.common.PatchRequest;
import com.linkedin.restli.internal.server.util.DataMapUtils;
import com.linkedin.restli.server.resources.BaseResource;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.Setter;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.restli.EmbeddedRestliServer;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.runtime.spec_store.FSSpecStore;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;


@Test(groups = { "gobblin.service" }, singleThreaded = true)
public class FlowConfigV2Test {
  private FlowConfigV2Client _client;
  private EmbeddedRestliServer _server;
  private File _testDirectory;
  private TestRequesterService _requesterService;
  private GroupOwnershipService groupOwnershipService;
  private File groupConfigFile;

  private static final String TEST_SPEC_STORE_DIR = "/tmp/flowConfigV2Test/";
  private static final String TEST_GROUP_NAME = "testGroup1";
  private static final String TEST_FLOW_NAME = "testFlow1";
  private static final String TEST_FLOW_NAME_2 = "testFlow2";
  private static final String TEST_FLOW_NAME_3 = "testFlow3";
  private static final String TEST_FLOW_NAME_4 = "testFlow4";
  private static final String TEST_FLOW_NAME_5 = "testFlow5";
  private static final String TEST_FLOW_NAME_6 = "testFlow6";
  private static final String TEST_FLOW_NAME_7 = "testFlow7";
  private static final String TEST_FLOW_NAME_8 = "testFlow8";
  private static final String TEST_FLOW_NAME_9 = "testFlow9";
  private static final String TEST_SCHEDULE = "0 1/0 * ? * *";
  private static final String TEST_TEMPLATE_URI = "FS:///templates/test.template";

  private static final ServiceRequester TEST_REQUESTER = new ServiceRequester("testName", "USER_PRINCIPAL", "testFrom");
  private static final ServiceRequester TEST_REQUESTER2 = new ServiceRequester("testName2", "USER_PRINCIPAL", "testFrom");

  @BeforeClass
  public void setUp() throws Exception {
    ConfigBuilder configBuilder = ConfigBuilder.create();

    _testDirectory = Files.createTempDir();

    configBuilder
        .addPrimitive(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY, _testDirectory.getAbsolutePath())
        .addPrimitive(FSSpecStore.SPECSTORE_FS_DIR_KEY, TEST_SPEC_STORE_DIR);
    cleanUpDir(TEST_SPEC_STORE_DIR);

    Config config = configBuilder.build();
    final FlowCatalog flowCatalog = new FlowCatalog(config);
    final SpecCatalogListener mockListener = mock(SpecCatalogListener.class);
    when(mockListener.getName()).thenReturn(ServiceConfigKeys.GOBBLIN_SERVICE_JOB_SCHEDULER_LISTENER_CLASS);
    when(mockListener.onAddSpec(any())).thenReturn(new AddSpecResponse(""));
    flowCatalog.addListener(mockListener);
    flowCatalog.startAsync();
    flowCatalog.awaitRunning();

    _requesterService = new TestRequesterService(ConfigFactory.empty());

    this.groupConfigFile = new File(_testDirectory + "/TestGroups.json");
    String groups ="{\"testGroup\": \"testName,testName2\"}";
    Files.write(groups.getBytes(), this.groupConfigFile);
    Config groupServiceConfig = ConfigBuilder.create()
        .addPrimitive(LocalGroupOwnershipService.GROUP_MEMBER_LIST, this.groupConfigFile.getAbsolutePath())
        .build();

    groupOwnershipService = new LocalGroupOwnershipService(groupServiceConfig);

    Injector injector = Guice.createInjector(new Module() {
      @Override
      public void configure(Binder binder) {
        binder.bind(FlowConfigsV2ResourceHandler.class).toInstance(new FlowConfigV2ResourceLocalHandler(flowCatalog));
        // indicate that we are in unit testing since the resource is being blocked until flow catalog changes have
        // been made
        binder.bindConstant().annotatedWith(Names.named(FlowConfigsV2Resource.INJECT_READY_TO_USE)).to(Boolean.TRUE);
        binder.bind(RequesterService.class).toInstance(_requesterService);
        binder.bind(GroupOwnershipService.class).toInstance(groupOwnershipService);
      }
    });

    _server = EmbeddedRestliServer.builder().resources(
        Lists.<Class<? extends BaseResource>>newArrayList(FlowConfigsV2Resource.class)).injector(injector).build();

    _server.startAsync();
    _server.awaitRunning();

    Map<String, String> transportClientProperties = Maps.newHashMap();
    transportClientProperties.put(HttpClientFactory.HTTP_REQUEST_TIMEOUT, "10000");
    _client =
        new FlowConfigV2Client(String.format("http://localhost:%s/", _server.getPort()), transportClientProperties);
  }

  protected void cleanUpDir(String dir) throws Exception {
    File specStoreDir = new File(dir);
    if (specStoreDir.exists()) {
      FileUtils.deleteDirectory(specStoreDir);
    }
  }

  @Test
  public void testCheckFlowExecutionId() throws Exception {
    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1");

    FlowConfig flowConfig = new FlowConfig().setId(new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME))
        .setTemplateUris(TEST_TEMPLATE_URI).setProperties(new StringMap(flowProperties));
    FlowStatusId flowStatusId =_client.createFlowConfig(flowConfig);
    Assert.assertEquals(TEST_GROUP_NAME, flowStatusId.getFlowGroup());
    Assert.assertEquals(TEST_FLOW_NAME, flowStatusId.getFlowName());
    Assert.assertTrue(flowStatusId.getFlowExecutionId() != -1);

    flowConfig = new FlowConfig().setId(new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME_2))
        .setTemplateUris(TEST_TEMPLATE_URI).setProperties(new StringMap(flowProperties))
        .setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE).setRunImmediately(true));
    Assert.assertEquals(_client.createFlowConfig(flowConfig).getFlowExecutionId().longValue(), -1L);
  }

  @Test
  public void testPartialUpdate() throws Exception {
    FlowId flowId = new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME_3);
    _requesterService.setRequester(TEST_REQUESTER);

    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1");
    flowProperties.put("param2", "value2");
    flowProperties.put("param3", "value3");

    FlowConfig flowConfig = new FlowConfig().setId(new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME_3))
        .setTemplateUris(TEST_TEMPLATE_URI).setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE).setRunImmediately(false))
        .setProperties(new StringMap(flowProperties));

    // Set some initial config
    _client.createFlowConfig(flowConfig);

    // Change param2 to value4, delete param3
    String patchJson = "{\"schedule\":{\"$set\":{\"runImmediately\":true}},"
        + "\"properties\":{\"$set\":{\"param2\":\"value4\"},\"$delete\":[\"param3\"]}}";
    DataMap dataMap = DataMapUtils.readMap(IOUtils.toInputStream(patchJson));
    PatchRequest<FlowConfig> flowConfigPatch = PatchRequest.createFromPatchDocument(dataMap);

    _client.partialUpdateFlowConfig(flowId, flowConfigPatch);

    FlowConfig retrievedFlowConfig = _client.getFlowConfig(flowId);

    Assert.assertTrue(retrievedFlowConfig.getSchedule().isRunImmediately());
    Assert.assertEquals(retrievedFlowConfig.getProperties().get("param1"), "value1");
    Assert.assertEquals(retrievedFlowConfig.getProperties().get("param2"), "value4");
    Assert.assertFalse(retrievedFlowConfig.getProperties().containsKey("param3"));
  }

  @Test (expectedExceptions = RestLiResponseException.class)
  public void testBadPartialUpdate() throws Exception {
    FlowId flowId = new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME);

    String patchJson = "{\"schedule\":{\"$set\":{\"runImmediately\":true}},"
        + "\"properties\":{\"$set\":{\"param2\":\"value4\"},\"$delete\":[\"param3\"]}}";
    DataMap dataMap = DataMapUtils.readMap(IOUtils.toInputStream(patchJson));
    PatchRequest<FlowConfig> flowConfigPatch = PatchRequest.createFromPatchDocument(dataMap);

    // Throws exception since local handlers don't support partial update
    _client.partialUpdateFlowConfig(flowId, flowConfigPatch);
  }

  @Test
  public void testDisallowedRequester() throws Exception {
    try {
      ServiceRequester testRequester = new ServiceRequester("testName", "testType", "testFrom");
      _requesterService.setRequester(testRequester);

      Map<String, String> flowProperties = Maps.newHashMap();
      flowProperties.put("param1", "value1");

      FlowConfig flowConfig = new FlowConfig().setId(new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME_4))
          .setTemplateUris(TEST_TEMPLATE_URI)
          .setProperties(new StringMap(flowProperties));
      _client.createFlowConfig(flowConfig);

      testRequester.setName("testName2");
      _client.deleteFlowConfig(new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME_4));
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.ORDINAL_401_Unauthorized);
    }
  }

  @Test
  public void testGroupRequesterAllowed() throws Exception {
    ServiceRequester testRequester = new ServiceRequester("testName", "USER_PRINCIPAL", "testFrom");
    _requesterService.setRequester(testRequester);
    Map<String, String> flowProperties = Maps.newHashMap();

    FlowConfig flowConfig = new FlowConfig().setId(new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME_5))
        .setTemplateUris(TEST_TEMPLATE_URI)
        .setProperties(new StringMap(flowProperties))
        .setOwningGroup("testGroup");

     _client.createFlowConfig(flowConfig);

    testRequester.setName("testName2");
    _client.deleteFlowConfig(new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME_5));
  }

  @Test
  public void testGroupRequesterRejected() throws Exception {
    try {
      ServiceRequester testRequester = new ServiceRequester("testName", "USER_PRINCIPAL", "testFrom");
      _requesterService.setRequester(testRequester);
      Map<String, String> flowProperties = Maps.newHashMap();

      FlowConfig flowConfig = new FlowConfig().setId(new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME_6))
          .setTemplateUris(TEST_TEMPLATE_URI)
          .setProperties(new StringMap(flowProperties))
          .setOwningGroup("testGroup");

      _client.createFlowConfig(flowConfig);

      testRequester.setName("testName3");
      _client.deleteFlowConfig(new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME_6));
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.ORDINAL_401_Unauthorized);
    }
  }

  @Test
  public void testGroupUpdateRejected() throws Exception {
   _requesterService.setRequester(TEST_REQUESTER);
   Map<String, String> flowProperties = Maps.newHashMap();

   FlowConfig flowConfig = new FlowConfig().setId(new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME_7))
       .setTemplateUris(TEST_TEMPLATE_URI)
       .setProperties(new StringMap(flowProperties))
       .setOwningGroup("testGroup");

   _client.createFlowConfig(flowConfig);

   // Update should be rejected because testName is not part of dummyGroup
   flowConfig.setOwningGroup("dummyGroup");
   try {
     _client.updateFlowConfig(flowConfig);
     Assert.fail("Expected update to be rejected");
   } catch (RestLiResponseException e) {
     Assert.assertEquals(e.getStatus(), HttpStatus.ORDINAL_401_Unauthorized);
   }
  }

  @Test
  public void testRequesterUpdate() throws Exception {
    _requesterService.setRequester(TEST_REQUESTER);
    Map<String, String> flowProperties = Maps.newHashMap();

    FlowId flowId = new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME_8);
    FlowConfig flowConfig = new FlowConfig().setId(flowId)
        .setTemplateUris(TEST_TEMPLATE_URI)
        .setProperties(new StringMap(flowProperties))
        .setOwningGroup("testGroup");

    _client.createFlowConfig(flowConfig);

    // testName2 takes ownership of the flow
    flowProperties.put(RequesterService.REQUESTER_LIST, RequesterService.serialize(Lists.newArrayList(TEST_REQUESTER2)));
    flowConfig.setProperties(new StringMap(flowProperties));
    _requesterService.setRequester(TEST_REQUESTER2);
    _client.updateFlowConfig(flowConfig);

    // Check that the requester list was actually updated
    FlowConfig updatedFlowConfig = _client.getFlowConfig(flowId);
    Assert.assertEquals(RequesterService.deserialize(updatedFlowConfig.getProperties().get(RequesterService.REQUESTER_LIST)),
        Lists.newArrayList(TEST_REQUESTER2));
  }

  @Test
  public void testRequesterUpdateRejected() throws Exception {
    _requesterService.setRequester(TEST_REQUESTER);
    Map<String, String> flowProperties = Maps.newHashMap();

    FlowConfig flowConfig = new FlowConfig().setId(new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME_9))
        .setTemplateUris(TEST_TEMPLATE_URI)
        .setProperties(new StringMap(flowProperties));

    _client.createFlowConfig(flowConfig);

    // Update should be rejected because testName is not allowed to update the owner to testName2
    flowProperties.put(RequesterService.REQUESTER_LIST, RequesterService.serialize(Lists.newArrayList(TEST_REQUESTER2)));
    flowConfig.setProperties(new StringMap(flowProperties));
    try {
      _client.updateFlowConfig(flowConfig);
      Assert.fail("Expected update to be rejected");
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.ORDINAL_401_Unauthorized);
    }
  }

  @Test
  public void testInvalidFlowId() throws Exception {
    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1");
    StringBuilder sb1 = new StringBuilder();
    StringBuilder sb2 = new StringBuilder();
    int maxFlowNameLength = ServiceConfigKeys.MAX_FLOW_NAME_LENGTH;
    int maxFlowGroupLength = ServiceConfigKeys.MAX_FLOW_GROUP_LENGTH;
    while(maxFlowGroupLength-- >= 0) {
      sb1.append("A");
    }
    while(maxFlowNameLength-- >= 0) {
      sb2.append("A");
    }
    String TOO_LONG_FLOW_GROUP = sb1.toString();
    String TOO_LONG_FLOW_NAME = sb2.toString();

    FlowConfig flowConfig = new FlowConfig().setId(new FlowId().setFlowGroup(TOO_LONG_FLOW_GROUP).setFlowName(TOO_LONG_FLOW_NAME))
        .setTemplateUris(TEST_TEMPLATE_URI).setProperties(new StringMap(flowProperties));
    try {
      _client.createFlowConfig(flowConfig);
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.ORDINAL_422_Unprocessable_Entity);
      Assert.assertTrue(e.getMessage().contains("is out of range"));
      return;
    }

    Assert.fail();
  }

  @Test
  public void testRunFlow() throws Exception {
    String flowName = "testRunFlow";
    FlowId flowId = new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(flowName);
    _requesterService.setRequester(TEST_REQUESTER);

    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1");

    FlowConfig flowConfig = new FlowConfig().setId(new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(flowName))
        .setTemplateUris(TEST_TEMPLATE_URI).setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE).setRunImmediately(false))
        .setProperties(new StringMap(flowProperties));

    // Create initial flowConfig
    _client.createFlowConfig(flowConfig);

    // Trigger flow
    _client.runImmediately(flowId);

    // Verify runImmediately was changed to true
    Assert.assertTrue(_client.getFlowConfig(flowId).getSchedule().isRunImmediately());
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() throws Exception {
    if (_client != null) {
      _client.close();
    }
    if (_server != null) {
      _server.stopAsync();
      _server.awaitTerminated();
    }
    _testDirectory.delete();
    cleanUpDir(TEST_SPEC_STORE_DIR);
  }

  public class TestRequesterService extends RequesterService {
    @Setter
    private ServiceRequester requester;

    public TestRequesterService(Config config) {
      super(config);
    }

    @Override
    public List<ServiceRequester> findRequesters(BaseResource resource) {
      return requester == null ? Lists.newArrayList() : Lists.newArrayList(requester);
    }

    @Override
    public boolean isRequesterAllowed(
        List<ServiceRequester> originalRequesterList, List<ServiceRequester> currentRequesterList) {
      for (ServiceRequester s: currentRequesterList) {
        if (originalRequesterList.contains(s)) {
          return true;
        }
      }
      return false;
    }
  }
}
