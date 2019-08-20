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
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
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
import com.linkedin.restli.client.RestLiResponseException;
import com.linkedin.restli.common.PatchRequest;
import com.linkedin.restli.internal.server.util.DataMapUtils;
import com.linkedin.restli.server.resources.BaseResource;
import com.linkedin.restli.server.util.PatchApplier;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.restli.EmbeddedRestliServer;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.runtime.spec_store.FSSpecStore;


@Test(groups = { "gobblin.service" })
public class FlowConfigV2Test {
  private FlowConfigV2Client _client;
  private EmbeddedRestliServer _server;
  private File _testDirectory;

  private static final String TEST_SPEC_STORE_DIR = "/tmp/flowConfigTest/";
  private static final String TEST_GROUP_NAME = "testGroup1";
  private static final String TEST_FLOW_NAME = "testFlow1";
  private static final String TEST_SCHEDULE = "0 1/0 * ? * *";
  private static final String TEST_TEMPLATE_URI = "FS:///templates/test.template";

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

    flowCatalog.startAsync();
    flowCatalog.awaitRunning();

    Injector injector = Guice.createInjector(new Module() {
      @Override
      public void configure(Binder binder) {
        binder.bind(FlowConfigsResourceHandler.class).annotatedWith(Names.named(FlowConfigsV2Resource.FLOW_CONFIG_GENERATOR_INJECT_NAME)).toInstance(new FlowConfigV2ResourceLocalHandler(flowCatalog));
        // indicate that we are in unit testing since the resource is being blocked until flow catalog changes have
        // been made
        binder.bindConstant().annotatedWith(Names.named(FlowConfigsV2Resource.INJECT_READY_TO_USE)).to(Boolean.TRUE);
        binder.bind(RequesterService.class).annotatedWith(Names.named(FlowConfigsV2Resource.INJECT_REQUESTER_SERVICE)).toInstance(new NoopRequesterService(
            ConfigFactory.empty()));
      }
    });

    _server = EmbeddedRestliServer.builder().resources(
        Lists.<Class<? extends BaseResource>>newArrayList(FlowConfigsV2Resource.class)).injector(injector).build();

    _server.startAsync();
    _server.awaitRunning();

    _client =
        new FlowConfigV2Client(String.format("http://localhost:%s/", _server.getPort()));
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

    flowConfig = new FlowConfig().setId(new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME))
        .setTemplateUris(TEST_TEMPLATE_URI).setProperties(new StringMap(flowProperties))
        .setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE).setRunImmediately(true));
    Assert.assertEquals(_client.createFlowConfig(flowConfig).getFlowExecutionId().longValue(), -1L);
  }

  @Test
  public void testPartialUpdate() throws Exception {
    FlowId flowId = new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME);

    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1");
    flowProperties.put("param2", "value2");
    flowProperties.put("param3", "value3");

    FlowConfig flowConfig = new FlowConfig().setId(new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME))
        .setTemplateUris(TEST_TEMPLATE_URI).setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE).setRunImmediately(false))
        .setProperties(new StringMap(flowProperties));

    // Set some initial config
    _client.updateFlowConfig(flowConfig);

    // Change param2 to value4, delete param3
    String patchJson = "{\"schedule\":{\"$set\":{\"runImmediately\":true}},"
        + "\"properties\":{\"$set\":{\"param2\":\"value4\"},\"$delete\":[\"param3\"]}}";
    DataMap dataMap = DataMapUtils.readMap(IOUtils.toInputStream(patchJson));
    PatchRequest<FlowConfig> flowConfigPatch = PatchRequest.createFromPatchDocument(dataMap);

    PatchApplier.applyPatch(flowConfig, flowConfigPatch);

    _client.updateFlowConfig(flowConfig);

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
}
