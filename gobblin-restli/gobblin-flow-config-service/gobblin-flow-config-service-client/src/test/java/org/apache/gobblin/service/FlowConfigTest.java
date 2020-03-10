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
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.gobblin.runtime.spec_store.FSSpecStore;
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
import com.linkedin.data.template.StringMap;
import com.linkedin.restli.client.RestLiResponseException;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.resources.BaseResource;
import com.typesafe.config.Config;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.restli.EmbeddedRestliServer;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;


@Test(groups = { "gobblin.service" })
public class FlowConfigTest {
  private FlowConfigClient _client;
  private EmbeddedRestliServer _server;
  private File _testDirectory;

  private static final String TEST_SPEC_STORE_DIR = "/tmp/flowConfigTest/";
  private static final String TEST_GROUP_NAME = "testGroup1";
  private static final String TEST_FLOW_NAME = "testFlow1";
  private static final String TEST_SCHEDULE = "0 1/0 * ? * *";
  private static final String TEST_TEMPLATE_URI = "FS:///templates/test.template";
  private static final String TEST_DUMMY_GROUP_NAME = "dummyGroup";
  private static final String TEST_DUMMY_FLOW_NAME = "dummyFlow";

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
         binder.bind(FlowConfigsResourceHandler.class)
             .annotatedWith(Names.named(FlowConfigsResource.INJECT_FLOW_CONFIG_RESOURCE_HANDLER))
             .toInstance(new FlowConfigResourceLocalHandler(flowCatalog));

         // indicate that we are in unit testing since the resource is being blocked until flow catalog changes have
         // been made
         binder.bindConstant().annotatedWith(Names.named(FlowConfigsResource.INJECT_READY_TO_USE)).to(Boolean.TRUE);
         binder.bind(RequesterService.class)
             .annotatedWith(Names.named(FlowConfigsResource.INJECT_REQUESTER_SERVICE)).toInstance(new NoopRequesterService(config));
       }
    });

    _server = EmbeddedRestliServer.builder().resources(
        Lists.<Class<? extends BaseResource>>newArrayList(FlowConfigsResource.class)).injector(injector).build();

    _server.startAsync();
    _server.awaitRunning();

    _client =
        new FlowConfigClient(String.format("http://localhost:%s/", _server.getPort()));
  }

  private void cleanUpDir(String dir) throws Exception {
    File specStoreDir = new File(dir);
    if (specStoreDir.exists()) {
      FileUtils.deleteDirectory(specStoreDir);
    }
  }

  @Test
  public void testCreateBadSchedule() throws Exception {
    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1");

    FlowConfig flowConfig = new FlowConfig().setId(new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME))
        .setTemplateUris(TEST_TEMPLATE_URI).setSchedule(new Schedule().setCronSchedule("bad schedule").
            setRunImmediately(true))
        .setProperties(new StringMap(flowProperties));

    try {
      _client.createFlowConfig(flowConfig);
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.S_422_UNPROCESSABLE_ENTITY.getCode());
      return;
    }

    Assert.fail("Get should have gotten a 422 error");
  }

  @Test
  public void testCreateBadTemplateUri() throws Exception {
    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1");

    FlowConfig flowConfig = new FlowConfig().setId(new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME))
        .setTemplateUris("FILE://bad/uri").setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE).
            setRunImmediately(true))
        .setProperties(new StringMap(flowProperties));

    try {
      _client.createFlowConfig(flowConfig);
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.S_422_UNPROCESSABLE_ENTITY.getCode());
      return;
    }

    Assert.fail("Get should have gotten a 422 error");
  }

  @Test
  public void testCreate() throws Exception {
    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1");

    FlowConfig flowConfig = new FlowConfig().setId(new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME))
        .setTemplateUris(TEST_TEMPLATE_URI).setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE).
            setRunImmediately(true))
        .setProperties(new StringMap(flowProperties));

    _client.createFlowConfig(flowConfig);
  }

  @Test (dependsOnMethods = "testCreate")
  public void testCreateAgain() throws Exception {
    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1");

    FlowConfig flowConfig = new FlowConfig().setId(new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME))
        .setTemplateUris(TEST_TEMPLATE_URI).setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE))
        .setProperties(new StringMap(flowProperties));

    RestLiResponseException exception = null;
    try {
      _client.createFlowConfig(flowConfig);
    } catch (RestLiResponseException e) {
      exception = e;
    }

    Assert.assertNotNull(exception);
    Assert.assertEquals(exception.getStatus(), HttpStatus.S_409_CONFLICT.getCode());
  }

  @Test (dependsOnMethods = "testCreateAgain")
  public void testGet() throws Exception {
    FlowId flowId = new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME);
    FlowConfig flowConfig = _client.getFlowConfig(flowId);

    Assert.assertEquals(flowConfig.getId().getFlowGroup(), TEST_GROUP_NAME);
    Assert.assertEquals(flowConfig.getId().getFlowName(), TEST_FLOW_NAME);
    Assert.assertEquals(flowConfig.getSchedule().getCronSchedule(), TEST_SCHEDULE );
    Assert.assertEquals(flowConfig.getTemplateUris(), TEST_TEMPLATE_URI);
    Assert.assertTrue(flowConfig.getSchedule().isRunImmediately());
    // Add this asssert back when getFlowSpec() is changed to return the raw flow spec
    //Assert.assertEquals(flowConfig.getProperties().size(), 1);
    Assert.assertEquals(flowConfig.getProperties().get("param1"), "value1");
  }

  @Test (dependsOnMethods = "testGet")
  public void testUpdate() throws Exception {
    FlowId flowId = new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME);

    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1b");
    flowProperties.put("param2", "value2b");

    FlowConfig flowConfig = new FlowConfig().setId(new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME))
        .setTemplateUris(TEST_TEMPLATE_URI).setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE))
        .setProperties(new StringMap(flowProperties));

    _client.updateFlowConfig(flowConfig);

    FlowConfig retrievedFlowConfig = _client.getFlowConfig(flowId);

    Assert.assertEquals(retrievedFlowConfig.getId().getFlowGroup(), TEST_GROUP_NAME);
    Assert.assertEquals(retrievedFlowConfig.getId().getFlowName(), TEST_FLOW_NAME);
    Assert.assertEquals(retrievedFlowConfig.getSchedule().getCronSchedule(), TEST_SCHEDULE );
    Assert.assertEquals(retrievedFlowConfig.getTemplateUris(), TEST_TEMPLATE_URI);
    // Add this asssert when getFlowSpec() is changed to return the raw flow spec
    //Assert.assertEquals(flowConfig.getProperties().size(), 2);
    Assert.assertEquals(retrievedFlowConfig.getProperties().get("param1"), "value1b");
    Assert.assertEquals(retrievedFlowConfig.getProperties().get("param2"), "value2b");
  }

  @Test (dependsOnMethods = "testUpdate")
  public void testUnschedule() throws Exception {
    FlowId flowId = new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME);
    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1");
    flowProperties.put(ConfigurationKeys.FLOW_UNSCHEDULE_KEY, "true");

    FlowConfig flowConfig = new FlowConfig().setId(flowId)
        .setTemplateUris(TEST_TEMPLATE_URI).setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE).
            setRunImmediately(true))
        .setProperties(new StringMap(flowProperties));

    _client.updateFlowConfig(flowConfig);

    FlowConfig persistedFlowConfig = _client.getFlowConfig(flowId);

    Assert.assertFalse(persistedFlowConfig.getProperties().containsKey(ConfigurationKeys.FLOW_UNSCHEDULE_KEY));
    Assert.assertEquals(persistedFlowConfig.getSchedule().getCronSchedule(), FlowConfigResourceLocalHandler.NEVER_RUN_CRON_SCHEDULE.getCronSchedule());
  }

  @Test (dependsOnMethods = "testUnschedule")
  public void testDelete() throws Exception {
    FlowId flowId = new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME);

    // make sure flow config exists
    FlowConfig flowConfig = _client.getFlowConfig(flowId);
    Assert.assertEquals(flowConfig.getId().getFlowGroup(), TEST_GROUP_NAME);
    Assert.assertEquals(flowConfig.getId().getFlowName(), TEST_FLOW_NAME);

    _client.deleteFlowConfig(flowId);

    try {
      _client.getFlowConfig(flowId);
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.S_404_NOT_FOUND.getCode());
      return;
    }

    Assert.fail("Get should have gotten a 404 error");
  }

  @Test
  public void testBadGet() throws Exception {
    FlowId flowId = new FlowId().setFlowGroup(TEST_DUMMY_GROUP_NAME).setFlowName(TEST_DUMMY_FLOW_NAME);

    try {
      _client.getFlowConfig(flowId);
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.S_404_NOT_FOUND.getCode());
      return;
    }

    Assert.fail("Get should have raised a 404 error");
  }

  @Test
  public void testBadDelete() throws Exception {
    FlowId flowId = new FlowId().setFlowGroup(TEST_DUMMY_GROUP_NAME).setFlowName(TEST_DUMMY_FLOW_NAME);

    try {
      _client.getFlowConfig(flowId);
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.S_404_NOT_FOUND.getCode());
      return;
    }

    Assert.fail("Get should have raised a 404 error");
  }

  @Test
  public void testBadUpdate() throws Exception {
    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1b");
    flowProperties.put("param2", "value2b");

    FlowConfig flowConfig = new FlowConfig().setId(new FlowId().setFlowGroup(TEST_DUMMY_GROUP_NAME)
        .setFlowName(TEST_DUMMY_FLOW_NAME))
        .setTemplateUris(TEST_TEMPLATE_URI).setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE))
        .setProperties(new StringMap(flowProperties));

    try {
      _client.updateFlowConfig(flowConfig);
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.S_404_NOT_FOUND.getCode());
      return;
    }

    Assert.fail("Update should have raised a 404 error");
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

  private static int chooseRandomPort() throws IOException {
    ServerSocket socket = null;
    try {
      socket = new ServerSocket(0);
      return socket.getLocalPort();
    } finally {
      if (socket != null) {
        socket.close();
      }
    }
  }
}
