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

package gobblin.service;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;

import org.eclipse.jetty.http.HttpStatus;
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
import com.linkedin.restli.server.resources.BaseResource;
import com.typesafe.config.Config;

import gobblin.config.ConfigBuilder;
import gobblin.configuration.ConfigurationKeys;
import gobblin.restli.EmbeddedRestliServer;
import gobblin.runtime.api.MutableJobCatalog;
import gobblin.runtime.job_catalog.FSJobCatalog;


@Test(groups = { "gobblin.service" })
public class FlowConfigTest {
  private FlowConfigClient _client;
  private EmbeddedRestliServer _server;
  private File _testDirectory;

  private static final String TEST_GROUP_NAME = "testGroup1";
  private static final String TEST_FLOW_NAME = "testFlow1";
  private static final String TEST_SCHEDULE = "";
  private static final String TEST_TEMPLATE_URI = "FS:///templates/test.template";
  private static final String TEST_DUMMY_GROUP_NAME = "dummyGroup";
  private static final String TEST_DUMMY_FLOW_NAME = "dummyFlow";

  @BeforeClass
  public void setUp() throws Exception {
    ConfigBuilder configBuilder = ConfigBuilder.create();

    _testDirectory = Files.createTempDir();

    configBuilder.addPrimitive(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY, _testDirectory.getAbsolutePath());

    Config config = configBuilder.build();
    final FSJobCatalog jobCatalog = new FSJobCatalog(config);

    jobCatalog.startAsync();
    jobCatalog.awaitRunning();

    Injector injector = Guice.createInjector(new Module() {
       @Override
       public void configure(Binder binder) {
         binder.bind(MutableJobCatalog.class).toInstance(jobCatalog);
         // indicate that we are in unit testing since the resource is being blocked until flow catalog changes have
         // been made
         binder.bindConstant().annotatedWith(Names.named("inUnitTest")).to(true);
       }
    });

    _server = EmbeddedRestliServer.builder().resources(
        Lists.<Class<? extends BaseResource>>newArrayList(FlowConfigsResource.class)).injector(injector).build();

    _server.startAsync();
    _server.awaitRunning();

    _client =
        new FlowConfigClient(String.format("http://localhost:%s/", _server.getPort()));
  }

  @Test
  public void testCreate() throws Exception {
    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1");

    FlowConfig flowConfig = new FlowConfig().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME)
        .setTemplateUris(TEST_TEMPLATE_URI).setSchedule(TEST_SCHEDULE).setRunImmediately(true)
        .setProperties(new StringMap(flowProperties));

    _client.createFlowConfig(flowConfig);
  }

  @Test (dependsOnMethods = "testCreate")
  public void testCreateAgain() throws Exception {
    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1");

    FlowConfig flowConfig = new FlowConfig().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME)
        .setTemplateUris(TEST_TEMPLATE_URI).setSchedule(TEST_SCHEDULE).setProperties(new StringMap(flowProperties));

    try {
      _client.createFlowConfig(flowConfig);
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.CONFLICT_409);
      return;
    }

    Assert.fail("Get should have gotten a 409 error");
  }

  @Test (dependsOnMethods = "testCreateAgain")
  public void testGet() throws Exception {
    FlowConfigId flowConfigId = new FlowConfigId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME);
    FlowConfig flowConfig = _client.getFlowConfig(flowConfigId);

    Assert.assertEquals(flowConfig.getFlowGroup(), TEST_GROUP_NAME);
    Assert.assertEquals(flowConfig.getFlowName(), TEST_FLOW_NAME);
    Assert.assertEquals(flowConfig.getSchedule(), TEST_SCHEDULE );
    Assert.assertEquals(flowConfig.getTemplateUris(), TEST_TEMPLATE_URI);
    Assert.assertTrue(flowConfig.hasRunImmediately());
    Assert.assertTrue(flowConfig.isRunImmediately());
    // Add this asssert back when getFlowSpec() is changed to return the raw flow spec
    //Assert.assertEquals(flowConfig.getProperties().size(), 1);
    Assert.assertEquals(flowConfig.getProperties().get("param1"), "value1");
    Assert.assertFalse(flowConfig.getProperties().containsKey("gobblin.fsJobCatalog.version"));
    Assert.assertFalse(flowConfig.getProperties().containsKey("gobblin.fsJobCatalog.description"));
  }

  @Test (dependsOnMethods = "testGet")
  public void testUpdate() throws Exception {
    FlowConfigId flowConfigId = new FlowConfigId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME);

    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1b");
    flowProperties.put("param2", "value2b");

    FlowConfig flowConfig = new FlowConfig().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME)
        .setTemplateUris(TEST_TEMPLATE_URI).setSchedule(TEST_SCHEDULE).setProperties(new StringMap(flowProperties));

    _client.updateFlowConfig(flowConfig);

    FlowConfig retrievedFlowConfig = _client.getFlowConfig(flowConfigId);

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
    FlowConfig flowConfig = _client.getFlowConfig(flowConfigId);
    Assert.assertEquals(flowConfig.getFlowGroup(), TEST_GROUP_NAME);
    Assert.assertEquals(flowConfig.getFlowName(), TEST_FLOW_NAME);

    _client.deleteFlowConfig(flowConfigId);

    try {
      _client.getFlowConfig(flowConfigId);
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
      _client.getFlowConfig(flowConfigId);
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
      _client.getFlowConfig(flowConfigId);
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
      _client.updateFlowConfig(flowConfig);
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.NOT_FOUND_404);
      return;
    }

    Assert.fail("Get should have raised a 404 error");
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() throws Exception {
    if (_client != null) {
      _client.close();
    }
    if (_server != null) {
      _server.stopAsync();
      _server.awaitTerminated();    }
    _testDirectory.delete();
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
