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
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;

import org.apache.gobblin.restli.EmbeddedRestliServer;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.hadoop.fs.Path;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.RepositoryCache;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.util.FS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.linkedin.data.template.StringMap;
import com.linkedin.restli.client.RestLiResponseException;
import com.typesafe.config.Config;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.kafka.KafkaTestBase;
import org.apache.gobblin.metastore.MysqlJobStatusStateStoreFactory;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.service.modules.core.GitConfigMonitor;
import org.apache.gobblin.service.modules.core.GobblinServiceManager;
import org.apache.gobblin.service.modules.flow.MockedSpecCompiler;
import org.apache.gobblin.service.monitoring.FsJobStatusRetriever;
import org.apache.gobblin.testing.AssertWithBackoff;
import org.apache.gobblin.util.ConfigUtils;


public class GobblinServiceManagerTest {

  private static final Logger logger = LoggerFactory.getLogger(GobblinServiceManagerTest.class);
  private static Gson gson = new GsonBuilder().setPrettyPrinting().create();

  private static final String SERVICE_WORK_DIR = "/tmp/serviceWorkDir/";
  private static final String SPEC_STORE_PARENT_DIR = "/tmp/serviceCore/";
  private static final String SPEC_DESCRIPTION = "Test ServiceCore";
  private static final String TOPOLOGY_SPEC_STORE_DIR = "/tmp/serviceCore/topologyTestSpecStore";
  private static final String FLOW_SPEC_STORE_DIR = "/tmp/serviceCore/flowTestSpecStore";
  private static final String GIT_CLONE_DIR = "/tmp/serviceCore/clone";
  private static final String GIT_REMOTE_REPO_DIR = "/tmp/serviceCore/remote";
  private static final String GIT_LOCAL_REPO_DIR = "/tmp/serviceCore/local";
  private static final String JOB_STATUS_STATE_STORE_DIR = "/tmp/serviceCore/fsJobStatusRetriever";

  private static final String TEST_GROUP_NAME = "testGroup";
  private static final String TEST_FLOW_NAME = "testFlow";
  private static final FlowId TEST_FLOW_ID = new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME);

  private static final String TEST_SCHEDULE = "0 1/0 * ? * *";
  private static final String TEST_TEMPLATE_URI = "FS:///templates/test.template";
  private static final String TEST_DUMMY_GROUP_NAME = "dummyGroup";
  private static final String TEST_DUMMY_FLOW_NAME = "dummyFlow";
  private static final String TEST_GOBBLIN_EXECUTOR_NAME = "testGobblinExecutor";
  private static final String TEST_SOURCE_NAME = "testSource";
  private static final String TEST_SINK_NAME = "testSink";

  private final URI TEST_URI = FlowConfigResourceLocalHandler.FlowUriUtils.createFlowSpecUri(TEST_FLOW_ID);
  private MockGobblinServiceManager gobblinServiceManager;
  private FlowConfigV2Client flowConfigClient;

  private Git gitForPush;
  Properties serviceCoreProperties = new Properties();

  public GobblinServiceManagerTest() throws Exception {
  }

  @BeforeClass
  public void setup() throws Exception {
    cleanUpDir(SERVICE_WORK_DIR);
    cleanUpDir(SPEC_STORE_PARENT_DIR);
    ITestMetastoreDatabase testMetastoreDatabase = TestMetastoreDatabaseFactory.get();

    KafkaTestBase kafkaTestHelper = new KafkaTestBase();
    kafkaTestHelper.startServers();

    serviceCoreProperties.put(ConfigurationKeys.STATE_STORE_DB_USER_KEY, "testUser");
    serviceCoreProperties.put(ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, "testPassword");
    serviceCoreProperties.put(ConfigurationKeys.STATE_STORE_DB_URL_KEY, testMetastoreDatabase.getJdbcUrl());
    serviceCoreProperties.put("zookeeper.connect", kafkaTestHelper.getZkConnectString());
    serviceCoreProperties.put(ConfigurationKeys.STATE_STORE_FACTORY_CLASS_KEY, MysqlJobStatusStateStoreFactory.class.getName());

    serviceCoreProperties.put(ConfigurationKeys.TOPOLOGYSPEC_STORE_DIR_KEY, TOPOLOGY_SPEC_STORE_DIR);
    serviceCoreProperties.put(FlowCatalog.FLOWSPEC_STORE_DIR_KEY, FLOW_SPEC_STORE_DIR);
    serviceCoreProperties.put(ServiceConfigKeys.TOPOLOGY_FACTORY_TOPOLOGY_NAMES_KEY, TEST_GOBBLIN_EXECUTOR_NAME);
    serviceCoreProperties.put(ServiceConfigKeys.TOPOLOGY_FACTORY_PREFIX +  TEST_GOBBLIN_EXECUTOR_NAME + ".description",
        "StandaloneTestExecutor");
    serviceCoreProperties.put(ServiceConfigKeys.TOPOLOGY_FACTORY_PREFIX +  TEST_GOBBLIN_EXECUTOR_NAME + ".version",
        FlowSpec.Builder.DEFAULT_VERSION);
    serviceCoreProperties.put(ServiceConfigKeys.TOPOLOGY_FACTORY_PREFIX +  TEST_GOBBLIN_EXECUTOR_NAME + ".uri",
        "gobblinExecutor");
    serviceCoreProperties.put(ServiceConfigKeys.TOPOLOGY_FACTORY_PREFIX +  TEST_GOBBLIN_EXECUTOR_NAME + ".specExecutorInstance",
        "org.apache.gobblin.service.InMemorySpecExecutor");
    serviceCoreProperties.put(ServiceConfigKeys.TOPOLOGY_FACTORY_PREFIX +  TEST_GOBBLIN_EXECUTOR_NAME + ".specExecInstance.capabilities",
        TEST_SOURCE_NAME + ":" + TEST_SINK_NAME);

    serviceCoreProperties.put(ServiceConfigKeys.GOBBLIN_SERVICE_GIT_CONFIG_MONITOR_ENABLED_KEY, true);
    serviceCoreProperties.put(GitConfigMonitor.GIT_CONFIG_MONITOR_PREFIX + "." + ConfigurationKeys.GIT_MONITOR_REPO_URI, GIT_REMOTE_REPO_DIR);
    serviceCoreProperties.put(GitConfigMonitor.GIT_CONFIG_MONITOR_PREFIX + "." + ConfigurationKeys.GIT_MONITOR_REPO_DIR, GIT_LOCAL_REPO_DIR);
    serviceCoreProperties.put(GitConfigMonitor.GIT_CONFIG_MONITOR_PREFIX + "." + ConfigurationKeys.GIT_MONITOR_POLLING_INTERVAL, 5);

    serviceCoreProperties.put(FsJobStatusRetriever.CONF_PREFIX + "." + ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, JOB_STATUS_STATE_STORE_DIR);

    serviceCoreProperties.put(ServiceConfigKeys.GOBBLIN_SERVICE_JOB_STATUS_MONITOR_ENABLED_KEY, false);

    serviceCoreProperties.put(ServiceConfigKeys.GOBBLIN_SERVICE_FLOWCOMPILER_CLASS_KEY, MockedSpecCompiler.class.getCanonicalName());

    // Create a bare repository
    RepositoryCache.FileKey fileKey = RepositoryCache.FileKey.exact(new File(GIT_REMOTE_REPO_DIR), FS.DETECTED);
    fileKey.open(false).create(true);

    this.gitForPush = Git.cloneRepository().setURI(GIT_REMOTE_REPO_DIR).setDirectory(new File(GIT_CLONE_DIR)).call();

    // push an empty commit as a base for detecting changes
    this.gitForPush.commit().setMessage("First commit").call();
    this.gitForPush.push().setRemote("origin").setRefSpecs(new RefSpec("master")).call();

    this.gobblinServiceManager = new MockGobblinServiceManager("CoreService", "1",
        ConfigUtils.propertiesToConfig(serviceCoreProperties), Optional.of(new Path(SERVICE_WORK_DIR)));
    this.gobblinServiceManager.start();

    this.flowConfigClient = new FlowConfigV2Client(String.format("http://localhost:%s/",
        this.gobblinServiceManager.getRestLiServer().getPort()));
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
    try {
      this.gobblinServiceManager.stop();
    } catch (Exception e) {
      logger.warn("Could not cleanly stop Gobblin Service Manager", e);
    }

    try {
      cleanUpDir(SERVICE_WORK_DIR);
    } catch (Exception e) {
      logger.warn("Could not completely cleanup Work Dir");
    }

    try {
      cleanUpDir(SPEC_STORE_PARENT_DIR);
    } catch (Exception e) {
      logger.warn("Could not completely cleanup Spec Store Parent Dir");
    }
  }

  @Test
  public void testRestart() throws Exception {
    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1");
    flowProperties.put(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY, TEST_SOURCE_NAME);
    flowProperties.put(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, TEST_SINK_NAME);
    FlowConfig flowConfig = new FlowConfig().setId(new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(MockedSpecCompiler.UNCOMPILABLE_FLOW))
        .setTemplateUris(TEST_TEMPLATE_URI).setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE).setRunImmediately(false))
        .setProperties(new StringMap(flowProperties));
    FlowSpec spec = FlowConfigResourceLocalHandler.createFlowSpecForConfig(flowConfig);

    this.gobblinServiceManager.getFlowCatalog().getSpecStore().addSpec(spec);

    serviceReboot();

    List<Spec> specs = (List<Spec>) this.gobblinServiceManager.getFlowCatalog().getSpecs();
    Assert.assertEquals(specs.size(), 1, "Flow that was created is not " + "reflecting in FlowCatalog");
    Assert.assertEquals(specs.get(0).getUri(), spec.getUri());
    Assert.assertFalse(flowConfig.getSchedule().isRunImmediately());

    // clean it
    this.gobblinServiceManager.getFlowCatalog().remove(spec.getUri());
  }

  @Test (dependsOnMethods = "testRestart")
  public void testUncompilableJob() throws Exception {
    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1");
    flowProperties.put(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY, TEST_SOURCE_NAME);
    flowProperties.put(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, TEST_SINK_NAME);
    FlowId flowId = new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(MockedSpecCompiler.UNCOMPILABLE_FLOW);
    URI uri = FlowConfigResourceLocalHandler.FlowUriUtils.createFlowSpecUri(flowId);
    FlowConfig flowConfig = new FlowConfig().setId(flowId)
        .setTemplateUris(TEST_TEMPLATE_URI).setProperties(new StringMap(flowProperties));

    RestLiResponseException exception = null;
    try {
      this.flowConfigClient.createFlowConfig(flowConfig);
    } catch (RestLiResponseException e) {
      exception = e;
    }
    Assert.assertEquals(exception.getStatus(), HttpStatus.BAD_REQUEST_400);
    // uncompilable job should not be persisted
    Assert.assertEquals(this.gobblinServiceManager.getFlowCatalog().getSpecs().size(), 0,
        "Flow that was created is not " + "reflecting in FlowCatalog");
    Assert.assertFalse(this.gobblinServiceManager.getScheduler().getScheduledFlowSpecs().containsKey(uri.toString()));
  }

  @Test (dependsOnMethods = "testUncompilableJob")
  public void testRunOnceJob() throws Exception {
    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1");
    flowProperties.put(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY, TEST_SOURCE_NAME);
    flowProperties.put(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, TEST_SINK_NAME);

    FlowConfig flowConfig = new FlowConfig().setId(TEST_FLOW_ID)
        .setTemplateUris(TEST_TEMPLATE_URI).setProperties(new StringMap(flowProperties));

    this.flowConfigClient.createFlowConfig(flowConfig);

    // runOnce job should not be persisted
    Assert.assertEquals(this.gobblinServiceManager.getFlowCatalog().getSpecs().size(), 0,
          "Flow that was created is not " + "reflecting in FlowCatalog");
    AssertWithBackoff.create().maxSleepMs(200L).timeoutMs(2000L).backoffFactor(1)
          .assertTrue(input -> !this.gobblinServiceManager.getScheduler().getScheduledFlowSpecs().containsKey(TEST_URI.toString()),
              "Waiting for job to get orchestrated...");
  }

  @Test (dependsOnMethods = "testRunOnceJob")
  public void testExplainJob() throws Exception {
    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1");
    flowProperties.put(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY, TEST_SOURCE_NAME);
    flowProperties.put(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, TEST_SINK_NAME);

    FlowConfig flowConfig = new FlowConfig().setId(new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME))
        .setTemplateUris(TEST_TEMPLATE_URI).setProperties(new StringMap(flowProperties)).setExplain(true);

    this.flowConfigClient.createFlowConfig(flowConfig);

    // explain job should not be persisted
    Assert.assertEquals(this.gobblinServiceManager.getFlowCatalog().getSpecs().size(), 0,
        "Flow that was created is not " + "reflecting in FlowCatalog");
    Assert.assertFalse(this.gobblinServiceManager.getScheduler().getScheduledFlowSpecs().containsKey(TEST_URI.toString()));
  }

  @Test (dependsOnMethods = "testExplainJob")
  public void testCreate() throws Exception {
    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1");
    flowProperties.put(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY, TEST_SOURCE_NAME);
    flowProperties.put(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, TEST_SINK_NAME);

    FlowConfig flowConfig = new FlowConfig().setId(TEST_FLOW_ID)
        .setTemplateUris(TEST_TEMPLATE_URI).setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE).setRunImmediately(true))
        .setProperties(new StringMap(flowProperties));

    this.flowConfigClient.createFlowConfig(flowConfig);
    Assert.assertEquals(this.gobblinServiceManager.getFlowCatalog().getSpecs().size(), 1,
        "Flow that was created is not " + "reflecting in FlowCatalog");
    Assert.assertTrue(this.gobblinServiceManager.getScheduler().getScheduledFlowSpecs().containsKey(TEST_URI.toString()));
  }

  @Test (dependsOnMethods = "testCreate")
  public void testCreateAgain() throws Exception {
    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1");
    flowProperties.put(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY, TEST_SOURCE_NAME);
    flowProperties.put(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, TEST_SINK_NAME);

    FlowConfig flowConfig = new FlowConfig().setId(TEST_FLOW_ID)
        .setTemplateUris(TEST_TEMPLATE_URI).setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE))
        .setProperties(new StringMap(flowProperties));

    RestLiResponseException exception = null;
    try {
      this.flowConfigClient.createFlowConfig(flowConfig);
    } catch (RestLiResponseException e) {
      exception = e;
    }
    Assert.assertNotNull(exception);
    Assert.assertEquals(exception.getStatus(), HttpStatus.CONFLICT_409);
  }

  @Test (dependsOnMethods = "testCreateAgain")
  public void testGet() throws Exception {
    FlowConfig flowConfig = this.flowConfigClient.getFlowConfig(TEST_FLOW_ID);

    Assert.assertEquals(flowConfig.getId().getFlowGroup(), TEST_GROUP_NAME);
    Assert.assertEquals(flowConfig.getId().getFlowName(), TEST_FLOW_NAME);
    Assert.assertEquals(flowConfig.getSchedule().getCronSchedule(), TEST_SCHEDULE);
    Assert.assertEquals(flowConfig.getTemplateUris(), TEST_TEMPLATE_URI);
    Assert.assertTrue(flowConfig.getSchedule().isRunImmediately());
    // Add this assert back when getFlowSpec() is changed to return the raw flow spec
    //Assert.assertEquals(flowConfig.getProperties().size(), 1);
    Assert.assertEquals(flowConfig.getProperties().get("param1"), "value1");
  }

  @Test (dependsOnMethods = "testGet")
  public void testUpdate() throws Exception {
    FlowId flowId = new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME);

    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1b");
    flowProperties.put("param2", "value2b");
    flowProperties.put(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY, TEST_SOURCE_NAME);
    flowProperties.put(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, TEST_SINK_NAME);

    FlowConfig flowConfig = new FlowConfig().setId(new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME))
        .setTemplateUris(TEST_TEMPLATE_URI).setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE))
        .setProperties(new StringMap(flowProperties));

    this.flowConfigClient.updateFlowConfig(flowConfig);

    FlowConfig retrievedFlowConfig = this.flowConfigClient.getFlowConfig(flowId);

    Assert.assertTrue(this.gobblinServiceManager.getScheduler().getScheduledFlowSpecs().containsKey(TEST_URI.toString()));
    Assert.assertEquals(retrievedFlowConfig.getId().getFlowGroup(), TEST_GROUP_NAME);
    Assert.assertEquals(retrievedFlowConfig.getId().getFlowName(), TEST_FLOW_NAME);
    Assert.assertEquals(retrievedFlowConfig.getSchedule().getCronSchedule(), TEST_SCHEDULE);
    Assert.assertEquals(retrievedFlowConfig.getTemplateUris(), TEST_TEMPLATE_URI);
    // Add this asssert when getFlowSpec() is changed to return the raw flow spec
    //Assert.assertEquals(flowConfig.getProperties().size(), 2);
    Assert.assertEquals(retrievedFlowConfig.getProperties().get("param1"), "value1b");
    Assert.assertEquals(retrievedFlowConfig.getProperties().get("param2"), "value2b");
  }

  @Test (dependsOnMethods = "testUpdate")
  public void testDelete() throws Exception {
    FlowId flowId = new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME);
    URI uri = FlowConfigResourceLocalHandler.FlowUriUtils.createFlowSpecUri(flowId);

    // make sure flow config exists
    FlowConfig flowConfig = this.flowConfigClient.getFlowConfig(flowId);
    Assert.assertEquals(flowConfig.getId().getFlowGroup(), TEST_GROUP_NAME);
    Assert.assertEquals(flowConfig.getId().getFlowName(), TEST_FLOW_NAME);

    this.flowConfigClient.deleteFlowConfig(flowId);

    try {
      this.flowConfigClient.getFlowConfig(flowId);
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.NOT_FOUND_404);
      Assert.assertFalse(this.gobblinServiceManager.getScheduler().getScheduledFlowSpecs().containsKey(uri.toString()));
      return;
    }

    Assert.fail("Get should have gotten a 404 error");
  }

  @Test (dependsOnMethods = "testDelete")
  public void testGitCreate() throws Exception {
    // push a new config file
    File testFlowFile = new File(GIT_CLONE_DIR + "/gobblin-config/testGroup/testFlow.pull");
    testFlowFile.getParentFile().mkdirs();

    Files.write("flow.name=testFlow\nflow.group=testGroup\nparam1=value20\n", testFlowFile, Charsets.UTF_8);

    Collection<Spec> specs = this.gobblinServiceManager.getFlowCatalog().getSpecs();
    Assert.assertEquals(specs.size(), 0);

    // add, commit, push
    this.gitForPush.add().addFilepattern("gobblin-config/testGroup/testFlow.pull").call();
    this.gitForPush.commit().setMessage("second commit").call();
    this.gitForPush.push().setRemote("origin").setRefSpecs(new RefSpec("master")).call();

    // polling is every 5 seconds, so wait twice as long and check
    TimeUnit.SECONDS.sleep(10);

    // spec generated using git monitor do not have schedule, so their life cycle should be similar to runOnce jobs
    Assert.assertEquals(this.gobblinServiceManager.getFlowCatalog().getSpecs().size(), 0);

    AssertWithBackoff.create().maxSleepMs(200L).timeoutMs(2000L).backoffFactor(1)
        .assertTrue(input -> !this.gobblinServiceManager.getScheduler().getScheduledFlowSpecs().containsKey(TEST_URI.toString()),
            "Waiting for job to get orchestrated...");
  }

  @Test
  public void testBadGet() throws Exception {
    FlowId flowId = new FlowId().setFlowGroup(TEST_DUMMY_GROUP_NAME).setFlowName(TEST_DUMMY_FLOW_NAME);

    try {
      this.flowConfigClient.getFlowConfig(flowId);
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.NOT_FOUND_404);
      return;
    }

    Assert.fail("Get should have raised a 404 error");
  }

  @Test
  public void testBadDelete() throws Exception {
    FlowId flowId = new FlowId().setFlowGroup(TEST_DUMMY_GROUP_NAME).setFlowName(TEST_DUMMY_FLOW_NAME);

    try {
      this.flowConfigClient.getFlowConfig(flowId);
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

    FlowConfig flowConfig = new FlowConfig()
        .setId(new FlowId().setFlowGroup(TEST_DUMMY_GROUP_NAME).setFlowName(TEST_DUMMY_FLOW_NAME))
        .setTemplateUris(TEST_TEMPLATE_URI).setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE))
        .setProperties(new StringMap(flowProperties));

    try {
      this.flowConfigClient.updateFlowConfig(flowConfig);
    } catch (RestLiResponseException e) {
      Assert.assertEquals(e.getStatus(), HttpStatus.NOT_FOUND_404);
    }
  }

  private void serviceReboot() throws Exception {
    this.gobblinServiceManager.stop();
    this.gobblinServiceManager = new MockGobblinServiceManager("CoreService", "1",
        ConfigUtils.propertiesToConfig(serviceCoreProperties), Optional.of(new Path(SERVICE_WORK_DIR)));
    this.flowConfigClient = new FlowConfigV2Client(String.format("http://localhost:%s/",
        this.gobblinServiceManager.getRestLiServer().getPort()));
    this.gobblinServiceManager.start();
  }

  class MockGobblinServiceManager extends GobblinServiceManager {

    public MockGobblinServiceManager(String serviceName, String serviceId, Config config,
        Optional<Path> serviceWorkDirOptional)
        throws Exception {
      super(serviceName, serviceId, config, serviceWorkDirOptional);
    }

    protected EmbeddedRestliServer getRestLiServer() {
      return this.restliServer;
    }
  }
}