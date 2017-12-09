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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.typesafe.config.Config;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flow.IdentityFlowToJobSpecCompiler;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.runtime.spec_executorInstance.InMemorySpecExecutor;

public class IdentityFlowToJobSpecCompilerTest {
  private static final Logger logger = LoggerFactory.getLogger(IdentityFlowToJobSpecCompilerTest.class);

  private static final String TEST_TEMPLATE_CATALOG_PATH = "/tmp/gobblinTestTemplateCatalog_" + System.currentTimeMillis();
  private static final String TEST_TEMPLATE_CATALOG_URI = "file://" + TEST_TEMPLATE_CATALOG_PATH;

  private static final String TEST_TEMPLATE_NAME = "test.template";
  private static final String TEST_TEMPLATE_URI = "FS:///test.template";

  private static final String TEST_SOURCE_NAME = "testSource";
  private static final String TEST_SINK_NAME = "testSink";
  private static final String TEST_FLOW_GROUP = "testFlowGroup";
  private static final String TEST_FLOW_NAME = "testFlowName";

  private static final String SPEC_STORE_PARENT_DIR = "/tmp/orchestrator/";
  private static final String SPEC_DESCRIPTION = "Test Orchestrator";
  private static final String SPEC_VERSION = FlowSpec.Builder.DEFAULT_VERSION;
  private static final String TOPOLOGY_SPEC_STORE_DIR = "/tmp/orchestrator/topologyTestSpecStore_" + System.currentTimeMillis();
  private static final String FLOW_SPEC_STORE_DIR = "/tmp/orchestrator/flowTestSpecStore_" + System.currentTimeMillis();

  private IdentityFlowToJobSpecCompiler compilerWithTemplateCalague;
  private IdentityFlowToJobSpecCompiler compilerWithoutTemplateCalague;

  @BeforeClass
  public void setup() throws Exception {
    // Create dir for template catalog
    setupDir(TEST_TEMPLATE_CATALOG_PATH);

    // Create template to use in test
    List<String> templateEntries = new ArrayList<>();
    templateEntries.add("testProperty1 = \"testValue1\"");
    templateEntries.add("testProperty2 = \"test.Value1\"");
    templateEntries.add("testProperty3 = 100");
    FileUtils.writeLines(new File(TEST_TEMPLATE_CATALOG_PATH + "/" + TEST_TEMPLATE_NAME), templateEntries);

    // Initialize compiler with template catalog
    Properties compilerWithTemplateCatalogProperties = new Properties();
    compilerWithTemplateCatalogProperties.setProperty(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY, TEST_TEMPLATE_CATALOG_URI);
    this.compilerWithTemplateCalague = new IdentityFlowToJobSpecCompiler(ConfigUtils.propertiesToConfig(compilerWithTemplateCatalogProperties));

    // Add a topology to compiler
    this.compilerWithTemplateCalague.onAddSpec(initTopologySpec());

    // Initialize compiler without template catalog
    this.compilerWithoutTemplateCalague = new IdentityFlowToJobSpecCompiler(ConfigUtils.propertiesToConfig(new Properties()));

    // Add a topology to compiler
    this.compilerWithoutTemplateCalague.onAddSpec(initTopologySpec());
  }

  private void setupDir(String dir) throws Exception {
    FileUtils.forceMkdir(new File(dir));
  }

  private void cleanUpDir(String dir) throws Exception {
    File specStoreDir = new File(dir);
    if (specStoreDir.exists()) {
      FileUtils.deleteDirectory(specStoreDir);
    }
  }

  private TopologySpec initTopologySpec() {
    Properties properties = new Properties();
    properties.put("specStore.fs.dir", TOPOLOGY_SPEC_STORE_DIR);
    properties.put("specExecInstance.capabilities", TEST_SOURCE_NAME + ":" + TEST_SINK_NAME);
    Config config = ConfigUtils.propertiesToConfig(properties);
    SpecExecutor specExecutorInstance = new InMemorySpecExecutor(config);

    TopologySpec.Builder topologySpecBuilder = TopologySpec.builder(computeTopologySpecURI(SPEC_STORE_PARENT_DIR,
        TOPOLOGY_SPEC_STORE_DIR))
        .withConfig(config)
        .withDescription(SPEC_DESCRIPTION)
        .withVersion(SPEC_VERSION)
        .withSpecExecutor(specExecutorInstance);
    return topologySpecBuilder.build();
  }

  private FlowSpec initFlowSpec() {
    return initFlowSpec(TEST_FLOW_GROUP, TEST_FLOW_NAME, TEST_SOURCE_NAME, TEST_SINK_NAME);
  }

  private FlowSpec initFlowSpec(String flowGroup, String flowName, String source, String destination) {
    Properties properties = new Properties();
    properties.put(ConfigurationKeys.JOB_SCHEDULE_KEY, "* * * * *");
    properties.put(ConfigurationKeys.FLOW_GROUP_KEY, flowGroup);
    properties.put(ConfigurationKeys.FLOW_NAME_KEY, flowName);
    properties.put(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY, source);
    properties.put(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, destination);
    Config config = ConfigUtils.propertiesToConfig(properties);

    FlowSpec.Builder flowSpecBuilder = null;
    try {
      flowSpecBuilder = FlowSpec.builder(computeTopologySpecURI(SPEC_STORE_PARENT_DIR,
          FLOW_SPEC_STORE_DIR))
          .withConfig(config)
          .withDescription("dummy description")
          .withVersion(SPEC_VERSION)
          .withTemplate(new URI(TEST_TEMPLATE_URI));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    return flowSpecBuilder.build();
  }

  public static URI computeTopologySpecURI(String parent, String current) {
    // Make sure this is relative
    return PathUtils.relativizePath(new Path(current), new Path(parent)).toUri();
  }

  @AfterClass
  public void cleanUp() throws Exception {
    // Cleanup Template Catalog
    try {
      cleanUpDir(TEST_TEMPLATE_CATALOG_PATH);
    } catch (Exception e) {
      logger.warn("Could not completely cleanup Template catalog dir");
    }

    // Cleanup ToplogySpec Dir
    try {
      cleanUpDir(TOPOLOGY_SPEC_STORE_DIR);
    } catch (Exception e) {
      logger.warn("Could not completely cleanup ToplogySpec catalog dir");
    }

    // Cleanup FlowSpec Dir
    try {
      cleanUpDir(FLOW_SPEC_STORE_DIR);
    } catch (Exception e) {
      logger.warn("Could not completely cleanup FlowSpec catalog dir");
    }
  }

  @Test
  public void testCompilerWithTemplateCatalog() {
    FlowSpec flowSpec = initFlowSpec();

    // Run compiler on flowSpec
    Map<Spec, SpecExecutor> specExecutorMapping = this.compilerWithTemplateCalague.compileFlow(flowSpec);

    // Assert pre-requisites
    Assert.assertNotNull(specExecutorMapping, "Expected non null mapping.");
    Assert.assertTrue(specExecutorMapping.size() == 1, "Exepected 1 executor for FlowSpec.");

    // Assert FlowSpec compilation
    Spec spec = specExecutorMapping.keySet().iterator().next();
    Assert.assertTrue(spec instanceof JobSpec, "Expected JobSpec compiled from FlowSpec.");

    // Assert JobSpec properties
    JobSpec jobSpec = (JobSpec) spec;
    Assert.assertEquals(jobSpec.getConfig().getString("testProperty1"), "testValue1");
    Assert.assertEquals(jobSpec.getConfig().getString("testProperty2"), "test.Value1");
    Assert.assertEquals(jobSpec.getConfig().getString("testProperty3"), "100");
    Assert.assertEquals(jobSpec.getConfig().getString(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY), TEST_SOURCE_NAME);
    Assert.assertFalse(jobSpec.getConfig().hasPath(ConfigurationKeys.JOB_SCHEDULE_KEY));
    Assert.assertEquals(jobSpec.getConfig().getString(ConfigurationKeys.JOB_NAME_KEY), TEST_FLOW_NAME);
    Assert.assertEquals(jobSpec.getConfig().getString(ConfigurationKeys.JOB_GROUP_KEY), TEST_FLOW_GROUP);
    Assert.assertEquals(jobSpec.getConfig().getString(ConfigurationKeys.FLOW_NAME_KEY), TEST_FLOW_NAME);
    Assert.assertEquals(jobSpec.getConfig().getString(ConfigurationKeys.FLOW_GROUP_KEY), TEST_FLOW_GROUP);
    Assert.assertTrue(jobSpec.getConfig().hasPath(ConfigurationKeys.FLOW_EXECUTION_ID_KEY));
  }

  @Test
  public void testCompilerWithoutTemplateCatalog() {
    FlowSpec flowSpec = initFlowSpec();

    // Run compiler on flowSpec
    Map<Spec, SpecExecutor> specExecutorMapping = this.compilerWithoutTemplateCalague.compileFlow(flowSpec);

    // Assert pre-requisites
    Assert.assertNotNull(specExecutorMapping, "Expected non null mapping.");
    Assert.assertTrue(specExecutorMapping.size() == 1, "Exepected 1 executor for FlowSpec.");

    // Assert FlowSpec compilation
    Spec spec = specExecutorMapping.keySet().iterator().next();
    Assert.assertTrue(spec instanceof JobSpec, "Expected JobSpec compiled from FlowSpec.");

    // Assert JobSpec properties
    JobSpec jobSpec = (JobSpec) spec;
    Assert.assertTrue(!jobSpec.getConfig().hasPath("testProperty1"));
    Assert.assertTrue(!jobSpec.getConfig().hasPath("testProperty2"));
    Assert.assertTrue(!jobSpec.getConfig().hasPath("testProperty3"));
    Assert.assertEquals(jobSpec.getConfig().getString(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY), TEST_SOURCE_NAME);
    Assert.assertFalse(jobSpec.getConfig().hasPath(ConfigurationKeys.JOB_SCHEDULE_KEY));
    Assert.assertEquals(jobSpec.getConfig().getString(ConfigurationKeys.JOB_NAME_KEY), TEST_FLOW_NAME);
    Assert.assertEquals(jobSpec.getConfig().getString(ConfigurationKeys.JOB_GROUP_KEY), TEST_FLOW_GROUP);
    Assert.assertEquals(jobSpec.getConfig().getString(ConfigurationKeys.FLOW_NAME_KEY), TEST_FLOW_NAME);
    Assert.assertEquals(jobSpec.getConfig().getString(ConfigurationKeys.FLOW_GROUP_KEY), TEST_FLOW_GROUP);
    Assert.assertTrue(jobSpec.getConfig().hasPath(ConfigurationKeys.FLOW_EXECUTION_ID_KEY));
  }

  @Test
  public void testNoJobSpecCompilation() {
    FlowSpec flowSpec = initFlowSpec(TEST_FLOW_GROUP, TEST_FLOW_NAME, "unsupportedSource", "unsupportedSink");

    // Run compiler on flowSpec
    Map<Spec, SpecExecutor> specExecutorMapping = this.compilerWithTemplateCalague.compileFlow(flowSpec);

    // Assert pre-requisites
    Assert.assertNotNull(specExecutorMapping, "Expected non null mapping.");
    Assert.assertTrue(specExecutorMapping.size() == 0, "Exepected 1 executor for FlowSpec.");
  }
}