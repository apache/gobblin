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

import com.typesafe.config.Config;
import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.api.FlowSpec;
import gobblin.runtime.api.SpecExecutorInstanceProducer;
import gobblin.runtime.api.TopologySpec;
import gobblin.runtime.spec_executorInstance.InMemorySpecExecutorInstanceProducer;
import gobblin.service.ServiceConfigKeys;
import gobblin.service.modules.flow.MultiHopsFlowToJobSpecCompiler;
import gobblin.util.ConfigUtils;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

// All unit tests here will be with templateCatelogue.
public class MultiHopsFlowToJobSpecCompilerTest {
  private static final Logger logger = LoggerFactory.getLogger(IdentityFlowToJobSpecCompilerTest.class);

  private static final String TEST_TEMPLATE_CATALOG_PATH = "/tmp/gobblinTestTemplateCatalog_" + System.currentTimeMillis();
  private static final String TEST_TEMPLATE_CATALOG_URI = "file://" + TEST_TEMPLATE_CATALOG_PATH;

  private static final String TEST_TEMPLATE_NAME = "test.template";
  private static final String TEST_TEMPLATE_URI = "FS:///test.template";

  private static final String TEST_SOURCE_NAME = "testSource";
  private static final String TEST_HOP_NAME_A = "testHopA";
  private static final String TEST_HOP_NAME_B = "testHopB";
  private static final String TEST_SINK_NAME = "testSink";
  private static final String TEST_FLOW_GROUP = "testFlowGroup";
  private static final String TEST_FLOW_NAME = "testFlowName";

  private static final String SPEC_STORE_PARENT_DIR = "/tmp/orchestrator/";
  private static final String SPEC_DESCRIPTION = "Test Orchestrator";
  private static final String SPEC_VERSION = "1";
  private static final String TOPOLOGY_SPEC_STORE_DIR = "/tmp/orchestrator/topologyTestSpecStore_" + System.currentTimeMillis();
  private static final String FLOW_SPEC_STORE_DIR = "/tmp/orchestrator/flowTestSpecStore_" + System.currentTimeMillis();



  private MultiHopsFlowToJobSpecCompiler compilerWithTemplateCalague;

  @BeforeClass
  public void setUp() throws Exception{
    // Create dir for template catalog
    FileUtils.forceMkdir(new File(TEST_TEMPLATE_CATALOG_PATH));

    // Initialize complier with template catalog
    Properties compilerWithTemplateCatalogProperties = new Properties();
    compilerWithTemplateCatalogProperties.setProperty(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY, TEST_TEMPLATE_CATALOG_URI);

    // TODO:Set all policy-related configuration
    compilerWithTemplateCatalogProperties.setProperty();

    this.compilerWithTemplateCalague = new MultiHopsFlowToJobSpecCompiler(ConfigUtils.propertiesToConfig(compilerWithTemplateCatalogProperties));

    // Add a topology to compiler
    this.compilerWithTemplateCalague.onAddSpec(initTopologySpec());
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
  public void testAdjacencyListAndInvertedIndexGeneration(){
    compilerWithTemplateCalague.inMemoryAdjacencyListAndInvertedIndexGenerator();
    Map<String, List<String>>
    Assert.assertEquals(compilerWithTemplateCalague.getAdjacencyList().size(), 3);
    Assert.assert(compilerWithTemplateCalague.getAdjacencyList());
  }

  @Test
  public void testUserSpecifiedPathCompilation(){

  }

  @Test
  public void testDijkstraPathFinding(){

  }

  // The topology is: Src - A - B - Dest
  private TopologySpec initTopologySpec() {
    Properties properties = new Properties();
    properties.put("specStore.fs.dir", TOPOLOGY_SPEC_STORE_DIR);
    String capabilitiesString = TEST_SOURCE_NAME + ":" + TEST_HOP_NAME_A + ","
        + TEST_HOP_NAME_A + ":" + TEST_HOP_NAME_B + ","
        + TEST_HOP_NAME_B + ":" + TEST_SINK_NAME;
    properties.put("specExecInstance.capabilities", capabilitiesString);
    Config config = ConfigUtils.propertiesToConfig(properties);
    SpecExecutorInstanceProducer specExecutorInstanceProducer = new InMemorySpecExecutorInstanceProducer(config);

    TopologySpec.Builder topologySpecBuilder = TopologySpec.builder(
        IdentityFlowToJobSpecCompilerTest.computeTopologySpecURI(SPEC_STORE_PARENT_DIR,
        TOPOLOGY_SPEC_STORE_DIR))
        .withConfig(config)
        .withDescription(SPEC_DESCRIPTION)
        .withVersion(SPEC_VERSION)
        .withSpecExecutorInstanceProducer(specExecutorInstanceProducer);
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
      flowSpecBuilder = FlowSpec.builder(IdentityFlowToJobSpecCompilerTest.computeTopologySpecURI(SPEC_STORE_PARENT_DIR,
          FLOW_SPEC_STORE_DIR))
          .withConfig(config)
          .withDescription("dummy description")
          .withVersion("1")
          .withTemplate(new URI(TEST_TEMPLATE_URI));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    return flowSpecBuilder.build();
  }

  private void cleanUpDir(String dir) throws Exception {
    File specStoreDir = new File(dir);
    if (specStoreDir.exists()) {
      FileUtils.deleteDirectory(specStoreDir);
    }
  }

}
