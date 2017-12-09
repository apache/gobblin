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

package org.apache.gobblin.service.modules.orchestration;

import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.spec_executorInstance.InMemorySpecExecutor;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.typesafe.config.Config;

import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.runtime.app.ServiceBasedAppLauncher;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.runtime.spec_catalog.TopologyCatalog;
import org.apache.gobblin.service.modules.flow.IdentityFlowToJobSpecCompiler;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PathUtils;


public class OrchestratorTest {
  private static final Logger logger = LoggerFactory.getLogger(TopologyCatalog.class);
  private static Gson gson = new GsonBuilder().setPrettyPrinting().create();

  private static final String SPEC_STORE_PARENT_DIR = "/tmp/orchestrator/";
  private static final String SPEC_DESCRIPTION = "Test Orchestrator";
  private static final String SPEC_VERSION = FlowSpec.Builder.DEFAULT_VERSION;
  private static final String TOPOLOGY_SPEC_STORE_DIR = "/tmp/orchestrator/topologyTestSpecStore";
  private static final String FLOW_SPEC_STORE_DIR = "/tmp/orchestrator/flowTestSpecStore";
  private static final String FLOW_SPEC_GROUP_DIR = "/tmp/orchestrator/flowTestSpecStore/flowTestGroupDir";

  private ServiceBasedAppLauncher serviceLauncher;
  private TopologyCatalog topologyCatalog;
  private TopologySpec topologySpec;

  private FlowCatalog flowCatalog;
  private FlowSpec flowSpec;

  private Orchestrator orchestrator;

  @BeforeClass
  public void setup() throws Exception {
    cleanUpDir(TOPOLOGY_SPEC_STORE_DIR);
    cleanUpDir(FLOW_SPEC_STORE_DIR);

    Properties orchestratorProperties = new Properties();

    Properties topologyProperties = new Properties();
    topologyProperties.put("specStore.fs.dir", TOPOLOGY_SPEC_STORE_DIR);

    Properties flowProperties = new Properties();
    flowProperties.put("specStore.fs.dir", FLOW_SPEC_STORE_DIR);

    this.serviceLauncher = new ServiceBasedAppLauncher(orchestratorProperties, "OrchestratorCatalogTest");

    this.topologyCatalog = new TopologyCatalog(ConfigUtils.propertiesToConfig(topologyProperties),
        Optional.of(logger));
    this.serviceLauncher.addService(topologyCatalog);

    this.flowCatalog = new FlowCatalog(ConfigUtils.propertiesToConfig(flowProperties),
        Optional.of(logger));
    this.serviceLauncher.addService(flowCatalog);

    this.orchestrator = new Orchestrator(ConfigUtils.propertiesToConfig(orchestratorProperties),
        Optional.of(this.topologyCatalog), Optional.of(logger));
    this.topologyCatalog.addListener(orchestrator);
    this.flowCatalog.addListener(orchestrator);

    // Start application
    this.serviceLauncher.start();

    // Create Spec to play with
    this.topologySpec = initTopologySpec();
    this.flowSpec = initFlowSpec();
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
    properties.put("specExecInstance.capabilities", "source:destination");
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
    Properties properties = new Properties();
    properties.put("specStore.fs.dir", FLOW_SPEC_STORE_DIR);
    properties.put("specExecInstance.capabilities", "source:destination");
    properties.put("gobblin.flow.sourceIdentifier", "source");
    properties.put("gobblin.flow.destinationIdentifier", "destination");
    Config config = ConfigUtils.propertiesToConfig(properties);

    FlowSpec.Builder flowSpecBuilder = null;
    try {
      flowSpecBuilder = FlowSpec.builder(computeTopologySpecURI(SPEC_STORE_PARENT_DIR,
          FLOW_SPEC_GROUP_DIR))
          .withConfig(config)
          .withDescription(SPEC_DESCRIPTION)
          .withVersion(SPEC_VERSION)
          .withTemplate(new URI("templateURI"));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    return flowSpecBuilder.build();
  }

  public URI computeTopologySpecURI(String parent, String current) {
    // Make sure this is relative
    URI uri = PathUtils.relativizePath(new Path(current), new Path(parent)).toUri();
    return uri;
  }

  @AfterClass
  public void cleanUp() throws Exception {
    // Shutdown Catalog
    this.serviceLauncher.stop();

    File specStoreDir = new File(SPEC_STORE_PARENT_DIR);
    if (specStoreDir.exists()) {
      FileUtils.deleteDirectory(specStoreDir);
    }
  }

  @Test
  public void createTopologySpec() {
    IdentityFlowToJobSpecCompiler specCompiler = (IdentityFlowToJobSpecCompiler) this.orchestrator.getSpecCompiler();

    // List Current Specs
    Collection<Spec> specs = topologyCatalog.getSpecs();
    logger.info("[Before Create] Number of specs: " + specs.size());
    int i=0;
    for (Spec spec : specs) {
      TopologySpec topologySpec = (TopologySpec) spec;
      logger.info("[Before Create] Spec " + i++ + ": " + gson.toJson(topologySpec));
    }
    // Make sure TopologyCatalog is empty
    Assert.assertTrue(specs.size() == 0, "Spec store should be empty before addition");
    // Make sure TopologyCatalog Listener is empty
    Assert.assertTrue(specCompiler.getTopologySpecMap().size() == 0, "SpecCompiler should not know about any Topology "
        + "before addition");

    // Create and add Spec
    this.topologyCatalog.put(topologySpec);

    // List Specs after adding
    specs = topologyCatalog.getSpecs();
    logger.info("[After Create] Number of specs: " + specs.size());
    i = 0;
    for (Spec spec : specs) {
      topologySpec = (TopologySpec) spec;
      logger.info("[After Create] Spec " + i++ + ": " + gson.toJson(topologySpec));
    }
    // Make sure TopologyCatalog has the added Topology
    Assert.assertTrue(specs.size() == 1, "Spec store should contain 1 Spec after addition");
    // Make sure TopologyCatalog Listener knows about added Topology
    Assert.assertTrue(specCompiler.getTopologySpecMap().size() == 1, "SpecCompiler should contain 1 Spec after addition");
  }

  @Test (dependsOnMethods = "createTopologySpec")
  public void createFlowSpec() throws Exception {
    // Since only 1 Topology with 1 SpecProducer has been added in previous test
    // .. it should be available and responsible for our new FlowSpec
    IdentityFlowToJobSpecCompiler specCompiler = (IdentityFlowToJobSpecCompiler) this.orchestrator.getSpecCompiler();
    SpecExecutor sei = specCompiler.getTopologySpecMap().values().iterator().next().getSpecExecutor();

    // List Current Specs
    Collection<Spec> specs = flowCatalog.getSpecs();
    logger.info("[Before Create] Number of specs: " + specs.size());
    int i=0;
    for (Spec spec : specs) {
      FlowSpec flowSpec = (FlowSpec) spec;
      logger.info("[Before Create] Spec " + i++ + ": " + gson.toJson(flowSpec));
    }
    // Make sure FlowCatalog is empty
    Assert.assertTrue(specs.size() == 0, "Spec store should be empty before addition");
    // Make sure FlowCatalog Listener is empty
    Assert.assertTrue(((List)(sei.getProducer().get().listSpecs().get())).size() == 0, "SpecProducer should not know about "
        + "any Flow before addition");

    // Create and add Spec
    this.flowCatalog.put(flowSpec);

    // List Specs after adding
    specs = flowCatalog.getSpecs();
    logger.info("[After Create] Number of specs: " + specs.size());
    i = 0;
    for (Spec spec : specs) {
      flowSpec = (FlowSpec) spec;
      logger.info("[After Create] Spec " + i++ + ": " + gson.toJson(flowSpec));
    }

    // Make sure FlowCatalog has the added Flow
    Assert.assertTrue(specs.size() == 1, "Spec store should contain 1 Spec after addition");
    // Orchestrator is a no-op listener for any new FlowSpecs
    Assert.assertTrue(((List)(sei.getProducer().get().listSpecs().get())).size() == 0, "SpecProducer should contain 0 "
        + "Spec after addition");
  }

  @Test (dependsOnMethods = "createFlowSpec")
  public void deleteFlowSpec() throws Exception {
    // Since only 1 Flow has been added in previous test it should be available
    IdentityFlowToJobSpecCompiler specCompiler = (IdentityFlowToJobSpecCompiler) this.orchestrator.getSpecCompiler();
    SpecExecutor sei = specCompiler.getTopologySpecMap().values().iterator().next().getSpecExecutor();

    // List Current Specs
    Collection<Spec> specs = flowCatalog.getSpecs();
    logger.info("[Before Delete] Number of specs: " + specs.size());
    int i=0;
    for (Spec spec : specs) {
      FlowSpec flowSpec = (FlowSpec) spec;
      logger.info("[Before Delete] Spec " + i++ + ": " + gson.toJson(flowSpec));
    }
    // Make sure FlowCatalog has the previously added Flow
    Assert.assertTrue(specs.size() == 1, "Spec store should contain 1 Flow that was added in last test");
    // Orchestrator is a no-op listener for any new FlowSpecs, so no FlowSpecs should be around
    int specsInSEI = ((List)(sei.getProducer().get().listSpecs().get())).size();
    Assert.assertTrue(specsInSEI == 0, "SpecProducer should contain 0 "
        + "Spec after addition because Orchestrator is a no-op listener for any new FlowSpecs");

    // Remove the flow
    this.flowCatalog.remove(flowSpec.getUri());

    // List Specs after adding
    specs = flowCatalog.getSpecs();
    logger.info("[After Delete] Number of specs: " + specs.size());
    i = 0;
    for (Spec spec : specs) {
      flowSpec = (FlowSpec) spec;
      logger.info("[After Delete] Spec " + i++ + ": " + gson.toJson(flowSpec));
    }

    // Make sure FlowCatalog has the Flow removed
    Assert.assertTrue(specs.size() == 0, "Spec store should not contain Spec after deletion");
    // Make sure FlowCatalog Listener knows about the deletion
    specsInSEI = ((List)(sei.getProducer().get().listSpecs().get())).size();
    Assert.assertTrue(specsInSEI == 0, "SpecProducer should not contain "
        + "Spec after deletion");
  }
}