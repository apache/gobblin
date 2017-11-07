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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.typesafe.config.Config;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.jgrapht.graph.DirectedWeightedMultigraph;
import org.jgrapht.graph.WeightedMultigraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.spec_executorInstance.BaseServiceNodeImpl;
import org.apache.gobblin.runtime.api.FlowEdge;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.ServiceNode;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.runtime.spec_executorInstance.InMemorySpecExecutor;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flow.LoadBasedFlowEdgeImpl;
import org.apache.gobblin.service.modules.flow.MultiHopsFlowToJobSpecCompiler;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PathUtils;

import static org.apache.gobblin.service.modules.utils.FindPathUtils.*;


// All unit tests here will be with templateCatelogue.
public class MultiHopsFlowToJobSpecCompilerTest {
  private static final Logger logger = LoggerFactory.getLogger(MultiHopsFlowToJobSpecCompilerTest.class);

  private static final String TEST_TEMPLATE_CATALOG_PATH = "/tmp/gobblinTestTemplateCatalog_" + System.currentTimeMillis();
  private static final String TEST_TEMPLATE_CATALOG_URI = "file://" + TEST_TEMPLATE_CATALOG_PATH;

  private static final String TEST_TEMPLATE_NAME = "test.template";
  private static final String TEST_TEMPLATE_URI = "FS:///test.template";

  // The path to be discovered is TEST_SOURCE_NAME -> TEST_HOP_NAME_A -> TEST_HOP_NAME_B -> TEST_SINK_NAME
  private static final String TEST_SOURCE_NAME = "testSource";
  private static final String TEST_HOP_NAME_A = "testHopA";
  private static final String TEST_HOP_NAME_B = "testHopB";
  private static final String TEST_HOP_NAME_C = "testHopC";
  private static final String TEST_SINK_NAME = "testSink";
  private static final String TEST_FLOW_GROUP = "testFlowGroup";
  private static final String TEST_FLOW_NAME = "testFlowName";

  private static final String SPEC_STORE_PARENT_DIR = "/tmp/orchestrator/";
  private static final String SPEC_DESCRIPTION = "Test Orchestrator";
  private static final String SPEC_VERSION = FlowSpec.Builder.DEFAULT_VERSION;
  private static final String TOPOLOGY_SPEC_STORE_DIR = "/tmp/orchestrator/topologyTestSpecStore_" + System.currentTimeMillis();
  private static final String TOPOLOGY_SPEC_STORE_DIR_SECOND = "/tmp/orchestrator/topologyTestSpecStore_" + System.currentTimeMillis() + "_2";
  private static final String FLOW_SPEC_STORE_DIR = "/tmp/orchestrator/flowTestSpecStore_" + System.currentTimeMillis();

  private ServiceNode vertexSource;
  private ServiceNode vertexHopA;
  private ServiceNode vertexHopB;
  private ServiceNode vertexHopC;
  private ServiceNode vertexSink;

  private MultiHopsFlowToJobSpecCompiler compilerWithTemplateCalague;
  private Map<String, List<URI>> edgeTemplateMap;


  @BeforeClass
  public void setUp() throws Exception{
    // Create dir for template catalog
    FileUtils.forceMkdir(new File(TEST_TEMPLATE_CATALOG_PATH));

    // Create template to use in test
    List<String> templateEntries = new ArrayList<>();
    templateEntries.add("testProperty1 = \"testValue1\"");
    templateEntries.add("testProperty2 = \"test.Value1\"");
    templateEntries.add("testProperty3 = 100");
    FileUtils.writeLines(new File(TEST_TEMPLATE_CATALOG_PATH + "/" + TEST_TEMPLATE_NAME), templateEntries);

    // Initialize complier with template catalog
    Properties compilerWithTemplateCatalogProperties = new Properties();
    compilerWithTemplateCatalogProperties.setProperty(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY, TEST_TEMPLATE_CATALOG_URI);

    // Initialize compiler with common useful properties
    String testPath = TEST_SOURCE_NAME + "," + TEST_HOP_NAME_A + "," + TEST_HOP_NAME_B + "," + TEST_SINK_NAME;
    compilerWithTemplateCatalogProperties.setProperty(ServiceConfigKeys.POLICY_BASED_DATA_MOVEMENT_PATH, testPath);

    this.compilerWithTemplateCalague = new MultiHopsFlowToJobSpecCompiler(ConfigUtils.propertiesToConfig(compilerWithTemplateCatalogProperties));

    vertexSource = new BaseServiceNodeImpl(TEST_SOURCE_NAME);
    vertexHopA = new BaseServiceNodeImpl(TEST_HOP_NAME_A);
    vertexHopB = new BaseServiceNodeImpl(TEST_HOP_NAME_B);
    vertexHopC = new BaseServiceNodeImpl(TEST_HOP_NAME_C);
    vertexSink = new BaseServiceNodeImpl(TEST_SINK_NAME);

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
  public void testWeightedGraphConstruction(){
    FlowSpec flowSpec = initFlowSpec();
    TopologySpec topologySpec = initTopologySpec(TOPOLOGY_SPEC_STORE_DIR, TEST_SOURCE_NAME, TEST_HOP_NAME_A, TEST_HOP_NAME_B, TEST_SINK_NAME);
    this.compilerWithTemplateCalague.onAddSpec(topologySpec);

    // invocation of compileFlow trigger the weighedGraph construction
    this.compilerWithTemplateCalague.compileFlow(flowSpec);
    DirectedWeightedMultigraph<ServiceNode, FlowEdge> weightedGraph = compilerWithTemplateCalague.getWeightedGraph();

    Assert.assertTrue(weightedGraph.containsVertex(vertexSource));
    Assert.assertTrue(weightedGraph.containsVertex(vertexHopA));
    Assert.assertTrue(weightedGraph.containsVertex(vertexHopB));
    Assert.assertTrue(weightedGraph.containsVertex(vertexSink));

    FlowEdge edgeSrc2A = new LoadBasedFlowEdgeImpl(vertexSource, vertexHopA, topologySpec.getSpecExecutor());
    FlowEdge edgeA2B = new LoadBasedFlowEdgeImpl(vertexHopA, vertexHopB, topologySpec.getSpecExecutor());
    FlowEdge edgeB2Sink = new LoadBasedFlowEdgeImpl(vertexHopB, vertexSink, topologySpec.getSpecExecutor());

    Assert.assertTrue(weightedGraph.containsEdge(edgeSrc2A));
    Assert.assertTrue(weightedGraph.containsEdge(edgeA2B));
    Assert.assertTrue(weightedGraph.containsEdge(edgeB2Sink));

    Assert.assertTrue(edgeEqual(weightedGraph.getEdge(vertexSource, vertexHopA), edgeSrc2A));
    Assert.assertTrue(edgeEqual(weightedGraph.getEdge(vertexHopA, vertexHopB), edgeA2B));
    Assert.assertTrue(edgeEqual(weightedGraph.getEdge(vertexHopB, vertexSink), edgeB2Sink));

    this.compilerWithTemplateCalague.onDeleteSpec(topologySpec.getUri(), "");
  }

  @Test
  public void testUserSpecifiedPathCompilation(){
    // TODO
  }

  @Test
  public void testServicePolicy(){
    // Initialize compiler with some blacklist properties
    Properties properties = new Properties();
    properties.setProperty(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY, TEST_TEMPLATE_CATALOG_URI);
    String testPath = TEST_SOURCE_NAME + "," + TEST_HOP_NAME_A + "," + TEST_HOP_NAME_B + "," + TEST_SINK_NAME;
    properties.setProperty(ServiceConfigKeys.POLICY_BASED_DATA_MOVEMENT_PATH, testPath);
    properties.setProperty(ServiceConfigKeys.POLICY_BASED_BLOCKED_NODES,
        "testHopA");
    MultiHopsFlowToJobSpecCompiler compiler = new MultiHopsFlowToJobSpecCompiler(ConfigUtils.propertiesToConfig(properties));


    FlowSpec flowSpec = initFlowSpec();
    TopologySpec topologySpec = initTopologySpec(TOPOLOGY_SPEC_STORE_DIR, TEST_SOURCE_NAME, TEST_HOP_NAME_A, TEST_HOP_NAME_B, TEST_SINK_NAME);
    compiler.onAddSpec(topologySpec);

    // invocation of compileFlow trigger the weighedGraph construction
    compiler.compileFlow(flowSpec);

    compiler.servicePolicy.populateBlackListedEdges(compiler.getWeightedGraph());
    Assert.assertEquals(compiler.servicePolicy.getBlacklistedEdges().size(), 2);

    FlowEdge edgeSrc2A = new LoadBasedFlowEdgeImpl(vertexSource, vertexHopA, topologySpec.getSpecExecutor());
    FlowEdge edgeA2B = new LoadBasedFlowEdgeImpl(vertexHopA, vertexHopB, topologySpec.getSpecExecutor());

    Assert.assertTrue(compiler.servicePolicy.getBlacklistedEdges().contains(edgeSrc2A));
    Assert.assertTrue(compiler.servicePolicy.getBlacklistedEdges().contains(edgeA2B));

  }

  @Test (dependsOnMethods = "testWeightedGraphConstruction")
  public void testDijkstraPathFinding(){

    FlowSpec flowSpec = initFlowSpec();
    TopologySpec topologySpec_1 = initTopologySpec(TOPOLOGY_SPEC_STORE_DIR, TEST_SOURCE_NAME, TEST_HOP_NAME_A, TEST_HOP_NAME_B, TEST_SINK_NAME);
    TopologySpec topologySpec_2 = initTopologySpec(TOPOLOGY_SPEC_STORE_DIR_SECOND, TEST_SOURCE_NAME, TEST_HOP_NAME_B, TEST_HOP_NAME_C, TEST_SINK_NAME);
    this.compilerWithTemplateCalague.onAddSpec(topologySpec_1);
    this.compilerWithTemplateCalague.onAddSpec(topologySpec_2);

    // Get the edge -> Change the weight -> Materialized the edge change back to graph -> compile again -> Assertion
    this.compilerWithTemplateCalague.compileFlow(flowSpec);
    DirectedWeightedMultigraph<ServiceNode, FlowEdge> weightedGraph = compilerWithTemplateCalague.getWeightedGraph();
    FlowEdge a2b= weightedGraph.getEdge(vertexHopA, vertexHopB);
    FlowEdge b2c = weightedGraph.getEdge(vertexHopB, vertexHopC);
    FlowEdge c2s = weightedGraph.getEdge(vertexHopC, vertexSink);
    weightedGraph.setEdgeWeight(a2b, 1.99);
    weightedGraph.setEdgeWeight(b2c, 0.1);
    weightedGraph.setEdgeWeight(c2s, 0.2);

    // Best route: Src - B(1) - C(0.1) - sink (0.2)
    this.compilerWithTemplateCalague.compileFlow(flowSpec);
    List<FlowEdge> edgeList = dijkstraBasedPathFindingHelper(vertexSource, vertexSink, weightedGraph);

    FlowEdge src2b = weightedGraph.getEdge(vertexSource, vertexHopB);
    FlowEdge b2C = weightedGraph.getEdge(vertexHopB, vertexHopC);
    FlowEdge c2sink = weightedGraph.getEdge(vertexHopC, vertexSink);
    Assert.assertEquals(edgeList.get(0).getEdgeIdentity(), src2b.getEdgeIdentity());
    Assert.assertEquals(edgeList.get(1).getEdgeIdentity(), b2C.getEdgeIdentity());
    Assert.assertEquals(edgeList.get(2).getEdgeIdentity(), c2sink.getEdgeIdentity());

    this.compilerWithTemplateCalague.onDeleteSpec(topologySpec_1.getUri(), "");
    this.compilerWithTemplateCalague.onDeleteSpec(topologySpec_2.getUri(), "");
  }

  // The topology is: Src - A - B - Dest
  private TopologySpec initTopologySpec(String storeDir, String ... args) {
    Properties properties = new Properties();
    properties.put("specStore.fs.dir", storeDir);
    String capabilitiesString = "";
    for(int i =0 ; i < args.length - 1 ; i ++ ) {
      capabilitiesString = capabilitiesString + ( args[i] + ":" + args[i+1] + ",");
    }
    Assert.assertEquals(capabilitiesString.charAt(capabilitiesString.length() - 1) , ',');
    capabilitiesString = capabilitiesString.substring(0, capabilitiesString.length() - 1 );
    properties.put("specExecInstance.capabilities", capabilitiesString);
    properties.put("executorAttrs", new Properties());
    Config config = ConfigUtils.propertiesToConfig(properties);
    SpecExecutor specExecutorInstance = new InMemorySpecExecutor(config);

    TopologySpec.Builder topologySpecBuilder = TopologySpec.builder(
        IdentityFlowToJobSpecCompilerTest.computeTopologySpecURI(SPEC_STORE_PARENT_DIR,
            storeDir))
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

  private void cleanUpDir(String dir) throws Exception {
    File specStoreDir = new File(dir);
    if (specStoreDir.exists()) {
      FileUtils.deleteDirectory(specStoreDir);
    }
  }

  // Criteria for FlowEdge to be equal in testing context
  private boolean edgeEqual(FlowEdge a, FlowEdge b){
    return (a.getEdgeIdentity().equals(b.getEdgeIdentity()) &&
        ((LoadBasedFlowEdgeImpl)a).getEdgeLoad() == ((LoadBasedFlowEdgeImpl)b).getEdgeLoad());
  }

  // Use this function for
  private void populateTemplateMap(WeightedMultigraph<ServiceNode, FlowEdge> weightedGraph, URI exempliedURI){
    this.edgeTemplateMap.clear();
    Set<FlowEdge> allEdges = weightedGraph.edgeSet();
    for ( FlowEdge edge : allEdges ) {
      this.edgeTemplateMap.put(edge.getEdgeIdentity(), Arrays.asList(exempliedURI)) ;
    }
  }

  public static URI computeTopologySpecURI(String parent, String current) {
    // Make sure this is relative
    return PathUtils.relativizePath(new Path(current), new Path(parent)).toUri();
  }

}