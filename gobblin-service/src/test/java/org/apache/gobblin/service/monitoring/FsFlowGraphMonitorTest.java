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

package org.apache.gobblin.service.monitoring;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.io.FileUtils;
import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flow.MultiHopFlowCompiler;
import org.apache.gobblin.service.modules.flow.MultiHopFlowCompilerTest;
import org.apache.gobblin.service.modules.flowgraph.BaseFlowGraph;
import org.apache.gobblin.service.modules.flowgraph.DataNode;
import org.apache.gobblin.service.modules.flowgraph.FlowEdge;
import org.apache.gobblin.service.modules.flowgraph.FlowGraph;
import org.apache.gobblin.service.modules.flowgraph.FlowGraphConfigurationKeys;
import org.apache.gobblin.service.modules.template_catalog.UpdatableFSFlowTemplateCatalog;
import org.apache.hadoop.fs.Path;
import org.eclipse.jgit.transport.RefSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class FsFlowGraphMonitorTest {
  private static final Logger logger = LoggerFactory.getLogger(FsFlowGraphMonitorTest.class);
  private final File TEST_DIR = new File(FileUtils.getTempDirectory(), "flowGraphTemplates");
  private final File flowGraphTestDir = new File(TEST_DIR, "fsFlowGraphTestDir");
  private final File flowGraphDir = new File(flowGraphTestDir, "gobblin-flowgraph");
  private static final String NODE_1_FILE = "node1.properties";
  private final File node1Dir = new File(FileUtils.getTempDirectory(), "node1");
  private final File node1File = new File(node1Dir, NODE_1_FILE);
  private static final String NODE_2_FILE = "node2.properties";
  private final File node2Dir = new File(FileUtils.getTempDirectory(), "node2");
  private final File node2File = new File(node2Dir, NODE_2_FILE);
  private final File edge1Dir = new File(node1Dir, "node2");
  private final File edge1File = new File(edge1Dir, "edge1.properties");
  private final File sharedNodeFolder = new File(flowGraphTestDir, "nodes");

  private RefSpec masterRefSpec = new RefSpec("master");
  private Optional<UpdatableFSFlowTemplateCatalog> flowCatalog;
  private Config config;
  private AtomicReference<FlowGraph> flowGraph;
  private FsFlowGraphMonitor flowGraphMonitor;
  private Map<URI, TopologySpec> topologySpecMap;
  private File flowTemplateCatalogFolder;

  @BeforeClass
  public void setUp() throws Exception {
    cleanUpDir(TEST_DIR.toString());
    TEST_DIR.mkdirs();

    URI topologyCatalogUri = this.getClass().getClassLoader().getResource("topologyspec_catalog").toURI();
    this.topologySpecMap = MultiHopFlowCompilerTest.buildTopologySpecMap(topologyCatalogUri);

    this.config = ConfigBuilder.create()
        .addPrimitive(FsFlowGraphMonitor.FS_FLOWGRAPH_MONITOR_PREFIX + "."
            + ConfigurationKeys.FLOWGRAPH_ABSOLUTE_DIR, flowGraphTestDir.getAbsolutePath())
        .addPrimitive(FsFlowGraphMonitor.FS_FLOWGRAPH_MONITOR_PREFIX + "." + ConfigurationKeys.FLOWGRAPH_BASE_DIR, "gobblin-flowgraph")
        .addPrimitive(FsFlowGraphMonitor.FS_FLOWGRAPH_MONITOR_PREFIX + "." + ConfigurationKeys.FLOWGRAPH_POLLING_INTERVAL, 1)
        .addPrimitive(FsFlowGraphMonitor.FS_FLOWGRAPH_MONITOR_PREFIX + "." + FsFlowGraphMonitor.MONITOR_TEMPLATE_CATALOG_CHANGES, true)
        .build();

    // Create a FSFlowTemplateCatalog instance
    URI flowTemplateCatalogUri = this.getClass().getClassLoader().getResource("template_catalog").toURI();
    this.flowTemplateCatalogFolder = new File(TEST_DIR, "template_catalog");
    this.flowTemplateCatalogFolder.mkdirs();
    FileUtils.copyDirectory(new File(flowTemplateCatalogUri.getPath()), this.flowTemplateCatalogFolder);
    Properties properties = new Properties();
    this.flowGraphDir.mkdirs();
    properties.put(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY, this.flowTemplateCatalogFolder.getAbsolutePath());
    Config config = ConfigFactory.parseProperties(properties);
    Config templateCatalogCfg = config
        .withValue(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY,
            config.getValue(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY));
    this.flowCatalog = Optional.of(new UpdatableFSFlowTemplateCatalog(templateCatalogCfg,  new ReentrantReadWriteLock(true)));

    //Create a FlowGraph instance with defaults
    this.flowGraph = new AtomicReference<>(new BaseFlowGraph());
    MultiHopFlowCompiler mhfc = new MultiHopFlowCompiler(config, this.flowGraph);
    this.flowGraphMonitor = new FsFlowGraphMonitor(this.config, this.flowCatalog, mhfc, topologySpecMap, new CountDownLatch(1), true);
    this.flowGraphMonitor.startUp();
    this.flowGraphMonitor.setActive(true);
  }

  @Test
  public void testAddNode() throws Exception {
    String file1Contents = FlowGraphConfigurationKeys.DATA_NODE_IS_ACTIVE_KEY + "=true\nparam1=value1\n";
    String file2Contents = FlowGraphConfigurationKeys.DATA_NODE_IS_ACTIVE_KEY + "=true\nparam2=value2\n";

    addNode(this.node1Dir, this.node1File, file1Contents);
    addNode(this.node2Dir, this.node2File, file2Contents);

    // Let the monitor pick up the nodes that were recently added
    Thread.sleep(3000);
    for (int i = 0; i < 2; i++) {
      String nodeId = "node" + (i + 1);
      String paramKey = "param" + (i + 1);
      String paramValue = "value" + (i + 1);
      //Check if nodes have been added to the FlowGraph
      DataNode dataNode = this.flowGraph.get().getNode(nodeId);
      Assert.assertEquals(dataNode.getId(), nodeId);
      Assert.assertTrue(dataNode.isActive());
      Assert.assertEquals(dataNode.getRawConfig().getString(paramKey), paramValue);
    }
  }

  @Test (dependsOnMethods = "testAddNode")
  public void testAddEdge() throws Exception {
    //Build contents of edge file
    String fileContents = buildEdgeFileContents("node1", "node2", "edge1", "value1");
    addEdge(this.edge1Dir, this.edge1File, fileContents);
    // Let the monitor pick up the edges that were recently added
    Thread.sleep(3000);

    //Check if edge1 has been added to the FlowGraph
    testIfEdgeSuccessfullyAdded("node1", "node2", "edge1", "value1");
  }

  @Test (dependsOnMethods = "testAddEdge")
  public void testUpdateEdge() throws Exception {
    //Update edge1 file
    String fileContents = buildEdgeFileContents("node1", "node2", "edge1", "value2");
    addEdge(this.edge1Dir, this.edge1File, fileContents);
    // Let the monitor pick up the edges that were recently added
    Thread.sleep(3000);

    //Check if new edge1 has been added to the FlowGraph
    testIfEdgeSuccessfullyAdded("node1", "node2", "edge1", "value2");
  }

  @Test (dependsOnMethods = "testUpdateEdge")
  public void testUpdateNode() throws Exception {
    //Update param1 value in node1 and check if updated node is added to the graph
    String fileContents = FlowGraphConfigurationKeys.DATA_NODE_IS_ACTIVE_KEY + "=true\nparam1=value3\n";
    addNode(this.node1Dir, this.node1File, fileContents);
    // Let the monitor pick up the edges that were recently added
    Thread.sleep(3000);
    //Check if node has been updated in the FlowGraph
    DataNode dataNode = this.flowGraph.get().getNode("node1");
    Assert.assertEquals(dataNode.getId(), "node1");
    Assert.assertTrue(dataNode.isActive());
    Assert.assertEquals(dataNode.getRawConfig().getString("param1"), "value3");
  }

  @Test (dependsOnMethods = "testUpdateNode")
  public void testSetUpExistingGraph() throws Exception {
    // Create a FlowGraph instance with defaults
    this.flowGraphMonitor.shutDown();
    this.flowGraph = new AtomicReference<>(new BaseFlowGraph());
    MultiHopFlowCompiler mhfc = new MultiHopFlowCompiler(config, this.flowGraph);

    this.flowGraphMonitor = new FsFlowGraphMonitor(this.config, this.flowCatalog, mhfc, this.topologySpecMap, new CountDownLatch(1), true);
    this.flowGraphMonitor.startUp();
    this.flowGraphMonitor.setActive(true);

    // Let the monitor repopulate the flowgraph
    Thread.sleep(3000);

    Assert.assertNotNull(this.flowGraph.get().getNode("node1"));
    Assert.assertNotNull(this.flowGraph.get().getNode("node2"));
    Assert.assertEquals(this.flowGraph.get().getEdges("node1").size(), 1);
  }

  @Test (dependsOnMethods = "testSetUpExistingGraph")
  public void testSharedFlowgraphHelper() throws Exception {
    this.flowGraphMonitor.shutDown();
    Config sharedFlowgraphConfig = ConfigFactory.empty()
        .withValue(ServiceConfigKeys.GOBBLIN_SERVICE_FLOWGRAPH_HELPER_KEY, ConfigValueFactory.fromAnyRef("org.apache.gobblin.service.modules.flowgraph.SharedFlowGraphHelper"))
        .withFallback(this.config);


    this.flowGraph = new AtomicReference<>(new BaseFlowGraph());
    MultiHopFlowCompiler mhfc = new MultiHopFlowCompiler(config, this.flowGraph);
    // Set up node 3
    File node3Folder = new File(this.flowGraphDir, "node3");
    node3Folder.mkdirs();
    File node3File = new File(this.sharedNodeFolder, "node3.conf");
    String file3Contents = FlowGraphConfigurationKeys.DATA_NODE_IS_ACTIVE_KEY + "=true\nparam3=value3\n";

    // Have different default values for node 1
    File node1File = new File(this.sharedNodeFolder, "node1.properties");
    String file1Contents = FlowGraphConfigurationKeys.DATA_NODE_IS_ACTIVE_KEY + "=true\nparam2=value10\n";

    createNewFile(this.sharedNodeFolder, node3File, file3Contents);
    createNewFile(this.sharedNodeFolder, node1File, file1Contents);

    this.flowGraphMonitor = new FsFlowGraphMonitor(sharedFlowgraphConfig, this.flowCatalog, mhfc, this.topologySpecMap, new CountDownLatch(1), true);
    this.flowGraphMonitor.startUp();
    this.flowGraphMonitor.setActive(true);
    // Let the monitor repopulate the flowgraph
    Thread.sleep(3000);
    Assert.assertNotNull(this.flowGraph.get().getNode("node3"));
    DataNode node1 = this.flowGraph.get().getNode("node1");
    Assert.assertTrue(node1.isActive());
    Assert.assertEquals(node1.getRawConfig().getString("param2"), "value10");
  }

  @Test (dependsOnMethods = "testSharedFlowgraphHelper")
  public void testUpdateOnlyTemplates() throws Exception {
    Assert.assertEquals(this.flowGraph.get().getEdges("node1").size(), 1);

    //If deleting all the templates, the cache of flow templates will be cleared and the flowgraph will be unable to add edges on reload.
    cleanUpDir(this.flowTemplateCatalogFolder.getAbsolutePath());
    Thread.sleep(3000);
    Assert.assertEquals(this.flowGraph.get().getEdges("node1").size(), 0);

    URI flowTemplateCatalogUri = this.getClass().getClassLoader().getResource("template_catalog").toURI();
    // Adding the flowtemplates back will make the edges eligible to be added again on reload.
    FileUtils.copyDirectory(new File(flowTemplateCatalogUri.getPath()), this.flowTemplateCatalogFolder);

    Thread.sleep(3000);
    Assert.assertEquals(this.flowGraph.get().getEdges("node1").size(), 1);
  }

  @Test (dependsOnMethods = "testUpdateOnlyTemplates")
  public void testRemoveEdge() throws Exception {
    //Node1 has 1 edge before delete
    Collection<FlowEdge> edgeSet = this.flowGraph.get().getEdges("node1");
    Assert.assertEquals(edgeSet.size(), 1);
    File edgeFile = new File(this.flowGraphDir.getAbsolutePath(), node1Dir.getName() + Path.SEPARATOR_CHAR + edge1Dir.getName() + Path.SEPARATOR_CHAR + edge1File.getName());

    edgeFile.delete();
    // Let the monitor pick up the edges that were recently deleted
    Thread.sleep(3000);

    //Check if edge1 has been deleted from the graph
    edgeSet = this.flowGraph.get().getEdges("node1");
    Assert.assertEquals(edgeSet.size(), 0);
  }

  @Test (dependsOnMethods = "testRemoveEdge")
  public void testRemoveNode() throws Exception {
    //Ensure node1 and node2 are present in the graph before delete
    DataNode node1 = this.flowGraph.get().getNode("node1");
    Assert.assertNotNull(node1);
    DataNode node2 = this.flowGraph.get().getNode("node2");
    Assert.assertNotNull(node2);


    File node1FlowGraphFile = new File(this.flowGraphDir.getAbsolutePath(), node1Dir.getName());
    File node2FlowGraphFile = new File(this.flowGraphDir.getAbsolutePath(), node2Dir.getName());
    //delete node files
    FileUtils.deleteDirectory(node1FlowGraphFile);
    FileUtils.deleteDirectory(node2FlowGraphFile);
    // Let the monitor pick up the edges that were recently deleted
    Thread.sleep(3000);

    //Check if node1 and node 2 have been deleted from the graph
    node1 = this.flowGraph.get().getNode("node1");
    Assert.assertNull(node1);
    node2 = this.flowGraph.get().getNode("node2");
    Assert.assertNull(node2);
  }


  @AfterClass
  public void tearDown() throws Exception {
    cleanUpDir(TEST_DIR.toString());
  }

  private void createNewFile(File dir, File file, String fileContents) throws IOException {
    if (!dir.exists()) {
      dir.mkdirs();
    }
    file.createNewFile();
    Files.write(fileContents, file, Charsets.UTF_8);
  }

  private void addNode(File nodeDir, File nodeFile, String fileContents) throws IOException {
    createNewFile(nodeDir, nodeFile, fileContents);
    File destinationFile = new File(this.flowGraphDir.getAbsolutePath(), nodeDir.getName() + Path.SEPARATOR_CHAR + nodeFile.getName());
    logger.info(destinationFile.toString());
    if (destinationFile.exists()) {
      // clear file
      Files.write(new byte[0], destinationFile);
      Files.write(fileContents, destinationFile, Charsets.UTF_8);
    } else {
      FileUtils.moveDirectory(nodeDir, destinationFile.getParentFile());
    }
  }

  private void addEdge(File edgeDir, File edgeFile, String fileContents) throws Exception {
    createNewFile(edgeDir, edgeFile, fileContents);
    File destinationFile =  new File(this.flowGraphDir.getAbsolutePath(), edgeDir.getParentFile().getName() + Path.SEPARATOR_CHAR +  edgeDir.getName() + Path.SEPARATOR_CHAR + edgeFile.getName());
    if (destinationFile.exists()) {
      // clear old properties file
      Files.write(new byte[0], destinationFile);
      Files.write(fileContents, destinationFile, Charsets.UTF_8);
    } else {
      FileUtils.moveDirectory(edgeDir, destinationFile.getParentFile());
    }
  }

  private String buildEdgeFileContents(String node1, String node2, String edgeName, String value) {
    String fileContents = FlowGraphConfigurationKeys.FLOW_EDGE_SOURCE_KEY + "=" + node1 + "\n"
        + FlowGraphConfigurationKeys.FLOW_EDGE_DESTINATION_KEY + "=" + node2 + "\n"
        + FlowGraphConfigurationKeys.FLOW_EDGE_NAME_KEY + "=" + edgeName + "\n"
        + FlowGraphConfigurationKeys.FLOW_EDGE_IS_ACTIVE_KEY + "=true\n"
        + FlowGraphConfigurationKeys.FLOW_EDGE_TEMPLATE_DIR_URI_KEY + "=FS:///flowEdgeTemplate\n"
        + FlowGraphConfigurationKeys.FLOW_EDGE_SPEC_EXECUTORS_KEY + "=testExecutor1,testExecutor2\n"
        + "key1=" + value + "\n";
    return fileContents;
  }

  private void testIfEdgeSuccessfullyAdded(String node1, String node2, String edgeName, String value) throws ExecutionException, InterruptedException {
    Collection<FlowEdge> edgeSet = this.flowGraph.get().getEdges(node1);
    Assert.assertEquals(edgeSet.size(), 1);
    FlowEdge flowEdge = edgeSet.iterator().next();
    Assert.assertEquals(flowEdge.getId(), Joiner.on("_").join(node1, node2, edgeName));
    Assert.assertEquals(flowEdge.getSrc(), node1);
    Assert.assertEquals(flowEdge.getDest(), node2);
    Assert.assertEquals(flowEdge.getExecutors().get(0).getConfig().get().getString("specStore.fs.dir"), "/tmp1");
    Assert.assertEquals(flowEdge.getExecutors().get(0).getConfig().get().getString("specExecInstance.capabilities"), "s1:d1");
    Assert.assertEquals(flowEdge.getExecutors().get(0).getClass().getSimpleName(), "InMemorySpecExecutor");
    Assert.assertEquals(flowEdge.getExecutors().get(1).getConfig().get().getString("specStore.fs.dir"), "/tmp2");
    Assert.assertEquals(flowEdge.getExecutors().get(1).getConfig().get().getString("specExecInstance.capabilities"), "s2:d2");
    Assert.assertEquals(flowEdge.getExecutors().get(1).getClass().getSimpleName(), "InMemorySpecExecutor");
    Assert.assertEquals(flowEdge.getConfig().getString("key1"), value);
  }

  private void cleanUpDir(String dir) {
    File dirToDelete = new File(dir);

    // cleanup is flaky on Travis, so retry a few times and then suppress the error if unsuccessful
    for (int i = 0; i < 5; i++) {
      try {
        if (dirToDelete.exists()) {
          FileUtils.deleteDirectory(dirToDelete);
        }
        // if delete succeeded then break out of loop
        break;
      } catch (IOException e) {
        logger.warn("Cleanup delete directory failed for directory: " + dir, e);
      }
    }
  }
}