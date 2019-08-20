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
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.gobblin.service.modules.template_catalog.FSFlowTemplateCatalog;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.dircache.DirCache;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.RepositoryCache;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.util.FS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flow.MultiHopFlowCompilerTest;
import org.apache.gobblin.service.modules.flowgraph.BaseFlowGraph;
import org.apache.gobblin.service.modules.flowgraph.DataNode;
import org.apache.gobblin.service.modules.flowgraph.FlowEdge;
import org.apache.gobblin.service.modules.flowgraph.FlowGraphConfigurationKeys;


public class GitFlowGraphMonitorTest {
  private static final Logger logger = LoggerFactory.getLogger(GitFlowGraphMonitor.class);
  private Repository remoteRepo;
  private Git gitForPush;
  private static final String TEST_DIR = "/tmp/gitFlowGraphTestDir";
  private final File remoteDir = new File(TEST_DIR + "/remote");
  private final File cloneDir = new File(TEST_DIR + "/clone");
  private final File flowGraphDir = new File(cloneDir, "/gobblin-flowgraph");
  private static final String NODE_1_FILE = "node1.properties";
  private final File node1Dir = new File(flowGraphDir, "node1");
  private final File node1File = new File(node1Dir, NODE_1_FILE);
  private static final String NODE_2_FILE = "node2.properties";
  private final File node2Dir = new File(flowGraphDir, "node2");
  private final File node2File = new File(node2Dir, NODE_2_FILE);
  private final File edge1Dir = new File(node1Dir, "node2");
  private final File edge1File = new File(edge1Dir, "edge1.properties");

  private RefSpec masterRefSpec = new RefSpec("master");
  private Optional<FSFlowTemplateCatalog> flowCatalog;
  private Config config;
  private BaseFlowGraph flowGraph;
  private GitFlowGraphMonitor gitFlowGraphMonitor;

  @BeforeClass
  public void setUp() throws Exception {
    cleanUpDir(TEST_DIR);

    // Create a bare repository
    RepositoryCache.FileKey fileKey = RepositoryCache.FileKey.exact(remoteDir, FS.DETECTED);
    this.remoteRepo = fileKey.open(false);
    this.remoteRepo.create(true);

    this.gitForPush = Git.cloneRepository().setURI(this.remoteRepo.getDirectory().getAbsolutePath()).setDirectory(cloneDir).call();

    // push an empty commit as a base for detecting changes
    this.gitForPush.commit().setMessage("First commit").call();
    this.gitForPush.push().setRemote("origin").setRefSpecs(this.masterRefSpec).call();

    URI topologyCatalogUri = this.getClass().getClassLoader().getResource("topologyspec_catalog").toURI();
    Map<URI, TopologySpec> topologySpecMap = MultiHopFlowCompilerTest.buildTopologySpecMap(topologyCatalogUri);

    this.config = ConfigBuilder.create()
        .addPrimitive(GitFlowGraphMonitor.GIT_FLOWGRAPH_MONITOR_PREFIX + "."
            + ConfigurationKeys.GIT_MONITOR_REPO_URI, this.remoteRepo.getDirectory().getAbsolutePath())
        .addPrimitive(GitFlowGraphMonitor.GIT_FLOWGRAPH_MONITOR_PREFIX + "." + ConfigurationKeys.GIT_MONITOR_REPO_DIR, TEST_DIR + "/git-flowgraph")
        .addPrimitive(GitFlowGraphMonitor.GIT_FLOWGRAPH_MONITOR_PREFIX + "." + ConfigurationKeys.GIT_MONITOR_POLLING_INTERVAL, 5)
        .build();

    // Create a FSFlowTemplateCatalog instance
    URI flowTemplateCatalogUri = this.getClass().getClassLoader().getResource("template_catalog").toURI();
    Properties properties = new Properties();
    properties.put(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY, flowTemplateCatalogUri.toString());
    Config config = ConfigFactory.parseProperties(properties);
    Config templateCatalogCfg = config
        .withValue(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY,
            config.getValue(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY));
    this.flowCatalog = Optional.of(new FSFlowTemplateCatalog(templateCatalogCfg));

    //Create a FlowGraph instance with defaults
    this.flowGraph = new BaseFlowGraph();

    this.gitFlowGraphMonitor = new GitFlowGraphMonitor(this.config, this.flowCatalog, this.flowGraph, topologySpecMap, new CountDownLatch(1));
    this.gitFlowGraphMonitor.setActive(true);
  }

  @Test
  public void testAddNode() throws IOException, GitAPIException {
    String file1Contents = FlowGraphConfigurationKeys.DATA_NODE_IS_ACTIVE_KEY + "=true\nparam1=value1\n";
    String file2Contents = FlowGraphConfigurationKeys.DATA_NODE_IS_ACTIVE_KEY + "=true\nparam2=value2\n";

    addNode(this.node1Dir, this.node1File, file1Contents);
    addNode(this.node2Dir, this.node2File, file2Contents);

    this.gitFlowGraphMonitor.processGitConfigChanges();

    for (int i = 0; i < 1; i++) {
      String nodeId = "node" + (i + 1);
      String paramKey = "param" + (i + 1);
      String paramValue = "value" + (i + 1);
      //Check if nodes have been added to the FlowGraph
      DataNode dataNode = this.flowGraph.getNode(nodeId);
      Assert.assertEquals(dataNode.getId(), nodeId);
      Assert.assertTrue(dataNode.isActive());
      Assert.assertEquals(dataNode.getRawConfig().getString(paramKey), paramValue);
    }
  }

  @Test (dependsOnMethods = "testAddNode")
  public void testAddEdge()
      throws IOException, GitAPIException, ExecutionException, InterruptedException {
    //Build contents of edge file
    String fileContents = buildEdgeFileContents("node1", "node2", "edge1", "value1");
    addEdge(this.edge1Dir, this.edge1File, fileContents);

    this.gitFlowGraphMonitor.processGitConfigChanges();

    //Check if edge1 has been added to the FlowGraph
    testIfEdgeSuccessfullyAdded("node1", "node2", "edge1", "value1");
  }

  @Test (dependsOnMethods = "testAddNode")
  public void testUpdateEdge()
      throws IOException, GitAPIException, URISyntaxException, ExecutionException, InterruptedException {
    //Update edge1 file
    String fileContents = buildEdgeFileContents("node1", "node2", "edge1", "value2");

    addEdge(this.edge1Dir, this.edge1File, fileContents);

    // add, commit, push
    this.gitForPush.add().addFilepattern(formEdgeFilePath(this.edge1Dir.getParentFile().getName(), this.edge1Dir.getName(), this.edge1File.getName())).call();
    this.gitForPush.commit().setMessage("Edge commit").call();
    this.gitForPush.push().setRemote("origin").setRefSpecs(this.masterRefSpec).call();

    this.gitFlowGraphMonitor.processGitConfigChanges();

    //Check if new edge1 has been added to the FlowGraph
    testIfEdgeSuccessfullyAdded("node1", "node2", "edge1", "value2");
  }

  @Test (dependsOnMethods = "testUpdateEdge")
  public void testUpdateNode()
      throws IOException, GitAPIException, URISyntaxException, ExecutionException, InterruptedException {
    //Update param1 value in node1 and check if updated node is added to the graph
    String fileContents = FlowGraphConfigurationKeys.DATA_NODE_IS_ACTIVE_KEY + "=true\nparam1=value3\n";
    addNode(this.node1Dir, this.node1File, fileContents);

    this.gitFlowGraphMonitor.processGitConfigChanges();
    //Check if node has been updated in the FlowGraph
    DataNode dataNode = this.flowGraph.getNode("node1");
    Assert.assertEquals(dataNode.getId(), "node1");
    Assert.assertTrue(dataNode.isActive());
    Assert.assertEquals(dataNode.getRawConfig().getString("param1"), "value3");
  }


  @Test (dependsOnMethods = "testUpdateNode")
  public void testRemoveEdge() throws GitAPIException, IOException {
    // delete a config file
    edge1File.delete();

    //Node1 has 1 edge before delete
    Set<FlowEdge> edgeSet = this.flowGraph.getEdges("node1");
    Assert.assertEquals(edgeSet.size(), 1);

    // delete, commit, push
    DirCache ac = this.gitForPush.rm().addFilepattern(formEdgeFilePath(this.edge1Dir.getParentFile().getName(),
        this.edge1Dir.getName(), this.edge1File.getName())).call();
    RevCommit cc = this.gitForPush.commit().setMessage("Edge remove commit").call();
    this.gitForPush.push().setRemote("origin").setRefSpecs(this.masterRefSpec).call();

    this.gitFlowGraphMonitor.processGitConfigChanges();

    //Check if edge1 has been deleted from the graph
    edgeSet = this.flowGraph.getEdges("node1");
    Assert.assertTrue(edgeSet.size() == 0);
  }

  @Test (dependsOnMethods = "testRemoveEdge")
  public void testRemoveNode() throws GitAPIException, IOException {
    //delete node files
    node1File.delete();
    node2File.delete();

    //Ensure node1 and node2 are present in the graph before delete
    DataNode node1 = this.flowGraph.getNode("node1");
    Assert.assertNotNull(node1);
    DataNode node2 = this.flowGraph.getNode("node2");
    Assert.assertNotNull(node2);

    // delete, commit, push
    this.gitForPush.rm().addFilepattern(formNodeFilePath(this.node1Dir.getName(), this.node1File.getName())).call();
    this.gitForPush.rm().addFilepattern(formNodeFilePath(this.node2Dir.getName(), this.node2File.getName())).call();
    this.gitForPush.commit().setMessage("Node remove commit").call();
    this.gitForPush.push().setRemote("origin").setRefSpecs(this.masterRefSpec).call();

    this.gitFlowGraphMonitor.processGitConfigChanges();

    //Check if node1 and node 2 have been deleted from the graph
    node1 = this.flowGraph.getNode("node1");
    Assert.assertNull(node1);
    node2 = this.flowGraph.getNode("node2");
    Assert.assertNull(node2);
  }

  @Test (dependsOnMethods = "testRemoveNode")
  public void testChangesReorder() throws GitAPIException, IOException, ExecutionException, InterruptedException {
    String node1FileContents = FlowGraphConfigurationKeys.DATA_NODE_IS_ACTIVE_KEY + "=true\nparam1=value1\n";
    String node2FileContents = FlowGraphConfigurationKeys.DATA_NODE_IS_ACTIVE_KEY + "=true\nparam2=value2\n";

    String edgeFileContents = buildEdgeFileContents("node1", "node2", "edge1", "value1");

    createNewFile(this.node1Dir, this.node1File, node1FileContents);
    createNewFile(this.node2Dir, this.node2File, node2FileContents);
    createNewFile(this.edge1Dir, this.edge1File, edgeFileContents);

    // add, commit, push
    this.gitForPush.add().addFilepattern(formNodeFilePath(this.node1Dir.getName(), this.node1File.getName())).call();
    this.gitForPush.add().addFilepattern(formNodeFilePath(this.node2Dir.getName(), this.node2File.getName())).call();
    this.gitForPush.commit().setMessage("Add nodes commit").call();
    this.gitForPush.push().setRemote("origin").setRefSpecs(this.masterRefSpec).call();

    this.gitForPush.add().addFilepattern(formEdgeFilePath(this.edge1Dir.getParentFile().getName(), this.edge1Dir.getName(), this.edge1File.getName())).call();
    this.gitForPush.commit().setMessage("Add nodes and edges commit").call();
    this.gitForPush.push().setRemote("origin").setRefSpecs(this.masterRefSpec).call();

    this.gitFlowGraphMonitor.processGitConfigChanges();
    //Ensure node1 and node2 are present in the graph
    DataNode node1 = this.flowGraph.getNode("node1");
    Assert.assertNotNull(node1);
    DataNode node2 = this.flowGraph.getNode("node2");
    Assert.assertNotNull(node2);
    testIfEdgeSuccessfullyAdded("node1", "node2", "edge1", "value1");

    //Delete node1, edge node1->node2 files
    node1File.delete();
    edge1File.delete();

    //Commit1: delete node1 and edge node1->node2
    this.gitForPush.rm().addFilepattern(formNodeFilePath(this.node1Dir.getName(), this.node1File.getName())).call();
    this.gitForPush.rm().addFilepattern(formEdgeFilePath(this.edge1Dir.getParentFile().getName(), this.edge1Dir.getName(), this.edge1File.getName())).call();
    this.gitForPush.commit().setMessage("Delete node1 and edge1 commit").call();
    this.gitForPush.push().setRemote("origin").setRefSpecs(this.masterRefSpec).call();

    //Commit2: add node1 back
    createNewFile(this.node1Dir, this.node1File, node1FileContents);
    this.gitForPush.add().addFilepattern(formNodeFilePath(this.node1Dir.getName(), this.node1File.getName())).call();
    this.gitForPush.commit().setMessage("Add node1 commit").call();
    this.gitForPush.push().setRemote("origin").setRefSpecs(this.masterRefSpec).call();

    this.gitFlowGraphMonitor.processGitConfigChanges();
    node1 = this.flowGraph.getNode("node1");
    Assert.assertNotNull(node1);
    Assert.assertEquals(this.flowGraph.getEdges(node1).size(), 0);
  }

  @AfterClass
  public void tearDown() throws Exception {
    cleanUpDir(TEST_DIR);
  }

  private void createNewFile(File dir, File file, String fileContents) throws IOException {
    dir.mkdirs();
    file.createNewFile();
    Files.write(fileContents, file, Charsets.UTF_8);
  }

  private void addNode(File nodeDir, File nodeFile, String fileContents) throws IOException, GitAPIException {
    createNewFile(nodeDir, nodeFile, fileContents);

    // add, commit, push node
    this.gitForPush.add().addFilepattern(formNodeFilePath(nodeDir.getName(), nodeFile.getName())).call();
    this.gitForPush.commit().setMessage("Node commit").call();
    this.gitForPush.push().setRemote("origin").setRefSpecs(this.masterRefSpec).call();
  }

  private void addEdge(File edgeDir, File edgeFile, String fileContents) throws IOException, GitAPIException {
    createNewFile(edgeDir, edgeFile, fileContents);

    // add, commit, push edge
    this.gitForPush.add().addFilepattern(formEdgeFilePath(edgeDir.getParentFile().getName(), edgeDir.getName(), edgeFile.getName())).call();
    this.gitForPush.commit().setMessage("Edge commit").call();
    this.gitForPush.push().setRemote("origin").setRefSpecs(this.masterRefSpec).call();
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
    Set<FlowEdge> edgeSet = this.flowGraph.getEdges(node1);
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

  private String formNodeFilePath(String groupDir, String fileName) {
    return this.flowGraphDir.getName() + SystemUtils.FILE_SEPARATOR + groupDir + SystemUtils.FILE_SEPARATOR + fileName;
  }

  private String formEdgeFilePath(String parentDir, String groupDir, String fileName) {
    return this.flowGraphDir.getName() + SystemUtils.FILE_SEPARATOR + parentDir + SystemUtils.FILE_SEPARATOR + groupDir + SystemUtils.FILE_SEPARATOR + fileName;
  }

  private void cleanUpDir(String dir) {
    File specStoreDir = new File(dir);

    // cleanup is flaky on Travis, so retry a few times and then suppress the error if unsuccessful
    for (int i = 0; i < 5; i++) {
      try {
        if (specStoreDir.exists()) {
          FileUtils.deleteDirectory(specStoreDir);
        }
        // if delete succeeded then break out of loop
        break;
      } catch (IOException e) {
        logger.warn("Cleanup delete directory failed for directory: " + dir, e);
      }
    }
  }
}