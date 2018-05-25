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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.gobblin.service.modules.flowgraph.FlowEdge;
import org.apache.gobblin.service.modules.flowgraph.FlowGraphConfigurationKeys;
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
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.BaseFlowGraph;
import org.apache.gobblin.service.modules.flowgraph.DataNode;
import org.apache.gobblin.service.modules.template_catalog.FSFlowCatalog;


public class GitFlowGraphMonitorTest {
  private static final Logger logger = LoggerFactory.getLogger(GitFlowGraphMonitor.class);
  private Repository remoteRepo;
  private Git gitForPush;
  private static final String TEST_DIR = "/tmp/gitFlowGraphTestDir/";
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
  private FSFlowCatalog flowCatalog;
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

    this.config = ConfigBuilder.create()
        .addPrimitive(ConfigurationKeys.GIT_FLOWGRAPH_MONITOR_REPO_URI, this.remoteRepo.getDirectory().getAbsolutePath())
        .addPrimitive(ConfigurationKeys.GIT_FLOWGRAPH_MONITOR_REPO_DIR, TEST_DIR + "/git-flowgraph")
        .addPrimitive(ConfigurationKeys.GIT_FLOWGRAPH_MONITOR_POLLING_INTERVAL, 5)
        .build();

    // Create a FSFlowCatalog instance
    URI flowTemplateCatalogUri = this.getClass().getClassLoader().getResource("template_catalog").toURI();
    Properties properties = new Properties();
    properties.put(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY, flowTemplateCatalogUri.toString());
    Config config = ConfigFactory.parseProperties(properties);
    Config templateCatalogCfg = config
        .withValue(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY,
            config.getValue(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY));
    this.flowCatalog = new FSFlowCatalog(templateCatalogCfg);

    //Create a FlowGraph instance with defaults
    this.flowGraph = new BaseFlowGraph();

    this.gitFlowGraphMonitor = new GitFlowGraphMonitor(this.config, this.flowCatalog, this.flowGraph);
    this.gitFlowGraphMonitor.setActive(true);
  }

  private void testAddNodeHelper(File nodeDir, File nodeFile, String nodeId, String paramValue)
      throws IOException, GitAPIException {
    // push a new node file
    nodeDir.mkdirs();
    nodeFile.createNewFile();
    Files.write( FlowGraphConfigurationKeys.DATA_NODE_IS_ACTIVE_KEY + "=true\nparam1=" + paramValue +"\n", nodeFile, Charsets.UTF_8);

    // add, commit, push node
    this.gitForPush.add().addFilepattern(formNodeFilePath(nodeDir.getName(), nodeFile.getName())).call();
    this.gitForPush.commit().setMessage("Node commit").call();
    this.gitForPush.push().setRemote("origin").setRefSpecs(this.masterRefSpec).call();

    this.gitFlowGraphMonitor.processGitConfigChanges();

    //Check if node1 has been added to the FlowGraph
    DataNode dataNode = this.flowGraph.getNode(nodeId);
    Assert.assertEquals(dataNode.getId(), nodeId);
    Assert.assertTrue(dataNode.isActive());
    Assert.assertEquals(dataNode.getProps().getString("param1"), paramValue);
  }

  @Test
  public void testAddNode()
      throws IOException, GitAPIException, URISyntaxException, ExecutionException, InterruptedException {
    testAddNodeHelper(this.node1Dir, this.node1File, "node1", "value1");
    testAddNodeHelper(this.node2Dir, this.node2File, "node2", "value2");
  }

  @Test (dependsOnMethods = "testAddNode")
  public void testAddEdge()
      throws IOException, GitAPIException, URISyntaxException, ExecutionException, InterruptedException {
    // push a new node file
    this.edge1Dir.mkdirs();
    this.edge1File.createNewFile();

    Files.write(FlowGraphConfigurationKeys.FLOW_EDGE_SOURCE_KEY + "=node1\n" +
                FlowGraphConfigurationKeys.FLOW_EDGE_DESTINATION_KEY + "=node2\n" +
                FlowGraphConfigurationKeys.FLOW_EDGE_NAME_KEY + "=edge1\n" +
                FlowGraphConfigurationKeys.FLOW_EDGE_IS_ACTIVE_KEY + "=true\n" +
                FlowGraphConfigurationKeys.FLOW_EDGE_TEMPLATE_URI_KEY + "=FS:///test-template/flow.conf\n" +
                FlowGraphConfigurationKeys.FLOW_EDGE_SPEC_EXECUTORS_KEY + ".0." + FlowGraphConfigurationKeys.FLOW_EDGE_SPEC_EXECUTOR_CLASS_KEY+"=org.apache.gobblin.runtime.spec_executorInstance.InMemorySpecExecutor\n" +
                FlowGraphConfigurationKeys.FLOW_EDGE_SPEC_EXECUTORS_KEY + ".0.specStore.fs.dir=/tmp1\n" +
                FlowGraphConfigurationKeys.FLOW_EDGE_SPEC_EXECUTORS_KEY + ".0.specExecInstance.capabilities=s1:d1\n" +
                FlowGraphConfigurationKeys.FLOW_EDGE_SPEC_EXECUTORS_KEY + ".1."+FlowGraphConfigurationKeys.FLOW_EDGE_SPEC_EXECUTOR_CLASS_KEY+"=org.apache.gobblin.runtime.spec_executorInstance.InMemorySpecExecutor\n" +
                FlowGraphConfigurationKeys.FLOW_EDGE_SPEC_EXECUTORS_KEY + ".1.specStore.fs.dir=/tmp2\n" +
                FlowGraphConfigurationKeys.FLOW_EDGE_SPEC_EXECUTORS_KEY + ".1.specExecInstance.capabilities=s2:d2\n", edge1File, Charsets.UTF_8);

    // add, commit, push
    this.gitForPush.add().addFilepattern(formEdgeFilePath(this.edge1Dir.getParentFile().getName(), this.edge1Dir.getName(), this.edge1File.getName())).call();
    this.gitForPush.commit().setMessage("Edge commit").call();
    this.gitForPush.push().setRemote("origin").setRefSpecs(this.masterRefSpec).call();

    this.gitFlowGraphMonitor.processGitConfigChanges();

    //Check if edge1 has been added to the FlowGraph
    Set<FlowEdge> edgeSet = this.flowGraph.getEdges("node1");
    Assert.assertEquals(edgeSet.size(), 1);
    FlowEdge flowEdge = edgeSet.iterator().next();
    Assert.assertEquals(flowEdge.getEndPoints().get(0), "node1");
    Assert.assertEquals(flowEdge.getEndPoints().get(1), "node2");
    Assert.assertEquals(flowEdge.getExecutors().get(0).getConfig().get().getString("specStore.fs.dir"),"/tmp1");
    Assert.assertEquals(flowEdge.getExecutors().get(0).getConfig().get().getString("specExecInstance.capabilities"),"s1:d1");
    Assert.assertEquals(flowEdge.getExecutors().get(0).getClass().getSimpleName(),"InMemorySpecExecutor");
    Assert.assertEquals(flowEdge.getExecutors().get(1).getConfig().get().getString("specStore.fs.dir"),"/tmp2");
    Assert.assertEquals(flowEdge.getExecutors().get(1).getConfig().get().getString("specExecInstance.capabilities"),"s2:d2");
    Assert.assertEquals(flowEdge.getExecutors().get(1).getClass().getSimpleName(),"InMemorySpecExecutor");
  }

  @Test (dependsOnMethods = "testAddNode")
  public void testUpdateEdge()
      throws IOException, GitAPIException, URISyntaxException, ExecutionException, InterruptedException {
    //Update edge1 file
    Files.write(FlowGraphConfigurationKeys.FLOW_EDGE_SOURCE_KEY + "=node1\n" +
        FlowGraphConfigurationKeys.FLOW_EDGE_DESTINATION_KEY + "=node2\n" +
        FlowGraphConfigurationKeys.FLOW_EDGE_NAME_KEY + "=edge1\n" +
        FlowGraphConfigurationKeys.FLOW_EDGE_IS_ACTIVE_KEY + "=true\n" +
        FlowGraphConfigurationKeys.FLOW_EDGE_TEMPLATE_URI_KEY + "=FS:///test-template/flow.conf\n" +
        FlowGraphConfigurationKeys.FLOW_EDGE_SPEC_EXECUTORS_KEY + ".0." + FlowGraphConfigurationKeys.FLOW_EDGE_SPEC_EXECUTOR_CLASS_KEY+"=org.apache.gobblin.runtime.spec_executorInstance.InMemorySpecExecutor\n" +
        FlowGraphConfigurationKeys.FLOW_EDGE_SPEC_EXECUTORS_KEY + ".0.specStore.fs.dir=/tmp1\n" +
        FlowGraphConfigurationKeys.FLOW_EDGE_SPEC_EXECUTORS_KEY + ".0.specExecInstance.capabilities=s1:d1\n" +
        FlowGraphConfigurationKeys.FLOW_EDGE_SPEC_EXECUTORS_KEY + ".1."+FlowGraphConfigurationKeys.FLOW_EDGE_SPEC_EXECUTOR_CLASS_KEY+"=org.apache.gobblin.runtime.spec_executorInstance.InMemorySpecExecutor\n" +
        FlowGraphConfigurationKeys.FLOW_EDGE_SPEC_EXECUTORS_KEY + ".1.specStore.fs.dir=/tmp2\n" +
        FlowGraphConfigurationKeys.FLOW_EDGE_SPEC_EXECUTORS_KEY + ".1.specExecInstance.capabilities=s2:d2\n" +
        "key1=value1\n", edge1File, Charsets.UTF_8);

    // add, commit, push
    this.gitForPush.add().addFilepattern(formEdgeFilePath(this.edge1Dir.getParentFile().getName(), this.edge1Dir.getName(), this.edge1File.getName())).call();
    this.gitForPush.commit().setMessage("Edge commit").call();
    this.gitForPush.push().setRemote("origin").setRefSpecs(this.masterRefSpec).call();

    this.gitFlowGraphMonitor.processGitConfigChanges();

    //Check if new edge1 has been added to the FlowGraph
    Set<FlowEdge> edgeSet = this.flowGraph.getEdges("node1");
    Assert.assertEquals(edgeSet.size(), 1);
    FlowEdge flowEdge = edgeSet.iterator().next();
    Assert.assertEquals(flowEdge.getEndPoints().get(0), "node1");
    Assert.assertEquals(flowEdge.getEndPoints().get(1), "node2");
    Assert.assertEquals(flowEdge.getExecutors().get(0).getConfig().get().getString("specStore.fs.dir"),"/tmp1");
    Assert.assertEquals(flowEdge.getExecutors().get(0).getConfig().get().getString("specExecInstance.capabilities"),"s1:d1");
    Assert.assertEquals(flowEdge.getExecutors().get(0).getClass().getSimpleName(),"InMemorySpecExecutor");
    Assert.assertEquals(flowEdge.getExecutors().get(1).getConfig().get().getString("specStore.fs.dir"),"/tmp2");
    Assert.assertEquals(flowEdge.getExecutors().get(1).getConfig().get().getString("specExecInstance.capabilities"),"s2:d2");
    Assert.assertEquals(flowEdge.getExecutors().get(1).getClass().getSimpleName(),"InMemorySpecExecutor");
    Assert.assertEquals(flowEdge.getProps().getString("key1"), "value1");
  }

  @Test (dependsOnMethods = "testUpdateEdge")
  public void testUpdateNode()
      throws IOException, GitAPIException, URISyntaxException, ExecutionException, InterruptedException {
    //Update param1 value in node1 and check if updated node is added to the graph
    testAddNodeHelper(this.node1Dir, this.node1File, "node1", "value3");
  }


  @Test (dependsOnMethods = "testUpdateNode")
  public void testRemoveEdge() throws GitAPIException, IOException {
    // delete a config file
    edge1File.delete();

    //Node1 has 1 edge before delete
    Set<FlowEdge> edgeSet = this.flowGraph.getEdges("node1");
    Assert.assertEquals(edgeSet.size(), 1);

    // delete, commit, push
    DirCache ac = this.gitForPush.rm().addFilepattern(formEdgeFilePath(this.edge1Dir.getParentFile().getName(), this.edge1Dir.getName(), this.edge1File.getName()))
        .call();
    RevCommit cc = this.gitForPush.commit().setMessage("Edge remove commit").call();
    this.gitForPush.push().setRemote("origin").setRefSpecs(this.masterRefSpec).call();

    this.gitFlowGraphMonitor.processGitConfigChanges();

    //Check if edge1 has been deleted from the graph
    edgeSet = this.flowGraph.getEdges("node1");
    Assert.assertTrue(edgeSet.size() == 0);
  }

  @Test (dependsOnMethods = "testRemoveEdge")
  public void testRemoveNode() throws GitAPIException, IOException {
    //delete node file
    node1File.delete();

    //node1 is present in the graph before delete
    DataNode node1 = this.flowGraph.getNode("node1");
    Assert.assertNotNull(node1);

    // delete, commit, push
    DirCache ac = this.gitForPush.rm().addFilepattern(formNodeFilePath(this.node1Dir.getName(), this.node1File.getName())).call();
    RevCommit cc = this.gitForPush.commit().setMessage("Node remove commit").call();
    this.gitForPush.push().setRemote("origin").setRefSpecs(this.masterRefSpec).call();

    this.gitFlowGraphMonitor.processGitConfigChanges();

    //Check if node1 has been deleted from the graph
    node1 = this.flowGraph.getNode("node1");
    Assert.assertNull(node1);
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

  private String formNodeFilePath(String groupDir, String fileName) {
    return this.flowGraphDir.getName() + SystemUtils.FILE_SEPARATOR + groupDir + SystemUtils.FILE_SEPARATOR + fileName;
  }

  private String formEdgeFilePath(String parentDir, String groupDir, String fileName) {
    return this.flowGraphDir.getName() + SystemUtils.FILE_SEPARATOR + parentDir + SystemUtils.FILE_SEPARATOR + groupDir + SystemUtils.FILE_SEPARATOR + fileName;
  }

  @AfterClass
  public void tearDown() throws Exception {
    cleanUpDir(TEST_DIR);
  }
}