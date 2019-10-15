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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.diff.DiffEntry;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.modules.flowgraph.DataNode;
import org.apache.gobblin.service.modules.flowgraph.FlowEdge;
import org.apache.gobblin.service.modules.flowgraph.FlowEdgeFactory;
import org.apache.gobblin.service.modules.flowgraph.FlowGraph;
import org.apache.gobblin.service.modules.flowgraph.FlowGraphConfigurationKeys;
import org.apache.gobblin.service.modules.template_catalog.FSFlowTemplateCatalog;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 * Service that monitors for changes to {@link org.apache.gobblin.service.modules.flowgraph.FlowGraph} from a git repository.
 * The git repository must have an inital commit that has no files since that is used as a base for getting
 * the change list.
 * The {@link DataNode}s and {@link FlowEdge}s in FlowGraph need to be organized with the following directory structure on git:
 * <root_flowGraph_dir>/<nodeName>/<nodeName>.properties
 * <root_flowGraph_dir>/<nodeName1>/<nodeName2>/<edgeName>.properties
 */
@Slf4j
public class GitFlowGraphMonitor extends GitMonitoringService {
  public static final String GIT_FLOWGRAPH_MONITOR_PREFIX = "gobblin.service.gitFlowGraphMonitor";

  private static final String PROPERTIES_EXTENSIONS = "properties";
  private static final String CONF_EXTENSIONS = StringUtils.EMPTY;
  private static final String FLOW_EDGE_LABEL_JOINER_CHAR = "_";
  private static final String DEFAULT_GIT_FLOWGRAPH_MONITOR_REPO_DIR = "git-flowgraph";
  private static final String DEFAULT_GIT_FLOWGRAPH_MONITOR_FLOWGRAPH_DIR = "gobblin-flowgraph";
  private static final String DEFAULT_GIT_FLOWGRAPH_MONITOR_BRANCH_NAME = "master";

  private static final int NODE_FILE_DEPTH = 3;
  private static final int EDGE_FILE_DEPTH = 4;
  private static final int DEFAULT_GIT_FLOWGRAPH_MONITOR_POLLING_INTERVAL = 60;

  private static final Config DEFAULT_FALLBACK =
      ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(ConfigurationKeys.GIT_MONITOR_REPO_DIR, DEFAULT_GIT_FLOWGRAPH_MONITOR_REPO_DIR)
          .put(ConfigurationKeys.GIT_MONITOR_CONFIG_BASE_DIR, DEFAULT_GIT_FLOWGRAPH_MONITOR_FLOWGRAPH_DIR)
          .put(ConfigurationKeys.GIT_MONITOR_BRANCH_NAME, DEFAULT_GIT_FLOWGRAPH_MONITOR_BRANCH_NAME)
          .put(ConfigurationKeys.GIT_MONITOR_POLLING_INTERVAL, DEFAULT_GIT_FLOWGRAPH_MONITOR_POLLING_INTERVAL)
          .put(JAVA_PROPS_EXTENSIONS, PROPERTIES_EXTENSIONS)
          .put(HOCON_FILE_EXTENSIONS, CONF_EXTENSIONS)
          .put(SHOULD_CHECKPOINT_HASHES, false)
          .build());

  private Optional<? extends FSFlowTemplateCatalog> flowTemplateCatalog;
  private FlowGraph flowGraph;
  private final Map<URI, TopologySpec> topologySpecMap;
  private final Config emptyConfig = ConfigFactory.empty();
  private final CountDownLatch initComplete;

  public GitFlowGraphMonitor(Config config, Optional<? extends FSFlowTemplateCatalog> flowTemplateCatalog,
      FlowGraph graph, Map<URI, TopologySpec> topologySpecMap, CountDownLatch initComplete) {
    super(config.getConfig(GIT_FLOWGRAPH_MONITOR_PREFIX).withFallback(DEFAULT_FALLBACK));
    this.flowTemplateCatalog = flowTemplateCatalog;
    this.flowGraph = graph;
    this.topologySpecMap = topologySpecMap;
    this.initComplete = initComplete;
  }

  /**
   * Determine if the service should poll Git. Current behavior is both master and slave(s) will poll Git for
   * changes to {@link FlowGraph}.
   */
  @Override
  public boolean shouldPollGit() {
    return this.isActive;
  }

  /**
   * Sort the changes in a commit so that changes to node files appear before changes to edge files. This is done so that
   * node related changes are applied to the FlowGraph before edge related changes. An example where the order matters
   * is the case when a commit adds a new node n2 as well as adds an edge from an existing node n1 to n2. To ensure that the
   * addition of edge n1->n2 is successful, node n2 must exist in the graph and so needs to be added first. For deletions,
   * the order does not matter and ordering the changes in the commit will result in the same FlowGraph state as if the changes
   * were unordered. In other words, deletion of a node deletes all its incident edges from the FlowGraph. So processing an
   * edge deletion later results in a no-op. Note that node and edge files do not change depth in case of modifications.
   *
   * If there are multiple commits between successive polls to Git, the re-ordering of changes across commits should not
   * affect the final state of the FlowGraph. This is because, the order of changes for a given file type (i.e. node or edge)
   * is preserved.
   */
  @Override
  void processGitConfigChanges() throws GitAPIException, IOException {
    List<DiffEntry> changes = this.gitRepo.getChanges();
    Collections.sort(changes, (o1, o2) -> {
      Integer o1Depth = (o1.getNewPath() != null) ? (new Path(o1.getNewPath())).depth() : (new Path(o1.getOldPath())).depth();
      Integer o2Depth = (o2.getNewPath() != null) ? (new Path(o2.getNewPath())).depth() : (new Path(o2.getOldPath())).depth();
      return o1Depth.compareTo(o2Depth);
    });
    processGitConfigChangesHelper(changes);
    //Decrements the latch count. The countdown latch is initialized to 1. So after the first time the latch is decremented,
    // the following operation should be a no-op.
    this.initComplete.countDown();
  }

  /**
   * Add an element (i.e., a {@link DataNode}, or a {@link FlowEdge} to
   * the {@link FlowGraph} for an added, updated or modified node or edge file.
   * @param change
   */
  @Override
  public void addChange(DiffEntry change) {
    Path path = new Path(change.getNewPath());
    if (path.depth() == NODE_FILE_DEPTH) {
      addDataNode(change);
    } else if (path.depth() == EDGE_FILE_DEPTH) {
      addFlowEdge(change);
    }
  }

  /**
   * Remove an element (i.e. either a {@link DataNode} or a {@link FlowEdge} from the {@link FlowGraph} for
   * a renamed or deleted {@link DataNode} or {@link FlowEdge} file.
   * @param change
   */
  @Override
  public void removeChange(DiffEntry change) {
    Path path = new Path(change.getOldPath());
    if (path.depth() == NODE_FILE_DEPTH) {
      removeDataNode(change);
    } else if (path.depth() == EDGE_FILE_DEPTH) {
      removeFlowEdge(change);
    }
  }

  /**
   * Add a {@link DataNode} to the {@link FlowGraph}. The method uses the {@link FlowGraphConfigurationKeys#DATA_NODE_CLASS} config
   * to instantiate a {@link DataNode} from the node config file.
   * @param change
   */
  private void addDataNode(DiffEntry change) {
    if (checkFilePath(change.getNewPath(), NODE_FILE_DEPTH)) {
      Path nodeFilePath = new Path(this.repositoryDir, change.getNewPath());
      try {
        Config config = loadNodeFileWithOverrides(nodeFilePath);
        Class dataNodeClass = Class.forName(ConfigUtils.getString(config, FlowGraphConfigurationKeys.DATA_NODE_CLASS,
            FlowGraphConfigurationKeys.DEFAULT_DATA_NODE_CLASS));
        DataNode dataNode = (DataNode) GobblinConstructorUtils.invokeLongestConstructor(dataNodeClass, config);
        if (!this.flowGraph.addDataNode(dataNode)) {
          log.warn("Could not add DataNode {} to FlowGraph; skipping", dataNode.getId());
        } else {
          log.info("Added Datanode {} to FlowGraph", dataNode.getId());
        }
      } catch (Exception e) {
        log.warn("Could not add DataNode defined in {} due to exception {}", change.getNewPath(), e);
      }
    }
  }

  /**
   * Remove a {@link DataNode} from the {@link FlowGraph}. The method extracts the nodeId of the
   * {@link DataNode} from the node config file and uses it to delete the associated {@link DataNode}.
   * @param change
   */
  private void removeDataNode(DiffEntry change) {
    if (checkFilePath(change.getOldPath(), NODE_FILE_DEPTH)) {
      Path nodeFilePath = new Path(this.repositoryDir, change.getOldPath());
      Config config = getNodeConfigWithOverrides(ConfigFactory.empty(), nodeFilePath);
      String nodeId = config.getString(FlowGraphConfigurationKeys.DATA_NODE_ID_KEY);
      if (!this.flowGraph.deleteDataNode(nodeId)) {
        log.warn("Could not remove DataNode {} from FlowGraph; skipping", nodeId);
      } else {
        log.info("Removed DataNode {} from FlowGraph", nodeId);
      }
    }
  }

  /**
   * Add a {@link FlowEdge} to the {@link FlowGraph}. The method uses the {@link FlowEdgeFactory} instance
   * provided by the {@link FlowGraph} to build a {@link FlowEdge} from the edge config file.
   * @param change
   */
  private void addFlowEdge(DiffEntry change) {
    if (checkFilePath(change.getNewPath(), EDGE_FILE_DEPTH)) {
      Path edgeFilePath = new Path(this.repositoryDir, change.getNewPath());
      try {
        Config edgeConfig = loadEdgeFileWithOverrides(edgeFilePath);
        List<SpecExecutor> specExecutors = getSpecExecutors(edgeConfig);
        Class flowEdgeFactoryClass = Class.forName(ConfigUtils.getString(edgeConfig, FlowGraphConfigurationKeys.FLOW_EDGE_FACTORY_CLASS,
            FlowGraphConfigurationKeys.DEFAULT_FLOW_EDGE_FACTORY_CLASS));
        FlowEdgeFactory flowEdgeFactory = (FlowEdgeFactory) GobblinConstructorUtils.invokeLongestConstructor(flowEdgeFactoryClass, edgeConfig);
        if (flowTemplateCatalog.isPresent()) {
          FlowEdge edge = flowEdgeFactory.createFlowEdge(edgeConfig, flowTemplateCatalog.get(), specExecutors);
          if (!this.flowGraph.addFlowEdge(edge)) {
            log.warn("Could not add edge {} to FlowGraph; skipping", edge.getId());
          } else {
            log.info("Added edge {} to FlowGraph", edge.getId());
          }
        } else {
          log.warn("Could not add edge defined in {} to FlowGraph as FlowTemplateCatalog is absent", change.getNewPath());
        }
      } catch (Exception e) {
        log.warn("Could not add edge defined in {} due to exception {}", change.getNewPath(), e.getMessage());
      }
    }
  }

  /**
   * Remove a {@link FlowEdge} from the {@link FlowGraph}. The method uses {@link FlowEdgeFactory}
   * to construct the edgeId of the {@link FlowEdge} from the config file and uses it to delete the associated
   * {@link FlowEdge}.
   * @param change
   */
  private void removeFlowEdge(DiffEntry change) {
    if (checkFilePath(change.getOldPath(), EDGE_FILE_DEPTH)) {
      Path edgeFilePath = new Path(this.repositoryDir, change.getOldPath());
      try {
        Config config = getEdgeConfigWithOverrides(ConfigFactory.empty(), edgeFilePath);
        String edgeId = config.getString(FlowGraphConfigurationKeys.FLOW_EDGE_ID_KEY);
        if (!this.flowGraph.deleteFlowEdge(edgeId)) {
          log.warn("Could not remove edge {} from FlowGraph; skipping", edgeId);
        } else {
          log.info("Removed edge {} from FlowGraph", edgeId);
        }
      } catch (Exception e) {
        log.warn("Could not remove edge defined in {} due to exception {}", edgeFilePath, e.getMessage());
      }
    }
  }

  /**
   * check whether the file has the proper naming and hierarchy
   * @param file the relative path from the repo root
   * @return false if the file does not conform
   */
  private boolean checkFilePath(String file, int depth) {
    // The file is either a node file or an edge file and needs to be stored at either:
    // flowGraphDir/nodeName/nodeName.properties (if it is a node file), or
    // flowGraphDir/nodeName/nodeName/edgeName.properties (if it is an edge file)

    Path filePath = new Path(file);
    String fileExtension = Files.getFileExtension(filePath.getName());
    if (filePath.depth() != depth || !checkFileLevelRelativeToRoot(filePath, depth)
        || !(this.javaPropsExtensions.contains(fileExtension))) {
      log.warn("Changed file does not conform to directory structure and file name format, skipping: "
          + filePath);
      return false;
    }
    return true;
  }

  /**
   * Helper to check if a file has proper hierarchy.
   * @param filePath path of the node/edge file
   * @param depth expected depth of the file
   * @return true if the file conforms to the expected hierarchy
   */
  private boolean checkFileLevelRelativeToRoot(Path filePath, int depth) {
    if (filePath == null) {
      return false;
    }
    Path path = filePath;
    for (int i = 0; i < depth - 1; i++) {
      path = path.getParent();
    }
    if (!path.getName().equals(folderName)) {
      return false;
    }
    return true;
  }

  /**
   * Helper that overrides the data.node.id property with name derived from the node file path
   * @param nodeConfig node config
   * @param nodeFilePath path of the node file
   * @return config with overridden data.node.id
   */
  private Config getNodeConfigWithOverrides(Config nodeConfig, Path nodeFilePath) {
    String nodeId = nodeFilePath.getParent().getName();
    return nodeConfig.withValue(FlowGraphConfigurationKeys.DATA_NODE_ID_KEY, ConfigValueFactory.fromAnyRef(nodeId));
  }

  /**
   * Helper that overrides the flow edge properties with name derived from the edge file path
   * @param edgeConfig edge config
   * @param edgeFilePath path of the edge file
   * @return config with overridden edge properties
   */
  private Config getEdgeConfigWithOverrides(Config edgeConfig, Path edgeFilePath) {
    String source = edgeFilePath.getParent().getParent().getName();
    String destination = edgeFilePath.getParent().getName();
    String edgeName = Files.getNameWithoutExtension(edgeFilePath.getName());

    return edgeConfig.withValue(FlowGraphConfigurationKeys.FLOW_EDGE_SOURCE_KEY, ConfigValueFactory.fromAnyRef(source))
        .withValue(FlowGraphConfigurationKeys.FLOW_EDGE_DESTINATION_KEY, ConfigValueFactory.fromAnyRef(destination))
        .withValue(FlowGraphConfigurationKeys.FLOW_EDGE_ID_KEY, ConfigValueFactory.fromAnyRef(getEdgeId(source, destination, edgeName)));
  }

  /**
   * This method first retrieves  the logical names of all the {@link org.apache.gobblin.runtime.api.SpecExecutor}s
   * for this edge and returns the SpecExecutors from the {@link TopologySpec} map.
   * @param edgeConfig containing the logical names of SpecExecutors for this edge.
   * @return a {@link List<SpecExecutor>}s for this edge.
   */
  private List<SpecExecutor> getSpecExecutors(Config edgeConfig)
      throws URISyntaxException {
    //Get the logical names of SpecExecutors where the FlowEdge can be executed.
    List<String> specExecutorNames = ConfigUtils.getStringList(edgeConfig, FlowGraphConfigurationKeys.FLOW_EDGE_SPEC_EXECUTORS_KEY);
    //Load all the SpecExecutor configurations for this FlowEdge from the SpecExecutor Catalog.
    List<SpecExecutor> specExecutors = new ArrayList<>();
    for (String specExecutorName: specExecutorNames) {
      URI specExecutorUri = new URI(specExecutorName);
      specExecutors.add(this.topologySpecMap.get(specExecutorUri).getSpecExecutor());
    }
    return specExecutors;
  }

  /**
   * Load the node file.
   * @param filePath path of the node file relative to the repository root
   * @return the configuration object
   * @throws IOException
   */
  private Config loadNodeFileWithOverrides(Path filePath) throws IOException {
    Config nodeConfig = this.pullFileLoader.loadPullFile(filePath, emptyConfig, false);
    return getNodeConfigWithOverrides(nodeConfig, filePath);
  }

  /**
   * Load the edge file.
   * @param filePath path of the edge file relative to the repository root
   * @return the configuration object
   * @throws IOException
   */
  private Config loadEdgeFileWithOverrides(Path filePath) throws IOException {
    Config edgeConfig = this.pullFileLoader.loadPullFile(filePath, emptyConfig, false);
    return getEdgeConfigWithOverrides(edgeConfig, filePath);
  }

  /**
   * Get an edge label from the edge properties
   * @param source source data node id
   * @param destination destination data node id
   * @param edgeName simple name of the edge (e.g. file name without extension of the edge file)
   * @return a string label identifying the edge
   */
  private String getEdgeId(String source, String destination, String edgeName) {
    return Joiner.on(FLOW_EDGE_LABEL_JOINER_CHAR).join(source, destination, edgeName);
  }

}
