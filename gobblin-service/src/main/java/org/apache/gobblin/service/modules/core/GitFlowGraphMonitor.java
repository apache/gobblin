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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.eclipse.jgit.diff.DiffEntry;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.service.modules.flowgraph.DataNode;
import org.apache.gobblin.service.modules.flowgraph.FlowEdge;
import org.apache.gobblin.service.modules.flowgraph.FlowEdgeFactory;
import org.apache.gobblin.service.modules.flowgraph.FlowGraph;
import org.apache.gobblin.service.modules.flowgraph.FlowGraphConfigurationKeys;
import org.apache.gobblin.service.modules.template_catalog.FSFlowCatalog;
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
  public static final String GIT_FLOWGRAPH_MONITOR_PREFIX = "gitFlowGraphMonitor";

  private static final String PROPERTIES_EXTENSIONS = "properties";
  private static final String CONF_EXTENSIONS = StringUtils.EMPTY;
  private static final String FLOW_EDGE_LABEL_JOINER_CHAR = ":";
  private static final String DEFAULT_GIT_FLOWGRAPH_MONITOR_REPO_DIR = "git-flowgraph";
  private static final String DEFAULT_GIT_FLOWGRAPH_MONITOR_FLOWGRAPH_DIR = "gobblin-flowgraph";
  private static final String DEFAULT_GIT_FLOWGRAPH_MONITOR_BRANCH_NAME = "master";

  private static final int NODE_FILE_DEPTH = 3;
  private static final int EDGE_FILE_DEPTH = 4;
  private static final int DEFAULT_GIT_FLOWGRAPH_MONITOR_POLLING_INTERVAL = 60;

  private static final Config DEFAULT_FALLBACK =
      ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(ConfigurationKeys.GIT_MONITOR_REPO_DIR, DEFAULT_GIT_FLOWGRAPH_MONITOR_REPO_DIR)
          .put(ConfigurationKeys.GIT_MONITOR_FOLDER_NAME, DEFAULT_GIT_FLOWGRAPH_MONITOR_FLOWGRAPH_DIR)
          .put(ConfigurationKeys.GIT_MONITOR_BRANCH_NAME, DEFAULT_GIT_FLOWGRAPH_MONITOR_BRANCH_NAME)
          .put(ConfigurationKeys.GIT_MONITOR_POLLING_INTERVAL, DEFAULT_GIT_FLOWGRAPH_MONITOR_POLLING_INTERVAL)
          .put(JAVA_PROPS_EXTENSIONS, PROPERTIES_EXTENSIONS)
          .put(HOCON_FILE_EXTENSIONS, CONF_EXTENSIONS)
          .build());

  private FSFlowCatalog flowCatalog;
  private FlowGraph flowGraph;
  private final Config emptyConfig = ConfigFactory.empty();

  public GitFlowGraphMonitor(Config config, FSFlowCatalog flowCatalog, FlowGraph graph) {
    super(config.getConfig(GIT_FLOWGRAPH_MONITOR_PREFIX).withFallback(DEFAULT_FALLBACK));
    this.flowCatalog = flowCatalog;
    this.flowGraph = graph;
  }

  /**
   * Determine if the service should poll Git.
   */
  @Override
  public boolean shouldPollGit() {
    return this.isActive;
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
        }
      } catch (Exception e) {
        log.warn("Could not add DataNode defined in {} due to exception {}", change.getNewPath(), e.getMessage());
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
        Config config = loadEdgeFileWithOverrides(edgeFilePath);
        Class flowEdgeFactoryClass = Class.forName(ConfigUtils.getString(config, FlowGraphConfigurationKeys.FLOW_EDGE_FACTORY_CLASS,
            FlowGraphConfigurationKeys.DEFAULT_FLOW_EDGE_FACTORY_CLASS));
        FlowEdgeFactory flowEdgeFactory = (FlowEdgeFactory) GobblinConstructorUtils.invokeLongestConstructor(flowEdgeFactoryClass, config);
        FlowEdge edge = flowEdgeFactory.createFlowEdge(config, flowCatalog);
        if (!this.flowGraph.addFlowEdge(edge)) {
          log.warn("Could not add edge {} to FlowGraph; skipping", edge.getId());
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
          log.warn("Could not remove FlowEdge {} from FlowGraph; skipping", edgeId);
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
