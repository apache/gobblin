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

package org.apache.gobblin.service.modules.flowgraph;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.modules.template_catalog.FSFlowTemplateCatalog;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PullFileLoader;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 * Provides the common set of functionalities needed by {@link FlowGraphMonitor} to read changes in files and
 * apply them to a {@link FlowGraph}
 * Assumes that the directory structure between flowgraphs configuration files are the same.
 *
 * Assumes that the flowgraph follows this format
 *  /gobblin-flowgraph
 *    /nodeA
 *      /nodeB
 *        edgeAB.properties
 *      A.properties
 *    /nodeB
 *      B.properties
 *
 */
@Slf4j
public class BaseFlowGraphHelper {
  private static final int NODE_FILE_DEPTH = 3;
  private static final int EDGE_FILE_DEPTH = 4;
  private static final String FLOW_EDGE_LABEL_JOINER_CHAR = "_";

  final String baseDirectory;
  private final Config emptyConfig = ConfigFactory.empty();

  private final Optional<? extends FSFlowTemplateCatalog> flowTemplateCatalog;
  private final Map<URI, TopologySpec> topologySpecMap;
  protected MetricContext metricContext;
  final String flowGraphFolderName;
  final PullFileLoader pullFileLoader;
  protected final Set<String> javaPropsExtensions;
  protected final Set<String> hoconFileExtensions;
  protected final Optional<ContextAwareMeter> flowGraphUpdateFailedMeter;

  public BaseFlowGraphHelper(Optional<? extends FSFlowTemplateCatalog> flowTemplateCatalog,
      Map<URI, TopologySpec> topologySpecMap, String baseDirectory, String flowGraphFolderName,
      String javaPropsExtentions, String hoconFileExtensions, boolean instrumentationEnabled, Config config) {
    this.flowTemplateCatalog = flowTemplateCatalog;
    this.topologySpecMap = topologySpecMap;
    this.baseDirectory = baseDirectory;
    this.flowGraphFolderName = flowGraphFolderName;
    Path folderPath = new Path(baseDirectory, this.flowGraphFolderName);
    this.javaPropsExtensions = Sets.newHashSet(javaPropsExtentions.split(","));
    this.hoconFileExtensions = Sets.newHashSet(hoconFileExtensions.split(","));
    if (instrumentationEnabled) {
      this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(config), BaseFlowGraphHelper.class);
      this.flowGraphUpdateFailedMeter = Optional.of(this.metricContext.contextAwareMeter(ServiceMetricNames.FLOWGRAPH_UPDATE_FAILED_METER));
    } else {
      this.flowGraphUpdateFailedMeter = Optional.absent();
    }
    try {
      this.pullFileLoader = new PullFileLoader(folderPath,
          FileSystem.get(URI.create(ConfigurationKeys.LOCAL_FS_URI), new Configuration()), this.javaPropsExtensions,
          this.hoconFileExtensions);
    } catch (IOException e) {
      throw new RuntimeException("Could not create pull file loader", e);
    }
  }

  /**
   * Add a {@link DataNode} to the {@link FlowGraph}. The method uses the {@link FlowGraphConfigurationKeys#DATA_NODE_CLASS} config
   * to instantiate a {@link DataNode} from the node config file.
   * @param path of node to add
   */
  protected void addDataNode(FlowGraph graph, java.nio.file.Path path) {
    if (!java.nio.file.Files.isDirectory(path) && checkFilePath(path.toString(), getNodeFileDepth())) {
      Path nodeFilePath = new Path(this.baseDirectory, path.toString());
      try {
        Config config = loadNodeFileWithOverrides(nodeFilePath);
        Class dataNodeClass = Class.forName(ConfigUtils.getString(config, FlowGraphConfigurationKeys.DATA_NODE_CLASS,
            FlowGraphConfigurationKeys.DEFAULT_DATA_NODE_CLASS));
        DataNode dataNode = (DataNode) GobblinConstructorUtils.invokeLongestConstructor(dataNodeClass, config);
        if (!graph.addDataNode(dataNode)) {
          log.warn("Could not add DataNode {} to FlowGraph; skipping", dataNode.getId());
        } else {
          log.info("Added Datanode {} to FlowGraph", dataNode.getId());
        }
      } catch (Exception e) {
        if (this.flowGraphUpdateFailedMeter.isPresent()) {
          this.flowGraphUpdateFailedMeter.get().mark();
        }
        log.warn(String.format("Could not add DataNode defined in %s due to exception: ", path), e);
      }
    }
  }

  /**
   * Add a {@link FlowEdge} to the {@link FlowGraph}. The method uses the {@link FlowEdgeFactory} instance
   * provided by the {@link FlowGraph} to build a {@link FlowEdge} from the edge config file.
   * @param path of edge to add
   */
  protected void addFlowEdge(FlowGraph graph, java.nio.file.Path path) {
    if (!java.nio.file.Files.isDirectory(path) && checkFilePath(path.toString(), getEdgeFileDepth())) {
      Path edgeFilePath = new Path(this.baseDirectory, path.toString());
      try {
        Config edgeConfig = loadEdgeFileWithOverrides(edgeFilePath);
        List<SpecExecutor> specExecutors = getSpecExecutors(edgeConfig);
        Class flowEdgeFactoryClass = Class.forName(
            ConfigUtils.getString(edgeConfig, FlowGraphConfigurationKeys.FLOW_EDGE_FACTORY_CLASS,
                FlowGraphConfigurationKeys.DEFAULT_FLOW_EDGE_FACTORY_CLASS));
        FlowEdgeFactory flowEdgeFactory =
            (FlowEdgeFactory) GobblinConstructorUtils.invokeLongestConstructor(flowEdgeFactoryClass, edgeConfig);
        if (flowTemplateCatalog.isPresent()) {
          FlowEdge edge = flowEdgeFactory.createFlowEdge(edgeConfig, flowTemplateCatalog.get(), specExecutors);
          if (!graph.addFlowEdge(edge)) {
            log.warn("Could not add edge {} to FlowGraph; skipping", edge.getId());
          } else {
            log.info("Added edge {} to FlowGraph", edge.getId());
          }
        } else {
          log.warn("Could not add edge defined in {} to FlowGraph as FlowTemplateCatalog is absent", path);
        }
      } catch (Exception e) {
        log.warn("Could not add edge defined in {} due to exception", path, e);
        if (this.flowGraphUpdateFailedMeter.isPresent()) {
          this.flowGraphUpdateFailedMeter.get().mark();
        }
      }
    }
  }

  /**
   * check whether the file has the proper naming and hierarchy for nodes and edges
   * @param file the relative path from the root of the flowgraph
   * @return false if the file does not conform
   */
  protected boolean checkFilePath(String file, int depth) {
    // The file is either a node file or an edge file and needs to be stored at either:
    // flowGraphDir/nodeName/nodeName.properties (if it is a node file), or
    // flowGraphDir/nodeName/nodeName/edgeName.properties (if it is an edge file)

    Path filePath = new Path(file);
    String fileExtension = Files.getFileExtension(filePath.getName());
    if (!checkFileLevelRelativeToRoot(filePath, depth) || !(this.javaPropsExtensions.contains(fileExtension)
        || this.hoconFileExtensions.contains(fileExtension))) {
      log.warn("Changed file does not conform to directory structure and file name format, skipping: " + filePath);
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
  public boolean checkFileLevelRelativeToRoot(Path filePath, int depth) {
    if (filePath == null) {
      return false;
    }
    Path path = filePath;
    for (int i = 0; i < depth - 1; i++) {
      path = path.getParent();
    }
    return path != null ? path.getName().equals(flowGraphFolderName) : false;
  }

  /**
   * Helper that overrides the data.node.id property with name derived from the node file path
   * @param nodeConfig node config
   * @param nodeFilePath path of the node file
   * @return config with overridden data.node.id
   */
  protected Config getNodeConfigWithOverrides(Config nodeConfig, Path nodeFilePath) {
    String nodeId = nodeFilePath.getParent().getName();
    return nodeConfig.withValue(FlowGraphConfigurationKeys.DATA_NODE_ID_KEY, ConfigValueFactory.fromAnyRef(nodeId));
  }

  /**
   * Helper that overrides the flow edge properties with name derived from the edge file path
   * @param edgeConfig edge config
   * @param edgeFilePath path of the edge file
   * @return config with overridden edge properties
   */
   protected Config getEdgeConfigWithOverrides(Config edgeConfig, Path edgeFilePath) {
    String source = edgeFilePath.getParent().getParent().getName();
    String destination = edgeFilePath.getParent().getName();
    String edgeName = Files.getNameWithoutExtension(edgeFilePath.getName());

    return edgeConfig.withValue(FlowGraphConfigurationKeys.FLOW_EDGE_SOURCE_KEY, ConfigValueFactory.fromAnyRef(source))
        .withValue(FlowGraphConfigurationKeys.FLOW_EDGE_DESTINATION_KEY, ConfigValueFactory.fromAnyRef(destination))
        .withValue(FlowGraphConfigurationKeys.FLOW_EDGE_ID_KEY,
            ConfigValueFactory.fromAnyRef(getEdgeId(source, destination, edgeName)));
  }

  /**
   * This method first retrieves  the logical names of all the {@link org.apache.gobblin.runtime.api.SpecExecutor}s
   * for this edge and returns the SpecExecutors from the {@link TopologySpec} map.
   * @param edgeConfig containing the logical names of SpecExecutors for this edge.
   * @return a {@link List<SpecExecutor>}s for this edge.
   */
  private List<SpecExecutor> getSpecExecutors(Config edgeConfig)
      throws URISyntaxException, IOException {
    //Get the logical names of SpecExecutors where the FlowEdge can be executed.
    List<String> specExecutorNames =
        ConfigUtils.getStringList(edgeConfig, FlowGraphConfigurationKeys.FLOW_EDGE_SPEC_EXECUTORS_KEY);
    //Load all the SpecExecutor configurations for this FlowEdge from the SpecExecutor Catalog.
    List<SpecExecutor> specExecutors = new ArrayList<>(specExecutorNames.size());
    for (String specExecutorName : specExecutorNames) {
      URI specExecutorUri = new URI(specExecutorName);
      if (!this.topologySpecMap.containsKey(specExecutorUri)) {
        throw new IOException(String.format("Spec executor %s does not exist in the topologySpecStore.", specExecutorUri));
      }
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
  protected Config loadNodeFileWithOverrides(Path filePath)
      throws IOException {
    Config nodeConfig = this.pullFileLoader.loadPullFile(filePath, emptyConfig, false, false);
    return getNodeConfigWithOverrides(nodeConfig, filePath);
  }

  /**
   * Load the edge file.
   * @param filePath path of the edge file relative to the repository root
   * @return the configuration object
   * @throws IOException
   */
  protected Config loadEdgeFileWithOverrides(Path filePath)
      throws IOException {
    Config edgeConfig = this.pullFileLoader.loadPullFile(filePath, emptyConfig, false, false);
    return getEdgeConfigWithOverrides(edgeConfig, filePath);
  }

  /**
   * Loads the entire flowgraph from the path configured in {@link org.apache.gobblin.configuration.ConfigurationKeys.FLOWGRAPH_BASE_DIR }
   * Expects nodes to be in the format of /flowGraphName/nodeA/nodeA.properties
   * Expects edges to be in the format of /flowGraphName/nodeA/nodeB/edgeAB.properties
   * The current flowgraph will be swapped atomically with the new flowgraph that is loaded
   */
  public FlowGraph generateFlowGraph() {
    FlowGraph newFlowGraph = new BaseFlowGraph();
    java.nio.file.Path graphPath = new File(this.baseDirectory).toPath();
    try {
      List<java.nio.file.Path> edges = new ArrayList<>();
      // All nodes must be added first before edges, otherwise edges may have a missing source or destination.
      // Need to convert files to Hadoop Paths to be compatible with FileAlterationListener
      java.nio.file.Files.walk(graphPath).forEach(fileName -> {
        if (checkFileLevelRelativeToRoot(new Path(fileName.toString()), getNodeFileDepth())) {
          addDataNode(newFlowGraph, fileName);
        } else if (checkFileLevelRelativeToRoot(new Path(fileName.toString()), getEdgeFileDepth())) {
          edges.add(fileName);
        }
      });
      for (java.nio.file.Path edge : edges) {
        addFlowEdge(newFlowGraph, edge);
      }
      return newFlowGraph;
    } catch (IOException e) {
      // Log and report error, but do not break or crash the flowgraph so that currently running flows can continue
      if (this.flowGraphUpdateFailedMeter.isPresent()) {
        this.flowGraphUpdateFailedMeter.get().mark();
      }
      log.error(String.format("Error while populating file based flowgraph at path %s", graphPath), e);
      return null;
    }
  }

  /**
   * Get an edge label from the edge properties
   * @param source source data node id
   * @param destination destination data node id
   * @param edgeName simple name of the edge (e.g. file name without extension of the edge file)
   * @return a string label identifying the edge
   */
  public String getEdgeId(String source, String destination, String edgeName) {
    return Joiner.on(FLOW_EDGE_LABEL_JOINER_CHAR).join(source, destination, edgeName);
  }

  protected int getNodeFileDepth() {
    return NODE_FILE_DEPTH;
  }

  protected int getEdgeFileDepth() {
    return EDGE_FILE_DEPTH;
  }
}
