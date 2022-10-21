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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.modules.template_catalog.FSFlowTemplateCatalog;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 * Supports a configuration of a flowgraph where it can support multiple sub-flowgraphs within its directory
 * Node definitions are shared between each subgraph, but can be overwritten within the subgraph
 * Edge definitions are only defined in the subgraphs
 * e.g.
 * /gobblin-flowgraph-absolute-dir
 *   /subgraphA
 *     /nodeA (NODE_FOLDER_DEPTH)
 *       /nodeB
 *         edgeAB.properties
 *   /subgraphB
 *     /nodeA
 *       /nodeB
 *         edgeAB.properties (EDGE_FILE_DEPTH)
 *       A.properties (NODE_FILE_DEPTH)
 *  /nodes
 *    A.properties
 *    B.properties
 */
@Slf4j
public class SharedFlowGraphHelper extends BaseFlowGraphHelper {

  protected String sharedNodeFolder;
  private static String SHARED_NODE_FOLDER_NAME = "nodes";
  private static int NODE_FOLDER_DEPTH = 2;

  public SharedFlowGraphHelper(Optional<? extends FSFlowTemplateCatalog> flowTemplateCatalog,
      Map<URI, TopologySpec> topologySpecMap, String baseDirectory, String flowGraphFolderName,
      String javaPropsExtentions, String hoconFileExtensions, boolean instrumentationEnabled, Config config) {
    super(flowTemplateCatalog, topologySpecMap, baseDirectory, flowGraphFolderName, javaPropsExtentions, hoconFileExtensions, instrumentationEnabled, config);
    this.sharedNodeFolder = baseDirectory + File.separator + SHARED_NODE_FOLDER_NAME;
  }

  /**
   * Looks into the sharedNodeFolder to use those configurations as fallbacks for the node to add
   * Otherwise if the shared node does not exist, attempt to add the node in the same manner as {@link BaseFlowGraphHelper}
   * @param graph
   * @param path of node folder in the subgraph, so path is expected to be a directory
   */
  @Override
  protected void addDataNode(FlowGraph graph, java.nio.file.Path path) {
    try {
      // Load node from shared folder first if it exists
      Config sharedNodeConfig = ConfigFactory.empty();
      List<String> nodeFileSuffixes = new ArrayList<>(this.javaPropsExtensions);
      nodeFileSuffixes.addAll(this.hoconFileExtensions);
      // Since there can be multiple file types supported, check if there is a shared node definition that matches any of the file types
      // If multiple definitions in the same folder, only load one of them
      // Assume that configuration overrides in subfolders use the same file type for the same node
      Config nodeConfig = ConfigFactory.empty();
      for (String fileSuffix: nodeFileSuffixes) {
        String nodePropertyFile = path.getFileName().toString() + "." + fileSuffix;
        File sharedNodeFile = new File(this.sharedNodeFolder, nodePropertyFile);
        if (sharedNodeFile.exists()) {
          nodeConfig = loadNodeFileWithOverrides(new Path(sharedNodeFile.getPath()));
        }
        File nodeFilePath = new File(path.toString(), nodePropertyFile);
        if (nodeFilePath.exists()) {
          nodeConfig = loadNodeFileWithOverrides(new Path(nodeFilePath.getPath())).withFallback(nodeConfig);
        }
        if (!nodeConfig.isEmpty()) {
          break;
        }
      }
      if (nodeConfig.isEmpty()) {
        throw new IOException(
            String.format("Cannot find expected node file starting with %s in %s or %s", path.getFileName().toString(), sharedNodeFolder,
                path));
      }
      Class dataNodeClass = Class.forName(ConfigUtils.getString(nodeConfig, FlowGraphConfigurationKeys.DATA_NODE_CLASS,
          FlowGraphConfigurationKeys.DEFAULT_DATA_NODE_CLASS));
      DataNode dataNode = (DataNode) GobblinConstructorUtils.invokeLongestConstructor(dataNodeClass, nodeConfig);
      if (!graph.addDataNode(dataNode)) {
        log.warn("Could not add DataNode {} to FlowGraph; skipping", dataNode.getId());
      } else {
        log.info("Added Datanode {} to FlowGraph", dataNode.getId());
      }
    } catch (IOException | ReflectiveOperationException e) {
      if (this.flowGraphUpdateFailedMeter.isPresent()) {
        this.flowGraphUpdateFailedMeter.get().mark();
      }
      log.warn(String.format("Could not add DataNode defined in %s due to exception: ", path), e);
    }
  }

  @Override
  protected Config getNodeConfigWithOverrides(Config nodeConfig, Path nodeFilePath) {
    String nodeId = FilenameUtils.removeExtension(nodeFilePath.getName().toString());
    return nodeConfig.withValue(FlowGraphConfigurationKeys.DATA_NODE_ID_KEY, ConfigValueFactory.fromAnyRef(nodeId));
  }

  @Override
  protected int getNodeFileDepth() {
    return NODE_FOLDER_DEPTH;
  }
}

