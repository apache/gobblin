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

package org.apache.gobblin.service.modules.flowgraph.pathfinder;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.dataset.DatasetDescriptor;
import org.apache.gobblin.service.modules.dataset.DatasetDescriptorUtils;
import org.apache.gobblin.service.modules.flow.FlowEdgeContext;
import org.apache.gobblin.service.modules.flow.FlowGraphPath;
import org.apache.gobblin.service.modules.flow.FlowUtils;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.flowgraph.DataNode;
import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.FlowEdge;
import org.apache.gobblin.service.modules.flowgraph.FlowGraph;
import org.apache.gobblin.service.modules.restli.FlowConfigUtils;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


@Alpha
@Slf4j
public abstract class AbstractPathFinder implements PathFinder {
  private static final String SOURCE_PREFIX = "source";
  private static final String DESTINATION_PREFIX = "destination";

  private List<DataNode> destNodes;

  FlowGraph flowGraph;

  DataNode srcNode;

  DatasetDescriptor srcDatasetDescriptor;
  DatasetDescriptor destDatasetDescriptor;

  //Maintain path of FlowEdges as parent-child map
  Map<FlowEdgeContext, FlowEdgeContext> pathMap;

  //Flow Execution Id
  protected Long flowExecutionId;
  protected FlowSpec flowSpec;
  protected Config flowConfig;

  AbstractPathFinder(FlowGraph flowGraph, FlowSpec flowSpec) throws ReflectiveOperationException {
    this(flowGraph, flowSpec, new HashMap<>());
  }

  AbstractPathFinder(FlowGraph flowGraph, FlowSpec flowSpec, Map<String, String> dataNodeAliasMap)
      throws ReflectiveOperationException {
    this.flowGraph = flowGraph;
    this.flowSpec = flowSpec;
    this.flowExecutionId = FlowUtils.getOrCreateFlowExecutionId(flowSpec);
    this.flowConfig = flowSpec.getConfig().withValue(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, ConfigValueFactory.fromAnyRef(flowExecutionId));

    //Get src/dest DataNodes from the flow config
    String srcNodeId = FlowConfigUtils.getDataNode(flowConfig, ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY, dataNodeAliasMap);
    List<String> destNodeIds = FlowConfigUtils.getDataNodes(flowConfig, ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, dataNodeAliasMap);

    this.srcNode = this.flowGraph.getNode(srcNodeId);
    Preconditions.checkArgument(srcNode != null, "Flowgraph does not have a node with id " + srcNodeId);
    for (String destNodeId : destNodeIds) {
      DataNode destNode = this.flowGraph.getNode(destNodeId);
      Preconditions.checkArgument(destNode != null, "Flowgraph does not have a node with id " + destNodeId);
      if (this.destNodes == null) {
        this.destNodes = new ArrayList<>();
      }
      this.destNodes.add(destNode);
    }

    // All dest nodes should be the same class
    if (this.destNodes != null && this.destNodes.stream().map(Object::getClass).collect(Collectors.toSet()).size() > 1) {
      throw new RuntimeException("All destination nodes must use the same DataNode class");
    }

    //Should apply retention?
    boolean shouldApplyRetention = ConfigUtils.getBoolean(flowConfig, ConfigurationKeys.FLOW_APPLY_RETENTION, true);
    //Should apply retention on input dataset?
    boolean shouldApplyRetentionOnInput = ConfigUtils.getBoolean(flowConfig, ConfigurationKeys.FLOW_APPLY_INPUT_RETENTION, false);

    if ((shouldApplyRetentionOnInput) && (!shouldApplyRetention)) {
      //Invalid retention config
      throw new RuntimeException("Invalid retention configuration - shouldApplyRetentionOnInput = " + shouldApplyRetentionOnInput +
          ", and shouldApplyRetention = " + shouldApplyRetention);
    }

    //Get src/dest dataset descriptors from the flow config
    Config srcDatasetDescriptorConfig =
        flowConfig.getConfig(DatasetDescriptorConfigKeys.FLOW_INPUT_DATASET_DESCRIPTOR_PREFIX);
    Config destDatasetDescriptorConfig =
        flowConfig.getConfig(DatasetDescriptorConfigKeys.FLOW_OUTPUT_DATASET_DESCRIPTOR_PREFIX);

    //Add retention config for source and destination dataset descriptors.
    if (shouldApplyRetentionOnInput) {
      // We should run retention on source dataset. To ensure a retention is run, set
      // isRetentionApplied=false for source dataset.
      srcDatasetDescriptorConfig = srcDatasetDescriptorConfig
          .withValue(DatasetDescriptorConfigKeys.IS_RETENTION_APPLIED_KEY, ConfigValueFactory.fromAnyRef(false));
    } else {
      // Don't apply retention on source dataset.
      //
      // If ConfigurationKeys.FLOW_APPLY_RETENTION is true, isRetentionApplied is set to true for the source dataset.
      // The PathFinder will therefore treat the source dataset as one on which retention has already been
      // applied, preventing retention from running on the source dataset.
      //
      // On the other hand, if ConfigurationKeys.FLOW_APPLY_RETENTION is false
      // we do not apply retention - neither on the source dataset nor anywhere along the path to the destination.
      srcDatasetDescriptorConfig = srcDatasetDescriptorConfig
          .withValue(DatasetDescriptorConfigKeys.IS_RETENTION_APPLIED_KEY, ConfigValueFactory.fromAnyRef(shouldApplyRetention));
    }
    destDatasetDescriptorConfig = destDatasetDescriptorConfig.withValue(DatasetDescriptorConfigKeys.IS_RETENTION_APPLIED_KEY, ConfigValueFactory.fromAnyRef(shouldApplyRetention));

    //Add the retention configs to the FlowConfig
    flowConfig = flowConfig.withValue(ConfigurationKeys.FLOW_APPLY_RETENTION, ConfigValueFactory.fromAnyRef(shouldApplyRetention));
    flowConfig = flowConfig.withValue(ConfigurationKeys.FLOW_APPLY_INPUT_RETENTION, ConfigValueFactory.fromAnyRef(shouldApplyRetentionOnInput));

    srcDatasetDescriptorConfig = srcDatasetDescriptorConfig.withFallback(getDefaultConfig(this.srcNode));
    if (this.destNodes != null) {
      destDatasetDescriptorConfig = destDatasetDescriptorConfig.withFallback(getDefaultConfig(this.destNodes.get(0)));
    }

    this.srcDatasetDescriptor = DatasetDescriptorUtils.constructDatasetDescriptor(srcDatasetDescriptorConfig);
    this.destDatasetDescriptor = DatasetDescriptorUtils.constructDatasetDescriptor(destDatasetDescriptorConfig);
  }

  public static Config getDefaultConfig(DataNode dataNode) {
    Config defaultConfig = ConfigFactory.empty();

    if (dataNode.getDefaultDatasetDescriptorClass() != null) {
      defaultConfig = defaultConfig.withValue(DatasetDescriptorConfigKeys.CLASS_KEY, ConfigValueFactory.fromAnyRef(dataNode.getDefaultDatasetDescriptorClass()));
    }

    if (dataNode.getDefaultDatasetDescriptorPlatform() != null) {
      defaultConfig = defaultConfig.withValue(DatasetDescriptorConfigKeys.PLATFORM_KEY, ConfigValueFactory.fromAnyRef(dataNode.getDefaultDatasetDescriptorPlatform()));
    }

    return defaultConfig;
  }

  boolean isPathFound(DataNode currentNode, DataNode destNode, DatasetDescriptor currentDatasetDescriptor,
      DatasetDescriptor destDatasetDescriptor) {
    return (currentNode.equals(destNode)) && (currentDatasetDescriptor.equals(destDatasetDescriptor));
  }

  /**
   * A helper method that sorts the {@link FlowEdge}s incident on srcNode based on whether the FlowEdge has an
   * output {@link DatasetDescriptor} that is compatible with the targetDatasetDescriptor.
   * @param dataNode the {@link DataNode} to be expanded for determining candidate edges.
   * @param currentDatasetDescriptor Output {@link DatasetDescriptor} of the current edge.
   * @param destDatasetDescriptor Target {@link DatasetDescriptor}.
   * @return prioritized list of {@link FlowEdge}s to be added to the edge queue for expansion.
   */
  List<FlowEdgeContext> getNextEdges(DataNode dataNode, DatasetDescriptor currentDatasetDescriptor,
      DatasetDescriptor destDatasetDescriptor) {
    List<FlowEdgeContext> prioritizedEdgeList = new LinkedList<>();
    List<String> edgeIds = ConfigUtils.getStringList(this.flowConfig, ConfigurationKeys.WHITELISTED_EDGE_IDS);
    for (FlowEdge flowEdge : this.flowGraph.getEdges(dataNode)) {
      if (!edgeIds.isEmpty() && !edgeIds.contains(flowEdge.getId())) {
        continue;
      }
      try {
        DataNode edgeDestination = this.flowGraph.getNode(flowEdge.getDest());
        //Base condition: Skip this FLowEdge, if it is inactive or if the destination of this edge is inactive.
        if (!edgeDestination.isActive() || !flowEdge.isActive()) {
          continue;
        }

        boolean foundExecutor = false;
        //Iterate over all executors for this edge. Find the first one that resolves the underlying flow template.
        for (SpecExecutor specExecutor : flowEdge.getExecutors()) {
          Config mergedConfig = getMergedConfig(flowEdge);
          List<Pair<DatasetDescriptor, DatasetDescriptor>> datasetDescriptorPairs =
              flowEdge.getFlowTemplate().getDatasetDescriptors(mergedConfig, false);
          for (Pair<DatasetDescriptor, DatasetDescriptor> datasetDescriptorPair : datasetDescriptorPairs) {
            DatasetDescriptor inputDatasetDescriptor = datasetDescriptorPair.getLeft();
            DatasetDescriptor outputDatasetDescriptor = datasetDescriptorPair.getRight();

            try {
              flowEdge.getFlowTemplate().tryResolving(mergedConfig, datasetDescriptorPair.getLeft(), datasetDescriptorPair.getRight());
            } catch (JobTemplate.TemplateException | ConfigException | SpecNotFoundException e) {
              flowSpec.addCompilationError(flowEdge.getSrc(), flowEdge.getDest(), "Error compiling edge " + flowEdge.toString() + ": " + e.toString());
              continue;
            }

            if (inputDatasetDescriptor.contains(currentDatasetDescriptor)) {
              DatasetDescriptor edgeOutputDescriptor = makeOutputDescriptorSpecific(currentDatasetDescriptor, outputDatasetDescriptor);
              FlowEdgeContext flowEdgeContext = new FlowEdgeContext(flowEdge, currentDatasetDescriptor, edgeOutputDescriptor, mergedConfig,
                  specExecutor);

              if (destDatasetDescriptor.getFormatConfig().contains(outputDatasetDescriptor.getFormatConfig())) {
                /*
                Add to the front of the edge list if platform-independent properties of the output descriptor is compatible
                with those of destination dataset descriptor.
                In other words, we prioritize edges that perform data transformations as close to the source as possible.
                */
                prioritizedEdgeList.add(0, flowEdgeContext);
              } else {
                prioritizedEdgeList.add(flowEdgeContext);
              }
              foundExecutor = true;
            }
          }
          // Found a SpecExecutor. Proceed to the next FlowEdge.
          // TODO: Choose the min-cost executor for the FlowEdge as opposed to the first one that resolves.
          if (foundExecutor) {
            break;
          }
        }
      } catch (IOException | ReflectiveOperationException | SpecNotFoundException | JobTemplate.TemplateException e) {
        //Skip the edge; and continue
        log.warn("Skipping edge {} with config {} due to exception: {}", flowEdge.getId(), flowConfig.toString(), e);
      }
    }
    return prioritizedEdgeList;
  }

  /**
   * A helper method to make the output {@link DatasetDescriptor} of a {@link FlowEdge} "specific". More precisely,
   * we replace any "placeholder" configurations in the output {@link DatasetDescriptor} with specific configuration
   * values obtained from the input {@link DatasetDescriptor}. A placeholder configuration is one which is not
   * defined or is set to {@link DatasetDescriptorConfigKeys#DATASET_DESCRIPTOR_CONFIG_ANY}.
   *
   * Example: Consider a {@link FlowEdge} that applies retention on an input dataset. Further assume that this edge
   * is applicable to datasets of all formats. The input and output descriptors of this edge may be described using the following
   * configs:
   * inputDescriptor = Config(SimpleConfigObject({"class":"org.apache.gobblin.service.modules.dataset.FSDatasetDescriptor",
   * "codec":"any","encrypt":{"algorithm":"any","keystore_encoding":"any","keystore_type":"any"},"format":"any",
   * "isRetentionApplied":false,"path":"/data/encrypted/testTeam/testDataset","platform":"hdfs"}))
   *
   * outputDescriptor = Config(SimpleConfigObject({"class":"org.apache.gobblin.service.modules.dataset.FSDatasetDescriptor",
   * "codec":"any","encrypt":{"algorithm":"any","keystore_encoding":"any","keystore_type":"any"},"format":"any",
   * "isRetentionApplied":true,"path":"/data/encrypted/testTeam/testDataset","platform":"hdfs"}))
   *
   * Let the intermediate dataset descriptor "arriving" at this edge be described using the following config:
   * currentDescriptor = Config(SimpleConfigObject({"class":"org.apache.gobblin.service.modules.dataset.FSDatasetDescriptor",
   * "codec":"gzip","encrypt":{"algorithm":"aes_rotating","keystore_encoding":"base64","keystore_type":"json"},"format":"json",
   * "isRetentionApplied":false,"path":"/data/encrypted/testTeam/testDataset","platform":"hdfs"})).
   *
   * This method replaces the placeholder configs in outputDescriptor with specific values from currentDescriptor to return:
   * returnedDescriptor = Config(SimpleConfigObject({"class":"org.apache.gobblin.service.modules.dataset.FSDatasetDescriptor",
   * "codec":"gzip","encrypt":{"algorithm":"aes_rotating","keystore_encoding":"base64","keystore_type":"json"},"format":"json",
   * "isRetentionApplied":<b>true</b>,"path":"/data/encrypted/testTeam/testDataset","platform":"hdfs"})).
   *
   * @param currentDescriptor intermediate {@link DatasetDescriptor} obtained during path finding.
   * @param outputDescriptor output {@link DatasetDescriptor} of a {@link FlowEdge}.
   * @return {@link DatasetDescriptor} with placeholder configs in outputDescriptor substituted with specific values
   * from the currentDescriptor.
   */

  private DatasetDescriptor makeOutputDescriptorSpecific(DatasetDescriptor currentDescriptor, DatasetDescriptor outputDescriptor)
      throws ReflectiveOperationException {
    Config config = outputDescriptor.getRawConfig();

    for (Map.Entry<String, ConfigValue> entry : currentDescriptor.getRawConfig().entrySet()) {
      String entryValue = entry.getValue().unwrapped().toString();
      if (!isPlaceHolder(entryValue)) {
        String entryValueInOutputDescriptor = ConfigUtils.getString(config, entry.getKey(), StringUtils.EMPTY);
        if (isPlaceHolder(entryValueInOutputDescriptor)) {
          config = config.withValue(entry.getKey(), ConfigValueFactory.fromAnyRef(entryValue));
        }
      }
    }
    return GobblinConstructorUtils.invokeLongestConstructor(outputDescriptor.getClass(), config);
  }

  /**
   * A placeholder configuration is one which is not defined or is set to {@link DatasetDescriptorConfigKeys#DATASET_DESCRIPTOR_CONFIG_ANY}.
   * @param value to be examined for determining if it is a placeholder.
   * @return true if the value is null or empty or equals {@link DatasetDescriptorConfigKeys#DATASET_DESCRIPTOR_CONFIG_ANY}.
   */
  private boolean isPlaceHolder(String value) {
    return Strings.isNullOrEmpty(value) || value.equals(DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY);
  }
  /**
   * Build the merged config for each {@link FlowEdge}, which is a combination of (in the precedence described below):
   * <ul>
   *   <p> the user provided flow config </p>
   *   <p> edge specific properties/overrides </p>
   *   <p> source node config </p>
   *   <p> destination node config </p>
   * </ul>
   * Each {@link JobTemplate}'s config will eventually be resolved against this merged config.
   * @param flowEdge An instance of {@link FlowEdge}.
   * @return the merged config derived as described above.
   */
  private Config getMergedConfig(FlowEdge flowEdge) {
    Config srcNodeConfig = this.flowGraph.getNode(flowEdge.getSrc()).getRawConfig().atPath(SOURCE_PREFIX);
    Config destNodeConfig = this.flowGraph.getNode(flowEdge.getDest()).getRawConfig().atPath(DESTINATION_PREFIX);
    Config mergedConfig = flowConfig.withFallback(flowEdge.getConfig()).withFallback(srcNodeConfig).withFallback(destNodeConfig);
    return mergedConfig;
  }

  /**
   *
   * @param flowEdgeContext of the last {@link FlowEdge} in the path.
   * @return a {@link Dag} of {@link JobExecutionPlan}s for the input {@link FlowSpec}.
   */
  List<FlowEdgeContext> constructPath(FlowEdgeContext flowEdgeContext) {
    //Backtrace from the last edge using the path map and push each edge into a LIFO data structure.
    List<FlowEdgeContext> path = new LinkedList<>();
    path.add(flowEdgeContext);
    FlowEdgeContext currentFlowEdgeContext = flowEdgeContext;
    //While we are not at the first edge in the path, add the edge to the path
    while (!this.pathMap.get(currentFlowEdgeContext).equals(currentFlowEdgeContext)) {
      path.add(0, this.pathMap.get(currentFlowEdgeContext));
      currentFlowEdgeContext = this.pathMap.get(currentFlowEdgeContext);
    }
    return path;
  }

  @Override
  public FlowGraphPath findPath() throws PathFinderException {

    FlowGraphPath flowGraphPath = new FlowGraphPath(flowSpec, flowExecutionId);
    // Path computation must be thread-safe to guarantee read consistency. In other words, we prevent concurrent read/write access to the
    // flow graph.
    for (DataNode destNode : this.destNodes) {
      List<FlowEdgeContext> path = findPathUnicast(destNode);
      if (path != null) {
        log.info("Path to destination node {} found for flow {}. Path - {}", destNode.getId(), flowSpec.getUri(), path);
        flowGraphPath.addPath(path);
      } else {
        log.error("Path to destination node {} could not be found for flow {}.", destNode.getId(), flowSpec.getUri());
        //No path to at least one of the destination nodes.
        return null;
      }
    }
    return flowGraphPath;
  }

  public abstract List<FlowEdgeContext> findPathUnicast(DataNode destNode) throws PathFinderException;
}
