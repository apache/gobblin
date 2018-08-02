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

package org.apache.gobblin.service.modules.flow;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.dataset.DatasetDescriptor;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.flowgraph.DataNode;
import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.FlowEdge;
import org.apache.gobblin.service.modules.flowgraph.FlowGraph;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


@Alpha
@Slf4j
public class FlowGraphPathFinder {
  private static final String SOURCE_PREFIX = "source";
  private static final String DESTINATION_PREFIX = "destination";

  private FlowGraph flowGraph;
  private FlowSpec flowSpec;
  private Config flowConfig;

  private DataNode srcNode;
  private List<DataNode> destNodes;

  private DatasetDescriptor srcDatasetDescriptor;
  private DatasetDescriptor destDatasetDescriptor;

  //Maintain path of FlowEdges as parent-child map
  private Map<FlowEdgeContext, FlowEdgeContext> pathMap;

  //Flow Execution Id
  private Long flowExecutionId;

  /**
   * Constructor.
   * @param flowGraph
   */
  public FlowGraphPathFinder(FlowGraph flowGraph, FlowSpec flowSpec) {
    this.flowGraph = flowGraph;
    this.flowSpec = flowSpec;
    this.flowConfig = flowSpec.getConfig();

    //Get src/dest DataNodes from the flow config
    String srcNodeId = ConfigUtils.getString(flowConfig, ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY, "");

    List<String> destNodeIds = ConfigUtils.getStringList(flowConfig, ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY);
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
    //Get src/dest dataset descriptors from the flow config
    Config srcDatasetDescriptorConfig =
        flowConfig.getConfig(DatasetDescriptorConfigKeys.FLOW_INPUT_DATASET_DESCRIPTOR_PREFIX);
    Config destDatasetDescriptorConfig =
        flowConfig.getConfig(DatasetDescriptorConfigKeys.FLOW_OUTPUT_DATASET_DESCRIPTOR_PREFIX);

    try {
      Class srcdatasetDescriptorClass =
          Class.forName(srcDatasetDescriptorConfig.getString(DatasetDescriptorConfigKeys.CLASS_KEY));
      this.srcDatasetDescriptor = (DatasetDescriptor) GobblinConstructorUtils
          .invokeLongestConstructor(srcdatasetDescriptorClass, srcDatasetDescriptorConfig);
      Class destDatasetDescriptorClass =
          Class.forName(destDatasetDescriptorConfig.getString(DatasetDescriptorConfigKeys.CLASS_KEY));
      this.destDatasetDescriptor = (DatasetDescriptor) GobblinConstructorUtils
          .invokeLongestConstructor(destDatasetDescriptorClass, destDatasetDescriptorConfig);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public FlowGraphPath findPath() throws PathFinderException {
    // Generate flow execution id for this compilation
    this.flowExecutionId = System.currentTimeMillis();

    FlowGraphPath flowGraphPath = new FlowGraphPath(flowSpec, flowExecutionId);
    //Path computation must be thread-safe to guarantee read consistency. In other words, we prevent concurrent read/write access to the
    // flow graph.
    // TODO: we can easily improve the performance by using a ReentrantReadWriteLock associated with the FlowGraph. This will
    // allow multiple concurrent readers to not be blocked on each other, as long as there are no writers.
    synchronized (this.flowGraph) {
      for (DataNode destNode : this.destNodes) {
        List<FlowEdgeContext> path = findPathBFS(destNode);
        if (path != null) {
          flowGraphPath.addPath(path);
        } else {
          //No path to at least one of the destination nodes.
          return null;
        }
      }
    }
    return flowGraphPath;
  }

  /**
   * A simple path finding algorithm based on Breadth-First Search. At every step the algorithm adds the adjacent {@link FlowEdge}s
   * to a queue. The {@link FlowEdge}s whose output {@link DatasetDescriptor} matches the destDatasetDescriptor are
   * added first to the queue. This ensures that dataset transformations are always performed closest to the source.
   * @return a path of {@link FlowEdgeContext}s starting at the srcNode and ending at the destNode.
   */
  private List<FlowEdgeContext> findPathBFS(DataNode destNode)
      throws PathFinderException {
    try {
      //Initialization of auxiliary data structures used for path computation
      this.pathMap = new HashMap<>();

      //Base condition 1: Source Node or Dest Node is inactive; return null
      if (!srcNode.isActive() || !destNode.isActive()) {
        log.warn("Either source node {} or destination node {} is inactive; skipping path computation.",
            this.srcNode.getId(), destNode.getId());
        return null;
      }

      //Base condition 2: Check if we are already at the target. If so, return an empty path.
      if ((srcNode.equals(destNode)) && destDatasetDescriptor.contains(srcDatasetDescriptor)) {
        return new ArrayList<>();
      }

      LinkedList<FlowEdgeContext> edgeQueue = new LinkedList<>();
      edgeQueue.addAll(getNextEdges(srcNode, srcDatasetDescriptor, destDatasetDescriptor));
      for (FlowEdgeContext flowEdgeContext : edgeQueue) {
        this.pathMap.put(flowEdgeContext, flowEdgeContext);
      }

      //At every step, pop an edge E from the edge queue. Mark the edge E as visited. Generate the list of adjacent edges
      // to the edge E. For each adjacent edge E', do the following:
      //    1. check if the FlowTemplate described by E' is resolvable using the flowConfig, and
      //    2. check if the output dataset descriptor of edge E is compatible with the input dataset descriptor of the
      //       edge E'. If yes, add the edge E' to the edge queue.
      // If the edge E' satisfies 1 and 2, add it to the edge queue for further consideration.
      while (!edgeQueue.isEmpty()) {
        FlowEdgeContext flowEdgeContext = edgeQueue.pop();

        DataNode currentNode = this.flowGraph.getNode(flowEdgeContext.getEdge().getDest());
        DatasetDescriptor currentOutputDatasetDescriptor = flowEdgeContext.getOutputDatasetDescriptor();

        //Are we done?
        if (isPathFound(currentNode, destNode, currentOutputDatasetDescriptor, destDatasetDescriptor)) {
          return constructPath(flowEdgeContext);
        }

        //Expand the currentNode to its adjacent edges and add them to the queue.
        List<FlowEdgeContext> nextEdges =
            getNextEdges(currentNode, currentOutputDatasetDescriptor, destDatasetDescriptor);
        for (FlowEdgeContext childFlowEdgeContext : nextEdges) {
          //Add a pointer from the child edge to the parent edge, if the child edge is not already in the
          // queue.
          if (!this.pathMap.containsKey(childFlowEdgeContext)) {
            edgeQueue.add(childFlowEdgeContext);
            this.pathMap.put(childFlowEdgeContext, flowEdgeContext);
          }
        }
      }
      //No path found. Return null.
      return null;
    } catch (SpecNotFoundException | JobTemplate.TemplateException | IOException | URISyntaxException e) {
      throw new PathFinderException(
          "Exception encountered when computing path from src: " + this.srcNode.getId() + " to dest: " + destNode
              .getId(), e);
    }
  }

  private boolean isPathFound(DataNode currentNode, DataNode destNode, DatasetDescriptor currentDatasetDescriptor,
      DatasetDescriptor destDatasetDescriptor) {
    if ((currentNode.equals(destNode)) && (currentDatasetDescriptor.equals(destDatasetDescriptor))) {
      return true;
    }
    return false;
  }

  /**
   * A helper method that sorts the {@link FlowEdge}s incident on srcNode based on whether the FlowEdge has an
   * output {@link DatasetDescriptor} that is compatible with the targetDatasetDescriptor.
   * @param dataNode
   * @param currentDatasetDescriptor Output {@link DatasetDescriptor} of the current edge.
   * @param destDatasetDescriptor Target {@link DatasetDescriptor}.
   * @return prioritized list of {@link FlowEdge}s to be added to the edge queue for expansion.
   */
  private List<FlowEdgeContext> getNextEdges(DataNode dataNode, DatasetDescriptor currentDatasetDescriptor,
      DatasetDescriptor destDatasetDescriptor) {
    List<FlowEdgeContext> prioritizedEdgeList = new LinkedList<>();
    for (FlowEdge flowEdge : this.flowGraph.getEdges(dataNode)) {
      try {
        DataNode edgeDestination = this.flowGraph.getNode(flowEdge.getDest());
        //Base condition: Skip this FLowEdge, if it is inactive or if the destination of this edge is inactive.
        if (!edgeDestination.isActive() || !flowEdge.isActive()) {
          continue;
        }

        boolean foundExecutor = false;
        //Iterate over all executors for this edge. Find the first one that resolves the underlying flow template.
        for (SpecExecutor specExecutor : flowEdge.getExecutors()) {
          Config mergedConfig = getMergedConfig(flowEdge, specExecutor);
          List<Pair<DatasetDescriptor, DatasetDescriptor>> datasetDescriptorPairs =
              flowEdge.getFlowTemplate().getResolvingDatasetDescriptors(mergedConfig);
          for (Pair<DatasetDescriptor, DatasetDescriptor> datasetDescriptorPair : datasetDescriptorPairs) {
            DatasetDescriptor inputDatasetDescriptor = datasetDescriptorPair.getLeft();
            DatasetDescriptor outputDatasetDescriptor = datasetDescriptorPair.getRight();
            if (inputDatasetDescriptor.contains(currentDatasetDescriptor)) {
              FlowEdgeContext flowEdgeContext;
              if (outputDatasetDescriptor.contains(currentDatasetDescriptor)) {
                //If datasets described by the currentDatasetDescriptor is a subset of the datasets described
                // by the outputDatasetDescriptor (i.e. currentDatasetDescriptor is more "specific" than outputDatasetDescriptor, e.g.
                // as in the case of a "distcp" edge), we propagate the more "specific" dataset descriptor forward.
                flowEdgeContext =
                    new FlowEdgeContext(flowEdge, currentDatasetDescriptor, currentDatasetDescriptor, mergedConfig,
                        specExecutor);
              } else {
                //outputDatasetDescriptor is more specific (e.g. if it is a dataset transformation edge)
                flowEdgeContext =
                    new FlowEdgeContext(flowEdge, currentDatasetDescriptor, outputDatasetDescriptor, mergedConfig,
                        specExecutor);
              }
              if (destDatasetDescriptor.getFormatConfig().contains(outputDatasetDescriptor.getFormatConfig())) {
                //Add to the front of the edge list if platform-independent properties of the output descriptor is compatible
                // with those of destination dataset descriptor.
                // In other words, we prioritize edges that perform data transformations as close to the source as possible.
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
      } catch (IOException | ReflectiveOperationException | InterruptedException | ExecutionException | SpecNotFoundException
          | JobTemplate.TemplateException e) {
        //Skip the edge; and continue
        log.warn("Skipping edge {} with config {} due to exception: {}", flowEdge.getId(), flowConfig.toString(), e);
      }
    }
    return prioritizedEdgeList;
  }

  /**
   * Build the merged config for each {@link FlowEdge}, which is a combination of (in the precedence described below):
   * <ul>
   *   <p> the user provided flow config </p>
   *   <p> edge specific properties/overrides </p>
   *   <p> spec executor config/overrides </p>
   *   <p> source node config </p>
   *   <p> destination node config </p>
   * </ul>
   * Each {@link JobTemplate}'s config will eventually be resolved against this merged config.
   * @param flowEdge An instance of {@link FlowEdge}.
   * @param specExecutor A {@link SpecExecutor}.
   * @return the merged config derived as described above.
   */
  private Config getMergedConfig(FlowEdge flowEdge, SpecExecutor specExecutor)
      throws ExecutionException, InterruptedException {
    Config srcNodeConfig = this.flowGraph.getNode(flowEdge.getSrc()).getRawConfig().atPath(SOURCE_PREFIX);
    Config destNodeConfig = this.flowGraph.getNode(flowEdge.getDest()).getRawConfig().atPath(DESTINATION_PREFIX);
    Config mergedConfig = flowConfig.withFallback(specExecutor.getConfig().get()).withFallback(flowEdge.getConfig())
        .withFallback(srcNodeConfig).withFallback(destNodeConfig);
    return mergedConfig;
  }

  /**
   *
   * @param flowEdgeContext of the last {@link FlowEdge} in the path.
   * @return a {@link Dag} of {@link JobExecutionPlan}s for the input {@link FlowSpec}.
   * @throws IOException
   * @throws SpecNotFoundException
   * @throws JobTemplate.TemplateException
   * @throws URISyntaxException
   */
  private List<FlowEdgeContext> constructPath(FlowEdgeContext flowEdgeContext)
      throws IOException, SpecNotFoundException, JobTemplate.TemplateException, URISyntaxException {
    //Backtrace from the last edge using the path map and push each edge into a LIFO data structure.
    List<FlowEdgeContext> path = new LinkedList<>();
    path.add(flowEdgeContext);
    FlowEdgeContext currentFlowEdgeContext = flowEdgeContext;
    while (true) {
      path.add(0, this.pathMap.get(currentFlowEdgeContext));
      currentFlowEdgeContext = this.pathMap.get(currentFlowEdgeContext);
      //Are we at the first edge in the path?
      if (this.pathMap.get(currentFlowEdgeContext).equals(currentFlowEdgeContext)) {
        break;
      }
    }
    return path;
  }

  public static class PathFinderException extends Exception {
    public PathFinderException(String message, Throwable cause) {
      super(message, cause);
    }

    public PathFinderException(String message) {
      super(message);
    }
  }
}

