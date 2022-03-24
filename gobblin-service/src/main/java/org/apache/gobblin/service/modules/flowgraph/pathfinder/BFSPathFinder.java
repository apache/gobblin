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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.service.modules.dataset.DatasetDescriptor;
import org.apache.gobblin.service.modules.flow.FlowEdgeContext;
import org.apache.gobblin.service.modules.flowgraph.DataNode;
import org.apache.gobblin.service.modules.flowgraph.FlowEdge;
import org.apache.gobblin.service.modules.flowgraph.FlowGraph;


/**
 * An implementation of {@link PathFinder} that assumes an unweighted {@link FlowGraph} and computes the
 * shortest path using a variant of the BFS path-finding algorithm. This implementation has two key differences from the
 * traditional BFS implementations:
 *  <ul>
 *    <p> the input graph is a multi-graph i.e. there could be multiple edges between each pair of nodes, and </p>
 *    <p> each edge has a label associated with it. In our case, the label corresponds to the set of input/output
 *    dataset descriptors that are accepted by the edge.</p>
 *  </ul>
 *  Given these differences, we maintain:
 *    <p> a {@link HashMap} of list of visited edges, as opposed to list of visited
 *  vertices as in the case of traditional BFS, and </p>
 *    <p> for each edge, we maintain additional state that includes the input/output dataset descriptor
 *  associated with the particular visitation of that edge. </p>
 *  This additional information allows us to accurately mark edges as visited and guarantee termination of the algorithm.
 */
@Alpha
@Slf4j
public class BFSPathFinder extends AbstractPathFinder {
  /**
   * Constructor.
   * @param flowGraph
   */
  public BFSPathFinder(FlowGraph flowGraph, FlowSpec flowSpec)
      throws ReflectiveOperationException {
    this(flowGraph, flowSpec, new HashMap<>());
  }

  public BFSPathFinder(FlowGraph flowGraph, FlowSpec flowSpec, Map<String, String> dataNodeAliasMap) throws ReflectiveOperationException {
    super(flowGraph, flowSpec, dataNodeAliasMap);
  }

  /**
   * A simple path finding algorithm based on Breadth-First Search. At every step the algorithm adds the adjacent {@link FlowEdge}s
   * to a queue. The {@link FlowEdge}s whose output {@link DatasetDescriptor} matches the destDatasetDescriptor are
   * added first to the queue. This ensures that dataset transformations are always performed closest to the source.
   * @return a path of {@link FlowEdgeContext}s starting at the srcNode and ending at the destNode.
   */
  public List<FlowEdgeContext> findPathUnicast(DataNode destNode) {
    //Initialization of auxiliary data structures used for path computation
    this.pathMap = new HashMap<>();

    //Path computation must be thread-safe to guarantee read consistency. In other words, we prevent concurrent read/write access to the
    // flow graph.
    //Base condition 1: Source Node or Dest Node is inactive; return null
    if (!srcNode.isActive() || !destNode.isActive()) {
      log.warn("Either source node {} or destination node {} is inactive; skipping path computation.",
          this.srcNode.getId(), destNode.getId());
      return null;
    }

    //Base condition 2: Check if we are already at the target. If so, return an empty path.
    if ((srcNode.equals(destNode)) && destDatasetDescriptor.contains(srcDatasetDescriptor)) {
      return new ArrayList<>(0);
    }

    LinkedList<FlowEdgeContext> edgeQueue =
        new LinkedList<>(getNextEdges(srcNode, srcDatasetDescriptor, destDatasetDescriptor));
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
  }
}

