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

package org.apache.gobblin.service.modules.utils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.gobblin.runtime.api.FlowEdge;
import org.apache.gobblin.runtime.api.ServiceNode;
import org.apache.gobblin.service.modules.flow.LoadBasedFlowEdgeImpl;
import org.jgrapht.graph.DirectedWeightedMultigraph;

import lombok.extern.slf4j.Slf4j;

import avro.shaded.com.google.common.annotations.VisibleForTesting;

@Slf4j
public class FindPathUtils {
  // Since Author{autumnust@gmail.com} couldn't find the proper way to conduct Library provided by JGraphT
  // on the customized-edge Graph, here is the raw implementation of Dijkstra algorithm for finding shortest path.

  /**
   * Given sourceNode and targetNode, find the shortest path and return shortest path.
   * @return Each edge on this shortest path, in order.
   *
   */
  @VisibleForTesting
  public static List<FlowEdge> dijkstraBasedPathFindingHelper(ServiceNode sourceNode, ServiceNode targetNode,
      DirectedWeightedMultigraph<ServiceNode, FlowEdge> weightedGraph) {
    Map<DistancedNode, ArrayList<FlowEdge>> shortestPath = new HashMap<>();
    Map<DistancedNode, Double> shortestDist = new HashMap<>();
    PriorityQueue<DistancedNode> pq = new PriorityQueue<>(new Comparator<DistancedNode>() {
      @Override
      public int compare(DistancedNode o1, DistancedNode o2) {
        if (o1.getDistToSrc() < o2.getDistToSrc()) {
          return -1;
        } else {
          return 1;
        }
      }
    });
    pq.add(new DistancedNode(sourceNode, 0.0));

    Set<FlowEdge> visitedEdge = new HashSet<>();

    while(!pq.isEmpty()) {
      DistancedNode node = pq.poll();
      if (node.getNode().getNodeName().equals(targetNode.getNodeName())) {
        // Searching finished
        return shortestPath.get(node);
      }

      Set<FlowEdge> outgoingEdges = weightedGraph.outgoingEdgesOf(node.getNode());
      for (FlowEdge outGoingEdge:outgoingEdges) {
        // Since it is a multi-graph problem, should use edge for deduplicaiton instead of vertex.
        if (visitedEdge.contains(outGoingEdge)) {
          continue;
        }

        DistancedNode adjacentNode = new DistancedNode(weightedGraph.getEdgeTarget(outGoingEdge));
        if (shortestDist.containsKey(adjacentNode)) {
          adjacentNode.setDistToSrc(shortestDist.get(adjacentNode));
        }

        double newDist = node.getDistToSrc() + ((LoadBasedFlowEdgeImpl) outGoingEdge).getEdgeLoad();

        if (newDist < adjacentNode.getDistToSrc()) {
          if (pq.contains(adjacentNode)) {
            pq.remove(adjacentNode);
          }

          // Update the shortest path.
          ArrayList<FlowEdge> path = shortestPath.containsKey(node)
              ? new ArrayList<>(shortestPath.get(node)) : new ArrayList<>();
          path.add(outGoingEdge);
          shortestPath.put(adjacentNode, path);
          shortestDist.put(adjacentNode, newDist);

          adjacentNode.setDistToSrc(newDist);
          pq.add(adjacentNode);
        }
        visitedEdge.add(outGoingEdge);
      }
    }
    log.error("No path found");
    return new ArrayList<>();
  }
}
