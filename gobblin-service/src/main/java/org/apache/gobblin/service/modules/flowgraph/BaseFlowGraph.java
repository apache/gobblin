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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.gobblin.annotation.Alpha;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * A thread-safe implementation of {@link FlowGraph}. The implementation maintains the following data structures:
 *    <p>nodes - the set of {@link DataNode}s in the graph</p>
 *    <p>nodesToEdges - the mapping from each {@link DataNode} to its outgoing {@link FlowEdge}s</p>
 *    <p>dataNodeMap - the mapping from a node identifier to the {@link DataNode} instance</p>
 *    <p>flowEdgeMap - the mapping from a edge label to the {@link FlowEdge} instance</p>
 */
@Alpha
@Slf4j
public class BaseFlowGraph implements FlowGraph {
  @Getter
  private Set<DataNode> nodes = new HashSet<>();
  @Getter
  private FlowEdgeFactory flowEdgeFactory;

  private Map<DataNode, Set<FlowEdge>> nodesToEdges = new HashMap<>();
  private Map<String, DataNode> dataNodeMap = new HashMap<>();
  private Map<String, FlowEdge> flowEdgeMap = new HashMap<>();

  //Default constructor using default {@link DataNode} and {@link FlowEdge} factory classes.
  public BaseFlowGraph() {
    this.flowEdgeFactory = new BaseFlowEdge.Factory();
  }

  //Constructor
  public BaseFlowGraph(FlowEdgeFactory flowEdgeFactory) {
    this.flowEdgeFactory = flowEdgeFactory;
  }

  /**
   * Lookup a node by its identifier.
   *
   * @param nodeId node identifier
   * @return {@link DataNode} with nodeId as the identifier.
   */
  public DataNode getNode(String nodeId) {
    return this.dataNodeMap.getOrDefault(nodeId, null);
  }

  /**
   * Add a {@link DataNode} to the {@link FlowGraph}. If the node already "exists" in the {@link FlowGraph} (i.e. the
   * FlowGraph already has another node with the same id), we remove the old node and add the new one. The
   * edges incident on the old node are preserved.
   * @param node to be added to the {@link FlowGraph}
   * @return true if node is successfully added to the {@link FlowGraph}.
   */
  @Override
  public synchronized boolean addDataNode(DataNode node) {
    if(!this.nodes.add(node)) {
      //Node already exists. Remove "old" node and add the new one.
      this.nodes.remove(node);
      this.nodes.add(node);
    }
    //Get edges adjacent to the node if it already exists
    Set<FlowEdge> edges = this.nodesToEdges.getOrDefault(node, new HashSet<>());
    this.nodesToEdges.put(node, edges);
    this.dataNodeMap.put(node.getId(), node);
    return true;
  }

  /**
   * Add a {@link FlowEdge} to the {@link FlowGraph}. Addition of edge succeeds only if both the end points of the
   * edge are already nodes in the FlowGraph. If a {@link FlowEdge} already exists, the old FlowEdge is removed and
   * the new one added in its place.
   * @param edge
   * @return true if addition of {@FlowEdge} is successful.
   */
  @Override
  public synchronized boolean addFlowEdge(FlowEdge edge) {
    String srcNode = edge.getEndPoints().get(0);
    String dstNode = edge.getEndPoints().get(1);
    if(!dataNodeMap.containsKey(srcNode) || !dataNodeMap.containsKey(dstNode)) {
      return false;
    }
    DataNode dataNode = getNode(srcNode);
    if(dataNode != null) {
      Set<FlowEdge> adjacentEdges = this.nodesToEdges.get(dataNode);
      if(!adjacentEdges.add(edge)) {
        adjacentEdges.remove(edge);
        adjacentEdges.add(edge);
      }
      this.nodesToEdges.put(dataNode, adjacentEdges);
      String edgeId = edge.getId();
      this.flowEdgeMap.put(edgeId, edge);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Delete a {@link DataNode} by its identifier
   * @param nodeId identifier of the {@link DataNode} to be deleted.
   * @return true if {@link DataNode} is successfully deleted.
   */
  @Override
  public synchronized boolean deleteDataNode(String nodeId) {
    if(this.dataNodeMap.containsKey(nodeId) && deleteDataNode(this.dataNodeMap.get(nodeId))) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * Delete a {@DataNode} from the {@link FlowGraph}.
   * @param node to be deleted.
   * @return true if {@link DataNode} is successfully deleted.
   */
  public synchronized boolean deleteDataNode(DataNode node) {
    if(nodes.contains(node)) {
      //Delete node from node set and dataNodeMap
      nodes.remove(node);
      dataNodeMap.remove(node.getId());

      //Delete all the edges adjacent to the node. First, delete edges from flowEdgeMap and next, remove the edges
      // from nodesToEdges
      for(FlowEdge edge: nodesToEdges.get(node)) {
        flowEdgeMap.remove(edge.getId());
      }
      nodesToEdges.remove(node);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Delete a {@link DataNode} by its identifier
   * @param edgeId identifier of the {@link FlowEdge} to be deleted.
   * @return true if {@link FlowEdge} is successfully deleted.
   */
  @Override
  public synchronized boolean deleteFlowEdge(String edgeId) {
    if(flowEdgeMap.containsKey(edgeId) && deleteFlowEdge(flowEdgeMap.get(edgeId))) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * Delete a {@FlowEdge} from the {@link FlowGraph}.
   * @param edge to be deleted.
   * @return true if {@link FlowEdge} is successfully deleted. If the source of a {@link FlowEdge} does not exist or
   * if the {@link FlowEdge} is not in the graph, return false.
   */
  public synchronized boolean deleteFlowEdge(FlowEdge edge) {
    if(!dataNodeMap.containsKey(edge.getEndPoints().get(0))) {
      return false;
    }
    DataNode node = dataNodeMap.get(edge.getEndPoints().get(0));
    if(!nodesToEdges.get(node).contains(edge)) {
      return false;
    }
    this.nodesToEdges.get(node).remove(edge);
    this.flowEdgeMap.remove(edge.getId());
    return true;
  }

  /**
   * Get the set of edges adjacent to a {@link DataNode}
   * @param nodeId identifier of the node
   * @return Set of {@link FlowEdge}s adjacent to the node.
   */
  @Override
  public Set<FlowEdge> getEdges(String nodeId) {
    DataNode dataNode = this.dataNodeMap.getOrDefault(nodeId, null);
    return getEdges(dataNode);
  }

  /**
   * Get the set of edges adjacent to a {@link DataNode}
   * @param node {@link DataNode}
   * @return Set of {@link FlowEdge}s adjacent to the node.
   */
  @Override
  public Set<FlowEdge> getEdges(DataNode node) {
    return (node != null)? this.nodesToEdges.getOrDefault(node, null) : null;
  }

}
