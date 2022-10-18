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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.service.modules.flow.FlowGraphPath;
import org.apache.gobblin.service.modules.flowgraph.pathfinder.PathFinder;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 * A thread-safe implementation of {@link FlowGraph}. The implementation maintains the following data structures:
 *   <p>dataNodeMap - the mapping from a node identifier to the {@link DataNode} instance</p>
 *   <p>nodesToEdges - the mapping from each {@link DataNode} to its outgoing {@link FlowEdge}s</p>
 *   <p>flowEdgeMap - the mapping from a edge label to the {@link FlowEdge} instance</p>
 *
 *   Read/Write Access to the {@link FlowGraph} is synchronized via a {@link ReentrantReadWriteLock}.
 */
@Alpha
@Slf4j
public class BaseFlowGraph implements FlowGraph {
  // Synchronize read/write access while the flowgraph is in the middle of an update
  private final ReadWriteLock rwLock = new ReentrantReadWriteLock(true);

  private final Map<DataNode, Set<FlowEdge>> nodesToEdges = new HashMap<>();
  private final Map<String, DataNode> dataNodeMap = new HashMap<>();
  private final Map<String, FlowEdge> flowEdgeMap = new HashMap<>();
  private final Map<String, String> dataNodeAliasMap;

  public BaseFlowGraph() {
    this(new HashMap<>());
  }

  public BaseFlowGraph(Map<String, String> dataNodeAliasMap) {
    this.dataNodeAliasMap = dataNodeAliasMap;
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
  public boolean addDataNode(DataNode node) {
    try {
      rwLock.writeLock().lock();
      //Get edges adjacent to the node if it already exists
      Set<FlowEdge> edges = this.nodesToEdges.getOrDefault(node, new HashSet<>());
      this.nodesToEdges.put(node, edges);
      this.dataNodeMap.put(node.getId(), node);
    } finally {
      rwLock.writeLock().unlock();
    }
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
  public boolean addFlowEdge(FlowEdge edge) {
    try {
      rwLock.writeLock().lock();
      String srcNode = edge.getSrc();
      String dstNode = edge.getDest();
      if (!dataNodeMap.containsKey(srcNode) || !dataNodeMap.containsKey(dstNode)) {
        return false;
      }
      DataNode dataNode = getNode(srcNode);
      if (dataNode == null) {
        return false;
      }
      Set<FlowEdge> adjacentEdges = this.nodesToEdges.get(dataNode);
      if (!adjacentEdges.add(edge)) {
        adjacentEdges.remove(edge);
        adjacentEdges.add(edge);
      }
      this.nodesToEdges.put(dataNode, adjacentEdges);
      String edgeId = edge.getId();
      this.flowEdgeMap.put(edgeId, edge);
      return true;
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  /**
   * Delete a {@link DataNode} by its identifier
   * @param nodeId identifier of the {@link DataNode} to be deleted.
   * @return true if {@link DataNode} is successfully deleted.
   */
  @Override
  public boolean deleteDataNode(String nodeId) {
    try {
      rwLock.writeLock().lock();
      return this.dataNodeMap.containsKey(nodeId) && deleteDataNode(this.dataNodeMap.get(nodeId));
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  /**
   * Delete a {@DataNode} from the {@link FlowGraph}.
   * @param node to be deleted.
   * @return true if {@link DataNode} is successfully deleted.
   */
  public boolean deleteDataNode(DataNode node) {
    try {
      rwLock.writeLock().lock();
      if (!dataNodeMap.containsKey(node.getId())) {
        return false;
      }
      //Delete node from dataNodeMap
      dataNodeMap.remove(node.getId());

      //Delete all the edges adjacent to the node. First, delete edges from flowEdgeMap and next, remove the edges
      // from nodesToEdges
      for (FlowEdge edge : nodesToEdges.get(node)) {
        flowEdgeMap.remove(edge.getId());
      }
      nodesToEdges.remove(node);
      return true;

    } finally {
      rwLock.writeLock().unlock();
    }
  }

  /**
   * Delete a {@link DataNode} by its identifier
   * @param edgeId identifier of the {@link FlowEdge} to be deleted.
   * @return true if {@link FlowEdge} is successfully deleted.
   */
  @Override
  public boolean deleteFlowEdge(String edgeId) {
    try {
      rwLock.writeLock().lock();
      return flowEdgeMap.containsKey(edgeId) && deleteFlowEdge(flowEdgeMap.get(edgeId));
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  /**
   * Delete a {@FlowEdge} from the {@link FlowGraph}.
   * @param edge to be deleted.
   * @return true if {@link FlowEdge} is successfully deleted. If the source of a {@link FlowEdge} does not exist or
   * if the {@link FlowEdge} is not in the graph, return false.
   */
  public boolean deleteFlowEdge(FlowEdge edge) {
    try {
      rwLock.writeLock().lock();
      if (!dataNodeMap.containsKey(edge.getSrc())) {
        return false;
      }
      DataNode node = dataNodeMap.get(edge.getSrc());
      if (!nodesToEdges.get(node).contains(edge)) {
        return false;
      }
      this.nodesToEdges.get(node).remove(edge);
      this.flowEdgeMap.remove(edge.getId());
      return true;
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  /**
   * Get the set of edges adjacent to a {@link DataNode}
   * @param nodeId identifier of the node
   * @return Set of {@link FlowEdge}s adjacent to the node.
   */
  @Override
  public Set<FlowEdge> getEdges(String nodeId) {
    try {
      rwLock.readLock().lock();
      DataNode dataNode = this.dataNodeMap.getOrDefault(nodeId, null);
      return getEdges(dataNode);
    } finally {
      rwLock.readLock().unlock();
    }
  }

  /**
   * Get the set of edges adjacent to a {@link DataNode}
   * @param node {@link DataNode}
   * @return Set of {@link FlowEdge}s adjacent to the node.
   */
  @Override
  public Set<FlowEdge> getEdges(DataNode node) {
    try {
      rwLock.readLock().lock();
      return (node != null) ? this.nodesToEdges.getOrDefault(node, null) : null;
    } finally {
      rwLock.readLock().unlock();
    }
  }

  /**{@inheritDoc}**/
  @Override
  public FlowGraphPath findPath(FlowSpec flowSpec)
      throws PathFinder.PathFinderException, ReflectiveOperationException {
    try {
      rwLock.readLock().lock();
      //Instantiate a PathFinder.
      Class pathFinderClass = Class.forName(
          ConfigUtils.getString(flowSpec.getConfig(), FlowGraphConfigurationKeys.FLOW_GRAPH_PATH_FINDER_CLASS,
              FlowGraphConfigurationKeys.DEFAULT_FLOW_GRAPH_PATH_FINDER_CLASS));
      PathFinder pathFinder =
          (PathFinder) GobblinConstructorUtils.invokeLongestConstructor(pathFinderClass, this, flowSpec,
              dataNodeAliasMap);
      return pathFinder.findPath();
    } finally {
      rwLock.readLock().unlock();
    }
  }
}
