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

import java.util.Collection;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.service.modules.flow.FlowGraphPath;
import org.apache.gobblin.service.modules.flowgraph.pathfinder.PathFinder;


/**
 * An interface for {@link FlowGraph}. A {@link FlowGraph} consists of {@link DataNode}s and {@link FlowEdge}s.
 * The interface provides methods for adding and removing {@link DataNode}s and {@link FlowEdge}s to the {@link FlowGraph}.
 * In addition the interface provides methods to return factory classes for creation of {@link DataNode}s and {@link FlowEdge}s.
 */

@Alpha
public interface FlowGraph {

  /**
   * Get a {@link DataNode} from the node identifier
   * @param nodeId {@link DataNode} identifier.
   * @return the {@link DataNode} object if the node is present in the {@link FlowGraph}.
   */
  DataNode getNode(String nodeId);

  /**
   * Add a {@link DataNode} to the {@link FlowGraph}
   * @param node {@link DataNode} to be added
   * @return true if {@link DataNode} is added to the {@link FlowGraph} successfully.
   */
  boolean addDataNode(DataNode node);

  /**
   * Add a {@link FlowEdge} to the {@link FlowGraph}
   * @param edge {@link FlowEdge} to be added
   * @return true if {@link FlowEdge} is added to the {@link FlowGraph} successfully.
   */
  boolean addFlowEdge(FlowEdge edge);

  /**
   * Remove a {@link DataNode} and all its incident edges from the {@link FlowGraph}
   * @param nodeId identifier of the {@link DataNode} to be removed
   * @return true if {@link DataNode} is removed from the {@link FlowGraph} successfully.
   */
  boolean deleteDataNode(String nodeId);

  /**
   * Remove a {@link FlowEdge} from the {@link FlowGraph}
   * @param edgeId label of the edge to be removed
   * @return true if edge is removed from the {@link FlowGraph} successfully.
   */
  boolean deleteFlowEdge(String edgeId);

  /**
   * Get a collection of edges adjacent to a {@link DataNode}. Useful for path finding algorithms and graph
   * traversal algorithms such as Djikstra's shortest-path algorithm, BFS
   * @param nodeId identifier of the {@link DataNode}
   * @return a collection of edges adjacent to the {@link DataNode}
   */
  Collection<FlowEdge> getEdges(String nodeId);

  /**
   * Get a collection of edges adjacent to a {@link DataNode}.
   * @param node {@link DataNode}
   * @return a collection of edges adjacent to the {@link DataNode}
   */
  Collection<FlowEdge> getEdges(DataNode node);

  /**
   * A method that takes a {@link FlowSpec} containing the source and destination {@link DataNode}s, as well as the
   * source and target {@link org.apache.gobblin.service.modules.dataset.DatasetDescriptor}s, and returns a sequence
   * of fully resolved {@link org.apache.gobblin.runtime.api.JobSpec}s that will move the source dataset
   * from the source datanode, perform any necessary transformations and land the dataset at the destination node
   * in the format described by the target {@link org.apache.gobblin.service.modules.dataset.DatasetDescriptor}.
   *
   * @param flowSpec a {@link org.apache.gobblin.runtime.api.Spec} containing a high-level description of input flow.
   * @return an instance of {@link FlowGraphPath} that encapsulates a sequence of {@link org.apache.gobblin.runtime.api.JobSpec}s
   * satisfying flowSpec.
   */
  FlowGraphPath findPath(FlowSpec flowSpec)
      throws PathFinder.PathFinderException, ReflectiveOperationException;
}
