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

import com.typesafe.config.Config;


/**
 * Representation of a node in the FlowGraph. Each node has a unique identifier and a list of outgoing
 * {@link FlowEdge}s adjacent to it.
 */
public interface DataNode {
  /**
   * @return The name of node.
   * It should be the identifier of a {@link DataNode}.
   */
  String getId();

  /**
   * @return The attributes of a {@link DataNode}.
   */
  Config getProps();

  /**
   * @return true if the {@link DataNode} is active
   */
  boolean isActive();

  /**
   * @return a list of {@link FlowEdge}s incident on the {@link DataNode}.
   */
  Collection<FlowEdge> getFlowEdges();

  /**
   * Add a {@link FlowEdge} to a node.
   */
  void addFlowEdge(FlowEdge edge);
}
