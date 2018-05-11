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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;

import lombok.Getter;

public class BaseFlowGraph implements FlowGraph {
  @Getter
  private Set<DataNode> nodes;

  private Map<String, DataNode> dataNodeMap;

  public DataNode getNode(String id) {
    if (dataNodeMap != null) {
      return this.dataNodeMap.getOrDefault(id, null);
    }
    return null;
  }

  @Override
  public void addNode(DataNode node) {
    if (this.nodes != null) {
      this.nodes = new HashSet<>();
    }
    this.nodes.add(node);
    this.dataNodeMap.put(node.getId(), node);
  }

  @Override
  public void addEdge(FlowEdge edge) {
    String srcNode = edge.getEndPoints().get(0);
    String dstNode = edge.getEndPoints().get(1);

    Preconditions
        .checkArgument(dataNodeMap.containsKey(srcNode), "Src node " + srcNode + " not present in the FlowGraph");
    Preconditions
        .checkArgument(dataNodeMap.containsKey(dstNode), "Dst node " + dstNode + " not present in the FlowGraph");

    DataNode dataNode = dataNodeMap.get(srcNode);
    dataNode.addFlowEdge(edge);
  }
}
