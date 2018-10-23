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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

import lombok.Getter;

import org.apache.gobblin.annotation.Alpha;


/**
 * An implementation of Dag. Assumes that nodes have unique values. Nodes with duplicate values will produce
 * unpredictable behavior.
 */
@Alpha
@Getter
public class Dag<T> {
  private List<DagNode<T>> startNodes;
  private List<DagNode<T>> endNodes;
  // Map to maintain parent to children mapping.
  private Map<DagNode, List<DagNode<T>>> parentChildMap;
  private List<DagNode<T>> nodes;

  public Dag(List<DagNode<T>> dagNodes) {
    this.nodes = dagNodes;
    //Build dag
    this.build();
  }

  /**
   * Constructs the dag from the Node list.
   */
  private void build() {
    this.startNodes = new ArrayList<>();
    this.endNodes = new ArrayList<>();
    this.parentChildMap = new HashMap<>();
    for (DagNode node : this.nodes) {
      //If a Node has no parent Node, add it to the list of start Nodes
      if (node.getParentNodes() == null) {
        this.startNodes.add(node);
      } else {
        List<DagNode> parentNodeList = node.getParentNodes();
        for (DagNode parentNode : parentNodeList) {
          if (parentChildMap.containsKey(parentNode)) {
            parentChildMap.get(parentNode).add(node);
          } else {
            parentChildMap.put(parentNode, Lists.newArrayList(node));
          }
        }
      }
    }
    //Iterate over all the Nodes and add a Node to the list of endNodes if it is not present in the parentChildMap
    for (DagNode node : this.nodes) {
      if (!parentChildMap.containsKey(node)) {
        this.endNodes.add(node);
      }
    }
  }

  public List<DagNode<T>> getChildren(DagNode node) {
    return parentChildMap.getOrDefault(node, Collections.EMPTY_LIST);
  }

  public List<DagNode<T>> getParents(DagNode node) {
    return (node.parentNodes != null)? node.parentNodes : Collections.EMPTY_LIST;
  }

  public boolean isEmpty() {
    return this.nodes.isEmpty();
  }

  /**
   * Concatenate two dags together. Join the "other" dag to "this" dag and return "this" dag.
   * The concatenate method ensures that all the jobs of "this" dag (which may have multiple end nodes)
   * are completed before starting any job of the "other" dag. This is done by adding each endNode of this dag as
   * a parent of every startNode of the other dag.
   *
   * @param other dag to concatenate to this dag
   * @return the concatenated dag
   */
  public Dag<T> concatenate(Dag<T> other) {
    if (other == null || other.isEmpty()) {
      return this;
    }
    if (this.isEmpty()) {
      return other;
    }
    for (DagNode node : this.endNodes) {
      this.parentChildMap.put(node, Lists.newArrayList());
      for (DagNode otherNode : other.startNodes) {
        this.parentChildMap.get(node).add(otherNode);
        otherNode.addParentNode(node);
      }
      this.endNodes = other.endNodes;
    }
    //Append all the entries from the other dag's parentChildMap to this dag's parentChildMap
    for (Map.Entry<DagNode, List<DagNode<T>>> entry: other.parentChildMap.entrySet()) {
      this.parentChildMap.put(entry.getKey(), entry.getValue());
    }
    this.nodes.addAll(other.nodes);
    return this;
  }

  /**
   * Merge the "other" dag to "this" dag and return "this" dag as a forest of the two dags.
   * More specifically, the merge() operation takes two dags and returns a disjoint union of the two dags.
   *
   * @param other dag to merge to this dag
   * @return the disjoint union of the two dags
   */

  public Dag<T> merge(Dag<T> other) {
    if (other == null || other.isEmpty()) {
      return this;
    }
    if (this.isEmpty()) {
      return other;
    }
    //Append all the entries from the other dag's parentChildMap to this dag's parentChildMap
    for (Map.Entry<DagNode, List<DagNode<T>>> entry: other.parentChildMap.entrySet()) {
      this.parentChildMap.put(entry.getKey(), entry.getValue());
    }
    //Append the startNodes, endNodes and nodes from the other dag to this dag.
    this.startNodes.addAll(other.startNodes);
    this.endNodes.addAll(other.endNodes);
    this.nodes.addAll(other.nodes);
    return this;
  }

  @Getter
  public static class DagNode<T> {
    private T value;
    //List of parent Nodes that are dependencies of this Node.
    private List<DagNode<T>> parentNodes;

    //Constructor
    public DagNode(T value) {
      this.value = value;
    }

    public void addParentNode(DagNode<T> node) {
      if (parentNodes == null) {
        parentNodes = Lists.newArrayList(node);
        return;
      }
      parentNodes.add(node);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DagNode that = (DagNode) o;
      if (!this.getValue().equals(that.getValue())) {
        return false;
      }
      return true;
    }

    @Override
    public int hashCode() {
      return this.getValue().hashCode();
    }
  }
}
