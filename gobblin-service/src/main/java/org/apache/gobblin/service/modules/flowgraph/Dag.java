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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;

import static org.apache.gobblin.service.modules.orchestration.DagActionStore.NO_JOB_NAME_DEFAULT;


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
  private Map<DagNode<T>, List<DagNode<T>>> parentChildMap;
  private final List<DagNode<T>> nodes;

  @Setter
  @Deprecated // because this field is not persisted in mysql and contains information in very limited cases
  private String message;
  @Setter
  @Deprecated // because this field is not persisted in mysql and contains information in very limited cases
  private String flowEvent;
  // Keep track of when the final flow status is emitted, in milliseconds to avoid many duplicate events
  @Setter @Getter
  @Deprecated // because this field is not persisted in mysql and contains information in very limited cases
  private long eventEmittedTimeMillis = -1;

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
    for (DagNode<T> node : this.nodes) {
      //If a Node has no parent Node, add it to the list of start Nodes
      if (node.getParentNodes() == null) {
        this.startNodes.add(node);
      } else {
        List<DagNode<T>> parentNodeList = node.getParentNodes();
        for (DagNode<T> parentNode : parentNodeList) {
          if (parentChildMap.containsKey(parentNode)) {
            parentChildMap.get(parentNode).add(node);
          } else {
            parentChildMap.put(parentNode, Lists.newArrayList(node));
          }
        }
      }
    }
    //Iterate over all the Nodes and add a Node to the list of endNodes if it is not present in the parentChildMap
    for (DagNode<T> node : this.nodes) {
      if (!parentChildMap.containsKey(node)) {
        this.endNodes.add(node);
      }
    }
  }

  public List<DagNode<T>> getChildren(DagNode<T> node) {
    return parentChildMap.getOrDefault(node, Collections.emptyList());
  }

  public List<DagNode<T>> getParents(DagNode<T> node) {
    return (node.parentNodes != null) ? node.parentNodes : Collections.emptyList();
  }

  /**
   * Get the ancestors of a given set of {@link DagNode}s in the {@link Dag}.
   * @param dagNodes set of nodes in the {@link Dag}.
   * @return the union of all ancestors of dagNodes in the dag.
   */
  private Set<DagNode<T>> getAncestorNodes(Set<DagNode<T>> dagNodes) {
    Set<DagNode<T>> ancestorNodes = new HashSet<>();
    for (DagNode<T> dagNode : dagNodes) {
      LinkedList<DagNode<T>> nodesToExpand = Lists.newLinkedList(this.getParents(dagNode));
      while (!nodesToExpand.isEmpty()) {
        DagNode<T> nextNode = nodesToExpand.poll();
        ancestorNodes.add(nextNode);
        nodesToExpand.addAll(this.getParents(nextNode));
      }
    }
    return ancestorNodes;
  }

  /**
   * This method computes a set of {@link DagNode}s which are the dependency nodes for concatenating this {@link Dag}
   * with any other {@link Dag}. The set of dependency nodes is the union of:
   * <p><ul>
   *   <li> The endNodes of this dag which are not forkable, and </li>
   *   <li> The parents of forkable nodes, such that no parent is an ancestor of another parent.</li>
   * </ul></p>
   *
   * @param forkNodes set of nodes of this {@link Dag} which are forkable
   * @return set of dependency nodes of this dag for concatenation with any other dag.
   */
  public Set<DagNode<T>> getDependencyNodes(Set<DagNode<T>> forkNodes) {
    Set<DagNode<T>> dependencyNodes = new HashSet<>();
    for (DagNode<T> endNode : endNodes) {
      if (!forkNodes.contains(endNode)) {
        dependencyNodes.add(endNode);
      }
    }

    //Get all ancestors of non-forkable nodes
    Set<DagNode<T>> ancestorNodes = this.getAncestorNodes(dependencyNodes);

    //Add ancestors of the parents of forkable nodes
    for (DagNode<T> dagNode: forkNodes) {
      List<DagNode<T>> parentNodes = this.getParents(dagNode);
      ancestorNodes.addAll(this.getAncestorNodes(Sets.newHashSet(parentNodes)));
    }

    for (DagNode<T> dagNode: forkNodes) {
      List<DagNode<T>> parentNodes = this.getParents(dagNode);
      for (DagNode<T> parentNode : parentNodes) {
        //Add parent node of a forkable node as a dependency, only if it is not already an ancestor of another
        // dependency.
        if (!ancestorNodes.contains(parentNode)) {
          dependencyNodes.add(parentNode);
        }
      }
    }
    return dependencyNodes;
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
    return concatenate(other, new HashSet<>());
  }

  /**
   * Concatenate two dags together. Join the "other" dag to "this" dag and return "this" dag.
   * The concatenate method ensures that all the jobs of "this" dag (which may have multiple end nodes)
   * are completed before starting any job of the "other" dag. This is done by adding each endNode of this dag, which is
   * not a fork node, as a parent of every startNode of the other dag.
   *
   * @param other dag to concatenate to this dag
   * @param forkNodes a set of nodes from this dag which are marked as forkable nodes. Each of these nodes will be added
   *                  to the list of end nodes of the concatenated dag. Essentially, a forkable node has no dependents
   *                  in the concatenated dag.
   * @return the concatenated dag
   */
  public Dag<T> concatenate(Dag<T> other, Set<DagNode<T>> forkNodes) {
    if (other == null || other.isEmpty()) {
      return this;
    }
    if (this.isEmpty()) {
      return other;
    }

    for (DagNode<T> node : getDependencyNodes(forkNodes)) {
      if (!this.parentChildMap.containsKey(node)) {
        this.parentChildMap.put(node, Lists.newArrayList());
      }
      for (DagNode<T> otherNode : other.startNodes) {
        this.parentChildMap.get(node).add(otherNode);
        otherNode.addParentNode(node);
      }
    }
    //Each node which is a forkable node is added to list of end nodes of the concatenated dag
    other.endNodes.addAll(forkNodes);
    this.endNodes = other.endNodes;

    //Append all the entries from the other dag's parentChildMap to this dag's parentChildMap
    this.parentChildMap.putAll(other.parentChildMap);

    //If there exists a node in the other dag with no parent nodes, add it to the list of start nodes of the
    // concatenated dag.
    other.startNodes.stream().filter(node -> other.getParents(node).isEmpty())
        .forEach(node -> this.startNodes.add(node));

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
    this.parentChildMap.putAll(other.parentChildMap);
    //Append the startNodes, endNodes and nodes from the other dag to this dag.
    this.startNodes.addAll(other.startNodes);
    this.endNodes.addAll(other.endNodes);
    this.nodes.addAll(other.nodes);
    return this;
  }

  /**
   * DagNode is essentially a job within a Dag, usually they are used interchangeably.
   */
  @Getter
  public static class DagNode<T> {
    private final T value;
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
      return this.getValue().equals(that.getValue());
    }

    @Override
    public int hashCode() {
      return this.getValue().hashCode();
    }
  }

  /**
   * @return A string representation of the Dag as a JSON Array.
   */
  @Override
  public String toString() {
    return this.getNodes().stream().map(node -> node.getValue().toString()).collect(Collectors.toList()).toString();
  }

  public enum FlowState {
    FAILED(-1),
    RUNNING(0),
    SUCCESSFUL(1);

    public final int value;

    FlowState(int value) {
      this.value = value;
    }
  }

  @Getter
  @EqualsAndHashCode
  public static class DagId {
    String flowGroup;
    String flowName;
    long flowExecutionId;

    public DagId(String flowGroup, String flowName, long flowExecutionId) {
      this.flowGroup = flowGroup;
      this.flowName = flowName;
      this.flowExecutionId = flowExecutionId;
    }

    @Override
    public String toString() {
      return Joiner.on("_").join(flowGroup, flowName, flowExecutionId);
    }

    DagActionStore.DagAction toDagAction(DagActionStore.DagActionType actionType) {
      return new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, NO_JOB_NAME_DEFAULT, actionType);
    }

    public FlowId getFlowId() {
      return new FlowId().setFlowGroup(this.flowGroup).setFlowName(this.flowName);
    }
  }
}
