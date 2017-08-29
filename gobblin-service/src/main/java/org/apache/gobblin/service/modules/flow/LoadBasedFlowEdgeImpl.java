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

import com.typesafe.config.Config;

import org.jgrapht.graph.DefaultWeightedEdge;
import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.api.FlowEdge;
import org.apache.gobblin.runtime.api.ServiceNode;
import org.apache.gobblin.runtime.api.SpecExecutor;

import lombok.Getter;

/**
 * A base implementation of a flowEdge in the weight multi-edge graph.
 * For a weightedMultiGraph there could be multiple edges between two vertices.
 * Recall that a triplet of <SourceNode, targetNode, specExecutor> determines one edge.
 * It is expected that {@link org.jgrapht.graph.DirectedWeightedMultigraph#getAllEdges(Object, Object)}
 * can return multiple edges with the same pair of source and destination but different SpecExecutor.
 *
 * Each edge has a {@FlowEdgeProp} which is used to evaluate a edge.
 *
 * The {@link LoadBasedFlowEdgeImpl} exposes interface to getLoad of an edge directly.
 * In terms of implementation details,
 * Load of edge is equivalent to weight defined in {@link DefaultWeightedEdge}.
 * Since {@link #getWeight()} method is protected, {@link #getEdgeLoad()} will return the load.
 *
 * There's no setLoad, which is logically supposed to happen by invoking
 * {@link org.jgrapht.graph.DirectedWeightedMultigraph#setEdgeWeight(Object, double)}.
 *

 */
@Alpha
public class LoadBasedFlowEdgeImpl extends DefaultWeightedEdge implements FlowEdge {
  @Getter
  private ServiceNode sourceNode;
  @Getter
  private ServiceNode targetNode;
  @Getter
  private SpecExecutor specExecutorInstance;

  private final FlowEdgeProps flowEdgeProps;

  /**
   * {@link #edgeSecurity}'s initial value comes from {@link FlowEdgeProps},
   * and can be overrided by {@link #setEdgeSecurity(boolean)} and
   * {@link org.jgrapht.graph.DirectedWeightedMultigraph#setEdgeWeight(Object, double)} method afterwards.
   */
  private boolean edgeSecurity;


  public LoadBasedFlowEdgeImpl(ServiceNode sourceNode, ServiceNode targetNode,
      FlowEdgeProps flowEdgeProps, SpecExecutor specExecutorInstance) {
    this.sourceNode = sourceNode;
    this.targetNode = targetNode;
    this.flowEdgeProps = flowEdgeProps;
    this.specExecutorInstance = specExecutorInstance;
    this.edgeSecurity = flowEdgeProps.getInitialEdgeSafety();
  }

  public LoadBasedFlowEdgeImpl(ServiceNode sourceNode, ServiceNode targetNode,
      SpecExecutor specExecutor) {
    this(sourceNode, targetNode, new FlowEdgeProps(specExecutor.getAttrs()),
        specExecutor);
  }

  public double getEdgeLoad() {
    return getWeight();
  }

  @Override
  public String getEdgeIdentity() {
    return this.calculateEdgeIdentity(this.sourceNode, this.targetNode, this.specExecutorInstance);
  }

  /**
   * Since {@link #flowEdgeProps} is supposed to be read-only once instantiated, set method here
   * will overwrite the value in {@link #edgeSecurity}.
   */
  public void setEdgeSecurity(boolean security) {
    this.edgeSecurity = security;
  }

  @Override
  public Config getEdgeProperties() {
    return this.flowEdgeProps.getConfig();
  }

  @Override
  /**
   * Naive implementation for here.
   */
  public boolean isEdgeEnabled() {
    return this.edgeSecurity;
  }

  /**
   * A naive implementation of edge identity calculation.
   * @return
   */
  public static String calculateEdgeIdentity(ServiceNode sourceNode, ServiceNode targetNode, SpecExecutor specExecutorInstance){
    return sourceNode.getNodeName() + "-" + specExecutorInstance.getUri() + "-" + targetNode.getNodeName();
  }

  /**
   * Recall that we need a triplet to uniquely define a {@link FlowEdge}:
   * - {@link ServiceNode} sourceNode
   * - {@link ServiceNode} targetNode
   * - {@link SpecExecutor} SpecExecutor
   *
   * We DO NOT distinguish between two edges by other props like weight,
   * as the load should be an attribute of an edge.
   * These are IntelliJ-generated methods for equals and hashCode().
   *
   * @param o The object that being compared
   * @return If two {@link LoadBasedFlowEdgeImpl} are equivalent.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LoadBasedFlowEdgeImpl that = (LoadBasedFlowEdgeImpl) o;

    if (!sourceNode.equals(that.sourceNode)) {
      return false;
    }
    if (!targetNode.equals(that.targetNode)) {
      return false;
    }
    return specExecutorInstance.equals(that.specExecutorInstance);
  }

  @Override
  public int hashCode() {
    int result = sourceNode.hashCode();
    result = 31 * result + targetNode.hashCode();
    result = 31 * result + specExecutorInstance.hashCode();
    return result;
  }
}