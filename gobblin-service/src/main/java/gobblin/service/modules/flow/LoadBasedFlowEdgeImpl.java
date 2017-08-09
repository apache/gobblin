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

package gobblin.service.modules.flow;

import java.util.Properties;

import org.jgrapht.graph.DefaultWeightedEdge;

import gobblin.util.ConfigUtils;
import gobblin.annotation.Alpha;
import gobblin.runtime.api.FlowEdge;
import gobblin.runtime.api.ServiceNode;
import gobblin.runtime.api.SpecExecutor;

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
 * The {@link LoadBasedFlowEdgeImpl} exposes additional interface to get/set load to edge directly.
 * Load of edge is equivalent to weight defined in {@link DefaultWeightedEdge}.
 * Since {@link #getWeight()} method is protected, {@link #getEdgeLoad()} will return the load.
 * It is preferable to use {@link org.jgrapht.graph.DirectedWeightedMultigraph#setEdgeWeight(Object, double)}
 * as the setter, since weight modification is semantically issued by entities who has access to
 * {@link org.jgrapht.graph.DirectedWeightedMultigraph} object.
 */
@Alpha
public class LoadBasedFlowEdgeImpl extends DefaultWeightedEdge implements FlowEdge {
  @Getter
  private ServiceNode sourceNode;
  @Getter
  private ServiceNode targetNode;
  @Getter
  private SpecExecutor specExecutorInstance;

  private final FlowEdgeProps _flowEdgeProps;

  /**
   * {@link #edgeSecurity}'s value comes from {@link FlowEdgeProps} initially
   * and can be overrided by {@link #setEdgeSecurity(boolean)} and
   * {@link org.jgrapht.graph.DirectedWeightedMultigraph#setEdgeWeight(Object, double)} method afterwards.
   */
  private boolean edgeSecurity;


  public LoadBasedFlowEdgeImpl(ServiceNode sourceNode, ServiceNode targetNode,
      FlowEdgeProps flowEdgeProps, SpecExecutor specExecutorInstance) {
    this.sourceNode = sourceNode ;
    this.targetNode = targetNode ;
    this._flowEdgeProps = flowEdgeProps;
    this.specExecutorInstance = specExecutorInstance;
    this.edgeSecurity = flowEdgeProps.getInitialEdgeSafety();
  }

  public LoadBasedFlowEdgeImpl(ServiceNode sourceNode, ServiceNode targetNode,
      SpecExecutor specExecutor){
    this(sourceNode, targetNode, new FlowEdgeProps(ConfigUtils.configToProperties(specExecutor.getAttrs())),
        specExecutor);
  }

  public double getEdgeLoad(){
    return getWeight();
  }

  @Override
  public String getEdgeIdentity(){
    return this.calculateEdgeIdentity(this.sourceNode, this.targetNode, this.specExecutorInstance);
  }

  /**
   * Since {@link #_flowEdgeProps} is supposed to be read-only once instantiated, set method here
   * will overwrite the value in {@link #edgeSecurity}.
   */
  public void setEdgeSecurity(boolean security) {
    this.edgeSecurity = security;
  }

  @Override
  public Properties getEdgeProperties() {
    return this._flowEdgeProps.getProperties();
  }

  @Override
  /**
   * Naive implementation for here.
   */
  public boolean isEdgeValid() {
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
