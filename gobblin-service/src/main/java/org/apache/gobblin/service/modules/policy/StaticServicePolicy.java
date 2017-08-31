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

package org.apache.gobblin.service.modules.policy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;

import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.runtime.api.FlowEdge;
import org.apache.gobblin.runtime.api.ServiceNode;
import org.jgrapht.graph.DirectedWeightedMultigraph;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Defines {@link ServiceNode}s or {@link FlowEdge}s that should be blacklisted from the Flow-compiler process,
 * obtained only from configuration, which is the reason it is named as static.
 *
 * TODO: DynamicServicePolicy can obtain new blacklist candidate through Flow monitoring process, which is responsible
 * for monitoring the flow execution and react accordingly.
 *
 * Either user specify {@link ServiceNode} or {@link FlowEdge} to blacklist will all end up with a list of
 * {@link FlowEdge}s that won't be considered when selecting path for data transformation.
 */
@Slf4j
@Alias("static")
public class StaticServicePolicy implements ServicePolicy {

  @Getter
  Set<FlowEdge> blacklistedEdges;

  List<ServiceNode> serviceNodes;
  List<FlowEdge> flowEdges;

  public StaticServicePolicy() {
    serviceNodes = new ArrayList<>();
    flowEdges = new ArrayList<>();
    blacklistedEdges = new HashSet<>();
  }

  public StaticServicePolicy(List<ServiceNode> serviceNodes, List<FlowEdge> flowEdges) {
    Preconditions.checkNotNull(serviceNodes);
    Preconditions.checkNotNull(flowEdges);
    blacklistedEdges = new HashSet<>();
    this.serviceNodes = serviceNodes;
    this.flowEdges = flowEdges;
  }

  public void addServiceNode(ServiceNode serviceNode) {
    this.serviceNodes.add(serviceNode);
  }

  public void addFlowEdge(FlowEdge flowEdge){
    this.flowEdges.add(flowEdge);
  }

  @Override
  public void populateBlackListedEdges(DirectedWeightedMultigraph<ServiceNode, FlowEdge> graph) {
    for (ServiceNode node: serviceNodes) {
      if (graph.containsVertex(node)) {
        blacklistedEdges.addAll(graph.incomingEdgesOf(node));
        blacklistedEdges.addAll(graph.outgoingEdgesOf(node));
      } else {
        log.info("The graph " + graph + " doesn't contains node " + node.toString());
      }
    }

    for( FlowEdge flowEdge: flowEdges) {
      if (graph.containsEdge(flowEdge)) {
        blacklistedEdges.add(flowEdge);
      } else {
        log.info("The graph " + graph + "doesn't contains edge " + flowEdge.toString());
      }
    }
  }
}
