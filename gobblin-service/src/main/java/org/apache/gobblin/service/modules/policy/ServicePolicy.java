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

import java.util.Set;
import org.apache.gobblin.runtime.api.FlowEdge;
import org.apache.gobblin.runtime.api.ServiceNode;
import org.jgrapht.graph.DirectedWeightedMultigraph;


/**
 * ServicePolicy will be firstly checked before the compilation happen.
 * unexpcted edges will not be considered in compilation process.
 */
public interface ServicePolicy {

  /**
   * After initialization of {@link ServicePolicy}, the populating method need to invoked before
   * {@link #getBlacklistedEdges()} can return the expected result.
   *
   * This requirement exists because when {@link ServicePolicy} is initialized it is not necessary that a
   * {@link org.jgrapht.graph.WeightedMultigraph} has been constructed, neither we cannot know if user-specified edges
   * and nodes exist in {@link org.jgrapht.graph.WeightedMultigraph}.
   * The population of blacklisted Edges make sense after graph has been constructed.
   */
  public void populateBlackListedEdges(DirectedWeightedMultigraph<ServiceNode, FlowEdge> graph);

  /**
   * Should return all edges that being blacklisted by this policy.
   */
  public Set<FlowEdge> getBlacklistedEdges();

  public void addServiceNode(ServiceNode serviceNode);

  public void addFlowEdge(FlowEdge flowEdge);
}
