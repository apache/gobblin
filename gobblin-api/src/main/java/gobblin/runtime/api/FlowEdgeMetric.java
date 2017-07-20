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

package gobblin.runtime.api;

public interface FlowEdgeMetric<T> {
  /**
   * @return An abstraction of instance/cluster load of a specExecutor, will be considered as important reference for
   * edge weight in graph.
   */
  double getFlowEdgeLoad();

  void setFlowEdgeLoad(double load);

  /**
   * Once there are multiple metrics to evaluate a FlowEdge being defined,
   * there should be a way to combine different metrics to evaluate a edge.
   * @return The combined result of all metrics to be considered for evaluating a {@link FlowEdge}
   */
  T getCombinedMetric();
}
