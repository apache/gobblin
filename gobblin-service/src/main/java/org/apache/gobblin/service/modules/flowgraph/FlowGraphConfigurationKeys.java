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

public class FlowGraphConfigurationKeys {
  public static final String GOBBLIN_SERVICE_FLOWGRAPH_PREFIX = "gobblin.service.flowgraph.";

  /**
   *   {@link DataNode} configuration keys.
   */
  public static final String DATA_NODE_ID_KEY = GOBBLIN_SERVICE_FLOWGRAPH_PREFIX + "node.id";
  public static final String DATA_NODE_IS_ACTIVE_KEY = GOBBLIN_SERVICE_FLOWGRAPH_PREFIX + "node.isActive";

  /**
   * {@link FlowEdge} configuration keys.
   */
  public static final String FLOW_EDGE_END_POINTS_KEY = GOBBLIN_SERVICE_FLOWGRAPH_PREFIX + "edge.endPoints";
  public static final String FLOW_EDGE_IS_ACTIVE_KEY = GOBBLIN_SERVICE_FLOWGRAPH_PREFIX + "edge.isActive";
  public static final String FLOW_EDGE_TEMPLATE_URI_KEY = GOBBLIN_SERVICE_FLOWGRAPH_PREFIX + "edge.flowTemplateUri";
  public static final String FLOW_EDGE_SPEC_EXECUTORS_KEY = GOBBLIN_SERVICE_FLOWGRAPH_PREFIX + "edge.specExecutors";
  public static final String FLOW_EDGE_SPEC_EXECUTOR_CLASS_KEY = "specExecutorClass";
}
