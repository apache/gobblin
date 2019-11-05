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
  public static final String DATA_NODE_PREFIX = "data.node.";
  public static final String FLOW_EDGE_PREFIX = "flow.edge.";
  public static final String FLOW_GRAPH_PREFIX = "flow.graph.";
  /**
   *   {@link DataNode} related configuration keys.
   */
  public static final String DATA_NODE_CLASS = DATA_NODE_PREFIX + "class";
  public static final String DEFAULT_DATA_NODE_CLASS = "org.apache.gobblin.service.modules.flowgraph.BaseDataNode";
  public static final String DATA_NODE_ID_KEY = DATA_NODE_PREFIX + "id";
  public static final String DATA_NODE_IS_ACTIVE_KEY = DATA_NODE_PREFIX + "isActive";

  /**
   *   {@link org.apache.gobblin.service.modules.flowgraph.datanodes.HttpDataNode} related configuration keys.
   */
  public static final String DATA_NODE_HTTP_DOMAIN_KEY = DATA_NODE_PREFIX + "http.domain";
  public static final String DATA_NODE_HTTP_AUTHENTICATION_TYPE_KEY = DATA_NODE_PREFIX + "http.authentication.type";

  /**
   * {@link FlowEdge} related configuration keys.
   */
  public static final String FLOW_EDGE_FACTORY_CLASS = FLOW_EDGE_PREFIX + "factory.class";
  public static final String DEFAULT_FLOW_EDGE_FACTORY_CLASS = "org.apache.gobblin.service.modules.flowgraph.BaseFlowEdge$Factory";
  public static final String FLOW_EDGE_SOURCE_KEY = FLOW_EDGE_PREFIX + "source";
  public static final String FLOW_EDGE_DESTINATION_KEY = FLOW_EDGE_PREFIX + "destination";
  public static final String FLOW_EDGE_ID_KEY = FLOW_EDGE_PREFIX + "id";
  public static final String FLOW_EDGE_NAME_KEY = FLOW_EDGE_PREFIX + "name";
  public static final String FLOW_EDGE_IS_ACTIVE_KEY = FLOW_EDGE_PREFIX + "isActive";
  public static final String FLOW_EDGE_TEMPLATE_DIR_URI_KEY = FLOW_EDGE_PREFIX + "flowTemplateDirUri";
  public static final String FLOW_EDGE_SPEC_EXECUTORS_KEY = FLOW_EDGE_PREFIX + "specExecutors";
  public static final String FLOW_EDGE_SPEC_EXECUTOR_CLASS_KEY = "specExecInstance.class";

  /**
   * {@link org.apache.gobblin.service.modules.flowgraph.pathfinder.PathFinder} related configuration keys.
   */
  public static final String FLOW_GRAPH_PATH_FINDER_CLASS = FLOW_GRAPH_PREFIX + "pathfinder.class";
  public static final String DEFAULT_FLOW_GRAPH_PATH_FINDER_CLASS = "org.apache.gobblin.service.modules.flowgraph.pathfinder.BFSPathFinder";
}
