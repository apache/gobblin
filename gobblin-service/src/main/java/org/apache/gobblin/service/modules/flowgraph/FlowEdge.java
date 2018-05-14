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

import java.util.List;

import com.typesafe.config.Config;

import org.apache.gobblin.annotation.Alpha;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.gobblin.service.modules.template.FlowTemplate;
import org.apache.gobblin.runtime.api.SpecExecutor;


/**
 * Representation of an edge in a FlowGraph. Each {@link FlowEdge} encapsulates:
 * <p><ul>
 *   <li> two {@link DataNode}s as its end points
 *   <li>a {@FlowTemplate} that responsible for data movement between the {@DataNode}s.
 *   <li> a list of {@link SpecExecutor}s where the {@link FlowTemplate} can be executed.
 * </ul></p> and
 *
 */
@Alpha
public interface FlowEdge {
  /**
   *
   * @return the {@link DataNode} ids that are the end points of the edge.
   */
  List<String> getEndPoints();

  /**
   *
   * @return the {@link FlowTemplate} that performs the data movement along the edge.
   */
  FlowTemplate getFlowTemplate();

  /**
   *
   * @return a list of {@link SpecExecutor}s that can execute the {@link FlowTemplate} corresponding to this edge.
   */
  List<SpecExecutor> getExecutors();

  /**
   * Get properties of this edge.
   * @return the properties of this edge as a {@link Config} object.
   */
  Config getProps();

  /**
   *
   * @return true if the {@link FlowEdge} is active.
   */
  boolean isActive();

  /**
   *
   * @param user
   * @return true if the user has ACL permissions to access the {@link FlowEdge},
   */
  boolean isAccessible(UserGroupInformation user);
}
