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

import java.io.IOException;
import java.util.Properties;

import org.apache.gobblin.service.modules.template_catalog.FSFlowCatalog;

import com.typesafe.config.Config;


public interface FlowEdgeFactory {
  /**
   * Construct a {@link FlowEdge} from the edge properties
   * @param edgeProps properties of the {@link FlowEdge}
   * @param catalog an instance of {@link FSFlowCatalog} that returns {@link org.apache.gobblin.service.modules.template.FlowTemplate}s
   *               useful for creating a {@link FlowEdge}.
   * @return an instance of {@link FlowEdge}
   * @throws FlowEdgeCreationException
   */
  public FlowEdge createFlowEdge(Config edgeProps, FSFlowCatalog catalog) throws FlowEdgeCreationException;

  /**
   * Get an edge label from the edge properties
   * @param edgeProps properties of the edge
   * @return a string label identifying the edge
   */
  public String getEdgeId(Config edgeProps) throws IOException;

  public class FlowEdgeCreationException extends Exception {
    private static final String MESSAGE_FORMAT = "Failed to create FlowEdge because of: %s";

    public FlowEdgeCreationException(Exception e) {
      super(String.format(MESSAGE_FORMAT, e.getMessage()), e);
    }
  }
}
