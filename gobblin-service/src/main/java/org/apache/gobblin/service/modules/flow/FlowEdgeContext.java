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

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.service.modules.dataset.DatasetDescriptor;
import org.apache.gobblin.service.modules.flowgraph.FlowEdge;


/**
 * A helper class used to maintain additional context associated with each {@link FlowEdge} during path
 * computation while the edge is explored for its eligibility. The additional context includes the input
 * {@link DatasetDescriptor} of this edge which is compatible with the previous {@link FlowEdge}'s output
 * {@link DatasetDescriptor} (where "previous" means the immediately preceding {@link FlowEdge} visited before
 * the current {@link FlowEdge}), and the corresponding output dataset descriptor of the current {@link FlowEdge}.
 */
@AllArgsConstructor
@EqualsAndHashCode(exclude = {"mergedConfig", "specExecutor"})
@Getter
public class FlowEdgeContext {
  private FlowEdge edge;
  private DatasetDescriptor inputDatasetDescriptor;
  private DatasetDescriptor outputDatasetDescriptor;
  private Config mergedConfig;
  private SpecExecutor specExecutor;

  @Override
  public String toString() {
    return edge == null ? "Null" : edge.toString();
  }
}