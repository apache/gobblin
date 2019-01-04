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

import java.net.URI;
import java.util.Map;

import org.apache.gobblin.instrumented.Instrumentable;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecCatalogListener;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;


/***
 * Take in a logical {@link Spec} and compile corresponding materialized {@link Spec}s
 * and the mapping to {@link SpecExecutor} that they can be run on.
 */
public interface SpecCompiler extends SpecCatalogListener, Instrumentable {
  /***
   * Take in a logical {@link Spec} and compile corresponding materialized {@link Spec}s
   * and the mapping to {@link SpecExecutor} that they can be run on.
   * All the specs generated from the compileFlow must have a
   * {@value org.apache.gobblin.configuration.ConfigurationKeys#FLOW_EXECUTION_ID_KEY}
   * @param spec {@link Spec} to compile.
   * @return Map of materialized physical {@link Spec} and {@link SpecExecutor}.
   */
  Dag<JobExecutionPlan> compileFlow(Spec spec);

  /***
   * Map of {@link Spec} URI and {@link TopologySpec} the {@link SpecCompiler}
   * is aware about.
   * @return Map of {@link Spec} URI and {@link TopologySpec}
   */
  Map<URI, TopologySpec> getTopologySpecMap();

  /**
   * Mark the {@link SpecCompiler} active/inactive. Useful to trigger the initialization of {@link SpecCompiler}, if
   * necessary, before it can start compiling {@link org.apache.gobblin.runtime.api.FlowSpec}s.
   * @param active
   */
  void setActive(boolean active);

  /**
   * Waits for the {@link SpecCompiler} to become healthy. A {@link SpecCompiler} is healthy when all the component
   * services it depends on have been successfully initialized. For instance, the {@link MultiHopFlowCompiler} is healthy
   * when the {@link org.apache.gobblin.service.modules.flowgraph.DataNode}s and {@link org.apache.gobblin.service.modules.flowgraph.FlowEdge}s
   * can be added to the {@link org.apache.gobblin.service.modules.flowgraph.FlowGraph}. The {@link org.apache.gobblin.service.modules.flowgraph.FlowEdge}
   * instantiation in turn depends on the successful initialization of {@link org.apache.gobblin.runtime.spec_catalog.TopologyCatalog}, which
   * instantiates all the configured {@link SpecExecutor}s.
   */
  public void awaitHealthy() throws InterruptedException;
}