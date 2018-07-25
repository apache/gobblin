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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.core.GitFlowGraphMonitor;
import org.apache.gobblin.service.modules.flowgraph.BaseFlowGraph;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.flowgraph.FlowGraph;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.template_catalog.FSFlowCatalog;


/***
 * Take in a logical {@link Spec} ie flow and compile corresponding materialized job {@link Spec}
 * and its mapping to {@link SpecExecutor}.
 */
@Alpha
@Slf4j
public class MultiHopFlowCompiler extends BaseFlowToJobSpecCompiler {
  @Getter
  private FlowGraph flowGraph;
  private GitFlowGraphMonitor gitFlowGraphMonitor;
  @Getter
  private boolean active;

  public MultiHopFlowCompiler(Config config) {
    this(config, true);
  }

  public MultiHopFlowCompiler(Config config, boolean instrumentationEnabled) {
    this(config, Optional.<Logger>absent(), instrumentationEnabled);
  }

  public MultiHopFlowCompiler(Config config, Optional<Logger> log) {
    this(config, log, true);
  }

  public MultiHopFlowCompiler(Config config, Optional<Logger> log, boolean instrumentationEnabled) {
    super(config, log, instrumentationEnabled);
    Config templateCatalogCfg = config
        .withValue(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY,
            config.getValue(ServiceConfigKeys.TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY));
    FSFlowCatalog flowCatalog;
    try {
      flowCatalog = new FSFlowCatalog(templateCatalogCfg);
    } catch (IOException e) {
      throw new RuntimeException("Cannot instantiate " + getClass().getName(), e);
    }
    this.flowGraph = new BaseFlowGraph();
    this.gitFlowGraphMonitor = new GitFlowGraphMonitor(this.config, flowCatalog, this.flowGraph);
  }

  public void setActive(boolean active) {
    this.active = active;
    this.gitFlowGraphMonitor.setActive(active);
  }

  /**
   * TODO: We need to change signature of compileFlow to return a Dag instead of a HashMap to capture
   * job dependencies.
   * @param spec
   * @return
   */
  @Override
  public Map<Spec, SpecExecutor> compileFlow(Spec spec) {
    Preconditions.checkNotNull(spec);
    Preconditions.checkArgument(spec instanceof FlowSpec, "MultiHopFlowToJobSpecCompiler only accepts FlowSpecs");

    long startTime = System.nanoTime();
    Map<Spec, SpecExecutor> specExecutorMap = Maps.newLinkedHashMap();

    FlowSpec flowSpec = (FlowSpec) spec;
    String source = flowSpec.getConfig().getString(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY);
    String destination = flowSpec.getConfig().getString(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY);
    log.info(String.format("Compiling flow for source: %s and destination: %s", source, destination));

    FlowGraphPathFinder pathFinder = new FlowGraphPathFinder(this.flowGraph, flowSpec);
    try {
      Dag<JobExecutionPlan> jobExecutionPlanDag = pathFinder.findPath();
      //TODO: Just a dummy return value for now. compileFlow() signature needs to be modified to return a Dag instead
      // of a Map. For now just add all specs into the map.
      for (Dag.DagNode<JobExecutionPlan> node: jobExecutionPlanDag.getNodes()) {
        JobExecutionPlan jobExecutionPlan = node.getValue();
        specExecutorMap.put(jobExecutionPlan.getJobSpec(), jobExecutionPlan.getSpecExecutor());
      }
    } catch (FlowGraphPathFinder.PathFinderException e) {
      Instrumented.markMeter(this.flowCompilationFailedMeter);
      log.error(String.format("Exception encountered while compiling flow for source: %s and destination: %s", source, destination), e);
      return null;
    }
    Instrumented.markMeter(this.flowCompilationSuccessFulMeter);
    Instrumented.updateTimer(this.flowCompilationTimer, System.nanoTime() - startTime, TimeUnit.NANOSECONDS);

    return specExecutorMap;
  }

  @Override
  protected void populateEdgeTemplateMap() {
    log.warn("No population of templates based on edge happen in this implementation");
    return;
  }


}
