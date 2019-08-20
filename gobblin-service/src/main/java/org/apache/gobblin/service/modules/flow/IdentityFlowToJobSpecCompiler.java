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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.ServiceNode;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.spec.JobExecutionPlanDagFactory;


/***
 * Take in a logical {@link Spec} ie flow and compile corresponding materialized job {@link Spec}
 * and its mapping to {@link SpecExecutor}.
 */
@Alpha
public class IdentityFlowToJobSpecCompiler extends BaseFlowToJobSpecCompiler {

  public IdentityFlowToJobSpecCompiler(Config config) {
    super(config, true);
  }

  public IdentityFlowToJobSpecCompiler(Config config, boolean instrumentationEnabled) {
    super(config, Optional.<Logger>absent(), instrumentationEnabled);
  }

  public IdentityFlowToJobSpecCompiler(Config config, Optional<Logger> log) {
    super(config, log, true);
  }

  public IdentityFlowToJobSpecCompiler(Config config, Optional<Logger> log, boolean instrumentationEnabled) {
    super(config, log, instrumentationEnabled);
  }

  @Override
  public Dag<JobExecutionPlan> compileFlow(Spec spec) {
    Preconditions.checkNotNull(spec);
    Preconditions.checkArgument(spec instanceof FlowSpec, "IdentityFlowToJobSpecCompiler only converts FlowSpec to JobSpec");

    long startTime = System.nanoTime();

    FlowSpec flowSpec = (FlowSpec) spec;
    String source = flowSpec.getConfig().getString(ServiceConfigKeys.FLOW_SOURCE_IDENTIFIER_KEY);
    String destination = flowSpec.getConfig().getString(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY);
    log.info(String.format("Compiling flow for source: %s and destination: %s", source, destination));

    JobSpec jobSpec = jobSpecGenerator(flowSpec);
    Instrumented.markMeter(this.flowCompilationSuccessFulMeter);
    Instrumented.updateTimer(this.flowCompilationTimer, System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
    List<JobExecutionPlan> jobExecutionPlans;
    try {
      jobExecutionPlans = getJobExecutionPlans(source, destination, jobSpec);
    } catch (InterruptedException | ExecutionException e) {
      Instrumented.markMeter(this.flowCompilationFailedMeter);
      throw new RuntimeException("Cannot determine topology capabilities", e);
    }
    return new JobExecutionPlanDagFactory().createDag(jobExecutionPlans);
  }

  private List<JobExecutionPlan> getJobExecutionPlans(String source, String destination, JobSpec jobSpec)
      throws ExecutionException, InterruptedException {
    List<JobExecutionPlan> jobExecutionPlans = new ArrayList<>();

    for (TopologySpec topologySpec : topologySpecMap.values()) {
      Map<ServiceNode, ServiceNode> capabilities = topologySpec.getSpecExecutor().getCapabilities().get();
      for (Map.Entry<ServiceNode, ServiceNode> capability : capabilities.entrySet()) {
        log.info(String.format("Evaluating current JobSpec: %s against TopologySpec: %s with "
                + "capability of source: %s and destination: %s ", jobSpec.getUri(), topologySpec.getUri(),
            capability.getKey(), capability.getValue()));
        if (source.equals(capability.getKey().getNodeName()) && destination
            .equals(capability.getValue().getNodeName())) {
          JobExecutionPlan jobExecutionPlan = new JobExecutionPlan(jobSpec, topologySpec.getSpecExecutor());
          log.info(String
              .format("Current JobSpec: %s is executable on TopologySpec: %s. Added TopologySpec as candidate.",
                  jobSpec.getUri(), topologySpec.getUri()));

          log.info("Since we found a candidate executor, we will not try to compute more. "
              + "(Intended limitation for IdentityFlowToJobSpecCompiler)");
          jobExecutionPlans.add(jobExecutionPlan);
          return jobExecutionPlans;
        }
      }
    }
    return jobExecutionPlans;
  }
}