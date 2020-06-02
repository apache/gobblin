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
import java.util.Properties;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.spec_executorInstance.InMemorySpecExecutor;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.spec.JobExecutionPlanDagFactory;
import org.apache.gobblin.util.ConfigUtils;


/**
 * This mocked SpecCompiler class creates 3 dummy job specs to emulate flow spec compiler.
 * It can also be used to compile in a certain way or not to compile at all to write negative test cases.
 * It uses {@link InMemorySpecExecutor} for these dummy specs.
 */
public class MockedSpecCompiler extends IdentityFlowToJobSpecCompiler {

  private static final int NUMBER_OF_JOBS = 3;
  public static final String UNCOMPILABLE_FLOW = "uncompilableFlow";

  public MockedSpecCompiler(Config config) {
    super(config);
  }

  @Override
  public Dag<JobExecutionPlan> compileFlow(Spec spec) {
    String flowName = (String) ((FlowSpec) spec).getConfigAsProperties().get(ConfigurationKeys.FLOW_NAME_KEY);
    if (flowName.equalsIgnoreCase(UNCOMPILABLE_FLOW)) {
      return null;
    }

    List<JobExecutionPlan> jobExecutionPlans = new ArrayList<>();

    long flowExecutionId = System.currentTimeMillis();

    int i = 0;
    while(i++ < NUMBER_OF_JOBS) {
      String specUri = "/foo/bar/spec/" + i;
      Properties properties = new Properties();
      properties.put(ConfigurationKeys.FLOW_NAME_KEY, flowName);
      properties.put(ConfigurationKeys.FLOW_GROUP_KEY, ((FlowSpec)spec).getConfigAsProperties().get(ConfigurationKeys.FLOW_GROUP_KEY));
      properties.put(ConfigurationKeys.JOB_NAME_KEY, ((FlowSpec)spec).getConfigAsProperties().get(ConfigurationKeys.FLOW_NAME_KEY) + "_" + i);
      properties.put(ConfigurationKeys.JOB_GROUP_KEY, ((FlowSpec)spec).getConfigAsProperties().get(ConfigurationKeys.FLOW_GROUP_KEY) + "_" + i);
      properties.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, flowExecutionId);
      JobSpec jobSpec = JobSpec.builder(specUri)
          .withConfig(ConfigUtils.propertiesToConfig(properties))
          .withVersion("1")
          .withDescription("Spec Description")
          .build();
      jobExecutionPlans.add(new JobExecutionPlan(jobSpec, new InMemorySpecExecutor(ConfigFactory.empty())));
    }

    return new JobExecutionPlanDagFactory().createDag(jobExecutionPlans);
  }
}
