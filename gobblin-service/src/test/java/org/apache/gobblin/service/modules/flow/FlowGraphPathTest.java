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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.spec_executorInstance.InMemorySpecExecutor;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.spec.JobExecutionPlanDagFactory;

public class FlowGraphPathTest {

  /**
   * A method to create a {@link Dag <JobExecutionPlan>}.
   * @return a Dag.
   */
  public Dag<JobExecutionPlan> buildDag(int numNodes, int startNodeId, boolean isForkable) throws URISyntaxException {
    List<JobExecutionPlan> jobExecutionPlans = new ArrayList<>();
    Config baseConfig = ConfigBuilder.create().
        addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "group0").
        addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "flow0").
        addPrimitive(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, System.currentTimeMillis()).
        addPrimitive(ConfigurationKeys.JOB_GROUP_KEY, "group0").build();
    for (int i = startNodeId; i < startNodeId + numNodes; i++) {
      String suffix = Integer.toString(i);
      Config jobConfig = baseConfig.withValue(ConfigurationKeys.JOB_NAME_KEY, ConfigValueFactory.fromAnyRef("job" + suffix));
      if (isForkable && (i == startNodeId + numNodes - 1)) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_FORK_ON_CONCAT, ConfigValueFactory.fromAnyRef(true));
      }
      if (i > startNodeId) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job" + (i  - 1)));
      }
      JobSpec js = JobSpec.builder("test_job" + suffix).withVersion(suffix).withConfig(jobConfig).
          withTemplate(new URI("job" + suffix)).build();
      SpecExecutor specExecutor = InMemorySpecExecutor.createDummySpecExecutor(new URI("job" + i));
      JobExecutionPlan jobExecutionPlan = new JobExecutionPlan(js, specExecutor);
      jobExecutionPlans.add(jobExecutionPlan);
    }
    return new JobExecutionPlanDagFactory().createDag(jobExecutionPlans);
  }

  @Test
  public void testConcatenate() throws URISyntaxException {
    //Dag1: "job0->job1", Dag2: "job2->job3"
    Dag<JobExecutionPlan> dag1 = buildDag(2, 0, false);
    Dag<JobExecutionPlan> dag2 = buildDag(2, 2, false);
    Dag<JobExecutionPlan> dagNew = FlowGraphPath.concatenate(dag1, dag2);

    //Expected result: "job0"->"job1"->"job2"->"job3"
    Assert.assertEquals(dagNew.getStartNodes().size(), 1);
    Assert.assertEquals(dagNew.getEndNodes().size(), 1);
    Assert.assertEquals(dagNew.getNodes().size(), 4);
    Assert.assertEquals(dagNew.getStartNodes().get(0).getValue().getJobSpec().getConfig().getString(ConfigurationKeys.JOB_NAME_KEY), "job0");
    Assert.assertEquals(dagNew.getEndNodes().get(0).getValue().getJobSpec().getConfig().getString(ConfigurationKeys.JOB_NAME_KEY), "job3");
    Assert.assertEquals(dagNew.getEndNodes().get(0).getValue().getJobSpec().getConfig().getString(ConfigurationKeys.JOB_DEPENDENCIES), "job2");

    //Dag1: "job0", Dag2: "job1->job2", "job0" forkable
    dag1 = buildDag(1, 0, true);
    dag2 = buildDag(2, 1, false);
    dagNew = FlowGraphPath.concatenate(dag1, dag2);

    //Expected result: "job0", "job1" -> "job2"
    Assert.assertEquals(dagNew.getStartNodes().size(), 2);
    Assert.assertEquals(dagNew.getEndNodes().size(), 2);
    Assert.assertEquals(dagNew.getNodes().size(), 3);
    Assert.assertEquals(dagNew.getStartNodes().get(0).getValue().getJobSpec().getConfig().getString(ConfigurationKeys.JOB_NAME_KEY), "job0");
    Assert.assertEquals(dagNew.getStartNodes().get(1).getValue().getJobSpec().getConfig().getString(ConfigurationKeys.JOB_NAME_KEY), "job1");
    Assert.assertEquals(dagNew.getEndNodes().get(0).getValue().getJobSpec().getConfig().getString(ConfigurationKeys.JOB_NAME_KEY), "job2");
    Assert.assertEquals(dagNew.getEndNodes().get(0).getValue().getJobSpec().getConfig().getString(ConfigurationKeys.JOB_DEPENDENCIES), "job1");
    Assert.assertEquals(dagNew.getEndNodes().get(1).getValue().getJobSpec().getConfig().getString(ConfigurationKeys.JOB_NAME_KEY), "job0");
    Assert.assertFalse(dagNew.getEndNodes().get(1).getValue().getJobSpec().getConfig().hasPath(ConfigurationKeys.JOB_DEPENDENCIES));

    //Dag1: "job0->job1", Dag2: "job2->job3", "job1" forkable
    dag1 = buildDag(2, 0, true);
    dag2 = buildDag(2, 2, false);
    dagNew = FlowGraphPath.concatenate(dag1, dag2);

    //Expected result: "job0" -> "job1"
    //                        \-> "job2" -> "job3"
    Assert.assertEquals(dagNew.getStartNodes().size(), 1);
    Assert.assertEquals(dagNew.getEndNodes().size(), 2);
    Assert.assertEquals(dagNew.getNodes().size(), 4);
    Assert.assertEquals(dagNew.getStartNodes().get(0).getValue().getJobSpec().getConfig().getString(ConfigurationKeys.JOB_NAME_KEY), "job0");
    Assert.assertEquals(dagNew.getEndNodes().get(0).getValue().getJobSpec().getConfig().getString(ConfigurationKeys.JOB_NAME_KEY), "job3");
    Assert.assertEquals(dagNew.getEndNodes().get(0).getValue().getJobSpec().getConfig().getString(ConfigurationKeys.JOB_DEPENDENCIES), "job2");
    Assert.assertEquals(dagNew.getEndNodes().get(1).getValue().getJobSpec().getConfig().getString(ConfigurationKeys.JOB_NAME_KEY), "job1");
    Assert.assertEquals(dagNew.getEndNodes().get(1).getValue().getJobSpec().getConfig().getString(ConfigurationKeys.JOB_DEPENDENCIES), "job0");

    //Dag1: "job0", Dag2: "job1"
    dag1 = buildDag(1, 0, true);
    dag2 = buildDag(1, 1, false);
    dagNew = FlowGraphPath.concatenate(dag1, dag2);

    //Expected result: "job0","job1"
    Assert.assertEquals(dagNew.getStartNodes().size(), 2);
    Assert.assertEquals(dagNew.getEndNodes().size(), 2);
    Assert.assertEquals(dagNew.getNodes().size(), 2);
    Assert.assertEquals(dagNew.getStartNodes().get(0).getValue().getJobSpec().getConfig().getString(ConfigurationKeys.JOB_NAME_KEY), "job0");
    Assert.assertEquals(dagNew.getStartNodes().get(1).getValue().getJobSpec().getConfig().getString(ConfigurationKeys.JOB_NAME_KEY), "job1");
    Assert.assertFalse(dagNew.getStartNodes().get(1).getValue().getJobSpec().getConfig().hasPath(ConfigurationKeys.JOB_DEPENDENCIES));
    Assert.assertFalse(dagNew.getStartNodes().get(1).getValue().getJobSpec().getConfig().hasPath(ConfigurationKeys.JOB_DEPENDENCIES));
  }
}