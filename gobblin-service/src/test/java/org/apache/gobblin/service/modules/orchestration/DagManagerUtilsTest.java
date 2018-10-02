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
package org.apache.gobblin.service.modules.orchestration;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.spec_executorInstance.InMemorySpecExecutor;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.spec.JobExecutionPlanDagFactory;

import static org.testng.Assert.*;


public class DagManagerUtilsTest {

  /**
   * Create a {@link Dag <JobExecutionPlan>} with 2 parents and 1 child (i.e. a V-shaped dag).
   * @return a Dag.
   */
  public Dag<JobExecutionPlan> buildDag() throws URISyntaxException {
    List<JobExecutionPlan> jobExecutionPlans = new ArrayList<>();
    Config baseConfig = ConfigBuilder.create().
        addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "group0").
        addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "flow0").
        addPrimitive(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, System.currentTimeMillis()).
        addPrimitive(ConfigurationKeys.JOB_GROUP_KEY, "group0").build();
    for (int i = 0; i < 3; i++) {
      String suffix = Integer.toString(i);
      Config jobConfig = baseConfig.withValue(ConfigurationKeys.JOB_NAME_KEY, ConfigValueFactory.fromAnyRef("job" + suffix));
      if (i == 2) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job0,job1"));
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
  public void testGetNext() throws URISyntaxException {
    Dag<JobExecutionPlan> dag = buildDag();

    Set<Dag.DagNode<JobExecutionPlan>> dagNodeSet = DagManagerUtils.getNext(dag);
    Assert.assertEquals(dagNodeSet.size(), 2);

    //Set 1st job to complete and 2nd job running state
    JobExecutionPlan jobExecutionPlan1 = dag.getNodes().get(0).getValue();
    jobExecutionPlan1.setExecutionStatus(ExecutionStatus.COMPLETE);
    JobExecutionPlan jobExecutionPlan2 = dag.getNodes().get(1).getValue();
    jobExecutionPlan2.setExecutionStatus(ExecutionStatus.RUNNING);

    //No new job to run
    dagNodeSet = DagManagerUtils.getNext(dag);
    Assert.assertEquals(dagNodeSet.size(), 0);

    //Set 2nd job to complete; we must have 3rd job to run next
    jobExecutionPlan2.setExecutionStatus(ExecutionStatus.COMPLETE);
    dagNodeSet = DagManagerUtils.getNext(dag);
    Assert.assertEquals(dagNodeSet.size(), 1);

    //Set the 3rd job to running state, no new jobs to run
    JobExecutionPlan jobExecutionPlan3 = dag.getNodes().get(2).getValue();
    jobExecutionPlan3.setExecutionStatus(ExecutionStatus.RUNNING);
    dagNodeSet = DagManagerUtils.getNext(dag);
    Assert.assertEquals(dagNodeSet.size(), 0);

    //Set the 3rd job to complete; no new jobs to run
    jobExecutionPlan3.setExecutionStatus(ExecutionStatus.COMPLETE);
    dagNodeSet = DagManagerUtils.getNext(dag);
    Assert.assertEquals(dagNodeSet.size(), 0);


    dag = buildDag();
    dagNodeSet = DagManagerUtils.getNext(dag);
    Assert.assertEquals(dagNodeSet.size(), 2);
    //Set 1st job to failed; no new jobs to run
    jobExecutionPlan1 = dag.getNodes().get(0).getValue();
    jobExecutionPlan1.setExecutionStatus(ExecutionStatus.FAILED);
    dagNodeSet = DagManagerUtils.getNext(dag);
    Assert.assertEquals(dagNodeSet.size(), 0);
  }


}