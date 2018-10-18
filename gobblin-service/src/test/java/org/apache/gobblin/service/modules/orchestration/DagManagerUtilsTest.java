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
import java.util.Iterator;
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
import org.apache.gobblin.service.modules.flowgraph.Dag.DagNode;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.spec.JobExecutionPlanDagFactory;


public class DagManagerUtilsTest {

  /**
   * Create a {@link Dag <JobExecutionPlan>}.
   * @return a Dag.
   */
  private Dag<JobExecutionPlan> buildDag(String dagFailureOption) throws URISyntaxException {
    List<JobExecutionPlan> jobExecutionPlans = new ArrayList<>();
    Config baseConfig = ConfigBuilder.create().
        addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "group0").
        addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "flow0").
        addPrimitive(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, System.currentTimeMillis()).
        addPrimitive(ConfigurationKeys.JOB_GROUP_KEY, "group0").
        addPrimitive(ConfigurationKeys.FLOW_FAILURE_OPTION, dagFailureOption).build();
    for (int i = 0; i < 5; i++) {
      String suffix = Integer.toString(i);
      Config jobConfig = baseConfig.withValue(ConfigurationKeys.JOB_NAME_KEY, ConfigValueFactory.fromAnyRef("job" + suffix));
      if (i == 1 || i == 2) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job0"));
      }
      if (i == 3) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job1,job2"));
      }
      if (i == 4) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job2"));
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
  public void testGetNextFinishAllPossible() throws URISyntaxException {
    Dag<JobExecutionPlan> dag = buildDag(ConfigurationKeys.DEFAULT_FLOW_FAILURE_OPTION);
    testGetNextHelper(dag, ConfigurationKeys.DEFAULT_FLOW_FAILURE_OPTION);
  }

  @Test (dependsOnMethods = "testGetNextFinishAllPossible")
  public void testGetNextFinishRunning() throws URISyntaxException {
    String failureOption = "FINISH_RUNNING";
    Dag<JobExecutionPlan> dag = buildDag(failureOption);
    testGetNextHelper(dag, failureOption);
  }

  private void testGetNextHelper(Dag<JobExecutionPlan> dag, String flowFailureOption) {
    Set<DagNode<JobExecutionPlan>> dagNodeSet = DagManagerUtils.getNext(dag);
    Assert.assertEquals(dagNodeSet.size(), 1);

    //Set the execution status of job to ExecutionStatus.RUNNING.
    JobExecutionPlan jobExecutionPlan1 = dag.getNodes().get(0).getValue();
    jobExecutionPlan1.setExecutionStatus(ExecutionStatus.RUNNING);

    //No new job to run
    dagNodeSet = DagManagerUtils.getNext(dag);
    Assert.assertEquals(dagNodeSet.size(), 0);

    //Set execution status to complete. 2 new jobs ready to run
    jobExecutionPlan1.setExecutionStatus(ExecutionStatus.COMPLETE);
    dagNodeSet = DagManagerUtils.getNext(dag);
    Assert.assertEquals(dagNodeSet.size(), 2);

    //Set the jobstatuses of the next jobs to running.
    for (DagNode<JobExecutionPlan> dagNode: dagNodeSet) {
      JobExecutionPlan jobExecutionPlan = dagNode.getValue();
      jobExecutionPlan.setExecutionStatus(ExecutionStatus.RUNNING);
    }
    Assert.assertEquals(DagManagerUtils.getNext(dag).size(), 0);

    //Set the one of the jobs to FAILED while the other to COMPLETE.
    Iterator<DagNode<JobExecutionPlan>> dagNodeIterator = dagNodeSet.iterator();
    while (dagNodeIterator.hasNext()) {
      DagNode<JobExecutionPlan> dagNode = dagNodeIterator.next();
      JobExecutionPlan jobExecutionPlan = dagNode.getValue();
      String jobName = DagManagerUtils.getJobName(dagNode);
      if ("job1".equals(jobName)) {
        jobExecutionPlan.setExecutionStatus(ExecutionStatus.FAILED);
      } else {
        jobExecutionPlan.setExecutionStatus(ExecutionStatus.COMPLETE);
      }
    }

    dagNodeSet = DagManagerUtils.getNext(dag);
    if (ConfigurationKeys.DEFAULT_FLOW_FAILURE_OPTION.equals(flowFailureOption)) {
      Assert.assertEquals(dagNodeSet.size(), 1);
    } else {
      Assert.assertEquals(dagNodeSet.size(), 0);
    }
  }
}