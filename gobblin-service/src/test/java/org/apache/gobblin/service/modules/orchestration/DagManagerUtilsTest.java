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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.spec_executorInstance.MockedSpecExecutor;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.proc.DagProcUtils;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.spec.JobExecutionPlanDagFactory;
import org.apache.gobblin.util.ConfigUtils;

import static org.apache.gobblin.service.ExecutionStatus.*;


public class DagManagerUtilsTest {
  Random rand = new Random();

  @Test
  public void testGetJobSpecFromDag() throws Exception {
    Dag<JobExecutionPlan> testDag = DagTestUtils.buildDag("testDag", 1000L);
    JobSpec jobSpec = DagManagerUtils.getJobSpec(testDag.getNodes().get(0));
    Assert.assertEquals(jobSpec.getConfigAsProperties().size(), jobSpec.getConfig().entrySet().size());
    for (String key : jobSpec.getConfigAsProperties().stringPropertyNames()) {
      Assert.assertTrue(jobSpec.getConfig().hasPath(key));
      // Assume each key is a string because all job configs are currently strings
      Assert.assertEquals(jobSpec.getConfigAsProperties().get(key), jobSpec.getConfig().getString(key));
    }
  }

  @Test
  public void testIsDagFinishedSingleNode() throws URISyntaxException {
    long flowExecutionId = 12345L;
    String flowGroup = "fg";
    String flowName = "fn";

    Dag<JobExecutionPlan> dag =
        DagManagerTest.buildDag("1", flowExecutionId, DagManager.FailureOption.FINISH_ALL_POSSIBLE.name(), 1, "user5",
            ConfigFactory.empty().withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
                .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
                .withValue(ConfigurationKeys.JOB_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
                .withValue(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY,
                    ConfigValueFactory.fromAnyRef(MySqlDagManagementStateStoreTest.TEST_SPEC_EXECUTOR_URI)));

    setJobStatuses(dag, Collections.singletonList(COMPLETE));
    Assert.assertTrue(DagProcUtils.isDagFinished(dag));

    setJobStatuses(dag, Collections.singletonList(FAILED));
    Assert.assertTrue(DagProcUtils.isDagFinished(dag));

    setJobStatuses(dag, Collections.singletonList(CANCELLED));
    Assert.assertTrue(DagProcUtils.isDagFinished(dag));

    setJobStatuses(dag, Collections.singletonList(PENDING));
    Assert.assertFalse(DagProcUtils.isDagFinished(dag));

    setJobStatuses(dag, Collections.singletonList(PENDING_RETRY));
    Assert.assertFalse(DagProcUtils.isDagFinished(dag));

    setJobStatuses(dag, Collections.singletonList(PENDING_RESUME));
    Assert.assertFalse(DagProcUtils.isDagFinished(dag));

    setJobStatuses(dag, Collections.singletonList(ORCHESTRATED));
    Assert.assertFalse(DagProcUtils.isDagFinished(dag));

    setJobStatuses(dag, Collections.singletonList(RUNNING));
    Assert.assertFalse(DagProcUtils.isDagFinished(dag));
  }

  @Test
  public void testIsDagFinishedTwoNodes() throws URISyntaxException {
    long flowExecutionId = 12345L;
    String flowGroup = "fg";
    String flowName = "fn";

    Dag<JobExecutionPlan> dag =
        DagManagerTest.buildDag("1", flowExecutionId, DagManager.FailureOption.FINISH_ALL_POSSIBLE.name(), 2, "user5",
            ConfigFactory.empty().withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
                .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
                .withValue(ConfigurationKeys.JOB_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
                .withValue(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY,
                    ConfigValueFactory.fromAnyRef(MySqlDagManagementStateStoreTest.TEST_SPEC_EXECUTOR_URI)));

    setJobStatuses(dag, Arrays.asList(COMPLETE, PENDING));
    Assert.assertFalse(DagProcUtils.isDagFinished(dag));

    setJobStatuses(dag, Arrays.asList(COMPLETE, FAILED));
    Assert.assertTrue(DagProcUtils.isDagFinished(dag));

    setJobStatuses(dag, Arrays.asList(FAILED, PENDING));
    Assert.assertTrue(DagProcUtils.isDagFinished(dag));

    setJobStatuses(dag, Arrays.asList(CANCELLED, PENDING));
    Assert.assertTrue(DagProcUtils.isDagFinished(dag));
  }

  @Test
  public void testIsDagFinishedThreeNodes() throws URISyntaxException {
    long flowExecutionId = 12345L;
    String flowGroup = "fg";
    String flowName = "fn";

    Dag<JobExecutionPlan> dag = buildComplexDag3("1", flowExecutionId, DagManager.FailureOption.FINISH_ALL_POSSIBLE.name(), "user5",
        ConfigFactory.empty().withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
            .withValue(ConfigurationKeys.JOB_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY,
                ConfigValueFactory.fromAnyRef(MySqlDagManagementStateStoreTest.TEST_SPEC_EXECUTOR_URI)));

    setJobStatuses(dag, Arrays.asList(COMPLETE, PENDING, PENDING));
    Assert.assertFalse(DagProcUtils.isDagFinished(dag));

    setJobStatuses(dag, Arrays.asList(COMPLETE, FAILED, PENDING));
    Assert.assertTrue(DagProcUtils.isDagFinished(dag));

    setJobStatuses(dag, Arrays.asList(COMPLETE, CANCELLED, PENDING));
    Assert.assertTrue(DagProcUtils.isDagFinished(dag));
  }

  @Test
  public void testIsDagFinishedFourNodes() throws URISyntaxException {
    long flowExecutionId = 12345L;
    String flowGroup = "fg";
    String flowName = "fn";

    Dag<JobExecutionPlan> dag =
        buildComplexDag2("1", flowExecutionId, DagManager.FailureOption.FINISH_ALL_POSSIBLE.name(), "user5",
            ConfigFactory.empty().withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
                .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
                .withValue(ConfigurationKeys.JOB_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
                .withValue(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY,
                    ConfigValueFactory.fromAnyRef(MySqlDagManagementStateStoreTest.TEST_SPEC_EXECUTOR_URI)));

    setJobStatuses(dag, Arrays.asList(COMPLETE, PENDING, PENDING, PENDING));
    Assert.assertFalse(DagProcUtils.isDagFinished(dag));

    setJobStatuses(dag, Arrays.asList(FAILED, PENDING, PENDING, PENDING));
    Assert.assertTrue(DagProcUtils.isDagFinished(dag));

    setJobStatuses(dag, Arrays.asList(CANCELLED, PENDING, PENDING, PENDING));
    Assert.assertTrue(DagProcUtils.isDagFinished(dag));

    setJobStatuses(dag, Arrays.asList(PENDING, PENDING, PENDING, PENDING));
    Assert.assertFalse(DagProcUtils.isDagFinished(dag));
  }

  @Test
  public void testIsDagFinishedMultiNodes() throws URISyntaxException {
    Dag<JobExecutionPlan> dag = buildComplexDag1();
    setJobStatuses(dag, Arrays.asList(COMPLETE, COMPLETE, COMPLETE, COMPLETE, COMPLETE, COMPLETE, COMPLETE, COMPLETE, COMPLETE, COMPLETE));
    Assert.assertTrue(DagProcUtils.isDagFinished(dag));
    Collections.shuffle(dag.getNodes());
    Assert.assertTrue(DagProcUtils.isDagFinished(dag));

    Dag<JobExecutionPlan> dag2 = buildComplexDag1();
    setJobStatuses(dag2, Arrays.asList(COMPLETE, COMPLETE, COMPLETE, COMPLETE, PENDING, COMPLETE, COMPLETE, PENDING, COMPLETE, PENDING));
    Assert.assertFalse(DagProcUtils.isDagFinished(dag2));
    Collections.shuffle(dag2.getNodes());
    Assert.assertFalse(DagProcUtils.isDagFinished(dag2));

    Dag<JobExecutionPlan> dag3 = buildComplexDag1();
    setJobStatuses(dag3, Arrays.asList(FAILED, COMPLETE, COMPLETE, COMPLETE, PENDING, COMPLETE, COMPLETE, PENDING, COMPLETE, PENDING));
    Assert.assertTrue(DagProcUtils.isDagFinished(dag3));
    Collections.shuffle(dag3.getNodes());
    Assert.assertTrue(DagProcUtils.isDagFinished(dag3));

    Dag<JobExecutionPlan> dag4 = buildComplexDag1();
    setJobStatuses(dag4, Arrays.asList(COMPLETE, COMPLETE, COMPLETE, COMPLETE, COMPLETE, CANCELLED, COMPLETE, PENDING, PENDING, PENDING));
    Assert.assertFalse(DagProcUtils.isDagFinished(dag4));
    Collections.shuffle(dag4.getNodes());
    Assert.assertFalse(DagProcUtils.isDagFinished(dag4));

    Dag<JobExecutionPlan> dag5 = buildComplexDag1();
    setJobStatuses(dag5, Arrays.asList(COMPLETE, COMPLETE, COMPLETE, COMPLETE, COMPLETE, CANCELLED, COMPLETE, COMPLETE, PENDING, PENDING));
    Assert.assertTrue(DagProcUtils.isDagFinished(dag5));
    Collections.shuffle(dag5.getNodes());
    Assert.assertTrue(DagProcUtils.isDagFinished(dag5));

    Dag<JobExecutionPlan> dag6 = buildComplexDag1();
    setJobStatuses(dag6, Arrays.asList(COMPLETE, COMPLETE, COMPLETE, COMPLETE, COMPLETE, PENDING_RESUME, COMPLETE, COMPLETE, PENDING, PENDING));
    Assert.assertFalse(DagProcUtils.isDagFinished(dag6));
    Collections.shuffle(dag6.getNodes());
    Assert.assertFalse(DagProcUtils.isDagFinished(dag6));

    Dag<JobExecutionPlan> dag7 = buildComplexDag1();
    setJobStatuses(dag7, Arrays.asList(COMPLETE, COMPLETE, COMPLETE, COMPLETE, COMPLETE, PENDING_RETRY, COMPLETE, COMPLETE, PENDING, PENDING));
    Assert.assertFalse(DagProcUtils.isDagFinished(dag7));
    Collections.shuffle(dag7.getNodes());
    Assert.assertFalse(DagProcUtils.isDagFinished(dag7));

    Dag<JobExecutionPlan> dag8 = buildComplexDag1();
    setJobStatuses(dag8, Arrays.asList(COMPLETE, COMPLETE, COMPLETE, COMPLETE, COMPLETE, RUNNING, COMPLETE, COMPLETE, PENDING, PENDING));
    Assert.assertFalse(DagProcUtils.isDagFinished(dag8));
    Collections.shuffle(dag8.getNodes());
    Assert.assertFalse(DagProcUtils.isDagFinished(dag8));

    Dag<JobExecutionPlan> dag9 = buildComplexDag1();
    setJobStatuses(dag9, Arrays.asList(COMPLETE, COMPLETE, COMPLETE, FAILED, COMPLETE, COMPLETE, PENDING, COMPLETE, PENDING, COMPLETE));
    Assert.assertFalse(DagProcUtils.isDagFinished(dag9));
    Collections.shuffle(dag9.getNodes());
    Assert.assertFalse(DagProcUtils.isDagFinished(dag9));
  }

  @Test
  public void testIsDagFinishedWithFinishRunningFailureOption() throws URISyntaxException {
    long flowExecutionId = 12345L;
    String flowGroup = "fg";
    String flowName = "fn";
    Dag<JobExecutionPlan> dag = buildComplexDag4("1", flowExecutionId, DagManager.FailureOption.FINISH_RUNNING.name(), "user5",
        ConfigFactory.empty()
            .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
            .withValue(ConfigurationKeys.JOB_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
            .withValue(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY, ConfigValueFactory.fromAnyRef(
                MySqlDagManagementStateStoreTest.TEST_SPEC_EXECUTOR_URI)));

    setJobStatuses(dag, Arrays.asList(COMPLETE, CANCELLED, COMPLETE, PENDING, PENDING));
    Assert.assertTrue(DagProcUtils.isDagFinished(dag));

    setJobStatuses(dag, Arrays.asList(COMPLETE, CANCELLED, COMPLETE, RUNNING, PENDING));
    Assert.assertFalse(DagProcUtils.isDagFinished(dag));
  }

  private List<Dag.DagNode<JobExecutionPlan>> randomizeNodes(Dag<JobExecutionPlan> dag) {
    int size = dag.getNodes().size();
    List<Dag.DagNode<JobExecutionPlan>> randomizedDagNodes = new ArrayList<>();

    for (int i=0; i<size; i++) {
      int index = rand.nextInt(size);
      randomizedDagNodes.add(dag.getNodes().get(index));
    }

    return randomizedDagNodes;

  }

  private void setJobStatuses(Dag<JobExecutionPlan> dag, List<ExecutionStatus> statuses) {
    int i=0;
    for (ExecutionStatus status : statuses) {
      dag.getNodes().get(i++).getValue().setExecutionStatus(status);
    }
  }

  // This creates a dag like this
  //  D0  D1  D2  D3
  //  |   |   | \ |
  //  D4  D5  |  D6
  //  |   |  \|
  //  D7  |   D8
  //    \ |  /
  //      D9

  public static Dag<JobExecutionPlan> buildComplexDag1() throws URISyntaxException {
    List<JobExecutionPlan> jobExecutionPlans = new ArrayList<>();
    String id = "1";
    String flowGroup = "fg";
    String flowName = "fn";
    long flowExecutionId = 12345L;
    String flowFailureOption = DagManager.FailureOption.FINISH_ALL_POSSIBLE.name();
    String proxyUser = "user5";
    Config additionalConfig = ConfigFactory.empty()
        .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
        .withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
        .withValue(ConfigurationKeys.JOB_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup))
        .withValue(ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY, ConfigValueFactory.fromAnyRef(
            MySqlDagManagementStateStoreTest.TEST_SPEC_EXECUTOR_URI));

    for (int i = 0; i < 10; i++) {
      String suffix = Integer.toString(i);
      Config jobConfig = ConfigBuilder.create().
          addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "group" + id).
          addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "flow" + id).
          addPrimitive(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, flowExecutionId).
          addPrimitive(ConfigurationKeys.JOB_GROUP_KEY, "group" + id).
          addPrimitive(ConfigurationKeys.JOB_NAME_KEY, "job" + suffix).
          addPrimitive(ConfigurationKeys.FLOW_FAILURE_OPTION, flowFailureOption).
          addPrimitive(AzkabanProjectConfig.USER_TO_PROXY, proxyUser).build();
      jobConfig = additionalConfig.withFallback(jobConfig);
      if (i == 4) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job0"));
      } else if (i == 5) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job1"));
      } if (i == 6) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job2,job3"));
      } else if (i == 7) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job4"));
      } else if (i == 8) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job5,job2"));
      } else if (i == 9) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job7,job5,job8"));
      }
      JobSpec js = JobSpec.builder("test_job" + suffix).withVersion(suffix).withConfig(jobConfig).
          withTemplate(new URI("job" + suffix)).build();
      SpecExecutor specExecutor = MockedSpecExecutor.createDummySpecExecutor(new URI(
          ConfigUtils.getString(additionalConfig, ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY,"job" + i)));
      JobExecutionPlan jobExecutionPlan = new JobExecutionPlan(js, specExecutor);
      jobExecutionPlans.add(jobExecutionPlan);
    }
    return new JobExecutionPlanDagFactory().createDag(jobExecutionPlans);
  }

  // This creates a dag like this
  // D0 -> D1 -> D2 -> D3
  public static Dag<JobExecutionPlan> buildComplexDag2(String id, long flowExecutionId,
      String flowFailureOption, String proxyUser, Config additionalConfig) throws URISyntaxException {
    List<JobExecutionPlan> jobExecutionPlans = new ArrayList<>();

    for (int i = 0; i < 4; i++) {
      String suffix = Integer.toString(i);
      Config jobConfig = ConfigBuilder.create().
          addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "group" + id).
          addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "flow" + id).
          addPrimitive(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, flowExecutionId).
          addPrimitive(ConfigurationKeys.JOB_GROUP_KEY, "group" + id).
          addPrimitive(ConfigurationKeys.JOB_NAME_KEY, "job" + suffix).
          addPrimitive(ConfigurationKeys.FLOW_FAILURE_OPTION, flowFailureOption).
          addPrimitive(AzkabanProjectConfig.USER_TO_PROXY, proxyUser).build();
      jobConfig = additionalConfig.withFallback(jobConfig);
      if (i == 1) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job0"));
      } else if (i == 2) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job1"));
      } if (i == 3) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job2"));
      }
      JobSpec js = JobSpec.builder("test_job" + suffix).withVersion(suffix).withConfig(jobConfig).
          withTemplate(new URI("job" + suffix)).build();
      SpecExecutor specExecutor = MockedSpecExecutor.createDummySpecExecutor(new URI(
          ConfigUtils.getString(additionalConfig, ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY,"job" + i)));
      JobExecutionPlan jobExecutionPlan = new JobExecutionPlan(js, specExecutor);
      jobExecutionPlans.add(jobExecutionPlan);
    }
    return new JobExecutionPlanDagFactory().createDag(jobExecutionPlans);
  }

  // This creates a dag like this
  // D0  D1
  //   \/
  //   D2
  public static Dag<JobExecutionPlan> buildComplexDag3(String id, long flowExecutionId,
      String flowFailureOption, String proxyUser, Config additionalConfig) throws URISyntaxException {
    List<JobExecutionPlan> jobExecutionPlans = new ArrayList<>();

    for (int i = 0; i < 3; i++) {
      String suffix = Integer.toString(i);
      Config jobConfig = ConfigBuilder.create().
          addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "group" + id).
          addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "flow" + id).
          addPrimitive(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, flowExecutionId).
          addPrimitive(ConfigurationKeys.JOB_GROUP_KEY, "group" + id).
          addPrimitive(ConfigurationKeys.JOB_NAME_KEY, "job" + suffix).
          addPrimitive(ConfigurationKeys.FLOW_FAILURE_OPTION, flowFailureOption).
          addPrimitive(AzkabanProjectConfig.USER_TO_PROXY, proxyUser).build();
      jobConfig = additionalConfig.withFallback(jobConfig);
      if (i == 2) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job0,job1"));
      }
      JobSpec js = JobSpec.builder("test_job" + suffix).withVersion(suffix).withConfig(jobConfig).
          withTemplate(new URI("job" + suffix)).build();
      SpecExecutor specExecutor = MockedSpecExecutor.createDummySpecExecutor(new URI(
          ConfigUtils.getString(additionalConfig, ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY,"job" + i)));
      JobExecutionPlan jobExecutionPlan = new JobExecutionPlan(js, specExecutor);
      jobExecutionPlans.add(jobExecutionPlan);
    }
    return new JobExecutionPlanDagFactory().createDag(jobExecutionPlans);
  }

  // This creates a dag like this
  //   D0
  //  / \
  // D1  D2
  //    / \
  //   D3  D4
  public static Dag<JobExecutionPlan> buildComplexDag4(String id, long flowExecutionId,
      String flowFailureOption, String proxyUser, Config additionalConfig) throws URISyntaxException {
    List<JobExecutionPlan> jobExecutionPlans = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      String suffix = Integer.toString(i);
      Config jobConfig = ConfigBuilder.create().
          addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "group" + id).
          addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "flow" + id).
          addPrimitive(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, flowExecutionId).
          addPrimitive(ConfigurationKeys.JOB_GROUP_KEY, "group" + id).
          addPrimitive(ConfigurationKeys.JOB_NAME_KEY, "job" + suffix).
          addPrimitive(ConfigurationKeys.FLOW_FAILURE_OPTION, flowFailureOption).
          addPrimitive(AzkabanProjectConfig.USER_TO_PROXY, proxyUser).build();
      jobConfig = additionalConfig.withFallback(jobConfig);
      if (i == 1) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job0"));
      } else if (i == 2) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job0"));
      } else if (i == 3 || i == 4) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job2"));
      }
      JobSpec js = JobSpec.builder("test_job" + suffix).withVersion(suffix).withConfig(jobConfig).
          withTemplate(new URI("job" + suffix)).build();
      SpecExecutor specExecutor = MockedSpecExecutor.createDummySpecExecutor(new URI(
          ConfigUtils.getString(additionalConfig, ConfigurationKeys.SPECEXECUTOR_INSTANCE_URI_KEY,"job" + i)));
      JobExecutionPlan jobExecutionPlan = new JobExecutionPlan(js, specExecutor);
      jobExecutionPlans.add(jobExecutionPlan);
    }
    return new JobExecutionPlanDagFactory().createDag(jobExecutionPlans);
  }
}
