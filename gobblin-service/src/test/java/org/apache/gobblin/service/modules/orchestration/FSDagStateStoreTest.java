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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
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


public class FSDagStateStoreTest {
  private DagStateStore _dagStateStore;
  private final String dagStateStoreDir = "/tmp/fsDagStateStoreTest/dagStateStore";
  private File checkpointDir;

  @BeforeClass
  public void setUp() throws IOException {
    this.checkpointDir = new File(dagStateStoreDir);
    FileUtils.deleteDirectory(this.checkpointDir);
    Config config = ConfigFactory.empty().withValue(DagManager.DAG_STATESTORE_DIR, ConfigValueFactory.fromAnyRef(
        this.dagStateStoreDir));
    this._dagStateStore = new FSDagStateStore(config);
  }

  /**
   * Create a {@link Dag<JobExecutionPlan>} with one parent and one child.
   * @return a Dag.
   */
  public Dag<JobExecutionPlan> buildDag(String id, Long flowExecutionId) throws URISyntaxException {
    List<JobExecutionPlan> jobExecutionPlans = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      String suffix = Integer.toString(i);
      Config jobConfig = ConfigBuilder.create().
          addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "group" + id).
          addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "flow" + id).
          addPrimitive(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, flowExecutionId).
          addPrimitive(ConfigurationKeys.JOB_NAME_KEY, "job" + suffix).build();
      if (i > 0) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job" + (i - 1)));
      }
      JobSpec js = JobSpec.builder("test_job" + suffix).withVersion(suffix).withConfig(jobConfig).
          withTemplate(new URI("job" + suffix)).build();
      SpecExecutor specExecutor = new InMemorySpecExecutor(ConfigFactory.empty());
      JobExecutionPlan jobExecutionPlan = new JobExecutionPlan(js, specExecutor);
      jobExecutionPlan.setExecutionStatus(ExecutionStatus.RUNNING);
      jobExecutionPlans.add(jobExecutionPlan);
    }
    return new JobExecutionPlanDagFactory().createDag(jobExecutionPlans);
  }

  @Test
  public void testWriteCheckpoint() throws IOException, URISyntaxException {
    long flowExecutionId = System.currentTimeMillis();
    String flowGroupId = "0";
    Dag<JobExecutionPlan> dag = buildDag(flowGroupId, flowExecutionId);
    this._dagStateStore.writeCheckpoint(dag);

    String fileName = DagManagerUtils.generateDagId(dag) + FSDagStateStore.DAG_FILE_EXTENSION;
    File dagFile = new File(this.checkpointDir, fileName);
    Dag<JobExecutionPlan> dagDeserialized = ((FSDagStateStore) this._dagStateStore).getDag(dagFile);
    Assert.assertEquals(dagDeserialized.getNodes().size(), 2);
    Assert.assertEquals(dagDeserialized.getStartNodes().size(), 1);
    Assert.assertEquals(dagDeserialized.getEndNodes().size(), 1);
    Dag.DagNode<JobExecutionPlan> child = dagDeserialized.getEndNodes().get(0);
    Dag.DagNode<JobExecutionPlan> parent = dagDeserialized.getStartNodes().get(0);
    Assert.assertEquals(dagDeserialized.getParentChildMap().size(), 1);
    Assert.assertTrue(dagDeserialized.getParentChildMap().get(parent).contains(child));

    for (int i = 0; i < 2; i++) {
      JobExecutionPlan plan = dagDeserialized.getNodes().get(i).getValue();
      Config jobConfig = plan.getJobSpec().getConfig();
      Assert.assertEquals(jobConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY), "group" + flowGroupId);
      Assert.assertEquals(jobConfig.getString(ConfigurationKeys.FLOW_NAME_KEY), "flow" + flowGroupId);
      Assert.assertEquals(jobConfig.getLong(ConfigurationKeys.FLOW_EXECUTION_ID_KEY), flowExecutionId);
      Assert.assertEquals(plan.getExecutionStatus(), ExecutionStatus.RUNNING);
    }
  }

  @Test (dependsOnMethods = "testWriteCheckpoint")
  public void testCleanUp() throws IOException, URISyntaxException {
    long flowExecutionId = System.currentTimeMillis();
    String flowGroupId = "0";
    Dag<JobExecutionPlan> dag = buildDag(flowGroupId, flowExecutionId);
    this._dagStateStore.writeCheckpoint(dag);
    String fileName = DagManagerUtils.generateDagId(dag) + FSDagStateStore.DAG_FILE_EXTENSION;
    File dagFile = new File(this.checkpointDir, fileName);
    Assert.assertTrue(dagFile.exists());
    this._dagStateStore.cleanUp(dag);
    Assert.assertFalse(dagFile.exists());
  }

  @Test (dependsOnMethods = "testCleanUp")
  public void testGetDags() throws IOException, URISyntaxException {
    //Set up a new FSDagStateStore instance.
    setUp();
    List<Long> flowExecutionIds = Lists.newArrayList(System.currentTimeMillis(), System.currentTimeMillis() + 1);
    for (int i = 0; i < 2; i++) {
      String flowGroupId = Integer.toString(i);
      Dag<JobExecutionPlan> dag = buildDag(flowGroupId, flowExecutionIds.get(i));
      this._dagStateStore.writeCheckpoint(dag);
    }

    List<Dag<JobExecutionPlan>> dags = this._dagStateStore.getDags();
    Assert.assertEquals(dags.size(), 2);
    for (Dag<JobExecutionPlan> dag: dags) {
      Assert.assertEquals(dag.getNodes().size(), 2);
      Assert.assertEquals(dag.getStartNodes().size(), 1);
      Assert.assertEquals(dag.getEndNodes().size(), 1);
      Assert.assertEquals(dag.getParentChildMap().size(), 1);
    }
  }

  @AfterClass
  public void cleanUp() throws IOException {
    FileUtils.deleteDirectory(this.checkpointDir);
  }
}