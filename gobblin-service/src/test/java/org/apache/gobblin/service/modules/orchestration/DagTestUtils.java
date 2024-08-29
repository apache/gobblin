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
import java.util.Properties;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.runtime.spec_executorInstance.InMemorySpecExecutor;
import org.apache.gobblin.runtime.spec_executorInstance.MockedSpecExecutor;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.spec.JobExecutionPlanDagFactory;
import org.apache.gobblin.util.CompletedFuture;
import org.apache.gobblin.util.ConfigUtils;


public class DagTestUtils {
  private DagTestUtils() {

  }

  public static TopologySpec buildNaiveTopologySpec(String specUriInString) {
    String specStoreDir = "/tmp/specStoreDir";
    Properties properties = new Properties();
    properties.put("specStore.fs.dir", specStoreDir);
    properties.put("specExecInstance.capabilities", "source:destination");
    properties.put("specExecInstance.uri", specUriInString);
    properties.put("uri",specUriInString);

    Config specExecConfig = ConfigUtils.propertiesToConfig(properties);
    SpecExecutor specExecutorInstanceProducer = new InMemorySpecExecutor(specExecConfig);
    TopologySpec.Builder topologySpecBuilder = TopologySpec.builder(new Path(specStoreDir).toUri())
        .withConfig(specExecConfig)
        .withDescription("test")
        .withVersion("1")
        .withSpecExecutor(specExecutorInstanceProducer);

    return topologySpecBuilder.build();
  }

  /**
   * Create a {@link Dag < JobExecutionPlan >} with one parent and one child.
   * @return a Dag.
   */
  public static Dag<JobExecutionPlan> buildDag(String id, Long flowExecutionId) throws URISyntaxException {
    List<JobExecutionPlan> jobExecutionPlans = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      String suffix = Integer.toString(i);
      Config jobConfig = ConfigBuilder.create().
          addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, "group" + id).
          addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, "flow" + id).
          addPrimitive(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, flowExecutionId).
          addPrimitive(ConfigurationKeys.JOB_NAME_KEY, "job" + suffix).build();
      if (i == 1) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job0"));
      }
      JobSpec js = JobSpec.builder("test_job" + suffix).withVersion(suffix).withConfig(jobConfig).
          withTemplate(new URI("job" + suffix)).build();

      SpecExecutor specExecutor = buildNaiveTopologySpec("mySpecExecutor").getSpecExecutor();
      JobExecutionPlan jobExecutionPlan = new JobExecutionPlan(js, specExecutor);
      jobExecutionPlan.setExecutionStatus(ExecutionStatus.RUNNING);

      // Future of type CompletedFuture is used because in tests InMemorySpecProducer is used and that responds with CompletedFuture
      CompletedFuture<Boolean> future = new CompletedFuture<>(Boolean.TRUE, null);
      jobExecutionPlan.setJobFuture(Optional.of(future));

      jobExecutionPlans.add(jobExecutionPlan);
    }
    return new JobExecutionPlanDagFactory().createDag(jobExecutionPlans);
  }


  /**
   * Create a list of dags with only one node each
   * @return a Dag.
   */
  static List<Dag<JobExecutionPlan>> buildDagList(int numDags, String proxyUser, Config additionalConfig) throws URISyntaxException{
    List<Dag<JobExecutionPlan>> dagList = new ArrayList<>();
    for (int i = 0; i < numDags; i++) {
      dagList.add(buildDag(Integer.toString(i), System.currentTimeMillis(), DagProcessingEngine.FailureOption.FINISH_ALL_POSSIBLE.name(), 1,
          proxyUser, additionalConfig));
    }
    return dagList;
  }

  /**
   * Create a {@link Dag <JobExecutionPlan>}.
   * @return a Dag.
   */
  static Dag<JobExecutionPlan> buildDag(String id, Long flowExecutionId, String flowFailureOption, boolean flag)
      throws URISyntaxException {
    int numNodes = (flag) ? 3 : 5;
    return buildDag(id, flowExecutionId, flowFailureOption, numNodes);
  }

  static Dag<JobExecutionPlan> buildDag(String id, Long flowExecutionId, String flowFailureOption, int numNodes)
      throws URISyntaxException {
    return buildDag(id, flowExecutionId, flowFailureOption, numNodes, "testUser", ConfigFactory.empty());
  }

  public static Dag<JobExecutionPlan> buildDag(String id, Long flowExecutionId, String flowFailureOption, int numNodes, String proxyUser, Config additionalConfig)
      throws URISyntaxException {
    if (additionalConfig.hasPath(ConfigurationKeys.JOB_NAME_KEY)) {
      throw new RuntimeException("Please do not set " + ConfigurationKeys.JOB_NAME_KEY + " because this method is "
          + "using hard coded job names in setting " + ConfigurationKeys.JOB_DEPENDENCIES);
    }

    List<JobExecutionPlan> jobExecutionPlans = new ArrayList<>();

    for (int i = 0; i < numNodes; i++) {
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
      if ((i == 1) || (i == 2)) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job0"));
      } else if (i == 3) {
        jobConfig = jobConfig.withValue(ConfigurationKeys.JOB_DEPENDENCIES, ConfigValueFactory.fromAnyRef("job1"));
      } else if (i == 4) {
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
