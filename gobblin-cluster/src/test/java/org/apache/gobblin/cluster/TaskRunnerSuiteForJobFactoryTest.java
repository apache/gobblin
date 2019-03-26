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

package org.apache.gobblin.cluster;

import java.util.Map;

import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskFactory;
import org.testng.Assert;

import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.cluster.suite.IntegrationJobFactorySuite;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.MetricContext;


public class TaskRunnerSuiteForJobFactoryTest extends TaskRunnerSuiteThreadModel {

  private TaskFactory testJobFactory;

  public TaskRunnerSuiteForJobFactoryTest(IntegrationJobFactorySuite.TestJobFactorySuiteBuilder builder) {
    super(builder);
    this.testJobFactory = new TestJobFactory(builder, this.metricContext);
  }

  @Override
  protected Map<String, TaskFactory> getTaskFactoryMap() {
    Map<String, TaskFactory> taskFactoryMap = Maps.newHashMap();
    taskFactoryMap.put(GobblinTaskRunner.GOBBLIN_TASK_FACTORY_NAME, taskFactory);
    taskFactoryMap.put(GobblinTaskRunner.GOBBLIN_JOB_FACTORY_NAME, testJobFactory);
    return taskFactoryMap;
  }

  public class TestJobFactory extends GobblinHelixJobFactory {
    public TestJobFactory(IntegrationJobFactorySuite.TestJobFactorySuiteBuilder builder, MetricContext metricContext) {
      super (builder, metricContext);
      this.builder = builder;
    }

    @Override
    public Task createNewTask(TaskCallbackContext context) {
      return new TestHelixJobTask(context,
          jobsMapping,
          builder,
          new GobblinHelixJobLauncherMetrics("launcherInJobFactory", metricContext, 5),
          new GobblinHelixJobTask.GobblinHelixJobTaskMetrics(metricContext, 5),
          new GobblinHelixMetrics("helixMetricsInJobFactory", metricContext, 5));
    }
  }

  public class TestHelixJobTask extends GobblinHelixJobTask {
    public TestHelixJobTask(TaskCallbackContext context,
                            HelixJobsMapping jobsMapping,
                            TaskRunnerSuiteBase.Builder builder,
                            GobblinHelixJobLauncherMetrics launcherMetrics,
                            GobblinHelixJobTaskMetrics jobTaskMetrics,
                            GobblinHelixMetrics helixMetrics) {
      super(context,
            jobsMapping,
            builder,
            launcherMetrics,
            jobTaskMetrics,
            helixMetrics);
    }
  }

  @Slf4j
  public static class TestDistributedExecutionLauncher extends GobblinHelixDistributeJobExecutionLauncher {

    public TestDistributedExecutionLauncher(GobblinHelixDistributeJobExecutionLauncher.Builder builder) throws Exception {
      super(builder);
    }

    protected DistributeJobResult getResultFromUserContent() {
      DistributeJobResult rst = super.getResultFromUserContent();
      String jobName = this.jobPlanningProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
      try {
        Assert.assertFalse(this.jobsMapping.getPlanningJobId(jobName).isPresent());
      } catch (Exception e) {
        Assert.fail(e.toString());
      }
      IntegrationJobFactorySuite.completed.set(true);
      return rst;
    }

    @Alias("TestDistributedExecutionLauncherBuilder")
    public static class Builder extends GobblinHelixDistributeJobExecutionLauncher.Builder {
      public TestDistributedExecutionLauncher build() throws Exception {
        return new TestDistributedExecutionLauncher(this);
      }
    }
  }
}
