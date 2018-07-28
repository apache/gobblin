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

import java.util.List;
import java.util.Map;

import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskFactory;
import org.testng.Assert;

import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.cluster.suite.IntegrationJobTagSuite;
import org.apache.gobblin.configuration.ConfigurationKeys;

/**
 * A special {@link TaskRunnerSuiteBase} which can verify if the worker gets the correct jobs based on the tag association.
 */
@Slf4j
public class TaskRunnerSuiteForJobTagTest extends TaskRunnerSuiteThreadModel {
  private TaskFactory jobTagTestFactory;
  private String instanceName;

  public TaskRunnerSuiteForJobTagTest(IntegrationJobTagSuite.JobTagTaskRunnerSuiteBuilder builder) {
    super(builder);
    this.instanceName = builder.getInstanceName();
    this.jobTagTestFactory = new JobTagTestFactory(this.taskFactory);
  }

  @Override
  protected Map<String, TaskFactory> getTaskFactoryMap() {
    Map<String, TaskFactory> taskFactoryMap = Maps.newHashMap();
    taskFactoryMap.put(GobblinTaskRunner.GOBBLIN_TASK_FACTORY_NAME, jobTagTestFactory);
    return taskFactoryMap;
  }


  public class JobTagTestFactory implements TaskFactory {
    private TaskFactory factory;
    public JobTagTestFactory(TaskFactory factory) {
      this.factory = factory;
    }

    @Override
    public Task createNewTask(TaskCallbackContext context) {
      Map<String, String> configMap = context.getTaskConfig().getConfigMap();
      String jobName = configMap.get(ConfigurationKeys.JOB_NAME_KEY);
      List<String> allowedJobNames = IntegrationJobTagSuite.EXPECTED_JOB_NAMES.get(TaskRunnerSuiteForJobTagTest.this.instanceName);
      if (allowedJobNames.contains(jobName)) {
        log.info("{} has job name {}", instanceName, jobName);
      } else {
        Assert.fail(instanceName + " should not receive " + jobName);
      }
      return this.factory.createNewTask(context);
    }
  }
}
