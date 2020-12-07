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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Service;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.instrumented.StandardMetricsBridge;

/**
 * A sub-type of {@link TaskRunnerSuiteBase} suite which runs all tasks in separate JVMs.
 *
 * Please refer to {@link HelixTaskFactory#createNewTask(TaskCallbackContext)}.
 */
@Slf4j
class TaskRunnerSuiteProcessModel extends TaskRunnerSuiteBase {
  private final HelixTaskFactory taskFactory;
  TaskRunnerSuiteProcessModel(TaskRunnerSuiteBase.Builder builder) {
    super(builder);
    log.info("Running a task in a separate process is enabled.");
    taskFactory = new HelixTaskFactory(builder.getContainerMetrics(),
        GobblinTaskRunner.CLUSTER_CONF_PATH,
        builder.getConfig());
  }

  @Override
  protected Collection<StandardMetricsBridge.StandardMetrics> getMetricsCollection() {
    return ImmutableList.of();
  }

  @Override
  protected Map<String, TaskFactory> getTaskFactoryMap() {
    Map<String, TaskFactory> taskFactoryMap = Maps.newHashMap();

    taskFactoryMap.put(GobblinTaskRunner.GOBBLIN_TASK_FACTORY_NAME, taskFactory);

    //TODO: taskFactoryMap.put(GOBBLIN_JOB_FACTORY_NAME, jobFactory);
    return taskFactoryMap;
  }

  @Override
  protected List<Service> getServices() {
    return this.services;
  }
}
