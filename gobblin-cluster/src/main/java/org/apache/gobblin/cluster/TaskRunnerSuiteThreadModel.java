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

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.helix.task.TaskFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Service;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.StandardMetricsBridge;
import org.apache.gobblin.runtime.TaskExecutor;
import org.apache.gobblin.runtime.TaskStateTracker;
import org.apache.gobblin.runtime.services.JMXReportingService;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PathUtils;

/**
 * A sub-type of {@link TaskRunnerSuiteBase} suite which runs tasks in a thread pool.
 */
class TaskRunnerSuiteThreadModel extends TaskRunnerSuiteBase {
  private final TaskExecutor taskExecutor;
  private final GobblinTaskRunnerMetrics.TaskExecutionMetrics taskExecutionMetrics;

  TaskRunnerSuiteThreadModel(TaskRunnerSuiteBase.Builder builder) {
    super(builder);

    // initialize task related metrics
    this.taskExecutor = new TaskExecutor(ConfigUtils.configToProperties(builder.getConfig()));
    this.taskExecutionMetrics = new GobblinTaskRunnerMetrics.TaskExecutionMetrics(taskExecutor, metricContext);
    this.taskFactory = generateTaskFactory(taskExecutor, builder);
    this.jobFactory = new GobblinHelixJobFactory(builder, this.metricContext);
  }

  @Override
  protected Collection<StandardMetricsBridge.StandardMetrics> getMetricsCollection() {
    return ImmutableList.of(this.taskExecutionMetrics, this.jobFactory.getJobTaskMetrics(), this.jobFactory.getLauncherMetrics());
  }

  @Override
  protected Map<String, TaskFactory> getTaskFactoryMap() {
    Map<String, TaskFactory> taskFactoryMap = Maps.newHashMap();
    taskFactoryMap.put(GobblinTaskRunner.GOBBLIN_TASK_FACTORY_NAME, taskFactory);
    taskFactoryMap.put(GobblinTaskRunner.GOBBLIN_JOB_FACTORY_NAME, jobFactory);
    return taskFactoryMap;
  }

  @Override
  protected List<Service> getServices() {
    return this.services;
  }

  private GobblinHelixTaskFactory generateTaskFactory(TaskExecutor taskExecutor, Builder builder) {
    Properties properties = ConfigUtils.configToProperties(builder.getConfig());
    URI rootPathUri = PathUtils.getRootPath(builder.getAppWorkPath()).toUri();
    Config stateStoreJobConfig = ConfigUtils.propertiesToConfig(properties)
        .withValue(ConfigurationKeys.STATE_STORE_FS_URI_KEY,
            ConfigValueFactory.fromAnyRef(rootPathUri.toString()));

    TaskStateTracker taskStateTracker = new GobblinHelixTaskStateTracker(properties);

    services.add(taskExecutor);
    services.add(taskStateTracker);
    services.add(new JMXReportingService(
        ImmutableMap.of("task.executor", taskExecutor.getTaskExecutorQueueMetricSet())));

    return new GobblinHelixTaskFactory(builder,
                                       taskExecutor,
                                       taskStateTracker,
                                       stateStoreJobConfig);
  }
}
