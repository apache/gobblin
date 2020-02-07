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

import org.apache.hadoop.fs.Path;
import org.apache.helix.HelixManager;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.typesafe.config.Config;

import lombok.Getter;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.runtime.TaskExecutor;
import org.apache.gobblin.runtime.TaskStateTracker;
import org.apache.gobblin.runtime.util.StateStores;
import org.apache.gobblin.util.ConfigUtils;


/**
 * An implementation of Helix's {@link TaskFactory} for {@link GobblinHelixTask}s.
 *
 * @author Yinan Li
 */
@Alpha
public class GobblinHelixTaskFactory implements TaskFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinHelixTaskFactory.class);

  private static final String GOBBLIN_CLUSTER_NEW_HELIX_TASK_COUNTER = "gobblin.cluster.new.helix.task";

  private final Optional<ContainerMetrics> containerMetrics;
  private final HelixManager helixManager;
  private Optional<TaskDriver> taskDriver;
  private TaskRunnerSuiteBase.Builder builder;

  /**
   * A {@link Counter} to count the number of new {@link GobblinHelixTask}s that are created.
   */
  private final Optional<Counter> newTasksCounter;
  @Getter
  private final TaskExecutor taskExecutor;
  @Getter
  private final GobblinHelixTaskMetrics taskMetrics;
  private final TaskStateTracker taskStateTracker;
  private final Path appWorkDir;
  private final StateStores stateStores;
  private final TaskAttemptBuilder taskAttemptBuilder;

  public GobblinHelixTaskFactory(TaskRunnerSuiteBase.Builder builder,
      MetricContext metricContext,
      TaskStateTracker taskStateTracker,
      Config stateStoreConfig) {
    this(builder, metricContext, taskStateTracker, stateStoreConfig, Optional.absent());
  }

  /**
   * Constructor that allows passing in a {@link TaskDriver} instance. This constructor is exposed purely for
   * testing purposes to allow passing in a mock {@link TaskDriver} (e.g. see GobblinHelixTaskTest). For other cases, use
   * the constructor {@link #GobblinHelixTaskFactory(TaskRunnerSuiteBase.Builder, MetricContext, TaskStateTracker, Config)}.
   */
  @VisibleForTesting
  public GobblinHelixTaskFactory(TaskRunnerSuiteBase.Builder builder,
                                 MetricContext metricContext,
                                 TaskStateTracker taskStateTracker,
                                 Config stateStoreConfig,
                                 Optional<TaskDriver> taskDriver) {

    // initialize task related metrics
    int windowSizeInMin = ConfigUtils.getInt(builder.getConfig(),
        ConfigurationKeys.METRIC_TIMER_WINDOW_SIZE_IN_MINUTES,
        ConfigurationKeys.DEFAULT_METRIC_TIMER_WINDOW_SIZE_IN_MINUTES);
    this.taskExecutor = new TaskExecutor(ConfigUtils.configToProperties(builder.getConfig()));
    this.taskMetrics = new GobblinHelixTaskMetrics(taskExecutor, metricContext, windowSizeInMin);

    this.builder = builder;
    this.containerMetrics = builder.getContainerMetrics();
    this.helixManager = builder.getJobHelixManager();
    if (this.containerMetrics.isPresent()) {
      this.newTasksCounter = Optional.of(this.containerMetrics.get().getCounter(GOBBLIN_CLUSTER_NEW_HELIX_TASK_COUNTER));
    } else {
      this.newTasksCounter = Optional.absent();
    }
    this.taskStateTracker = taskStateTracker;

    this.appWorkDir = builder.getAppWorkPath();
    this.stateStores = new StateStores(stateStoreConfig,
        appWorkDir, GobblinClusterConfigurationKeys.OUTPUT_TASK_STATE_DIR_NAME,
        appWorkDir, GobblinClusterConfigurationKeys.INPUT_WORK_UNIT_DIR_NAME,
        appWorkDir,
        GobblinClusterConfigurationKeys.JOB_STATE_DIR_NAME);
    this.taskAttemptBuilder = createTaskAttemptBuilder();
    this.taskDriver = taskDriver;
  }

  private TaskAttemptBuilder createTaskAttemptBuilder() {
    TaskAttemptBuilder builder = new TaskAttemptBuilder(this.taskStateTracker, this.taskExecutor);
    builder.setContainerId(this.helixManager.getInstanceName());
    builder.setTaskStateStore(this.stateStores.getTaskStateStore());

    return builder;
  }

  @Override
  public Task createNewTask(TaskCallbackContext context) {
    if (this.newTasksCounter.isPresent()) {
      this.newTasksCounter.get().inc();
    }

    if (!this.taskDriver.isPresent()) {
      this.taskDriver = Optional.of(new TaskDriver(context.getManager()));
    }

    return new GobblinHelixTask(builder, context, this.taskAttemptBuilder, this.stateStores, this.taskMetrics, this.taskDriver.get());
  }
}
