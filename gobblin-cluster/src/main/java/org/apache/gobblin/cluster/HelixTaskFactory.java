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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.google.common.base.Optional;
import com.typesafe.config.Config;

import org.apache.gobblin.util.GobblinProcessBuilder;
import org.apache.gobblin.util.SystemPropertiesWrapper;


public class HelixTaskFactory implements TaskFactory {

  private static final Logger logger = LoggerFactory.getLogger(HelixTaskFactory.class);

  private static final String GOBBLIN_CLUSTER_NEW_HELIX_TASK_COUNTER =
      "gobblin.cluster.new.helix.task";

  private final Optional<ContainerMetrics> containerMetrics;

  /**
   * A {@link Counter} to count the number of new {@link GobblinHelixTask}s that are created.
   */
  private final Optional<Counter> newTasksCounter;
  private final SingleTaskLauncher launcher;

  public HelixTaskFactory(Optional<ContainerMetrics> containerMetrics, Path clusterConfPath, Config sysConfig) {
    this.containerMetrics = containerMetrics;
    if (this.containerMetrics.isPresent()) {
      this.newTasksCounter = Optional
          .of(this.containerMetrics.get().getCounter(GOBBLIN_CLUSTER_NEW_HELIX_TASK_COUNTER));
    } else {
      this.newTasksCounter = Optional.absent();
    }
    launcher = new SingleTaskLauncher(new GobblinProcessBuilder(), new SystemPropertiesWrapper(),
        clusterConfPath, sysConfig);
  }

  @Override
  public Task createNewTask(TaskCallbackContext context) {
    try {
      if (this.newTasksCounter.isPresent()) {
        this.newTasksCounter.get().inc();
      }
      Map<String, String> configMap = context.getTaskConfig().getConfigMap();
      return new SingleHelixTask(this.launcher, configMap);
    } catch (IOException ioe) {
      final String msg = "Failed to create a new SingleHelixTask";
      logger.error(msg, ioe);
      throw new GobblinClusterException(msg, ioe);
    }
  }
}

