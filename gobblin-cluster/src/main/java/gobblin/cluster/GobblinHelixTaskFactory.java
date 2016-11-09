/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.cluster;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;

import gobblin.annotation.Alpha;
import gobblin.runtime.TaskExecutor;
import gobblin.runtime.TaskStateTracker;


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

  /**
   * A {@link Counter} to count the number of new {@link GobblinHelixTask}s that are created.
   */
  private final Optional<Counter> newTasksCounter;
  private final TaskExecutor taskExecutor;
  private final TaskStateTracker taskStateTracker;
  private final FileSystem fs;
  private final Path appWorkDir;

  public GobblinHelixTaskFactory(Optional<ContainerMetrics> containerMetrics, TaskExecutor taskExecutor,
      TaskStateTracker taskStateTracker, FileSystem fs, Path appWorkDir) {
    this.containerMetrics = containerMetrics;
    if (this.containerMetrics.isPresent()) {
      this.newTasksCounter = Optional.of(this.containerMetrics.get().getCounter(GOBBLIN_CLUSTER_NEW_HELIX_TASK_COUNTER));
    } else {
      this.newTasksCounter = Optional.absent();
    }
    this.taskExecutor = taskExecutor;
    this.taskStateTracker = taskStateTracker;
    this.fs = fs;
    this.appWorkDir = appWorkDir;
  }

  @Override
  public Task createNewTask(TaskCallbackContext context) {
    try {
      if (this.newTasksCounter.isPresent()) {
        this.newTasksCounter.get().inc();
      }
      return new GobblinHelixTask(context, this.containerMetrics, this.taskExecutor, this.taskStateTracker, this.fs,
          this.appWorkDir);
    } catch (IOException ioe) {
      LOGGER.error("Failed to create a new GobblinHelixTask", ioe);
      throw Throwables.propagate(ioe);
    }
  }
}
