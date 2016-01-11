/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.yarn;

import java.util.Map;
import java.util.Properties;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;

import org.apache.helix.HelixManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import gobblin.metrics.GobblinMetrics;
import gobblin.runtime.AbstractTaskStateTracker;
import gobblin.runtime.Task;


/**
 * A concrete extension to {@link AbstractTaskStateTracker} for Gobblin on Yarn.
 *
 * <p>
 *   This class is currently still primitive and will be enhanced once we add more monitoring in place.
 * </p>
 *
 * @author Yinan Li
 */
public class GobblinHelixTaskStateTracker extends AbstractTaskStateTracker {

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinHelixTaskStateTracker.class);

  private final HelixManager helixManager;

  // Mapping between tasks and the task state reporters associated with them
  private final Map<String, ScheduledFuture<?>> scheduledReporters = Maps.newHashMap();

  public GobblinHelixTaskStateTracker(Properties properties, HelixManager helixManager) {
    super(properties, LOGGER);
    this.helixManager = helixManager;
  }

  @Override
  public void registerNewTask(Task task) {
    try {
      this.scheduledReporters.put(task.getTaskId(), scheduleTaskMetricsUpdater(new YarnTaskMetricsUpdater(task), task));
    } catch (RejectedExecutionException ree) {
      LOGGER.error(String.format("Scheduling of task state reporter for task %s was rejected", task.getTaskId()));
    }
  }

  @Override
  public void onTaskCompletion(Task task) {
    try {
      if (GobblinMetrics.isEnabled(task.getTaskState().getWorkunit())) {
        // Update record-level metrics after the task is done
        task.updateRecordMetrics();
        task.updateByteMetrics();
      }

      // Cancel the task state reporter associated with this task. The reporter might
      // not be found  for the given task because the task fails before the task is
      // registered. So we need to make sure the reporter exists before calling cancel.
      if (this.scheduledReporters.containsKey(task.getTaskId())) {
        this.scheduledReporters.remove(task.getTaskId()).cancel(false);
      }
    } finally {
      task.markTaskCompletion();
    }

    LOGGER.info(String.format("Task %s completed in %dms with state %s",
        task.getTaskId(), task.getTaskState().getTaskDuration(), task.getTaskState().getWorkingState()));
  }

  /**
   * An extension to {@link gobblin.runtime.AbstractTaskStateTracker.TaskMetricsUpdater}.
   */
  class YarnTaskMetricsUpdater extends AbstractTaskStateTracker.TaskMetricsUpdater {

    public YarnTaskMetricsUpdater(Task task) {
      super(task);
    }
  }
}
