/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime.local;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import gobblin.configuration.WorkUnitState;
import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.JobMetrics;
import gobblin.runtime.AbstractTaskStateTracker;
import gobblin.runtime.Task;
import gobblin.runtime.TaskExecutor;


/**
 * A concrete extension to {@link AbstractTaskStateTracker}  for standalone mode.
 *
 * TODO: rename this to LocalTaskStateTracker once {@link LocalTaskStateTracker} is fully retired.
 *
 * @author ynli
 */
public class LocalTaskStateTracker2 extends AbstractTaskStateTracker {

  private static final Logger LOG = LoggerFactory.getLogger(LocalTaskStateTracker2.class);

  // This is used to retry failed tasks
  private final TaskExecutor taskExecutor;

  // Mapping between tasks and the task state reporters associated with them
  private final Map<String, ScheduledFuture<?>> scheduledReporters = Maps.newHashMap();

  // Maximum number of task retries allowed
  private final int maxTaskRetries;

  public LocalTaskStateTracker2(Properties properties, TaskExecutor taskExecutor) {
    super(properties, LOG);
    this.taskExecutor = taskExecutor;
    this.maxTaskRetries = Integer.parseInt(properties.getProperty(
        ConfigurationKeys.MAX_TASK_RETRIES_KEY, Integer.toString(ConfigurationKeys.DEFAULT_MAX_TASK_RETRIES)));
  }

  @Override
  public void registerNewTask(Task task) {
    try {
      this.scheduledReporters.put(task.getTaskId(), scheduleTaskMetricsUpdater(new TaskMetricsUpdater(task), task));
    } catch (RejectedExecutionException ree) {
      LOG.error(String.format("Scheduling of task state reporter for task %s was rejected", task.getTaskId()));
    }
  }

  @Override
  public void onTaskCompletion(Task task) {
    if (JobMetrics.isEnabled(task.getTaskState().getWorkunit())) {
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

    // Check the task state and handle task retry if task failed and
    // it has not reached the maximum number of retries
    WorkUnitState.WorkingState state = task.getTaskState().getWorkingState();
    if (state == WorkUnitState.WorkingState.FAILED && task.getRetryCount() < this.maxTaskRetries) {
      this.taskExecutor.retry(task);
      return;
    }

    // Mark the completion of this task
    task.markTaskCompletion();

    // At this point, the task is considered being completed.
    LOG.info(String
        .format("Task %s completed in %dms with state %s", task.getTaskId(), task.getTaskState().getTaskDuration(),
            state));
  }
}
