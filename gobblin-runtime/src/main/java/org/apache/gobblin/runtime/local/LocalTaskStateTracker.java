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

package org.apache.gobblin.runtime.local;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.runtime.AbstractTaskStateTracker;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.NewTaskCompletionEvent;
import org.apache.gobblin.runtime.Task;
import org.apache.gobblin.runtime.TaskExecutor;


/**
 * A concrete extension to {@link AbstractTaskStateTracker} for standalone mode.
 *
 * @author Yinan Li
 */
public class LocalTaskStateTracker extends AbstractTaskStateTracker {

  private static final Logger LOG = LoggerFactory.getLogger(LocalTaskStateTracker.class);

  private final JobState jobState;

  // This is used to retry failed tasks
  private final TaskExecutor taskExecutor;

  // Mapping between tasks and the task state reporters associated with them
  private final Map<String, ScheduledFuture<?>> scheduledReporters = Maps.newHashMap();

  private final EventBus eventBus;

  // Maximum number of task retries allowed
  private final int maxTaskRetries;

  public LocalTaskStateTracker(Properties properties, JobState jobState, TaskExecutor taskExecutor,
      EventBus eventBus) {
    super(properties, LOG);

    this.jobState = jobState;
    this.taskExecutor = taskExecutor;
    this.eventBus = eventBus;
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
  public void onTaskRunCompletion(Task task) {
    try {
      // Check the task state and handle task retry if task failed and
      // it has not reached the maximum number of retries
      WorkUnitState.WorkingState state = task.getTaskState().getWorkingState();
      if (state == WorkUnitState.WorkingState.FAILED && task.getRetryCount() < this.maxTaskRetries) {
        this.taskExecutor.retry(task);
        return;
      }
    } catch (Throwable t) {
      LOG.error("Failed to process a task completion callback", t);
    }
    // Mark the completion of this task
    task.markTaskCompletion();
  }

  @Override
  public void onTaskCommitCompletion(Task task) {
    try {
      if (GobblinMetrics.isEnabled(task.getTaskState().getWorkunit())) {
        // Update record-level metrics after the task is done
        task.updateRecordMetrics();
        task.updateByteMetrics();
      }

      // Cancel the task state reporter associated with this task. The reporter might
      // not be found for the given task because the task fails before the task is
      // registered. So we need to make sure the reporter exists before calling cancel.
      if (this.scheduledReporters.containsKey(task.getTaskId())) {
        this.scheduledReporters.remove(task.getTaskId()).cancel(false);
      }
    } catch (Throwable t) {
      LOG.error("Failed to process a task completion callback", t);
    }

    // Add the TaskState of the completed task to the JobState so when the control
    // returns to the launcher, it sees the TaskStates of all completed tasks.
    this.jobState.addTaskState(task.getTaskState());

    // Notify the listeners for the completion of the task
    this.eventBus.post(new NewTaskCompletionEvent(ImmutableList.of(task.getTaskState())));

    // At this point, the task is considered being completed.
    LOG.info(String.format("Task %s completed in %dms with state %s", task.getTaskId(),
        task.getTaskState().getTaskDuration(), task.getTaskState().getWorkingState()));
  }
}
