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
import java.util.Properties;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.runtime.AbstractTaskStateTracker;
import org.apache.gobblin.runtime.Task;


/**
 * A concrete extension to {@link AbstractTaskStateTracker} for a Gobblin Cluster.
 *
 * <p>
 *   This class is currently still primitive and will be enhanced once we add more monitoring in place.
 * </p>
 *
 * @author Yinan Li
 */
@Slf4j
public class GobblinHelixTaskStateTracker extends AbstractTaskStateTracker {
  @VisibleForTesting
  static final String IS_TASK_METRICS_SCHEDULING_FAILURE_FATAL = "helixTaskTracker.isNewTaskRegFailureFatal";
  private static final String DEFAULT_TASK_METRICS_SCHEDULING_FAILURE_FATAL = "false";

  // Mapping between tasks and the task state reporters associated with them
  private final Map<String, ScheduledFuture<?>> scheduledReporters = Maps.newHashMap();
  private boolean isNewTaskRegFailureFatal;

  public GobblinHelixTaskStateTracker(Properties properties) {
    super(properties, log);
    isNewTaskRegFailureFatal = Boolean.parseBoolean(properties.getProperty(IS_TASK_METRICS_SCHEDULING_FAILURE_FATAL,
        DEFAULT_TASK_METRICS_SCHEDULING_FAILURE_FATAL));
  }

  @Override
  public void registerNewTask(Task task) {
    try {
      if (GobblinMetrics.isEnabled(task.getTaskState().getWorkunit())) {
        this.scheduledReporters.put(task.getTaskId(), scheduleTaskMetricsUpdater(new TaskMetricsUpdater(task), task));
      }
    } catch (RejectedExecutionException ree) {
      // Propagate the exception to caller that has full control of the life-cycle of a helix task.
      log.error(String.format("Scheduling of task state reporter for task %s was rejected", task.getTaskId()));
      if (isNewTaskRegFailureFatal) {
        Throwables.propagate(ree);
      }
    } catch (Throwable t) {
      String errorMsg = "Failure occurred for scheduling task state reporter, ";
      if (isNewTaskRegFailureFatal) {
        throw new RuntimeException(errorMsg, t);
      } else {
        log.error(errorMsg, t);
      }
    }
  }

  @Override
  public void onTaskRunCompletion(Task task) {
    task.markTaskCompletion();
  }

  @Override
  public void onTaskCommitCompletion(Task task) {
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

    log.info(String
        .format("Task %s completed in %dms with state %s", task.getTaskId(), task.getTaskState().getTaskDuration(),
            task.getTaskState().getWorkingState()));
  }

  /**
   * An extension to {@link AbstractTaskStateTracker.TaskMetricsUpdater}.
   */
  class TaskMetricsUpdater extends AbstractTaskStateTracker.TaskMetricsUpdater {

    public TaskMetricsUpdater(Task task) {
      super(task);
    }
  }
}
