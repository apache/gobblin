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

package org.apache.gobblin.runtime.util;

import java.util.List;
import java.util.concurrent.Callable;

import com.google.common.collect.Lists;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.TaskEvent;
import org.apache.gobblin.runtime.TaskState;


/**
 * An extension to {@link GobblinMetrics} specifically for tasks.
 *
 * @author Yinan Li
 */
public class TaskMetrics extends GobblinMetrics {

  protected final String jobId;

  protected TaskMetrics(TaskState taskState) {
    super(name(taskState), parentContextForTask(taskState), tagsForTask(taskState));
    this.jobId = taskState.getJobId();
  }

  /**
   * Get a {@link TaskMetrics} instance for the task with the given {@link TaskState} instance.
   *
   * @param taskState the given {@link TaskState} instance
   * @return a {@link TaskMetrics} instance
   */
  public static TaskMetrics get(final TaskState taskState) {
    return (TaskMetrics) GOBBLIN_METRICS_REGISTRY.getOrDefault(name(taskState), new Callable<GobblinMetrics>() {
      @Override
      public GobblinMetrics call() throws Exception {
        return new TaskMetrics(taskState);
      }
    });
  }

  /**
   * Remove the {@link TaskMetrics} instance for the task with the given {@link TaskMetrics} instance.
   *
   * @param taskState the given {@link TaskState} instance
   */
  public static void remove(TaskState taskState) {
    remove(name(taskState));
  }

  private static String name(TaskState taskState) {
    return "gobblin.metrics." + taskState.getJobId() + "." + taskState.getTaskId();
  }

  protected static List<Tag<?>> tagsForTask(TaskState taskState) {
    List<Tag<?>> tags = Lists.newArrayList();
    tags.add(new Tag<>(TaskEvent.METADATA_TASK_ID, taskState.getTaskId()));
    tags.add(new Tag<>(TaskEvent.METADATA_TASK_ATTEMPT_ID, taskState.getTaskAttemptId().or("")));
    tags.add(new Tag<>(ConfigurationKeys.DATASET_URN_KEY,
        taskState.getProp(ConfigurationKeys.DATASET_URN_KEY, ConfigurationKeys.DEFAULT_DATASET_URN)));
    tags.addAll(getCustomTagsFromState(taskState));
    return tags;
  }

  private static MetricContext parentContextForTask(TaskState taskState) {
    return JobMetrics.get(taskState.getProp(ConfigurationKeys.JOB_NAME_KEY), taskState.getJobId()).getMetricContext();
  }

  public static String taskInstanceRemoved(String metricName) {
    final String METRIC_SEPARATOR = "_";

    String[] taskIdTokens = metricName.split(METRIC_SEPARATOR);
    StringBuilder sb = new StringBuilder(taskIdTokens[0]);
    // chopping taskId and jobId from metric name
    for (int i = 1; i < taskIdTokens.length - 2; i++) {
      sb.append(METRIC_SEPARATOR).append(taskIdTokens[i]);
    }
    return sb.toString();
  }
}
