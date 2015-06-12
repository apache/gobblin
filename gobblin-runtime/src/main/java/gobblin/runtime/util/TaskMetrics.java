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

package gobblin.runtime.util;

import java.util.List;

import com.google.common.collect.Lists;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.GobblinMetricsRegistry;
import gobblin.metrics.MetricContext;
import gobblin.metrics.Tag;
import gobblin.runtime.TaskState;


/**
 * An extension to {@link GobblinMetrics} specifically for tasks.
 *
 * @author ynli
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
  public synchronized static TaskMetrics get(TaskState taskState) {
    GobblinMetricsRegistry registry = GobblinMetricsRegistry.getInstance();
    String name = name(taskState);
    if (!registry.containsKey(name)) {
      registry.putIfAbsent(name, new TaskMetrics(taskState));
    }
    return (TaskMetrics) registry.get(name);
  }

  /**
   * Remove the {@link TaskMetrics} instance for the task with the given {@link TaskMetrics} instance.
   *
   * @param taskState the given {@link TaskState} instance
   */
  public synchronized static void remove(TaskState taskState) {
    remove(name(taskState));
  }

  private static String name(TaskState taskState) {
    return "gobblin.metrics." + taskState.getJobId() + "." + taskState.getTaskId();
  }

  protected static List<Tag<?>> tagsForTask(TaskState taskState) {
    List<Tag<?>> tags = Lists.newArrayList();
    tags.add(new Tag<String>("taskId", taskState.getTaskId()));
    tags.addAll(getCustomTagsFromState(taskState));
    return tags;
  }

  private static MetricContext parentContextForTask(TaskState taskState) {
    return JobMetrics.get(taskState.getProp(ConfigurationKeys.JOB_NAME_KEY), taskState.getJobId())
        .getMetricContext();
  }

}
