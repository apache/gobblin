/*
 * (c) 2014 LinkedIn Corp. All rights reserved.
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

import java.util.ArrayList;
import java.util.List;

import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.GobblinMetricsRegistry;
import gobblin.metrics.MetricContext;
import gobblin.metrics.Tag;
import gobblin.runtime.TaskState;


public class TaskMetrics extends GobblinMetrics {

  protected String jobId;

  protected TaskMetrics(TaskState task) {
    super(name(task), parentContextForTask(task), tagsForTask(task));
    this.jobId = task.getJobId();
  }

  public synchronized static TaskMetrics get(TaskState task) {
    GobblinMetricsRegistry registry = GobblinMetricsRegistry.getInstance();
    if (!registry.containsKey(name(task))) {
      registry.putIfAbsent(name(task), new TaskMetrics(task));
    }
    return (TaskMetrics)registry.get(name(task));
  }

  public static String name(TaskState task) {
    return "gobblin.metrics." + task.getJobId() + "." + task.getTaskId();
  }

  private static List<Tag<?>> tagsForTask(TaskState task) {
    List<Tag<?>> tags = new ArrayList<Tag<?>>();
    tags.add(new Tag<String>("taskId", task.getTaskId()));
    return tags;
  }

  private static MetricContext parentContextForTask(TaskState task) {
    return JobMetrics.get(null, task.getJobId()).getMetricContext();
  }

}
