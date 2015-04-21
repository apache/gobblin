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

import gobblin.metrics.Tag;
import gobblin.runtime.TaskState;


public class TaskMetrics extends GobblinMetrics {

  protected String jobId;

  protected TaskMetrics(TaskState task) {
    super(name(task));
    this.jobId = task.getJobId();
    JobMetrics parentJobContext = JobMetrics.get(null, task.getJobId());
    List<Tag<?>> tags = new ArrayList<Tag<?>>();
    tags.add(new Tag<String>("taskId", task.getTaskId()));
    this.metricContext = parentJobContext.getMetricContext().
        childBuilder("gobblin.metrics." + task.getJobId() + "." + task.getTaskId()).
        addTags(tags).build();
  }

  public synchronized static TaskMetrics get(TaskState task) {
    if(!METRICS_MAP.containsKey(name(task))) {
      METRICS_MAP.putIfAbsent(name(task), new TaskMetrics(task));
    }
    return (TaskMetrics)METRICS_MAP.get(name(task));
  }

  public static String name(TaskState task) {
    return task.getJobId() + ":" + task.getTaskId();
  }

}
