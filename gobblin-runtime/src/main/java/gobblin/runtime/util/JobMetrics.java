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

import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.GobblinMetricsRegistry;
import gobblin.metrics.Tag;
import gobblin.runtime.JobState;
import gobblin.runtime.TaskState;


/**
 * An extension to {@link GobblinMetrics} specifically for job runs.
 *
 * @author ynli
 */
public class JobMetrics extends GobblinMetrics {

  protected final String jobName;

  protected JobMetrics(JobState job) {
    super(name(job), null, tagsForJob(job));
    this.jobName = job.getJobName();
  }

  /**
   * Get a new {@link GobblinMetrics} instance for a given job.
   *
   * @param jobName job name
   * @param jobId job ID
   * @return a new {@link GobblinMetrics} instance for the given job
   */
  public static JobMetrics get(String jobName, String jobId) {
    return get(new JobState(jobName, jobId));
  }

  /**
   * Get a {@link JobMetrics} instance for the job with the given {@link JobState} instance.
   *
   * @param jobState the given {@link JobState} instance
   * @return a {@link JobMetrics} instance
   */
  public static synchronized JobMetrics get(JobState jobState) {
    GobblinMetricsRegistry registry = GobblinMetricsRegistry.getInstance();
    String name = name(jobState);
    if (!registry.containsKey(name)) {
      registry.putIfAbsent(name, new JobMetrics(jobState));
    }
    return (JobMetrics) registry.get(name);
  }

  /**
   * Remove the {@link JobMetrics} instance for the job with the given {@link JobState} instance.
   *
   * <p>
   *   Removing a {@link JobMetrics} instance for a job will also remove the {@link TaskMetrics}s
   *   of every tasks of the job.
   * </p>
   * @param jobState the given {@link JobState} instance
   */
  public synchronized static void remove(JobState jobState) {
    remove(name(jobState));
    for (TaskState taskState : jobState.getTaskStates()) {
      TaskMetrics.remove(taskState);
    }
  }

  private static String name(JobState jobState) {
    return "gobblin.metrics." + jobState.getJobId();
  }

  private static List<Tag<?>> tagsForJob(JobState jobState) {
    List<Tag<?>> tags = Lists.newArrayList();
    tags.add(new Tag<String>("jobName", jobState.getJobName() == null ? "" : jobState.getJobName()));
    tags.add(new Tag<String>("jobId", jobState.getJobId()));
    return tags;
  }
}
