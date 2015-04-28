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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.GobblinMetricsRegistry;
import gobblin.metrics.Tag;
import gobblin.runtime.JobState;


public class JobMetrics extends GobblinMetrics {

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinMetrics.class);

  protected String jobName;

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

  public static synchronized JobMetrics get(JobState job) {
    GobblinMetricsRegistry registry = GobblinMetricsRegistry.getInstance();
    if (!registry.containsKey(name(job))) {
      registry.putIfAbsent(name(job), new JobMetrics(job));
    }
    return (JobMetrics)registry.get(name(job));
  }

  public static String name(JobState job) {
    return "gobblin.metrics." + job.getJobId();
  }

  private static List<Tag<?>> tagsForJob(JobState job) {
    List<Tag<?>> tags = new ArrayList<Tag<?>>();
    tags.add(new Tag<String>("jobName", job.getJobName() == null ? "" : job.getJobName()));
    tags.add(new Tag<String>("jobId", job.getJobId()));
    return tags;
  }
}
