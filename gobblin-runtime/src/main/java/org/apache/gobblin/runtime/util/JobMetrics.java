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

import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.JobEvent;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.util.ClustersNames;


/**
 * An extension to {@link GobblinMetrics} specifically for job runs.
 *
 * @author Yinan Li
 */
public class JobMetrics extends GobblinMetrics {

  protected final String jobName;

  protected JobMetrics(JobState job) {
    this(job, null);
  }

  protected JobMetrics(JobState job, MetricContext parentContext) {
    super(name(job), parentContext, tagsForJob(job));
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
   * Get a new {@link GobblinMetrics} instance for a given job.
   *
   * @param jobId job ID
   * @return a new {@link GobblinMetrics} instance for the given job
   */
  public static JobMetrics get(String jobId) {
    return get(null, jobId);
  }

  /**
   * Get a new {@link GobblinMetrics} instance for a given job.
   *
   * @param jobState the given {@link JobState} instance
   * @param parentContext is the parent {@link MetricContext}
   * @return a {@link JobMetrics} instance
   */
  public static JobMetrics get(final JobState jobState, final MetricContext parentContext) {
    return (JobMetrics) GOBBLIN_METRICS_REGISTRY.getOrDefault(name(jobState), new Callable<GobblinMetrics>() {
      @Override
      public GobblinMetrics call() throws Exception {
        return new JobMetrics(jobState, parentContext);
      }
    });
  }

  /**
   * Get a {@link JobMetrics} instance for the job with the given {@link JobState} instance.
   *
   * @param jobState the given {@link JobState} instance
   * @return a {@link JobMetrics} instance
   */
  public static JobMetrics get(final JobState jobState) {
    return (JobMetrics) GOBBLIN_METRICS_REGISTRY.getOrDefault(name(jobState), new Callable<GobblinMetrics>() {
      @Override
      public GobblinMetrics call() throws Exception {
        return new JobMetrics(jobState);
      }
    });
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
  public static void remove(JobState jobState) {
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
    tags.add(new Tag<>(JobEvent.METADATA_JOB_NAME, jobState.getJobName() == null ? "" : jobState.getJobName()));
    tags.add(new Tag<>(JobEvent.METADATA_JOB_ID, jobState.getJobId()));
    tags.addAll(getCustomTagsFromState(jobState));
    return tags;
  }

  /**
   * @deprecated use {@link ClustersNames#getInstance()#getClusterName()}
   */
  @Deprecated
  public static String getClusterIdentifierTag() {
    return ClustersNames.getInstance().getClusterName();
  }
}
