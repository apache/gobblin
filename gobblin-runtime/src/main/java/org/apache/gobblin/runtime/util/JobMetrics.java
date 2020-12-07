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

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

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
@Slf4j
public class JobMetrics extends GobblinMetrics {

  public static final CreatorTag DEFAULT_CREATOR_TAG = new CreatorTag( "driver");
  protected final String jobName;
  @Getter
  protected final CreatorTag creatorTag;
  protected JobMetrics(JobState job, CreatorTag tag) {
    this(job, null, tag);
  }

  protected JobMetrics(JobState job, MetricContext parentContext, CreatorTag creatorTag) {
    super(name(job), parentContext, tagsForJob(job));
    this.jobName = job.getJobName();
    this.creatorTag = creatorTag;
  }

  public static class CreatorTag extends Tag<String> {
    public CreatorTag(String value) {
      super("creator", value);
    }
  }

  /**
   * Get a new {@link GobblinMetrics} instance for a given job.
   * @deprecated  use {@link JobMetrics#get(String, String, CreatorTag)} instead.
   *
   * @param jobName job name
   * @param jobId job ID
   * @return a new {@link GobblinMetrics} instance for the given job
   */
  @Deprecated
  public static JobMetrics get(String jobName, String jobId) {
    return get(new JobState(jobName, jobId), DEFAULT_CREATOR_TAG);
  }

  /**
   * Get a new {@link GobblinMetrics} instance for a given job.
   *
   * @param creatorTag the unique id which can tell who initiates this get operation
   * @param jobName job name
   * @param jobId job ID
   * @param creatorTag who creates this job metrics
   * @return a new {@link GobblinMetrics} instance for the given job
   */
  public static JobMetrics get(String jobName, String jobId, CreatorTag creatorTag) {
    return get(new JobState(jobName, jobId), creatorTag);
  }

  /**
   * Get a new {@link GobblinMetrics} instance for a given job.
   *
   * @param creatorTag the unique id which can tell who initiates this get operation
   * @param jobId job ID
   * @return a new {@link GobblinMetrics} instance for the given job
   */
  public static JobMetrics get(String jobId, CreatorTag creatorTag) {
    return get(null, jobId, creatorTag);
  }

  /**
   * Get a new {@link GobblinMetrics} instance for a given job.
   *
   * @param creatorTag the unique id which can tell who initiates this get operation
   * @param jobState the given {@link JobState} instance
   * @param parentContext is the parent {@link MetricContext}
   * @return a {@link JobMetrics} instance
   */
  public static JobMetrics get(final JobState jobState, final MetricContext parentContext, CreatorTag creatorTag) {
    return (JobMetrics) GOBBLIN_METRICS_REGISTRY.getOrCreate(name(jobState), new Callable<GobblinMetrics>() {
      @Override
      public GobblinMetrics call() throws Exception {
        return new JobMetrics(jobState, parentContext, creatorTag);
      }
    });
  }

  /**
   * Get a {@link JobMetrics} instance for the job with the given {@link JobState} instance.
   * @deprecated  use {@link JobMetrics#get(JobState, CreatorTag)} instead.
   *
   * @param jobState the given {@link JobState} instance
   * @return a {@link JobMetrics} instance
   */
  @Deprecated
  public static JobMetrics get(final JobState jobState) {
    return (JobMetrics) GOBBLIN_METRICS_REGISTRY.getOrCreate(name(jobState), new Callable<GobblinMetrics>() {
      @Override
      public GobblinMetrics call() throws Exception {
        return new JobMetrics(jobState, DEFAULT_CREATOR_TAG);
      }
    });
  }

  /**
   * Get a {@link JobMetrics} instance for the job with the given {@link JobState} instance and a creator tag.
   *
   * @param creatorTag the unique id which can tell who initiates this get operation
   * @param jobState the given {@link JobState} instance
   * @return a {@link JobMetrics} instance
   */
  public static JobMetrics get(final JobState jobState, CreatorTag creatorTag) {
    return (JobMetrics) GOBBLIN_METRICS_REGISTRY.getOrCreate(name(jobState), new Callable<GobblinMetrics>() {
      @Override
      public GobblinMetrics call() throws Exception {
        return new JobMetrics(jobState, creatorTag);
      }
    });
  }

  /**
   * Remove the {@link JobMetrics} instance for the job with the given {@link JobState} instance.
   *
   * <p>
   *   Removing a {@link JobMetrics} instance for a job will also remove the {@link TaskMetrics}s
   *   of every tasks of the job. This is only used by job driver where there is no {@link ForkMetrics}.
   * </p>
   * @param jobState the given {@link JobState} instance
   */
  public static void remove(JobState jobState) {
    remove(name(jobState));
    for (TaskState taskState : jobState.getTaskStates()) {
      TaskMetrics.remove(taskState);
    }
  }

  /**
   * Attempt to remove the {@link JobMetrics} instance for the job with the given jobId.
   * It also checks the creator tag of this {@link JobMetrics}. If the given tag doesn't
   * match the creator tag, this {@link JobMetrics} won't be removed.
   */
  public static void attemptRemove(String jobId, Tag matchTag) {
    Optional<GobblinMetrics> gobblinMetricsOptional = GOBBLIN_METRICS_REGISTRY.get(
        GobblinMetrics.METRICS_ID_PREFIX + jobId);
    JobMetrics jobMetrics = gobblinMetricsOptional.isPresent()? (JobMetrics) (gobblinMetricsOptional.get()): null;

    // only remove if the tag matches
    if (jobMetrics != null && jobMetrics.getCreatorTag().equals(matchTag)) {
      log.info("Removing job metrics because creator matches : " + matchTag.getValue());
      GOBBLIN_METRICS_REGISTRY.remove(GobblinMetrics.METRICS_ID_PREFIX + jobId);
    }
  }

  private static String name(JobState jobState) {
    return METRICS_ID_PREFIX + jobState.getJobId();
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
