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

package gobblin.runtime;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.inject.Guice;
import com.google.inject.Injector;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.metastore.FsStateStore;
import gobblin.metastore.JobHistoryStore;
import gobblin.metastore.MetaStoreModule;
import gobblin.metastore.StateStore;
import gobblin.metrics.GobblinMetrics;
import gobblin.runtime.util.JobMetrics;
import gobblin.source.Source;
import gobblin.source.extractor.JobCommitPolicy;
import gobblin.util.JobLauncherUtils;


/**
 * A class carrying context information of a Gobblin job.
 *
 * @author ynli
 */
public class JobContext {

  // State store for persisting job states
  private final StateStore<JobState> jobStateStore;

  // Store for runtime job execution information
  private final Optional<JobHistoryStore> jobHistoryStoreOptional;

  private final String jobName;
  private final String jobId;
  private final JobState jobState;
  private final JobCommitPolicy jobCommitPolicy;
  private final boolean jobLockEnabled;
  private final Optional<JobMetrics> jobMetricsOptional;
  private final Source<?, ?> source;

  @SuppressWarnings("unchecked")
  public JobContext(Properties jobProps, Logger logger) throws Exception {
    Preconditions.checkArgument(jobProps.containsKey(ConfigurationKeys.JOB_NAME_KEY),
        "A job must have a job name specified by job.name");

    this.jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    this.jobId = jobProps.containsKey(ConfigurationKeys.JOB_ID_KEY) ?
        jobProps.getProperty(ConfigurationKeys.JOB_ID_KEY) : JobLauncherUtils.newJobId(this.jobName);
    jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, this.jobId);

    this.jobCommitPolicy = JobCommitPolicy.getCommitPolicy(jobProps);

    this.jobLockEnabled = Boolean.valueOf(
        jobProps.getProperty(ConfigurationKeys.JOB_LOCK_ENABLED_KEY, Boolean.TRUE.toString()));

    this.jobStateStore = new FsStateStore<JobState>(
        jobProps.getProperty(ConfigurationKeys.STATE_STORE_FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI),
        jobProps.getProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY),
        JobState.class);

    boolean jobHistoryStoreEnabled = Boolean.valueOf(
        jobProps.getProperty(ConfigurationKeys.JOB_HISTORY_STORE_ENABLED_KEY, Boolean.FALSE.toString()));
    if (jobHistoryStoreEnabled) {
      Injector injector = Guice.createInjector(new MetaStoreModule(jobProps));
      this.jobHistoryStoreOptional = Optional.of(injector.getInstance(JobHistoryStore.class));
    } else {
      this.jobHistoryStoreOptional = Optional.absent();
    }

    State jobPropsState = new State();
    jobPropsState.addAll(jobProps);
    JobState previousJobState = getPreviousJobState(this.jobName);
    this.jobState = new JobState(jobPropsState, previousJobState, this.jobName, this.jobId);
    // Remember the number of consecutive failures of this job in the past
    this.jobState.setProp(ConfigurationKeys.JOB_FAILURES_KEY,
        previousJobState.getPropAsInt(ConfigurationKeys.JOB_FAILURES_KEY, 0));

    if (GobblinMetrics.isEnabled(jobProps)) {
      this.jobMetricsOptional = Optional.of(JobMetrics.get(this.jobState));
      this.jobState.setProp(ConfigurationKeys.METRIC_CONTEXT_NAME_KEY, this.jobMetricsOptional.get().getName());
    } else {
      this.jobMetricsOptional = Optional.absent();
    }

    this.source = new SourceDecorator(
        (Source<?, ?>) Class.forName(jobProps.getProperty(ConfigurationKeys.SOURCE_CLASS_KEY)).newInstance(),
        this.jobId, logger);
  }

  /**
   * Get the job name.
   *
   * @return job name
   */
  public String getJobName() {
    return this.jobName;
  }

  /**
   * Get the job ID.
   *
   * @return job ID
   */
  public String getJobId() {
    return this.jobId;
  }

  /**
   * Get a {@link JobState} object representing the job state.
   *
   * @return a {@link JobState} object representing the job state
   */
  public JobState getJobState() {
    return this.jobState;
  }

  /**
   * Get a {@link JobCommitPolicy} instance for the policy on committing job output.
   *
   * @return a {@link JobCommitPolicy} instance for the policy on committing job output
   */
  public JobCommitPolicy getJobCommitPolicy() {
    return this.jobCommitPolicy;
  }

  /**
   * Check whether the use of job lock is enabled or not.
   *
   * @return {@code true} if the use of job lock is enabled or {@code false} otherwise
   */
  public boolean isJobLockEnabled() {
    return this.jobLockEnabled;
  }

  /**
   * Get an {@link Optional} of {@link JobMetrics}.
   *
   * @return an {@link Optional} of {@link JobMetrics}
   */
  public Optional<JobMetrics> getJobMetricsOptional() {
    return this.jobMetricsOptional;
  }

  /**
   * Get an instance of the {@link Source} class specified in the job configuration.
   *
   * @return an instance of the {@link Source} class specified in the job configuration
   */
  public Source<?, ?> getSource() {
    return this.source;
  }

  /**
   * Get an instance of {@link StateStore} for serializing/deserializing {@link JobState}.
   *
   * @return an instance of {@link StateStore} for serializing/deserializing {@link JobState}
   */
  public StateStore<JobState> getJobStateStore() {
    return this.jobStateStore;
  }

  /**
   * Get an {@link Optional} of {@link JobHistoryStore}.
   *
   * @return an {@link Optional} of {@link JobHistoryStore}
   */
  public Optional<JobHistoryStore> getJobHistoryStoreOptional() {
    return this.jobHistoryStoreOptional;
  }

  /**
   * Get the job state of the most recent run of the job.
   */
  private JobState getPreviousJobState(String jobName)
      throws IOException {
    if (this.jobStateStore.exists(jobName, "current" + AbstractJobLauncher.JOB_STATE_STORE_TABLE_SUFFIX)) {
      // Read the job state of the most recent run of the job
      List<JobState> previousJobStateList =
          this.jobStateStore.getAll(jobName, "current" + AbstractJobLauncher.JOB_STATE_STORE_TABLE_SUFFIX);
      if (!previousJobStateList.isEmpty()) {
        // There should be a single job state on the list if the list is not empty
        return previousJobStateList.get(0);
      }
    }

    return new JobState();
  }
}
