/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
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
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Getter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Guice;
import com.google.inject.Injector;

import gobblin.commit.CommitSequenceStore;
import gobblin.commit.DeliverySemantics;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.instrumented.Instrumented;
import gobblin.metastore.JobHistoryStore;
import gobblin.metastore.MetaStoreModule;
import gobblin.metrics.GobblinMetrics;
import gobblin.publisher.DataPublisher;
import gobblin.runtime.JobState.DatasetState;
import gobblin.runtime.commit.FsCommitSequenceStore;
import gobblin.runtime.util.JobMetrics;
import gobblin.source.Source;
import gobblin.source.extractor.JobCommitPolicy;
import gobblin.util.Either;
import gobblin.util.ExecutorsUtils;
import gobblin.util.HadoopUtils;
import gobblin.util.JobLauncherUtils;
import gobblin.util.executors.IteratorExecutor;


/**
 * A class carrying context information of a Gobblin job.
 *
 * @author Yinan Li
 */
public class JobContext {

  private static final Logger LOG = LoggerFactory.getLogger(JobContext.class);

  private static final String TASK_STAGING_DIR_NAME = "task-staging";
  private static final String TASK_OUTPUT_DIR_NAME = "task-output";

  private final String jobName;
  private final String jobId;
  private final JobState jobState;
  @Getter(AccessLevel.PACKAGE)
  private final JobCommitPolicy jobCommitPolicy;
  private final boolean jobLockEnabled;
  private final Optional<JobMetrics> jobMetricsOptional;
  private final Source<?, ?> source;

  // State store for persisting job states
  @Getter(AccessLevel.PACKAGE)
  private final FsDatasetStateStore datasetStateStore;

  // Store for runtime job execution information
  private final Optional<JobHistoryStore> jobHistoryStoreOptional;

  // Should commits be done in parallel
  private final boolean parallelizeCommit;
  private final int parallelCommits;

  @Getter
  private final DeliverySemantics semantics;

  @Getter
  private final Optional<CommitSequenceStore> commitSequenceStore;

  private final Logger logger;

  // A map from dataset URNs to DatasetStates (optional and maybe absent if not populated)
  private Optional<Map<String, JobState.DatasetState>> datasetStatesByUrns = Optional.absent();

  public JobContext(Properties jobProps, Logger logger) throws Exception {
    Preconditions.checkArgument(jobProps.containsKey(ConfigurationKeys.JOB_NAME_KEY),
        "A job must have a job name specified by job.name");

    this.jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    this.jobId = jobProps.containsKey(ConfigurationKeys.JOB_ID_KEY) ? jobProps.getProperty(ConfigurationKeys.JOB_ID_KEY)
        : JobLauncherUtils.newJobId(this.jobName);
    jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, this.jobId);

    this.jobCommitPolicy = JobCommitPolicy.getCommitPolicy(jobProps);

    this.jobLockEnabled =
        Boolean.valueOf(jobProps.getProperty(ConfigurationKeys.JOB_LOCK_ENABLED_KEY, Boolean.TRUE.toString()));

    // Add all job configuration properties so they are picked up by Hadoop
    Configuration conf = new Configuration();
    for (String key : jobProps.stringPropertyNames()) {
      conf.set(key, jobProps.getProperty(key));
    }

    this.datasetStateStore = createStateStore(jobProps, conf);
    this.jobHistoryStoreOptional = createJobHistoryStore(jobProps);

    State jobPropsState = new State();
    jobPropsState.addAll(jobProps);
    this.jobState = new JobState(jobPropsState, this.datasetStateStore.getLatestDatasetStatesByUrns(this.jobName),
        this.jobName, this.jobId);

    setTaskStagingAndOutputDirs();

    if (GobblinMetrics.isEnabled(jobProps)) {
      this.jobMetricsOptional = Optional.of(JobMetrics.get(this.jobState));
      this.jobState.setProp(Instrumented.METRIC_CONTEXT_NAME_KEY, this.jobMetricsOptional.get().getName());
    } else {
      this.jobMetricsOptional = Optional.absent();
    }

    this.semantics = DeliverySemantics.parse(this.jobState);
    this.commitSequenceStore = createCommitSequenceStore();

    this.source = createSource(jobProps);

    this.logger = logger;

    this.parallelizeCommit = this.jobState.getPropAsBoolean(ConfigurationKeys.PARALLELIZE_DATASET_COMMIT,
        ConfigurationKeys.DEFAULT_PARALLELIZE_DATASET_COMMIT);
    this.parallelCommits = this.parallelizeCommit
        ? this.jobState.getPropAsInt(ConfigurationKeys.DATASET_COMMIT_THREADS, ConfigurationKeys.DEFAULT_DATASET_COMMIT_THREADS)
        : 1;
  }

  protected FsDatasetStateStore createStateStore(Properties jobProps, Configuration conf) throws IOException {
    String stateStoreFsUri =
        jobProps.getProperty(ConfigurationKeys.STATE_STORE_FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI);
    FileSystem stateStoreFs = FileSystem.get(URI.create(stateStoreFsUri), conf);
    String stateStoreRootDir = jobProps.getProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY);
    if (jobProps.containsKey(ConfigurationKeys.STATE_STORE_ENABLED) &&
        !Boolean.parseBoolean(jobProps.getProperty(ConfigurationKeys.STATE_STORE_ENABLED))) {
      return new NoopDatasetStateStore(stateStoreFs, stateStoreRootDir);
    } else {
      return new FsDatasetStateStore(stateStoreFs, stateStoreRootDir);
    }
  }

  protected Optional<JobHistoryStore> createJobHistoryStore(Properties jobProps) {
    boolean jobHistoryStoreEnabled = Boolean
        .valueOf(jobProps.getProperty(ConfigurationKeys.JOB_HISTORY_STORE_ENABLED_KEY, Boolean.FALSE.toString()));
    if (jobHistoryStoreEnabled) {
      Injector injector = Guice.createInjector(new MetaStoreModule(jobProps));
      return Optional.of(injector.getInstance(JobHistoryStore.class));
    } else {
      return Optional.absent();
    }
  }

  protected Optional<CommitSequenceStore> createCommitSequenceStore() throws IOException {

    if (this.semantics != DeliverySemantics.EXACTLY_ONCE) {
      return Optional.<CommitSequenceStore> absent();
    }

    Preconditions.checkState(this.jobState.contains(FsCommitSequenceStore.GOBBLIN_RUNTIME_COMMIT_SEQUENCE_STORE_DIR));

    try (FileSystem fs = FileSystem.get(URI.create(this.jobState
        .getProp(FsCommitSequenceStore.GOBBLIN_RUNTIME_COMMIT_SEQUENCE_STORE_FS_URI, ConfigurationKeys.LOCAL_FS_URI)),
        HadoopUtils.getConfFromState(this.jobState))) {

      return Optional.<CommitSequenceStore> of(new FsCommitSequenceStore(fs,
          new Path(this.jobState.getProp(FsCommitSequenceStore.GOBBLIN_RUNTIME_COMMIT_SEQUENCE_STORE_DIR))));
    }
  }

  protected Source<?, ?> createSource(Properties jobProps)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    return new SourceDecorator<>(
        Source.class.cast(Class.forName(jobProps.getProperty(ConfigurationKeys.SOURCE_CLASS_KEY)).newInstance()),
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
  Source<?, ?> getSource() {
    return this.source;
  }

  /**
   * Check whether the use of job lock is enabled or not.
   *
   * @return {@code true} if the use of job lock is enabled or {@code false} otherwise
   */
  boolean isJobLockEnabled() {
    return this.jobLockEnabled;
  }

  protected void setTaskStagingAndOutputDirs() {
    if (this.jobState.contains(ConfigurationKeys.TASK_DATA_ROOT_DIR_KEY)) {

      // Add jobId to task data root dir
      String taskDataRootDirWithJobId =
          new Path(this.jobState.getProp(ConfigurationKeys.TASK_DATA_ROOT_DIR_KEY), this.jobId).toString();
      this.jobState.setProp(ConfigurationKeys.TASK_DATA_ROOT_DIR_KEY, taskDataRootDirWithJobId);

      setTaskStagingDir();
      setTaskOutputDir();
    } else {
      LOG.warn("Property " + ConfigurationKeys.TASK_DATA_ROOT_DIR_KEY + " is missing.");
    }
  }

  /**
   * If {@link ConfigurationKeys#WRITER_STAGING_DIR} (which is deprecated) is specified, use its value.
   *
   * Otherwise, if {@link ConfigurationKeys#TASK_DATA_ROOT_DIR_KEY} is specified, use its value
   * plus {@link #TASK_STAGING_DIR_NAME}.
   */
  private void setTaskStagingDir() {
    if (this.jobState.contains(ConfigurationKeys.WRITER_STAGING_DIR)) {
      LOG.warn(String.format("Property %s is deprecated. No need to use it if %s is specified.",
          ConfigurationKeys.WRITER_STAGING_DIR, ConfigurationKeys.TASK_DATA_ROOT_DIR_KEY));
    } else {
      String workingDir = this.jobState.getProp(ConfigurationKeys.TASK_DATA_ROOT_DIR_KEY);
      this.jobState.setProp(ConfigurationKeys.WRITER_STAGING_DIR,
          new Path(workingDir, TASK_STAGING_DIR_NAME).toString());
    }
  }

  /**
   * If {@link ConfigurationKeys#WRITER_OUTPUT_DIR} (which is deprecated) is specified, use its value.
   *
   * Otherwise, if {@link ConfigurationKeys#TASK_DATA_ROOT_DIR_KEY} is specified, use its value
   * plus {@link #TASK_OUTPUT_DIR_NAME}.
   */
  private void setTaskOutputDir() {
    if (this.jobState.contains(ConfigurationKeys.WRITER_OUTPUT_DIR)) {
      LOG.warn(String.format("Property %s is deprecated. No need to use it if %s is specified.",
          ConfigurationKeys.WRITER_OUTPUT_DIR, ConfigurationKeys.TASK_DATA_ROOT_DIR_KEY));
    } else {
      String workingDir = this.jobState.getProp(ConfigurationKeys.TASK_DATA_ROOT_DIR_KEY);
      this.jobState.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, new Path(workingDir, TASK_OUTPUT_DIR_NAME).toString());
    }
  }

  /**
   * Return whether staging data should be cleaned up on a per-task basis.
   *
   * @return {@code true} if staging data should be cleaned up on a per-task basis or {@code false} otherwise
   */
  boolean shouldCleanupStagingDataPerTask() {
    return this.jobState.getPropAsBoolean(ConfigurationKeys.CLEANUP_STAGING_DATA_PER_TASK,
        ConfigurationKeys.DEFAULT_CLEANUP_STAGING_DATA_PER_TASK);
  }

  /**
   * Get a {@link Map} from dataset URNs (as being specified by {@link ConfigurationKeys#DATASET_URN_KEY} to
   * {@link JobState.DatasetState} objects that represent the dataset states and store {@link TaskState}s
   * corresponding to the datasets.
   *
   * @see JobState#createDatasetStatesByUrns().
   *
   * @return a {@link Map} from dataset URNs to {@link JobState.DatasetState}s representing the dataset states
   */
  Map<String, JobState.DatasetState> getDatasetStatesByUrns() {
    return ImmutableMap.copyOf(this.datasetStatesByUrns.or(Maps.<String, JobState.DatasetState> newHashMap()));
  }

  /**
   * Store job execution information into the job history store.
   */
  void storeJobExecutionInfo() {
    if (this.jobHistoryStoreOptional.isPresent()) {
      try {
        this.logger.info("Writing job execution information to the job history store");
        this.jobHistoryStoreOptional.get().put(this.jobState.toJobExecutionInfo());
      } catch (IOException ioe) {
        this.logger.error("Failed to write job execution information to the job history store: " + ioe, ioe);
      }
    }
  }

  @Subscribe
  public void handleNewTaskCompletionEvent(NewTaskCompletionEvent newOutputTaskStateEvent) {
    LOG.info("{} more tasks of job {} have completed", newOutputTaskStateEvent.getTaskStates().size(), this.jobId);
    // Update the job execution history store upon new task completion
    storeJobExecutionInfo();
  }

  /**
   * Finalize the {@link JobState} before committing the job.
   */
  void finalizeJobStateBeforeCommit() {
    this.jobState.setEndTime(System.currentTimeMillis());
    this.jobState.setDuration(this.jobState.getEndTime() - this.jobState.getStartTime());

    for (TaskState taskState : this.jobState.getTaskStates()) {
      // Set fork.branches explicitly here so the rest job flow can pick it up
      this.jobState.setProp(ConfigurationKeys.FORK_BRANCHES_KEY,
          taskState.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1));
    }
  }

  /**
   * Commit the job on a per-dataset basis.
   */
  void commit() throws IOException {
    this.datasetStatesByUrns = Optional.of(computeDatasetStatesByUrns());
    final boolean shouldCommitDataInJob = shouldCommitDataInJob(this.jobState);
    final DeliverySemantics deliverySemantics = DeliverySemantics.parse(this.jobState);
    final int numCommitThreads = numCommitThreads();

    if (!shouldCommitDataInJob) {
      this.logger.info("Job will not commit data since data are committed by tasks.");
    }

    try {
      List<Either<Void, ExecutionException>> result =
          new IteratorExecutor<>(Iterables.transform(this.datasetStatesByUrns.get().entrySet(),
          new Function<Map.Entry<String, DatasetState>, Callable<Void>>() {
            @Nullable
            @Override
            public Callable<Void> apply(final Map.Entry<String, DatasetState> entry) {
              return createSafeDatasetCommit(shouldCommitDataInJob, deliverySemantics, entry.getKey(), entry.getValue(),
                  numCommitThreads > 1, JobContext.this);
            }
          }).iterator(), numCommitThreads, ExecutorsUtils.newThreadFactory(Optional.of(this.logger), Optional.of("Commit-thread-%d")))
          .executeAndGetResults();

      if (!IteratorExecutor.verifyAllSuccessful(result)) {
        this.jobState.setState(JobState.RunningState.FAILED);
        throw new IOException("Failed to commit dataset state for some dataset(s) of job " + this.jobId);
      }

    } catch (InterruptedException exc) {
      throw new IOException(exc);
    }

    this.jobState.setState(JobState.RunningState.COMMITTED);
  }

  private int numCommitThreads() {
    return this.parallelCommits;
  }

  /**
   * The only reason for this methods is so that we can test the parallelization of commits.
   * DO NOT OVERRIDE.
   */
  @VisibleForTesting
  protected Callable<Void> createSafeDatasetCommit(boolean shouldCommitDataInJob, DeliverySemantics deliverySemantics,
      String datasetUrn, JobState.DatasetState datasetState, boolean isMultithreaded, JobContext jobContext) {
    return new SafeDatasetCommit(shouldCommitDataInJob, deliverySemantics, datasetUrn, datasetState,
        isMultithreaded, jobContext);
  }

  protected Map<String, JobState.DatasetState> computeDatasetStatesByUrns() {
    return this.jobState.createDatasetStatesByUrns();
  }

  @SuppressWarnings("unchecked")
  public static Optional<Class<? extends DataPublisher>> getJobDataPublisherClass(State state)
      throws ReflectiveOperationException {
    if (!Strings.isNullOrEmpty(state.getProp(ConfigurationKeys.JOB_DATA_PUBLISHER_TYPE))) {
      return Optional.<Class<? extends DataPublisher>> of(
          (Class<? extends DataPublisher>) Class.forName(state.getProp(ConfigurationKeys.JOB_DATA_PUBLISHER_TYPE)));
    } else if (!Strings.isNullOrEmpty(state.getProp(ConfigurationKeys.DATA_PUBLISHER_TYPE))) {
      return Optional.<Class<? extends DataPublisher>> of(
          (Class<? extends DataPublisher>) Class.forName(state.getProp(ConfigurationKeys.DATA_PUBLISHER_TYPE)));
    } else {
      LOG.info("Property " + ConfigurationKeys.JOB_DATA_PUBLISHER_TYPE + " not specified");
      return Optional.<Class<? extends DataPublisher>> absent();
    }

  }

  /**
   * Whether data should be committed by the job (as opposed to being commited by the tasks).
   * Data should be committed by the job if either {@link ConfigurationKeys#JOB_COMMIT_POLICY_KEY} is set to "full",
   * or {@link ConfigurationKeys#PUBLISH_DATA_AT_JOB_LEVEL} is set to true.
   */
  private static boolean shouldCommitDataInJob(State state) {
    boolean jobCommitPolicyIsFull =
        JobCommitPolicy.getCommitPolicy(state.getProperties()) == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS;
    boolean publishDataAtJobLevel = state.getPropAsBoolean(ConfigurationKeys.PUBLISH_DATA_AT_JOB_LEVEL,
        ConfigurationKeys.DEFAULT_PUBLISH_DATA_AT_JOB_LEVEL);
    boolean jobDataPublisherSpecified =
        !Strings.isNullOrEmpty(state.getProp(ConfigurationKeys.JOB_DATA_PUBLISHER_TYPE));
    return jobCommitPolicyIsFull || publishDataAtJobLevel || jobDataPublisherSpecified;
  }

}
