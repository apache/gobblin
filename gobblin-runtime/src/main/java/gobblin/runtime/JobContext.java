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
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import lombok.Getter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;
import com.google.common.io.Closer;
import com.google.inject.Guice;
import com.google.inject.Injector;

import gobblin.commit.CommitSequence;
import gobblin.commit.CommitSequence.Builder;
import gobblin.commit.CommitSequenceStore;
import gobblin.commit.CommitStep;
import gobblin.commit.DeliverySemantics;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.instrumented.Instrumented;
import gobblin.metastore.JobHistoryStore;
import gobblin.metastore.MetaStoreModule;
import gobblin.metrics.GobblinMetrics;
import gobblin.publisher.CommitSequencePublisher;
import gobblin.publisher.DataPublisher;
import gobblin.publisher.UnpublishedHandling;
import gobblin.runtime.JobState.DatasetState;
import gobblin.runtime.commit.DatasetStateCommitStep;
import gobblin.runtime.commit.FsCommitSequenceStore;
import gobblin.runtime.util.JobMetrics;
import gobblin.source.Source;
import gobblin.source.extractor.JobCommitPolicy;
import gobblin.util.HadoopUtils;
import gobblin.util.JobLauncherUtils;


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
  private final JobCommitPolicy jobCommitPolicy;
  private final boolean jobLockEnabled;
  private final Optional<JobMetrics> jobMetricsOptional;
  private final Source<?, ?> source;

  // State store for persisting job states
  private final FsDatasetStateStore datasetStateStore;

  // Store for runtime job execution information
  private final Optional<JobHistoryStore> jobHistoryStoreOptional;

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

    String stateStoreFsUri =
        jobProps.getProperty(ConfigurationKeys.STATE_STORE_FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI);
    FileSystem stateStoreFs = FileSystem.get(URI.create(stateStoreFsUri), conf);
    String stateStoreRootDir = jobProps.getProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY);
    if (jobProps.containsKey(ConfigurationKeys.STATE_STORE_ENABLED) &&
        !Boolean.parseBoolean(jobProps.getProperty(ConfigurationKeys.STATE_STORE_ENABLED))) {
      this.datasetStateStore = new NoopDatasetStateStore(stateStoreFs, stateStoreRootDir);
    } else {
      this.datasetStateStore = new FsDatasetStateStore(stateStoreFs, stateStoreRootDir);
    }

    boolean jobHistoryStoreEnabled = Boolean
        .valueOf(jobProps.getProperty(ConfigurationKeys.JOB_HISTORY_STORE_ENABLED_KEY, Boolean.FALSE.toString()));
    if (jobHistoryStoreEnabled) {
      Injector injector = Guice.createInjector(new MetaStoreModule(jobProps));
      this.jobHistoryStoreOptional = Optional.of(injector.getInstance(JobHistoryStore.class));
    } else {
      this.jobHistoryStoreOptional = Optional.absent();
    }

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

    this.source = new SourceDecorator<>(
        Source.class.cast(Class.forName(jobProps.getProperty(ConfigurationKeys.SOURCE_CLASS_KEY)).newInstance()),
        this.jobId, logger);

    this.logger = logger;
  }

  private Optional<CommitSequenceStore> createCommitSequenceStore() throws IOException {

    if (this.semantics != DeliverySemantics.EXACTLY_ONCE) {
      return Optional.<CommitSequenceStore> absent();
    }

    Preconditions.checkState(this.jobState.contains(FsCommitSequenceStore.GOBBLIN_RUNTIME_COMMIT_SEQUENCE_STORE_DIR));

    FileSystem fs = FileSystem.get(URI.create(this.jobState
        .getProp(FsCommitSequenceStore.GOBBLIN_RUNTIME_COMMIT_SEQUENCE_STORE_FS_URI, ConfigurationKeys.LOCAL_FS_URI)),
        HadoopUtils.getConfFromState(this.jobState));

    return Optional.<CommitSequenceStore> of(new FsCommitSequenceStore(fs,
        new Path(this.jobState.getProp(FsCommitSequenceStore.GOBBLIN_RUNTIME_COMMIT_SEQUENCE_STORE_DIR))));
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

  private void setTaskStagingAndOutputDirs() {
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
  @SuppressWarnings("unchecked")
  void commit() throws IOException {
    this.datasetStatesByUrns = Optional.of(this.jobState.createDatasetStatesByUrns());
    boolean allDatasetsCommit = true;
    boolean shouldCommitDataInJob = shouldCommitDataInJob(this.jobState);
    DeliverySemantics deliverySemantics = DeliverySemantics.parse(this.jobState);

    if (!shouldCommitDataInJob) {
      this.logger.info("Job will not commit data since data are committed by tasks.");
    }

    for (Map.Entry<String, JobState.DatasetState> entry : this.datasetStatesByUrns.get().entrySet()) {
      allDatasetsCommit &= processDatasetCommit(shouldCommitDataInJob, deliverySemantics,
                                               entry.getKey(), entry.getValue());
    }

    if (!allDatasetsCommit) {
      this.jobState.setState(JobState.RunningState.FAILED);
      throw new IOException("Failed to commit dataset state for some dataset(s) of job " + this.jobId);
    }
    this.jobState.setState(JobState.RunningState.COMMITTED);
  }

  @SuppressWarnings("unchecked")
  boolean processDatasetCommit(boolean shouldCommitDataInJob,
                               DeliverySemantics deliverySemantics, String datasetUrn,
                               JobState.DatasetState datasetState)
          throws IOException {
    boolean success = true;

    finalizeDatasetStateBeforeCommit(datasetState);

    Class<? extends DataPublisher> dataPublisherClass = null;
    try (Closer closer = Closer.create()) {
      dataPublisherClass = getJobDataPublisherClass(datasetState)
          .or((Class<? extends DataPublisher>) Class.forName(ConfigurationKeys.DEFAULT_DATA_PUBLISHER_TYPE));
      if (!canCommitDataset(datasetState)) {
        this.logger.warn(String.format("Not committing dataset %s of job %s with commit policy %s and state %s",
            datasetUrn, this.jobId, this.jobCommitPolicy, datasetState.getState()));
        checkForUnpublishedWUHandling(datasetUrn, datasetState,dataPublisherClass, closer);
        success = false;
      }
    } catch (ReflectiveOperationException roe) {
      this.logger.warn("Unable to find publisher class: " + roe, roe);
      success = false;
    }

    if (!success) {
      return false;
    }

    Optional<CommitSequence.Builder> commitSequenceBuilder =
        Optional.<CommitSequence.Builder> absent();
    try (Closer closer = Closer.create()) {
      if (shouldCommitDataInJob) {
        this.logger.info(String.format("Committing dataset %s of job %s with commit policy %s and state %s",
            datasetUrn, this.jobId, this.jobCommitPolicy, datasetState.getState()));
        if (deliverySemantics == DeliverySemantics.EXACTLY_ONCE) {
          generateCommitSequenceBuilder(datasetState);
        } else {
          commitDataset(datasetState, closer.register(DataPublisher.getInstance(dataPublisherClass, datasetState)));
        }
      } else {
        if (datasetState.getState() == JobState.RunningState.SUCCESSFUL) {
          datasetState.setState(JobState.RunningState.COMMITTED);
        }
      }
    } catch (ReflectiveOperationException roe) {
      this.logger.error(
          String.format("Failed to instantiate data publisher for dataset %s of job %s.", datasetUrn, this.jobId),
          roe);
      success = false;
    } catch (IOException ioe) {
      this.logger.error(
          String.format("Failed to commit dataset state for dataset %s of job %s", datasetUrn, this.jobId), ioe);
      success = false;
    } finally {
      try {
        finalizeDatasetState(datasetState, datasetUrn);

        if (commitSequenceBuilder.isPresent()) {
          buildAndExecuteCommitSequence(commitSequenceBuilder.get(), datasetState, datasetUrn);
          datasetState.setState(JobState.RunningState.COMMITTED);
        } else {
          persistDatasetState(datasetUrn, datasetState);
        }
      } catch (IOException|RuntimeException ioe) {
        this.logger.error(
            String.format("Failed to persist dataset state for dataset %s of job %s", datasetUrn, this.jobId), ioe);
        success = false;
      }
    }
    return success;
  }

  void checkForUnpublishedWUHandling(String datasetUrn, JobState.DatasetState datasetState,
                                     Class<? extends DataPublisher> dataPublisherClass,
                                     Closer closer)
          throws ReflectiveOperationException, IOException {
    if (UnpublishedHandling.class.isAssignableFrom(dataPublisherClass)) {
      DataPublisher publisher = closer.register(DataPublisher.getInstance(dataPublisherClass, datasetState));
      this.logger
          .info(String.format("Calling publisher to handle unpublished work units for dataset %s of job %s.",
              datasetUrn, this.jobId));
      ((UnpublishedHandling) publisher).handleUnpublishedWorkUnits(datasetState.getTaskStatesAsWorkUnitStates());
    }
  }

  @SuppressWarnings("unchecked")
  private static Optional<Class<? extends DataPublisher>> getJobDataPublisherClass(JobState.DatasetState datasetState)
      throws ReflectiveOperationException {
    if (!Strings.isNullOrEmpty(datasetState.getProp(ConfigurationKeys.JOB_DATA_PUBLISHER_TYPE))) {
      return Optional.<Class<? extends DataPublisher>> of((Class<? extends DataPublisher>) Class
          .forName(datasetState.getProp(ConfigurationKeys.JOB_DATA_PUBLISHER_TYPE)));
    } else if (!Strings.isNullOrEmpty(datasetState.getProp(ConfigurationKeys.DATA_PUBLISHER_TYPE))) {
      return Optional.<Class<? extends DataPublisher>> of(
          (Class<? extends DataPublisher>) Class.forName(datasetState.getProp(ConfigurationKeys.DATA_PUBLISHER_TYPE)));
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
  public static boolean shouldCommitDataInJob(State state) {
    boolean jobCommitPolicyIsFull =
        JobCommitPolicy.getCommitPolicy(state.getProperties()) == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS;
    boolean publishDataAtJobLevel = state.getPropAsBoolean(ConfigurationKeys.PUBLISH_DATA_AT_JOB_LEVEL,
        ConfigurationKeys.DEFAULT_PUBLISH_DATA_AT_JOB_LEVEL);
    boolean jobDataPublisherSpecified =
        !Strings.isNullOrEmpty(state.getProp(ConfigurationKeys.JOB_DATA_PUBLISHER_TYPE));
    return jobCommitPolicyIsFull || publishDataAtJobLevel || jobDataPublisherSpecified;
  }

  /**
   * Finalize a given {@link JobState.DatasetState} before committing the dataset.
   *
   * This method is thread-safe.
   */
  private void finalizeDatasetStateBeforeCommit(JobState.DatasetState datasetState) {
    for (TaskState taskState : datasetState.getTaskStates()) {
      if (taskState.getWorkingState() != WorkUnitState.WorkingState.SUCCESSFUL
          && this.jobCommitPolicy == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS) {
        // The dataset state is set to FAILED if any task failed and COMMIT_ON_FULL_SUCCESS is used
        datasetState.setState(JobState.RunningState.FAILED);
        datasetState.setProp(ConfigurationKeys.JOB_FAILURES_KEY,
            datasetState.getPropAsInt(ConfigurationKeys.JOB_FAILURES_KEY, 0) + 1);
        return;
      }
    }

    datasetState.setState(JobState.RunningState.SUCCESSFUL);
    datasetState.setProp(ConfigurationKeys.JOB_FAILURES_KEY, 0);
  }

  /**
   * Check if it is OK to commit the output data of a dataset.
   *
   * <p>
   *   A dataset can be committed if and only if any of the following conditions is satisfied:
   *
   *   <ul>
   *     <li>The {@link JobCommitPolicy#COMMIT_ON_PARTIAL_SUCCESS} policy is used.</li>
   *     <li>The {@link JobCommitPolicy#COMMIT_SUCCESSFUL_TASKS} policy is used.</li>
   *     <li>The {@link JobCommitPolicy#COMMIT_ON_FULL_SUCCESS} policy is used and all of the tasks succeed.</li>
   *   </ul>
   * </p>
   * This method is thread-safe.
   */
  private boolean canCommitDataset(JobState.DatasetState datasetState) {
    // Only commit a dataset if 1) COMMIT_ON_PARTIAL_SUCCESS is used, or 2)
    // COMMIT_ON_FULL_SUCCESS is used and all of the tasks of the dataset have succeeded.
    return this.jobCommitPolicy == JobCommitPolicy.COMMIT_ON_PARTIAL_SUCCESS
        || this.jobCommitPolicy == JobCommitPolicy.COMMIT_SUCCESSFUL_TASKS
        || (this.jobCommitPolicy == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS
            && datasetState.getState() == JobState.RunningState.SUCCESSFUL);
  }

  /**
   * Commit the output data of a dataset.
   */
  private void commitDataset(JobState.DatasetState datasetState, DataPublisher publisher) {

    try {
      publisher.publish(datasetState.getTaskStates());
    } catch (Throwable t) {
      LOG.error("Failed to commit dataset", t);
      setTaskFailureException(datasetState.getTaskStates(), t);
    }

    // Set the dataset state to COMMITTED upon successful commit
    datasetState.setState(JobState.RunningState.COMMITTED);
  }

  @SuppressWarnings("unchecked")
  private Optional<CommitSequence.Builder> generateCommitSequenceBuilder(JobState.DatasetState datasetState) throws IOException {
    try (Closer closer = Closer.create()) {
      Class<? extends CommitSequencePublisher> dataPublisherClass =
          (Class<? extends CommitSequencePublisher>) Class.forName(datasetState
              .getProp(ConfigurationKeys.DATA_PUBLISHER_TYPE, ConfigurationKeys.DEFAULT_DATA_PUBLISHER_TYPE));
      CommitSequencePublisher publisher =
          (CommitSequencePublisher) closer.register(DataPublisher.getInstance(dataPublisherClass, datasetState));
      publisher.publish(datasetState.getTaskStates());
      return publisher.getCommitSequenceBuilder();
    } catch (Throwable t) {
      LOG.error("Failed to generate commit sequence", t);
      setTaskFailureException(datasetState.getTaskStates(), t);
      throw Throwables.propagate(t);
    }
  }

  private void buildAndExecuteCommitSequence(Builder builder, DatasetState datasetState, String datasetUrn)
      throws IOException {
    CommitSequence commitSequence =
        builder.addStep(buildDatasetStateCommitStep(datasetUrn, datasetState).get()).build();
    this.commitSequenceStore.get().put(commitSequence.getJobName(), datasetUrn, commitSequence);
    commitSequence.execute();
    this.commitSequenceStore.get().delete(commitSequence.getJobName(), datasetUrn);
  }

  /**
   * Persist dataset state of a given dataset identified by the dataset URN.
   */
  private void persistDatasetState(String datasetUrn, JobState.DatasetState datasetState) throws IOException {
    LOG.info("Persisting dataset state for dataset " + datasetUrn);
    this.datasetStateStore.persistDatasetState(datasetUrn, datasetState);
  }

  private static Optional<CommitStep> buildDatasetStateCommitStep(String datasetUrn,
      JobState.DatasetState datasetState) {
    LOG.info("Creating " + DatasetStateCommitStep.class.getSimpleName() + " for dataset " + datasetUrn);
    return Optional.of(new DatasetStateCommitStep.Builder<>().withProps(datasetState).withDatasetUrn(datasetUrn)
        .withDatasetState(datasetState).build());
  }

  private void finalizeDatasetState(JobState.DatasetState datasetState, String datasetUrn) {
    for (TaskState taskState : datasetState.getTaskStates()) {
      // Backoff the actual high watermark to the low watermark for each task that has not been committed
      if (taskState.getWorkingState() != WorkUnitState.WorkingState.COMMITTED) {
        taskState.backoffActualHighWatermark();
        if (this.jobCommitPolicy == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS) {
          // Determine the final dataset state based on the task states (post commit) and the job commit policy.
          // 1. If COMMIT_ON_FULL_SUCCESS is used, the processing of the dataset is considered failed if any
          //    task for the dataset failed to be committed.
          // 2. Otherwise, the processing of the dataset is considered successful even if some tasks for the
          //    dataset failed to be committed.
          datasetState.setState(JobState.RunningState.FAILED);
        }
      }
    }

    datasetState.setId(datasetUrn);
  }

  /**
   * Sets the {@link ConfigurationKeys#TASK_FAILURE_EXCEPTION_KEY} for each given {@link TaskState} to the given
   * {@link Throwable}.
   */
  private static void setTaskFailureException(Collection<TaskState> taskStates, Throwable t) {
    for (TaskState taskState : taskStates) {
      taskState.setTaskFailureException(t);
    }
  }
}
