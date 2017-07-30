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

package gobblin.runtime;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;

import gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import gobblin.broker.gobblin_scopes.JobScopeInstance;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.commit.CommitSequenceStore;
import gobblin.commit.DeliverySemantics;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.instrumented.Instrumented;
import gobblin.metastore.DatasetStateStore;
import gobblin.metastore.JobHistoryStore;
import gobblin.metastore.MetaStoreModule;
import gobblin.metrics.GobblinMetrics;
import gobblin.publisher.DataPublisher;
import gobblin.runtime.JobState.DatasetState;
import gobblin.runtime.commit.FsCommitSequenceStore;
import gobblin.runtime.util.JobMetrics;
import gobblin.source.Source;
import gobblin.source.extractor.JobCommitPolicy;
import gobblin.util.ClassAliasResolver;
import gobblin.util.ConfigUtils;
import gobblin.util.Either;
import gobblin.util.ExecutorsUtils;
import gobblin.util.HadoopUtils;
import gobblin.util.Id;
import gobblin.util.JobLauncherUtils;
import gobblin.util.executors.IteratorExecutor;

import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Getter;


/**
 * A class carrying context information of a Gobblin job.
 *
 * @author Yinan Li
 */
public class JobContext implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(JobContext.class);

  private static final String TASK_STAGING_DIR_NAME = "task-staging";
  private static final String TASK_OUTPUT_DIR_NAME = "task-output";

  private final String jobName;
  private final String jobId;
  private final String jobSequence;
  private final JobState jobState;
  @Getter(AccessLevel.PACKAGE)
  private final JobCommitPolicy jobCommitPolicy;
  private final Optional<JobMetrics> jobMetricsOptional;
  private final Source<?, ?> source;

  // State store for persisting job states
  @Getter(AccessLevel.PACKAGE)
  private final DatasetStateStore datasetStateStore;

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

  @Getter
  private final SharedResourcesBroker<GobblinScopeTypes> jobBroker;

  // A map from dataset URNs to DatasetStates (optional and maybe absent if not populated)
  private Optional<Map<String, JobState.DatasetState>> datasetStatesByUrns = Optional.absent();

  public JobContext(Properties jobProps, Logger logger, SharedResourcesBroker<GobblinScopeTypes> instanceBroker)
      throws Exception {
    Preconditions.checkArgument(jobProps.containsKey(ConfigurationKeys.JOB_NAME_KEY),
        "A job must have a job name specified by job.name");

    this.jobName = JobState.getJobNameFromProps(jobProps);
    this.jobId = jobProps.containsKey(ConfigurationKeys.JOB_ID_KEY) ? jobProps.getProperty(ConfigurationKeys.JOB_ID_KEY)
        : JobLauncherUtils.newJobId(this.jobName);
    this.jobSequence = Long.toString(Id.Job.parse(this.jobId).getSequence());
    jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, this.jobId);

    this.jobBroker = instanceBroker.newSubscopedBuilder(new JobScopeInstance(this.jobName, this.jobId))
        .withOverridingConfig(ConfigUtils.propertiesToConfig(jobProps)).build();
    this.jobCommitPolicy = JobCommitPolicy.getCommitPolicy(jobProps);

    this.datasetStateStore = createStateStore(ConfigUtils.propertiesToConfig(jobProps));
    this.jobHistoryStoreOptional = createJobHistoryStore(jobProps);

    State jobPropsState = new State();
    jobPropsState.addAll(jobProps);
    this.jobState =
        new JobState(jobPropsState, this.datasetStateStore.getLatestDatasetStatesByUrns(this.jobName), this.jobName,
            this.jobId);

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
    this.parallelCommits = this.parallelizeCommit ? this.jobState
        .getPropAsInt(ConfigurationKeys.DATASET_COMMIT_THREADS, ConfigurationKeys.DEFAULT_DATASET_COMMIT_THREADS) : 1;
  }

  protected DatasetStateStore createStateStore(Config jobConfig)
      throws IOException {
    boolean stateStoreEnabled = !jobConfig.hasPath(ConfigurationKeys.STATE_STORE_ENABLED) || jobConfig
        .getBoolean(ConfigurationKeys.STATE_STORE_ENABLED);

    String stateStoreType;

    if (!stateStoreEnabled) {
      stateStoreType = ConfigurationKeys.STATE_STORE_TYPE_NOOP;
    } else {
      stateStoreType = ConfigUtils
          .getString(jobConfig, ConfigurationKeys.STATE_STORE_TYPE_KEY, ConfigurationKeys.DEFAULT_STATE_STORE_TYPE);
    }

    ClassAliasResolver<DatasetStateStore.Factory> resolver = new ClassAliasResolver<>(DatasetStateStore.Factory.class);

    try {
      DatasetStateStore.Factory stateStoreFactory = resolver.resolveClass(stateStoreType).newInstance();
      return stateStoreFactory.createStateStore(jobConfig);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
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

  protected Optional<CommitSequenceStore> createCommitSequenceStore()
      throws IOException {

    if (this.semantics != DeliverySemantics.EXACTLY_ONCE) {
      return Optional.<CommitSequenceStore>absent();
    }

    Preconditions.checkState(this.jobState.contains(FsCommitSequenceStore.GOBBLIN_RUNTIME_COMMIT_SEQUENCE_STORE_DIR));

    try (FileSystem fs = FileSystem.get(URI.create(this.jobState
            .getProp(FsCommitSequenceStore.GOBBLIN_RUNTIME_COMMIT_SEQUENCE_STORE_FS_URI,
                ConfigurationKeys.LOCAL_FS_URI)), HadoopUtils.getConfFromState(this.jobState))) {

      return Optional.<CommitSequenceStore>of(new FsCommitSequenceStore(fs,
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
   * Get the job key.
   *
   * @return job key
   */
  public String getJobKey() {
    return this.jobSequence;
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

  protected void setTaskStagingAndOutputDirs() {

    // Add jobId to writer staging dir
    if (this.jobState.contains(ConfigurationKeys.WRITER_STAGING_DIR)) {
      String writerStagingDirWithJobId =
          new Path(this.jobState.getProp(ConfigurationKeys.WRITER_STAGING_DIR), this.jobId).toString();
      this.jobState.setProp(ConfigurationKeys.WRITER_STAGING_DIR, writerStagingDirWithJobId);
    }

    // Add jobId to writer output dir
    if (this.jobState.contains(ConfigurationKeys.WRITER_OUTPUT_DIR)) {
      String writerOutputDirWithJobId =
          new Path(this.jobState.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR), this.jobId).toString();
      this.jobState.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, writerOutputDirWithJobId);
    }

    // Add jobId to task data root dir
    if (this.jobState.contains(ConfigurationKeys.TASK_DATA_ROOT_DIR_KEY)) {
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
      LOG.info(String.format("Writer Staging Directory is set to %s.",
          this.jobState.getProp(ConfigurationKeys.WRITER_STAGING_DIR)));
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
      LOG.info(String.format("Writer Output Directory is set to %s.",
          this.jobState.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR)));
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
    return ImmutableMap.copyOf(this.datasetStatesByUrns.or(Maps.<String, JobState.DatasetState>newHashMap()));
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
      this.jobState
          .setProp(ConfigurationKeys.FORK_BRANCHES_KEY, taskState.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1));
    }
  }

  /**
   * Commit the job on a per-dataset basis.
   */
  void commit()
      throws IOException {
    commit(false);
  }

  /**
   * Commit the job based on whether the job is cancelled.
   */
  void commit(final boolean isJobCancelled)
      throws IOException {
    this.datasetStatesByUrns = Optional.of(computeDatasetStatesByUrns());
    final boolean shouldCommitDataInJob = shouldCommitDataInJob(this.jobState);
    final DeliverySemantics deliverySemantics = DeliverySemantics.parse(this.jobState);
    final int numCommitThreads = numCommitThreads();

    if (!shouldCommitDataInJob) {
      this.logger.info("Job will not commit data since data are committed by tasks.");
    }

    try {
      if (this.datasetStatesByUrns.isPresent()) {
        this.logger.info("Persisting dataset urns.");
        this.datasetStateStore.persistDatasetURNs(this.jobName, this.datasetStatesByUrns.get().keySet());
      }

      List<Either<Void, ExecutionException>> result = new IteratorExecutor<>(Iterables
          .transform(this.datasetStatesByUrns.get().entrySet(),
              new Function<Map.Entry<String, DatasetState>, Callable<Void>>() {
                @Nullable
                @Override
                public Callable<Void> apply(final Map.Entry<String, DatasetState> entry) {
                  return createSafeDatasetCommit(shouldCommitDataInJob, isJobCancelled, deliverySemantics,
                      entry.getKey(), entry.getValue(), numCommitThreads > 1, JobContext.this);
                }
              }).iterator(), numCommitThreads,
          ExecutorsUtils.newThreadFactory(Optional.of(this.logger), Optional.of("Commit-thread-%d")))
          .executeAndGetResults();

      IteratorExecutor.logFailures(result, LOG, 10);

      if (!IteratorExecutor.verifyAllSuccessful(result)) {
        this.jobState.setState(JobState.RunningState.FAILED);
        throw new IOException("Failed to commit dataset state for some dataset(s) of job " + this.jobId);
      }
    } catch (InterruptedException exc) {
      throw new IOException(exc);
    }
    this.jobState.setState(JobState.RunningState.COMMITTED);
    close();
  }

  @Override
  public void close()
      throws IOException {
    this.jobBroker.close();
  }

  private int numCommitThreads() {
    return this.parallelCommits;
  }

  /**
   * The only reason for this methods is so that we can test the parallelization of commits.
   * DO NOT OVERRIDE.
   */
  @VisibleForTesting
  protected Callable<Void> createSafeDatasetCommit(boolean shouldCommitDataInJob, boolean isJobCancelled,
      DeliverySemantics deliverySemantics, String datasetUrn, JobState.DatasetState datasetState,
      boolean isMultithreaded, JobContext jobContext) {
    return new SafeDatasetCommit(shouldCommitDataInJob, isJobCancelled, deliverySemantics, datasetUrn, datasetState,
        isMultithreaded, jobContext);
  }

  protected Map<String, JobState.DatasetState> computeDatasetStatesByUrns() {
    return this.jobState.createDatasetStatesByUrns();
  }

  @SuppressWarnings("unchecked")
  public static Optional<Class<? extends DataPublisher>> getJobDataPublisherClass(State state)
      throws ReflectiveOperationException {
    if (!Strings.isNullOrEmpty(state.getProp(ConfigurationKeys.JOB_DATA_PUBLISHER_TYPE))) {
      return Optional.<Class<? extends DataPublisher>>of(
          (Class<? extends DataPublisher>) Class.forName(state.getProp(ConfigurationKeys.JOB_DATA_PUBLISHER_TYPE)));
    } else if (!Strings.isNullOrEmpty(state.getProp(ConfigurationKeys.DATA_PUBLISHER_TYPE))) {
      return Optional.<Class<? extends DataPublisher>>of(
          (Class<? extends DataPublisher>) Class.forName(state.getProp(ConfigurationKeys.DATA_PUBLISHER_TYPE)));
    } else {
      LOG.info("Property " + ConfigurationKeys.JOB_DATA_PUBLISHER_TYPE + " not specified");
      return Optional.<Class<? extends DataPublisher>>absent();
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

  @Override
  public String toString() {
    return Objects.toStringHelper(JobContext.class.getSimpleName()).add("jobName", getJobName())
        .add("jobId", getJobId()).add("jobState", getJobState()).toString();
  }
}
