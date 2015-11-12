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

package gobblin.compaction.mapreduce;

import static gobblin.compaction.Dataset.DatasetState.COMPACTION_COMPLETE;
import static gobblin.compaction.Dataset.DatasetState.GIVEN_UP;
import static gobblin.compaction.Dataset.DatasetState.UNVERIFIED;
import static gobblin.compaction.Dataset.DatasetState.VERIFIED;
import static gobblin.compaction.mapreduce.MRCompactorJobRunner.Status.ABORTED;
import static gobblin.compaction.mapreduce.MRCompactorJobRunner.Status.COMMITTED;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import gobblin.compaction.Compactor;
import gobblin.compaction.Dataset;
import gobblin.compaction.event.CompactionSlaEventHelper;
import gobblin.compaction.verify.DataCompletenessVerifier;
import gobblin.compaction.verify.DataCompletenessVerifier.Results;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.Tag;
import gobblin.metrics.event.EventSubmitter;
import gobblin.metrics.event.sla.SlaEventSubmitter;
import gobblin.util.DatasetFilterUtils;
import gobblin.util.ExecutorsUtils;
import gobblin.util.HadoopUtils;
import gobblin.util.TagUtils;
import gobblin.util.recordcount.CompactionRecordCountProvider;
import gobblin.util.recordcount.IngestionRecordCountProvider;


/**
 * MapReduce-based {@link gobblin.compaction.Compactor}. Compaction will run on each qualified {@link Dataset}
 * under {@link #COMPACTION_INPUT_DIR}.
 *
 * Each subfolder in {@link #COMPACTION_INPUT_DIR} is considered a topic, if it satisfies
 * {@link #COMPACTION_BLACKLIST} and {@link #COMPACTION_WHITELIST}.
 * It is up to the {@link MRCompactorJobPropCreator} to determine what is a {@link Dataset}. For example,
 * a {@link Dataset} may be a topic, or a partition of the topic.
 *
 * For example, suppose {@link #COMPACTION_INPUT_DIR} is '/inputdata', and a topic is
 * '/inputdata/mytopic'. If this topic is considered a {@link Dataset}, all data under this folder will be compacted.
 * If a particular day's data of this topic is considered a {@link Dataset}, e.g.,
 * '/inputdata/mytopic/daily/2015/08/09', then data under this folder will be compacted.
 *
 * @author ziliu
 */

public class MRCompactor implements Compactor {

  private static final Logger LOG = LoggerFactory.getLogger(MRCompactor.class);

  public static final String COMPACTION_PREFIX = "compaction.";

  /**
   * Basic compaction properties.
   */
  public static final String COMPACTION_THREAD_POOL_SIZE = COMPACTION_PREFIX + "thread.pool.size";
  public static final int DEFAULT_COMPACTION_THREAD_POOL_SIZE = 30;
  public static final String COMPACTION_INPUT_DIR = COMPACTION_PREFIX + "input.dir";

  // The subdir name of input dataset paths, e.g., "hourly" in "/data/input/PasswordChangeEvent/hourly/2015/09/06".
  public static final String COMPACTION_INPUT_SUBDIR = COMPACTION_PREFIX + "input.subdir";
  public static final String DEFAULT_COMPACTION_INPUT_SUBDIR = "hourly";

  public static final String COMPACTION_DEST_DIR = COMPACTION_PREFIX + "dest.dir";

  // The subdir name of output dataset paths, e.g., "daily" in "/data/input/PasswordChangeEvent/daily/2015/09/06".
  public static final String COMPACTION_DEST_SUBDIR = COMPACTION_PREFIX + "dest.subdir";
  public static final String DEFAULT_COMPACTION_DEST_SUBDIR = "daily";

  // The output dir for compaction MR job, which will be moved to the final output dir for data publishing.
  public static final String COMPACTION_TMP_DEST_DIR = COMPACTION_PREFIX + "tmp.dest.dir";
  public static final String DEFAULT_COMPACTION_TMP_DEST_DIR = "/tmp/gobblin-compaction";

  public static final String COMPACTION_LATE_DIR_SUFFIX = "_late";

  public static final String COMPACTION_BLACKLIST = COMPACTION_PREFIX + "blacklist";
  public static final String COMPACTION_WHITELIST = COMPACTION_PREFIX + "whitelist";
  public static final String COMPACTION_HIGH_PRIORITY_TOPICS = COMPACTION_PREFIX + "high.priority.topics";
  public static final String COMPACTION_NORMAL_PRIORITY_TOPICS = COMPACTION_PREFIX + "normal.priority.topics";
  public static final String COMPACTION_JOBPROPS_CREATOR_CLASS = COMPACTION_PREFIX + "jobprops.creator.class";
  public static final String DEFAULT_COMPACTION_JOBPROPS_CREATOR_CLASS =
      "gobblin.compaction.mapreduce.MRCompactorTimeBasedJobPropCreator";
  public static final String COMPACTION_JOB_RUNNER_CLASS = COMPACTION_PREFIX + "job.runner.class";
  public static final String DEFAULT_COMPACTION_JOB_RUNNER_CLASS =
      "gobblin.compaction.mapreduce.avro.MRCompactorAvroKeyDedupJobRunner";
  public static final String COMPACTION_TIMEZONE = COMPACTION_PREFIX + "timezone";
  public static final String DEFAULT_COMPACTION_TIMEZONE = ConfigurationKeys.PST_TIMEZONE_NAME;
  public static final String COMPACTION_FILE_SYSTEM_URI = COMPACTION_PREFIX + "file.system.uri";
  public static final String COMPACTION_MR_JOB_TIMEOUT_MINUTES = COMPACTION_PREFIX + "mr.job.timeout.minutes";
  public static final long DEFAULT_COMPACTION_MR_JOB_TIMEOUT_MINUTES = Long.MAX_VALUE;

  //The provider that provides event counts for the compaction input files.
  public static final String COMPACTION_INPUT_RECORD_COUNT_PROVIDER = COMPACTION_PREFIX + "input.record.count.provider";
  public static final String DEFAULT_COMPACTION_INPUT_RECORD_COUNT_PROVIDER =
      IngestionRecordCountProvider.class.getName();

  //The provider that provides event counts for the compaction output files.
  public static final String COMPACTION_OUTPUT_RECORD_COUNT_PROVIDER =
      COMPACTION_PREFIX + "output.record.count.provider";
  public static final String DEFAULT_COMPACTION_OUTPUT_RECORD_COUNT_PROVIDER =
      CompactionRecordCountProvider.class.getName();

  // If a dataset has already been compacted and new (late) data is found, whether recompact this dataset.
  public static final String COMPACTION_RECOMPACT_FROM_INPUT_FOR_LATE_DATA =
      COMPACTION_PREFIX + "recompact.from.input.for.late.data";
  public static final boolean DEFAULT_COMPACTION_RECOMPACT_FROM_INPUT_FOR_LATE_DATA = false;

  // Whether the input data for the compaction is deduplicated.
  public static final String COMPACTION_INPUT_DEDUPLICATED = COMPACTION_PREFIX + "input.deduplicated";
  public static final boolean DEFAULT_COMPACTION_INPUT_DEDUPLICATED = false;

  // Whether the output of the compaction should be deduplicated.
  public static final String COMPACTION_OUTPUT_DEDUPLICATED = COMPACTION_PREFIX + "output.deduplicated";
  public static final boolean DEFAULT_COMPACTION_OUTPUT_DEDUPLICATED = true;

  public static final String COMPACTION_COMPLETENESS_VERIFICATION_PREFIX =
      COMPACTION_PREFIX + "completeness.verification.";

  public static final String COMPACTION_RECOMPACT_FROM_DEST_PATHS = COMPACTION_PREFIX + "recompact.from.dest.paths";
  public static final boolean DEFAULT_COMPACTION_RECOMPACT_FROM_DEST_PATHS = false;

  /**
   * Configuration properties related to data completeness verification.
   */
  public static final String COMPACTION_COMPLETENESS_VERIFICATION_BLACKLIST =
      COMPACTION_COMPLETENESS_VERIFICATION_PREFIX + "blacklist";
  public static final String COMPACTION_COMPLETENESS_VERIFICATION_WHITELIST =
      COMPACTION_COMPLETENESS_VERIFICATION_PREFIX + "whitelist";
  public static final String COMPACTION_VERIFICATION_TIMEOUT_MINUTES =
      COMPACTION_COMPLETENESS_VERIFICATION_PREFIX + "timeout.minutes";
  public static final long DEFAULT_COMPACTION_VERIFICATION_TIMEOUT_MINUTES = 30;
  public static final String COMPACTION_COMPLETENESS_VERIFICATION_ENABLED =
      COMPACTION_COMPLETENESS_VERIFICATION_PREFIX + "enabled";
  public static final boolean DEFAULT_COMPACTION_COMPLETENESS_VERIFICATION_ENABLED = false;

  // Number of datasets to be passed to DataCompletenessVerifier together. By passing multiple datasets together,
  // some costs in DataCompletenessVerifier (e.g., submitting a SQL query) can be amortized.
  public static final String COMPACTION_COMPLETENESS_VERIFICATION_NUM_DATASETS_VERIFIED_TOGETHER =
      COMPACTION_COMPLETENESS_VERIFICATION_PREFIX + "num.datasets.verified.together";
  public static final int DEFAULT_COMPACTION_COMPLETENESS_VERIFICATION_NUM_DATASETS_VERIFIED_TOGETHER = 10;

  // Whether to compact and publish a datatset if its completeness cannot be verified.
  public static final String COMPACTION_COMPLETENESS_VERIFICATION_PUBLISH_DATA_IF_CANNOT_VERIFY =
      COMPACTION_COMPLETENESS_VERIFICATION_PREFIX + "publish.data.if.cannot.verify";
  public static final boolean DEFAULT_COMPACTION_COMPLETENESS_VERIFICATION_PUBLISH_DATA_IF_CANNOT_VERIFY = false;

  /**
   * Compaction configuration properties used internally.
   */
  public static final String COMPACTION_SHOULD_DEDUPLICATE = COMPACTION_PREFIX + "should.deduplicate";
  public static final String COMPACTION_JOB_DEST_PARTITION = COMPACTION_PREFIX + "job.dest.partition";
  public static final String COMPACTION_ENABLE_SUCCESS_FILE =
      COMPACTION_PREFIX + "fileoutputcommitter.marksuccessfuljobs";
  public static final String COMPACTION_JOB_LATE_DATA_MOVEMENT_TASK = COMPACTION_PREFIX + "job.late.data.movement.task";
  public static final String COMPACTION_JOB_LATE_DATA_FILES = COMPACTION_PREFIX + "job.late.data.files";
  public static final String COMPACTION_COMPLETE_FILE_NAME = "_COMPACTION_COMPLETE";
  public static final String COMPACTION_LATE_FILES_DIRECTORY = "late";
  public static final String COMPACTION_JARS = COMPACTION_PREFIX + "jars";
  public static final String COMPACTION_TRACKING_EVENTS_NAMESPACE = COMPACTION_PREFIX + "tracking.events";

  private static final double HIGH_PRIORITY = 3.0;
  private static final double NORMAL_PRIORITY = 2.0;
  private static final double LOW_PRIORITY = 1.0;
  private static final long COMPACTION_JOB_WAIT_INTERVAL_SECONDS = 10;
  private static final Map<Dataset, Job> RUNNING_MR_JOBS = Maps.newConcurrentMap();

  private final State state;
  private final Configuration conf;
  private final String inputDir;
  private final String inputSubDir;
  private final String inputLateSubDir;
  private final String destDir;
  private final String destSubDir;
  private final String destLateSubDir;
  private final String tmpOutputDir;
  private final FileSystem fs;
  private final JobRunnerExecutor jobExecutor;
  private final Set<Dataset> datasets;
  private final Map<Dataset, MRCompactorJobRunner> jobRunnables;
  private final Closer closer;
  private final Optional<DataCompletenessVerifier> verifier;
  private final Stopwatch stopwatch;
  private final GobblinMetrics gobblinMetrics;
  private final EventSubmitter eventSubmitter;

  private final long dataVerifTimeoutMinutes;
  private final long compactionTimeoutMinutes;
  private final boolean shouldVerifDataCompl;
  private final boolean shouldPublishDataIfCannotVerifyCompl;

  public MRCompactor(Properties props) throws IOException {
    this.state = new State();
    state.addAll(props);
    this.conf = HadoopUtils.getConfFromState(state);
    this.inputDir = getInputDir();
    this.inputSubDir = getInputSubDir();
    this.inputLateSubDir = getInputLateSubDir();
    this.destDir = getDestDir();
    this.destSubDir = getDestSubDir();
    this.destLateSubDir = getDestLateSubDir();
    this.tmpOutputDir = getTmpOutputDir();
    this.fs = getFileSystem();
    this.datasets = Sets.newHashSet();
    this.jobExecutor = createJobExecutor();
    this.jobRunnables = Maps.newConcurrentMap();
    this.closer = Closer.create();
    this.stopwatch = Stopwatch.createStarted();
    this.gobblinMetrics = initializeMetrics();
    this.eventSubmitter = new EventSubmitter.Builder(
        GobblinMetrics.get(this.state.getProp(ConfigurationKeys.JOB_NAME_KEY)).getMetricContext(),
        MRCompactor.COMPACTION_TRACKING_EVENTS_NAMESPACE).build();

    this.dataVerifTimeoutMinutes = getDataVerifTimeoutMinutes();
    this.compactionTimeoutMinutes = getCompactionTimeoutMinutes();
    this.shouldVerifDataCompl = shouldVerifyDataCompleteness();
    this.verifier =
        this.shouldVerifDataCompl ? Optional.of(this.closer.register(new DataCompletenessVerifier(this.state)))
            : Optional.<DataCompletenessVerifier> absent();
    this.shouldPublishDataIfCannotVerifyCompl = shouldPublishDataIfCannotVerifyCompl();
  }

  private String getInputDir() {
    Preconditions.checkArgument(this.state.contains(COMPACTION_INPUT_DIR),
        "Missing required property " + COMPACTION_INPUT_DIR);
    return this.state.getProp(COMPACTION_INPUT_DIR);
  }

  private String getInputSubDir() {
    return this.state.getProp(COMPACTION_INPUT_SUBDIR, DEFAULT_COMPACTION_INPUT_SUBDIR);
  }

  private String getInputLateSubDir() {
    return this.state.getProp(COMPACTION_INPUT_SUBDIR, DEFAULT_COMPACTION_INPUT_SUBDIR) + COMPACTION_LATE_DIR_SUFFIX;
  }

  private String getDestDir() {
    Preconditions.checkArgument(this.state.contains(COMPACTION_DEST_DIR),
        "Missing required property " + COMPACTION_DEST_DIR);
    return this.state.getProp(COMPACTION_DEST_DIR);
  }

  private String getDestLateSubDir() {
    return this.state.getProp(COMPACTION_DEST_SUBDIR, DEFAULT_COMPACTION_DEST_SUBDIR) + COMPACTION_LATE_DIR_SUFFIX;
  }

  private String getDestSubDir() {
    return this.state.getProp(COMPACTION_DEST_SUBDIR, DEFAULT_COMPACTION_DEST_SUBDIR);
  }

  private String getTmpOutputDir() {
    return this.state.getProp(COMPACTION_TMP_DEST_DIR, DEFAULT_COMPACTION_TMP_DEST_DIR);
  }

  private FileSystem getFileSystem() throws IOException {
    if (this.state.contains(COMPACTION_FILE_SYSTEM_URI)) {
      URI uri = URI.create(this.state.getProp(COMPACTION_FILE_SYSTEM_URI));
      return FileSystem.get(uri, this.conf);
    } else {
      return FileSystem.get(this.conf);
    }
  }

  private JobRunnerExecutor createJobExecutor() {
    int threadPoolSize = getThreadPoolSize();
    BlockingQueue<Runnable> queue = new PriorityBlockingQueue<Runnable>();
    return new JobRunnerExecutor(threadPoolSize, threadPoolSize, Long.MAX_VALUE, TimeUnit.NANOSECONDS, queue);
  }

  private int getThreadPoolSize() {
    return this.state.getPropAsInt(COMPACTION_THREAD_POOL_SIZE, DEFAULT_COMPACTION_THREAD_POOL_SIZE);
  }

  private GobblinMetrics initializeMetrics() {
    List<Tag<?>> tags = Lists.newArrayList();
    tags.addAll(Tag.fromMap(TagUtils.getRuntimeTags()));
    GobblinMetrics gobblinMetrics = GobblinMetrics.get(state.getProp(ConfigurationKeys.JOB_NAME_KEY), null, tags);
    gobblinMetrics.startMetricReporting(state.getProperties());
    return gobblinMetrics;
  }

  @Override
  public void compact() throws IOException {
    try {
      copyDependencyJarsToHdfs();
      List<String> topics = findAllTopics();
      processTopics(topics);
      throwExceptionsIfAnyDatasetCompactionFailed();
    } catch (Throwable t) {

      // This throwable is logged here before propagated. Otherwise, if another throwable is thrown
      // in the finally-block, this throwable may be suppressed.
      LOG.error("Caught throwable during compaction", t);
      throw Throwables.propagate(t);
    } finally {
      try {
        shutdownExecutors();
        this.closer.close();
      } finally {
        deleteDependencyJars();
        gobblinMetrics.stopMetricReporting();
      }
    }
  }

  /**
   * Copy dependency jars from local fs to HDFS.
   */
  private void copyDependencyJarsToHdfs() throws IOException {
    if (!this.state.contains(ConfigurationKeys.JOB_JAR_FILES_KEY)) {
      return;
    }
    LocalFileSystem lfs = FileSystem.getLocal(this.conf);
    Path tmpJarFileDir = new Path(this.tmpOutputDir, "_gobblin_compaction_jars");
    this.state.setProp(COMPACTION_JARS, tmpJarFileDir.toString());
    this.fs.delete(tmpJarFileDir, true);
    for (String jarFile : this.state.getPropAsList(ConfigurationKeys.JOB_JAR_FILES_KEY)) {
      for (FileStatus status : lfs.globStatus(new Path(jarFile))) {
        Path tmpJarFile = new Path(this.fs.makeQualified(tmpJarFileDir), status.getPath().getName());
        this.fs.copyFromLocalFile(status.getPath(), tmpJarFile);
        LOG.info(String.format("%s will be added to classpath", tmpJarFile));
      }
    }
  }

  /**
   * Delete dependency jars from HDFS when job is done.
   */
  private void deleteDependencyJars() throws IllegalArgumentException, IOException {
    if (this.state.contains(COMPACTION_JARS)) {
      this.fs.delete(new Path(this.state.getProp(COMPACTION_JARS)), true);
    }
  }

  /**
   * Each subdir name in {@link ConfigurationKeys#COMPACTION_INPUT_DIR} is considered a topic, if it satisfies
   * {@link ConfigurationKeys#COMPACTION_BLACKLIST} and {@link ConfigurationKeys#COMPACTION_WHITELIST}.
   */
  @SuppressWarnings("deprecation")
  private List<String> findAllTopics() throws IOException {
    List<String> topics = Lists.newArrayList();
    List<Pattern> blacklist = getBlacklist();
    List<Pattern> whitelist = getWhitelist();
    for (FileStatus status : this.fs.listStatus(new Path(this.inputDir))) {
      if (status.isDir()) {
        String topic = status.getPath().getName();
        if (DatasetFilterUtils.survived(topic, blacklist, whitelist)) {
          LOG.info("found topic: " + status.getPath().getName());
          topics.add(status.getPath().getName());
        }
      }
    }
    return topics;
  }

  private List<Pattern> getBlacklist() {
    List<String> list = this.state.getPropAsList(COMPACTION_BLACKLIST, StringUtils.EMPTY);
    return DatasetFilterUtils.getPatternsFromStrings(list);
  }

  private List<Pattern> getWhitelist() {
    List<String> list = this.state.getPropAsList(COMPACTION_WHITELIST, StringUtils.EMPTY);
    return DatasetFilterUtils.getPatternsFromStrings(list);
  }

  private List<Pattern> getDataComplVerifBlacklist() {
    List<String> list = this.state.getPropAsList(COMPACTION_COMPLETENESS_VERIFICATION_BLACKLIST, StringUtils.EMPTY);
    return DatasetFilterUtils.getPatternsFromStrings(list);
  }

  private List<Pattern> getDataComplVerifWhitelist() {
    List<String> list = this.state.getPropAsList(COMPACTION_COMPLETENESS_VERIFICATION_WHITELIST, StringUtils.EMPTY);
    return DatasetFilterUtils.getPatternsFromStrings(list);
  }

  private void processTopics(List<String> topics) throws IOException {
    createJobPropsForTopics(topics);
    processCompactionJobs();
  }

  /**
   * Identify {@link Dataset}s from topics, and create compaction job properties for each {@link Dataset}.
   */
  private void createJobPropsForTopics(List<String> topics) {
    List<Pattern> highPriorityTopicPatterns = getHighPriorityTopicPatterns();
    List<Pattern> normalPriorityTopicPatterns = getNormalPriorityTopicPatterns();
    for (String topic : topics) {
      if (DatasetFilterUtils.stringInPatterns(topic, highPriorityTopicPatterns)) {
        createJobPropsForTopic(topic, HIGH_PRIORITY);
      } else if (DatasetFilterUtils.stringInPatterns(topic, normalPriorityTopicPatterns)) {
        createJobPropsForTopic(topic, NORMAL_PRIORITY);
      } else {
        createJobPropsForTopic(topic, LOW_PRIORITY);
      }
    }
  }

  private List<Pattern> getHighPriorityTopicPatterns() {
    List<String> list = this.state.getPropAsList(COMPACTION_HIGH_PRIORITY_TOPICS, StringUtils.EMPTY);
    return DatasetFilterUtils.getPatternsFromStrings(list);
  }

  private List<Pattern> getNormalPriorityTopicPatterns() {
    List<String> list = this.state.getPropAsList(COMPACTION_NORMAL_PRIORITY_TOPICS, StringUtils.EMPTY);
    return DatasetFilterUtils.getPatternsFromStrings(list);
  }

  /**
   * Identify {@link Dataset}s from a topic, and create compaction job properties for each {@link Dataset}.
   */
  private void createJobPropsForTopic(String topic, double priority) {
    LOG.info("Creating compaction jobs for topic " + topic + " with priority " + priority);
    MRCompactorJobPropCreator jobPropCreator = getJobPropCreator(topic, priority);
    try {
      this.datasets.addAll(jobPropCreator.createJobProps());
    } catch (Throwable t) {

      // If a throwable is caught when creating job properties for a topic, skip the topic and add the throwable
      // to the dataset.
      this.datasets.add(jobPropCreator.createFailedJobProps(t));
    }
  }

  /**
   * Get an instance of {@link MRCompactorJobPropCreator}, e.g., {@link MRCompactorTimeBasedJobPropCreator}
   * for time-based compaction.
   */

  MRCompactorJobPropCreator getJobPropCreator(String topic, double priority) {
    String builderClassName =
        this.state.getProp(COMPACTION_JOBPROPS_CREATOR_CLASS, DEFAULT_COMPACTION_JOBPROPS_CREATOR_CLASS) + "$Builder";

    try {
      return ((MRCompactorJobPropCreator.Builder<?>) Class.forName(builderClassName).newInstance()).withTopic(topic)
          .withPriority(priority).withTopicInputDir(new Path(this.inputDir, new Path(topic, this.inputSubDir)))
          .withTopicInputLateDir(new Path(this.inputDir, new Path(topic, this.inputLateSubDir)))
          .withTopicOutputDir(new Path(this.destDir, new Path(topic, this.destSubDir)))
          .withTopicOutputLateDir(new Path(this.destDir, new Path(topic, this.destLateSubDir)))
          .withTopicTmpOutputDir(new Path(this.tmpOutputDir, new Path(topic, this.destSubDir))).withFileSystem(this.fs)
          .withState(this.state).build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void processCompactionJobs() throws IOException {
    if (this.shouldVerifDataCompl) {
      verifyDataCompleteness();
    } else {
      setAllDatasetStatesToVerified();
    }

    this.submitCompactionJobsAndWaitForCompletion();
  }

  private boolean shouldVerifyDataCompleteness() {
    return this.state.getPropAsBoolean(COMPACTION_COMPLETENESS_VERIFICATION_ENABLED,
        DEFAULT_COMPACTION_COMPLETENESS_VERIFICATION_ENABLED);
  }

  private void verifyDataCompleteness() {
    List<Pattern> blacklist = getDataComplVerifBlacklist();
    List<Pattern> whitelist = getDataComplVerifWhitelist();
    int numDatasetsVerifiedTogether = getNumDatasetsVerifiedTogether();
    List<Dataset> datasetsToBeVerified = Lists.newArrayList();
    for (Dataset dataset : this.datasets) {
      if (dataset.state() != UNVERIFIED) {
        continue;
      }
      if (shouldVerifyCompletenessForDataset(dataset, blacklist, whitelist)) {
        datasetsToBeVerified.add(dataset);
        if (datasetsToBeVerified.size() >= numDatasetsVerifiedTogether) {
          ListenableFuture<Results> future = this.verifier.get().verify(datasetsToBeVerified);
          addCallback(datasetsToBeVerified, future);
          datasetsToBeVerified = Lists.newArrayList();
        }
      } else {
        dataset.setState(VERIFIED);
      }
    }

    if (!datasetsToBeVerified.isEmpty()) {
      ListenableFuture<Results> future = this.verifier.get().verify(datasetsToBeVerified);
      addCallback(datasetsToBeVerified, future);
    }
  }

  /**
   * A {@link Dataset} should be verified if its not already compacted, and it satisfies the blacklist and whitelist.
   */
  private boolean shouldVerifyCompletenessForDataset(Dataset dataset, List<Pattern> blacklist,
      List<Pattern> whitelist) {
    return !datasetAlreadyCompacted(this.fs, dataset)
        && DatasetFilterUtils.survived(dataset.topic(), blacklist, whitelist);
  }

  /**
   * A {@link Dataset} is considered already compacted if there is a file named
   * {@link MRCompactor#COMPACTION_COMPLETE_FILE_NAME} in its {@link Dataset#outputPath()}.
   */
  public static boolean datasetAlreadyCompacted(FileSystem fs, Dataset dataset) {
    Path filePath = new Path(dataset.outputPath(), MRCompactor.COMPACTION_COMPLETE_FILE_NAME);
    try {
      return fs.exists(filePath);
    } catch (IOException e) {
      LOG.error("Failed to verify the existence of file " + filePath, e);
      return false;
    }
  }

  public static long readCompactionTimestamp(FileSystem fs, Path compactionOutputPath) throws IOException {
    Closer closer = Closer.create();
    Path completionFilePath = new Path(compactionOutputPath, COMPACTION_COMPLETE_FILE_NAME);
    try {
      FSDataInputStream completionFileStream = closer.register(fs.open(completionFilePath));
      return completionFileStream.readLong();
    } catch (Throwable t) {
      LOG.error("Failed to read compaction timestamp from " + compactionOutputPath, t);
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  private void addCallback(final List<Dataset> datasetsToBeVerified, ListenableFuture<Results> future) {

    Futures.addCallback(future, new FutureCallback<Results>() {

      /**
       * On success, resubmit verification for the {@link Dataset}s that should be resubmitted
       * (i.e., verification didn't pass and it didn't timeout).
       */
      @Override
      public void onSuccess(Results results) {
        List<Dataset> datasetsToBeVerifiedAgain = Lists.newArrayList();
        for (Results.Result result : results) {
          Optional<MRCompactorJobRunner> jobRunner =
              Optional.fromNullable(MRCompactor.this.jobRunnables.get(result.dataset()));

          switch (result.status()) {
            case PASSED:
              LOG.info("Completeness verification for dataset " + result.dataset() + " passed.");
              submitSlaEvent(result.dataset(), "CompletenessVerified");
              result.dataset().setState(VERIFIED);
              if (jobRunner.isPresent()) {
                jobRunner.get().proceed();
              }
              break;
            case FAILED:
              if (shouldGiveUpVerification()) {
                LOG.info("Completeness verification for dataset " + result.dataset() + " has timed out.");
                submitSlaEvent(result.dataset(), "CompletenessCannotBeVerified");
                result.dataset().setState(GIVEN_UP);
                result.dataset().addThrowable(new RuntimeException(
                    String.format("Completeness verification for dataset %s failed or timed out.", result.dataset())));
              } else {
                LOG.info("Completeness verification for dataset " + result.dataset() + " failed. Will verify again.");
                datasetsToBeVerifiedAgain.add(result.dataset());
              }
              break;
          }
        }

        if (!datasetsToBeVerifiedAgain.isEmpty()) {
          ListenableFuture<Results> future2 = MRCompactor.this.verifier.get().verify(datasetsToBeVerifiedAgain);
          addCallback(datasetsToBeVerifiedAgain, future2);
        }
      }

      /**
       * On failure, resubmit verification for all {@link Dataset}s, unless timed out.
       */
      @Override
      public void onFailure(Throwable t) {
        LOG.error("Failed to verify completeness for the following datasets: " + datasetsToBeVerified, t);

        if (shouldGiveUpVerification()) {
          for (Dataset dataset : datasetsToBeVerified) {
            LOG.warn(String.format("Completeness verification for dataset %s has timed out.", dataset));
            submitSlaEvent(dataset, "CompletenessCannotBeVerified");
            dataset.setState(GIVEN_UP);
            dataset.addThrowable(new RuntimeException(
                String.format("Completeness verification for dataset %s failed or timed out.", dataset)));
          }
        } else {
          ListenableFuture<Results> future2 = MRCompactor.this.verifier.get().verify(datasetsToBeVerified);
          addCallback(datasetsToBeVerified, future2);
        }
      }
    });
  }

  /**
   * Get the number of {@link Dataset}s to be verified together. This allows multiple {@link Dataset}s
   * to share the same verification job, e.g., share the same query.
   */
  private int getNumDatasetsVerifiedTogether() {
    return this.state.getPropAsInt(COMPACTION_COMPLETENESS_VERIFICATION_NUM_DATASETS_VERIFIED_TOGETHER,
        DEFAULT_COMPACTION_COMPLETENESS_VERIFICATION_NUM_DATASETS_VERIFIED_TOGETHER);
  }

  private void setAllDatasetStatesToVerified() {
    for (Dataset dataset : this.datasets) {
      dataset.compareAndSetState(UNVERIFIED, VERIFIED);
    }
  }

  /**
   * Data completeness verification of a folder should give up if timed out.
   */
  private boolean shouldGiveUpVerification() {
    return this.stopwatch.elapsed(TimeUnit.MINUTES) >= this.dataVerifTimeoutMinutes;
  }

  private boolean shouldPublishDataIfCannotVerifyCompl() {
    return this.state.getPropAsBoolean(COMPACTION_COMPLETENESS_VERIFICATION_PUBLISH_DATA_IF_CANNOT_VERIFY,
        DEFAULT_COMPACTION_COMPLETENESS_VERIFICATION_PUBLISH_DATA_IF_CANNOT_VERIFY);
  }

  private void submitCompactionJobsAndWaitForCompletion() throws IOException {
    LOG.info("Submitting compaction jobs. Number of datasets: " + this.datasets.size());

    boolean allDatasetsCompleted = false;
    while (!allDatasetsCompleted) {
      allDatasetsCompleted = true;
      for (Dataset dataset : this.datasets) {
        MRCompactorJobRunner jobRunner = MRCompactor.this.jobRunnables.get(dataset);

        if (dataset.state() == VERIFIED || dataset.state() == UNVERIFIED) {
          allDatasetsCompleted = false;

          // Run compaction for a dataset, if it is not already running or completed
          if (jobRunner == null || jobRunner.status() == ABORTED) {
            runCompactionForDataset(dataset, dataset.state() == VERIFIED ? true : false);
          }
        } else if (dataset.state() == GIVEN_UP) {
          if (this.shouldPublishDataIfCannotVerifyCompl) {
            allDatasetsCompleted = false;
            if (jobRunner == null || jobRunner.status() == ABORTED) {
              runCompactionForDataset(dataset, true);
            } else {
              jobRunner.proceed();
            }
          } else {
            if (jobRunner != null) {
              jobRunner.abort();
            }
          }
        }
      }

      if (this.stopwatch.elapsed(TimeUnit.MINUTES) >= this.compactionTimeoutMinutes) {

        // Compaction timed out. Killing all compaction jobs running
        LOG.error("Compaction timed-out. Killing all running jobs");
        for (MRCompactorJobRunner jobRunner : MRCompactor.this.jobRunnables.values()) {
          jobRunner.abort();
        }
        break;
      }

      // Sleep for a few seconds before another round
      try {
        Thread.sleep(TimeUnit.SECONDS.toMillis(COMPACTION_JOB_WAIT_INTERVAL_SECONDS));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting", e);
      }
    }
  }

  /**
   * Run compaction job for a {@link Dataset}.
   *
   * @param dataset The input {@link Dataset} to run compaction for.
   * @param proceed Whether the compaction job is permitted to publish data. If data completeness verification
   * is enabled and the status of the inputFolder is UNVERIFIED, 'proceed' should be set to false.
   * Otherwise it should be set to true.
   */
  private void runCompactionForDataset(Dataset dataset, boolean proceed) {
    LOG.info("Running compaction for dataset " + dataset);

    try {
      MRCompactorJobRunner jobRunner = getMRCompactorJobRunner(dataset, dataset.priority());
      this.jobRunnables.put(dataset, jobRunner);
      if (proceed) {
        jobRunner.proceed();
      }
      this.jobExecutor.execute(jobRunner);
    } catch (Throwable t) {
      dataset.skip(t);
    }
  }

  /**
   * Get an instance of {@link MRCompactorJobRunner}.
   */
  private MRCompactorJobRunner getMRCompactorJobRunner(Dataset dataset, double priority) {
    try {
      @SuppressWarnings("unchecked")
      Class<? extends MRCompactorJobRunner> cls = (Class<? extends MRCompactorJobRunner>) Class
          .forName(this.state.getProp(COMPACTION_JOB_RUNNER_CLASS, DEFAULT_COMPACTION_JOB_RUNNER_CLASS));
      return cls.getDeclaredConstructor(Dataset.class, FileSystem.class, Double.class).newInstance(dataset, this.fs,
          priority);
    } catch (Exception e) {
      throw new RuntimeException("Cannot instantiate MRCompactorJobRunner", e);
    }
  }

  /**
   * Keep track of running MR jobs, so if the compaction is cancelled, the MR jobs can be killed.
   */
  public static void addRunningHadoopJob(Dataset dataset, Job job) {
    MRCompactor.RUNNING_MR_JOBS.put(dataset, job);
  }

  private long getCompactionTimeoutMinutes() {
    return this.state.getPropAsLong(COMPACTION_MR_JOB_TIMEOUT_MINUTES, DEFAULT_COMPACTION_MR_JOB_TIMEOUT_MINUTES);
  }

  private long getDataVerifTimeoutMinutes() {
    return this.state.getPropAsLong(COMPACTION_VERIFICATION_TIMEOUT_MINUTES,
        DEFAULT_COMPACTION_VERIFICATION_TIMEOUT_MINUTES);
  }

  private void throwExceptionsIfAnyDatasetCompactionFailed() {
    Set<Dataset> datasetsWithThrowables = getDatasetsWithThrowables();
    int numDatasetsWithThrowables = 0;
    for (Dataset dataset : datasetsWithThrowables) {
      numDatasetsWithThrowables++;
      for (Throwable t : dataset.throwables()) {
        LOG.error("Error processing dataset " + dataset, t);
        submitSlaEvent(dataset, "CompactionFailed");
      }
    }
    if (numDatasetsWithThrowables > 0) {
      throw new RuntimeException(String.format("Failed to process %d datasets.", numDatasetsWithThrowables));
    }
  }

  private void submitSlaEvent(Dataset dataset, String eventName) {
    CompactionSlaEventHelper.populateState(dataset, Optional.<Job> absent(), this.fs);
    new SlaEventSubmitter(this.eventSubmitter, eventName, dataset.jobProps().getProperties()).submit();
  }

  /**
   * Return all {@link Dataset}s where a {@link Throwable} is thrown from the compaction job.
   */
  private Set<Dataset> getDatasetsWithThrowables() {
    Set<Dataset> datasetsWithThrowables = Sets.newHashSet();
    for (Dataset dataset : this.datasets) {
      if (!dataset.throwables().isEmpty()) {
        datasetsWithThrowables.add(dataset);
      }
    }
    return datasetsWithThrowables;
  }

  private void shutdownExecutors() throws IOException {
    LOG.info("Shutting down Executors");
    ExecutorsUtils.shutdownExecutorService(this.jobExecutor, Optional.of(LOG));
  }

  @Override
  public void cancel() throws IOException {
    try {
      for (Map.Entry<Dataset, Job> entry : MRCompactor.RUNNING_MR_JOBS.entrySet()) {
        Job hadoopJob = entry.getValue();
        if (!hadoopJob.isComplete()) {
          LOG.info(String.format("Killing hadoop job %s for dataset %s", hadoopJob.getJobID(), entry.getKey()));
          hadoopJob.killJob();
        }
      }
    } finally {
      try {
        ExecutorsUtils.shutdownExecutorService(this.jobExecutor, Optional.of(LOG), 0, TimeUnit.NANOSECONDS);
      } finally {
        this.verifier.get().closeNow();
      }
    }
  }

  /**
   * A subclass of {@link ThreadPoolExecutor} for running compaction jobs, and performs necessary steps
   * after each compaction job finishes.
   */
  private class JobRunnerExecutor extends ThreadPoolExecutor {

    public JobRunnerExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
        BlockingQueue<Runnable> workQueue) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    /**
     * When a compaction job for a {@link Dataset} finishes, if it successfully published the data (t == null
     * && jobRunner.status() == {@link MRCompactorJobRunner.Status#COMMITTED}, or if it
     * threw any {@link Throwable} (t != null), mark the {@link Dataset} as
     * {@link Dataset.DatasetState#COMPACTION_COMPLETE}.
     * If the job failed to publish the data because the input data was not complete, reduce the priority of
     * the {@link Dataset}. A new compaction job will be submitted later with a lower priority.
     */
    @Override
    protected void afterExecute(Runnable r, Throwable t) {
      Preconditions.checkArgument(r instanceof MRCompactorJobRunner,
          String.format("Runnable expected to be instance of %s, actual %s", MRCompactorJobRunner.class.getSimpleName(),
              r.getClass().getSimpleName()));

      MRCompactorJobRunner jobRunner = (MRCompactorJobRunner) r;
      MRCompactor.this.jobRunnables.remove(jobRunner.getDataset());
      if (t == null) {
        if (jobRunner.status() == COMMITTED) {

          // Compaction job of a dataset is successful.
          jobRunner.getDataset().setState(COMPACTION_COMPLETE);
        } else
          if (jobRunner.getDataset().state() == GIVEN_UP && !MRCompactor.this.shouldPublishDataIfCannotVerifyCompl) {

          // Compaction job of a dataset has aborted, and data completeness verification has given up.
          // This dataset will not be compacted.
          LOG.info(String.format("Dataset %s will not be compacted, since data completeness cannot be verified",
              jobRunner.getDataset()));
          jobRunner.getDataset().setState(COMPACTION_COMPLETE);
        } else {

          // Compaction job of a dataset has aborted because data completeness is not verified.
          // Reduce priority and try again.
          jobRunner.getDataset().reducePriority();
        }
      } else {

        // Compaction job of a dataset has failed with a throwable.
        afterExecuteWithThrowable(jobRunner, t);
      }
    }

    private void afterExecuteWithThrowable(MRCompactorJobRunner jobRunner, Throwable t) {
      jobRunner.getDataset().skip(t);
    }
  }
}
