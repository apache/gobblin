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

package gobblin.compaction.mapreduce;

import static gobblin.compaction.dataset.Dataset.DatasetState.COMPACTION_COMPLETE;
import static gobblin.compaction.dataset.Dataset.DatasetState.GIVEN_UP;
import static gobblin.compaction.dataset.Dataset.DatasetState.UNVERIFIED;
import static gobblin.compaction.dataset.Dataset.DatasetState.VERIFIED;
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
import java.lang.reflect.InvocationTargetException;

import org.joda.time.DateTime;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import gobblin.compaction.Compactor;
import gobblin.compaction.listeners.CompactorCompletionListener;
import gobblin.compaction.listeners.CompactorCompletionListenerFactory;
import gobblin.compaction.listeners.CompactorListener;
import gobblin.compaction.dataset.Dataset;
import gobblin.compaction.dataset.DatasetsFinder;
import gobblin.compaction.dataset.TimeBasedSubDirDatasetsFinder;
import gobblin.compaction.event.CompactionSlaEventHelper;
import gobblin.compaction.verify.DataCompletenessVerifier;
import gobblin.compaction.verify.DataCompletenessVerifier.Results;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.Tag;
import gobblin.metrics.event.EventSubmitter;
import gobblin.util.ClassAliasResolver;
import gobblin.util.DatasetFilterUtils;
import gobblin.util.ExecutorsUtils;
import gobblin.util.HadoopUtils;
import gobblin.util.ClusterNameTags;
import gobblin.util.FileListUtils;
import gobblin.util.recordcount.CompactionRecordCountProvider;
import gobblin.util.recordcount.IngestionRecordCountProvider;
import gobblin.util.reflection.GobblinConstructorUtils;

/**
 * MapReduce-based {@link gobblin.compaction.Compactor}. Compaction will run on each qualified {@link Dataset}
 * under {@link #COMPACTION_INPUT_DIR}.
 *
 * @author Ziyang Liu
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
  public static final String COMPACTION_JOB_DIR = COMPACTION_PREFIX + "tmp.job.dir";
  public static final String COMPACTION_LATE_DIR_SUFFIX = "_late";

  public static final String COMPACTION_BLACKLIST = COMPACTION_PREFIX + "blacklist";
  public static final String COMPACTION_WHITELIST = COMPACTION_PREFIX + "whitelist";
  public static final String COMPACTION_HIGH_PRIORITY_TOPICS = COMPACTION_PREFIX + "high.priority.topics";
  public static final String COMPACTION_NORMAL_PRIORITY_TOPICS = COMPACTION_PREFIX + "normal.priority.topics";

  public static final String COMPACTION_JOB_RUNNER_CLASS = COMPACTION_PREFIX + "job.runner.class";
  public static final String DEFAULT_COMPACTION_JOB_RUNNER_CLASS =
      "gobblin.compaction.mapreduce.avro.MRCompactorAvroKeyDedupJobRunner";
  public static final String COMPACTION_TIMEZONE = COMPACTION_PREFIX + "timezone";
  public static final String DEFAULT_COMPACTION_TIMEZONE = ConfigurationKeys.PST_TIMEZONE_NAME;
  public static final String COMPACTION_FILE_SYSTEM_URI = COMPACTION_PREFIX + "file.system.uri";
  public static final String COMPACTION_MR_JOB_TIMEOUT_MINUTES = COMPACTION_PREFIX + "mr.job.timeout.minutes";
  public static final long DEFAULT_COMPACTION_MR_JOB_TIMEOUT_MINUTES = Long.MAX_VALUE;

  // Dataset finder to find datasets for compaction.
  public static final String COMPACTION_DATASETS_FINDER = COMPACTION_PREFIX + "datasets.finder";
  public static final String DEFAULT_COMPACTION_DATASETS_FINDER = TimeBasedSubDirDatasetsFinder.class.getName();

  public static final String COMPACTION_DATASETS_MAX_COUNT = COMPACTION_PREFIX + "datasets.max.count";
  public static final double DEFUALT_COMPACTION_DATASETS_MAX_COUNT = 1000000000;

  // Rename source directories as a compaction complete indication
  // Compaction jobs using this completion mode can't share input sources
  public static final String COMPACTION_RENAME_SOURCE_DIR_ENABLED = COMPACTION_PREFIX + "rename.source.dir.enabled";
  public static final boolean DEFAULT_COMPACTION_RENAME_SOURCE_DIR_ENABLED = false;
  public static final String COMPACTION_RENAME_SOURCE_DIR_SUFFIX = "_COMPLETE";


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

  // The threshold of new(late) data that will trigger recompaction per dataset.
  // It follows the pattern DATASET_NAME_REGEX:THRESHOLD;DATASET_NAME_REGEX:THRESHOLD, e.g., A.*,B.*:0.2; C.*,D.*:0.3.
  // Dataset names that match A.* or B.* will have threshold 0.2. Dataset names that match C.* or D.* will have threshold 0.3.
  public static final String COMPACTION_LATEDATA_THRESHOLD_FOR_RECOMPACT_PER_DATASET =
      COMPACTION_PREFIX + "latedata.threshold.for.recompact.per.topic";
  public static final double DEFAULT_COMPACTION_LATEDATA_THRESHOLD_FOR_RECOMPACT_PER_DATASET = 1.0;

  // The threshold of new (late) files that will trigger compaction per dataset.
  // The trigger is based on the file numbers in the late output directory
  public static final String COMPACTION_LATEDATA_THRESHOLD_FILE_NUM =
      COMPACTION_PREFIX + "latedata.threshold.file.num";
  public static final int DEFAULT_COMPACTION_LATEDATA_THRESHOLD_FILE_NUM = 1000;

  // The threshold of new (late) files that will trigger compaction per dataset.
  // The trigger is based on how long the file has been in the late output directory.
  public static final String COMPACTION_LATEDATA_THRESHOLD_DURATION =
      COMPACTION_PREFIX + "latedata.threshold.duration";
  public static final String DEFAULT_COMPACTION_LATEDATA_THRESHOLD_DURATION = "24h";


  public static final String COMPACTION_RECOMPACT_CONDITION = COMPACTION_PREFIX + "recompact.condition";
  public static final String DEFAULT_COMPACTION_RECOMPACT_CONDITION = "RecompactBasedOnRatio";

  public static final String COMPACTION_RECOMPACT_COMBINE_CONDITIONS = COMPACTION_PREFIX + "recompact.combine.conditions";
  public static final String COMPACTION_RECOMPACT_COMBINE_CONDITIONS_OPERATION = COMPACTION_PREFIX + "recompact.combine.conditions.operation";
  public static final String DEFAULT_COMPACTION_RECOMPACT_COMBINE_CONDITIONS_OPERATION = "or";

  public static final String COMPACTION_COMPLETE_LISTERNER = COMPACTION_PREFIX + "complete.listener";
  public static final String DEFAULT_COMPACTION_COMPLETE_LISTERNER = "SimpleCompactorCompletionHook";

  // Whether the input data for the compaction is deduplicated.
  public static final String COMPACTION_INPUT_DEDUPLICATED = COMPACTION_PREFIX + "input.deduplicated";
  public static final boolean DEFAULT_COMPACTION_INPUT_DEDUPLICATED = false;

  // Whether the output of the compaction should be deduplicated.
  public static final String COMPACTION_OUTPUT_DEDUPLICATED = COMPACTION_PREFIX + "output.deduplicated";
  public static final boolean DEFAULT_COMPACTION_OUTPUT_DEDUPLICATED = true;

  public static final String COMPACTION_COMPLETENESS_VERIFICATION_PREFIX =
      COMPACTION_PREFIX + "completeness.verification.";

  public static final String COMPACTION_RECOMPACT_FROM_DEST_PATHS = COMPACTION_PREFIX + "recompact.from.dest.paths";
  public static final String COMPACTION_RECOMPACT_ALL_DATA = COMPACTION_PREFIX + "recompact.all.data";
  public static final boolean DEFAULT_COMPACTION_RECOMPACT_FROM_DEST_PATHS = false;
  public static final boolean DEFAULT_COMPACTION_RECOMPACT_ALL_DATA = true;

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
  public static final String COMPACTION_JAR_SUBDIR = "_gobblin_compaction_jars";
  public static final String COMPACTION_TRACKING_EVENTS_NAMESPACE = COMPACTION_PREFIX + "tracking.events";

  public static final String COMPACTION_INPUT_PATH_TIME = COMPACTION_PREFIX + "input.path.time";

  private static final long COMPACTION_JOB_WAIT_INTERVAL_SECONDS = 10;
  private static final Map<Dataset, Job> RUNNING_MR_JOBS = Maps.newConcurrentMap();

  private final State state;
  private final List<? extends Tag<?>> tags;
  private final Configuration conf;
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
  private final Optional<CompactorListener> compactorListener;
  private final DateTime initilizeTime;
  private final long dataVerifTimeoutMinutes;
  private final long compactionTimeoutMinutes;
  private final boolean shouldVerifDataCompl;
  private final boolean shouldPublishDataIfCannotVerifyCompl;
  private final CompactorCompletionListener compactionCompleteListener;

  public MRCompactor(Properties props, List<? extends Tag<?>> tags, Optional<CompactorListener> compactorListener)
      throws IOException {
    this.state = new State();
    this.state.addAll(props);
    this.initilizeTime = getCurrentTime();
    this.tags = tags;
    this.conf = HadoopUtils.getConfFromState(this.state);
    this.tmpOutputDir = getTmpOutputDir();
    this.fs = getFileSystem();
    this.datasets = getDatasetsFinder().findDistinctDatasets();
    this.jobExecutor = createJobExecutor();
    this.jobRunnables = Maps.newConcurrentMap();
    this.closer = Closer.create();
    this.stopwatch = Stopwatch.createStarted();
    this.gobblinMetrics = initializeMetrics();
    this.eventSubmitter = new EventSubmitter.Builder(
        GobblinMetrics.get(this.state.getProp(ConfigurationKeys.JOB_NAME_KEY)).getMetricContext(),
        MRCompactor.COMPACTION_TRACKING_EVENTS_NAMESPACE).build();
    this.compactorListener = compactorListener;
    this.dataVerifTimeoutMinutes = getDataVerifTimeoutMinutes();
    this.compactionTimeoutMinutes = getCompactionTimeoutMinutes();
    this.shouldVerifDataCompl = shouldVerifyDataCompleteness();
    this.compactionCompleteListener = getCompactionCompleteListener();
    this.verifier =
        this.shouldVerifDataCompl ? Optional.of(this.closer.register(new DataCompletenessVerifier(this.state)))
            : Optional.<DataCompletenessVerifier> absent();
    this.shouldPublishDataIfCannotVerifyCompl = shouldPublishDataIfCannotVerifyCompl();
  }

  public DateTime getInitializeTime() {
    return this.initilizeTime;
  }

  private String getTmpOutputDir() {
    return this.state.getProp(COMPACTION_TMP_DEST_DIR, DEFAULT_COMPACTION_TMP_DEST_DIR);
  }

  private FileSystem getFileSystem() throws IOException {
    if (this.state.contains(COMPACTION_FILE_SYSTEM_URI)) {
      URI uri = URI.create(this.state.getProp(COMPACTION_FILE_SYSTEM_URI));
      return FileSystem.get(uri, this.conf);
    }
    return FileSystem.get(this.conf);
  }

  private DatasetsFinder getDatasetsFinder() {
    try {
      return (DatasetsFinder) Class
          .forName(this.state.getProp(COMPACTION_DATASETS_FINDER, DEFAULT_COMPACTION_DATASETS_FINDER))
          .getConstructor(State.class).newInstance(this.state);
    } catch (Exception e) {
      throw new RuntimeException("Failed to initiailize DatasetsFinder.", e);
    }
  }

  private DateTime getCurrentTime () {
    DateTimeZone timeZone = DateTimeZone
        .forID(this.state.getProp(MRCompactor.COMPACTION_TIMEZONE, MRCompactor.DEFAULT_COMPACTION_TIMEZONE));
    return new DateTime (timeZone);
  }

  private JobRunnerExecutor createJobExecutor() {
    int threadPoolSize = getThreadPoolSize();
    BlockingQueue<Runnable> queue = new PriorityBlockingQueue<>();
    return new JobRunnerExecutor(threadPoolSize, threadPoolSize, Long.MAX_VALUE, TimeUnit.NANOSECONDS, queue);
  }

  private int getThreadPoolSize() {
    return this.state.getPropAsInt(COMPACTION_THREAD_POOL_SIZE, DEFAULT_COMPACTION_THREAD_POOL_SIZE);
  }

  private GobblinMetrics initializeMetrics() {
    ImmutableList.Builder<Tag<?>> tags = ImmutableList.builder();
    tags.addAll(this.tags);
    tags.addAll(Tag.fromMap(ClusterNameTags.getClusterNameTags()));
    GobblinMetrics gobblinMetrics =
        GobblinMetrics.get(this.state.getProp(ConfigurationKeys.JOB_NAME_KEY), null, tags.build());
    gobblinMetrics.startMetricReporting(this.state.getProperties());
    return gobblinMetrics;
  }

  @Override
  public void compact() throws IOException {
    try {
      copyDependencyJarsToHdfs();
      processDatasets();
      throwExceptionsIfAnyDatasetCompactionFailed();
      onCompactionCompletion();
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
        this.gobblinMetrics.stopMetricsReporting();
      }
    }
  }

  private CompactorCompletionListener getCompactionCompleteListener () {
    ClassAliasResolver<CompactorCompletionListenerFactory> classAliasResolver = new ClassAliasResolver<>(CompactorCompletionListenerFactory.class);
    String listenerName= this.state.getProp(MRCompactor.COMPACTION_COMPLETE_LISTERNER,
        MRCompactor.DEFAULT_COMPACTION_COMPLETE_LISTERNER);
    try {
      CompactorCompletionListenerFactory factory = GobblinConstructorUtils.invokeFirstConstructor(
          classAliasResolver.resolveClass(listenerName), ImmutableList.of());
      return factory.createCompactorCompactionListener(this.state);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
        | ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private void onCompactionCompletion() {
    this.compactionCompleteListener.onCompactionCompletion(this);
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

  private void processDatasets() {
    createJobPropsForDatasets();
    processCompactionJobs();
  }

  /**
   * Create compaction job properties for {@link Dataset}s.
   */
  private void createJobPropsForDatasets() {
    final Set<Dataset> datasetsWithProps = Sets.newHashSet();
    for (Dataset dataset : this.datasets) {
      datasetsWithProps.addAll(createJobPropsForDataset(dataset));
    }

    this.datasets.clear();
    this.datasets.addAll(datasetsWithProps);
  }

  /**
   * Existing dataset in {@link #datasets} does not have job props.
   * Create compaction job properties for each given {@link Dataset}.
   * Update datasets based on the results of creating job props for them.
   */
  private List<Dataset> createJobPropsForDataset(Dataset dataset) {
    LOG.info("Creating compaction jobs for dataset " + dataset + " with priority " + dataset.priority());
    final MRCompactorJobPropCreator jobPropCreator = getJobPropCreator(dataset);
    List<Dataset> datasetsWithProps;
    try {
      datasetsWithProps = jobPropCreator.createJobProps();
    } catch (Throwable t) {
      // If a throwable is caught when creating job properties for a dataset, skip the topic and add the throwable
      // to the dataset.
      datasetsWithProps = ImmutableList.<Dataset> of(jobPropCreator.createFailedJobProps(t));
    }
    return datasetsWithProps;
  }

  /**
   * Get an instance of {@link MRCompactorJobPropCreator}.
   */
  MRCompactorJobPropCreator getJobPropCreator(Dataset dataset) {
    try {
      return new MRCompactorJobPropCreator.Builder().withDataset(dataset).withFileSystem(this.fs).withState(this.state)
          .build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Set<Dataset> getDatasets() {
    return this.datasets;
  }

  private void processCompactionJobs() {
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
    List<Pattern> blacklist =
        DatasetFilterUtils.getPatternList(this.state, COMPACTION_COMPLETENESS_VERIFICATION_BLACKLIST);
    List<Pattern> whitelist =
        DatasetFilterUtils.getPatternList(this.state, COMPACTION_COMPLETENESS_VERIFICATION_WHITELIST);
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
    boolean renamingRequired = this.state.getPropAsBoolean(COMPACTION_RENAME_SOURCE_DIR_ENABLED, DEFAULT_COMPACTION_RENAME_SOURCE_DIR_ENABLED);

    LOG.info ("Should verify completeness with renaming source dir : " + renamingRequired);

    return !datasetAlreadyCompacted(this.fs, dataset, renamingRequired)
        && DatasetFilterUtils.survived(dataset.getName(), blacklist, whitelist);
  }

  /**
   * Get all the renamed directories from the given paths
   * They are deepest level containing directories whose name has a suffix {@link MRCompactor#COMPACTION_RENAME_SOURCE_DIR_SUFFIX}
   * Also each directory needs to contain at least one file so empty directories will be excluded from the result
   */
  public static Set<Path> getDeepestLevelRenamedDirsWithFileExistence (FileSystem fs, Set<Path> paths) throws IOException {
    Set<Path> renamedDirs = Sets.newHashSet();
    for (FileStatus fileStatus : FileListUtils.listFilesRecursively(fs, paths)) {
      if (fileStatus.getPath().getParent().toString().endsWith(MRCompactor.COMPACTION_RENAME_SOURCE_DIR_SUFFIX)) {
        renamedDirs.add(fileStatus.getPath().getParent());
      }
    }

    return renamedDirs;
  }

    /**
     * Get all the unrenamed directories from the given paths
     * They are deepest level containing directories whose name doesn't have a suffix {@link MRCompactor#COMPACTION_RENAME_SOURCE_DIR_SUFFIX}
     * Also each directory needs to contain at least one file so empty directories will be excluded from the result
     */
  public static Set<Path> getDeepestLevelUnrenamedDirsWithFileExistence (FileSystem fs, Set<Path> paths) throws IOException {
    Set<Path> unrenamed = Sets.newHashSet();
    for (FileStatus fileStatus : FileListUtils.listFilesRecursively(fs, paths)) {
      if (!fileStatus.getPath().getParent().toString().endsWith(MRCompactor.COMPACTION_RENAME_SOURCE_DIR_SUFFIX)) {
        unrenamed.add(fileStatus.getPath().getParent());
      }
    }

    return unrenamed;
  }

  /**
   * Rename all the source directories for a specific dataset
   */
  public static void renameSourceDirAsCompactionComplete (FileSystem fs, Dataset dataset) {
    try {
      for (Path path: dataset.getRenamePaths()) {
        Path newPath = new Path (path.getParent(), path.getName() + MRCompactor.COMPACTION_RENAME_SOURCE_DIR_SUFFIX);
        LOG.info("[{}] Renaming {} to {}", dataset.getDatasetName(), path, newPath);
        fs.rename(path, newPath);
      }
    } catch (Exception e) {
      LOG.error ("Rename input path failed", e);
    }
  }

  /**
   * A {@link Dataset} is considered already compacted if either condition is true:
   * 1) When completion file strategy is used, a compaction completion means there is a file named
   *    {@link MRCompactor#COMPACTION_COMPLETE_FILE_NAME} in its {@link Dataset#outputPath()}.
   * 2) When renaming source directory strategy is used, a compaction completion means source directories
   *    {@link Dataset#inputPaths()} contains at least one directory which has been renamed to something with
   *    {@link MRCompactor#COMPACTION_RENAME_SOURCE_DIR_SUFFIX}.
   */
  public static boolean datasetAlreadyCompacted(FileSystem fs, Dataset dataset, boolean renameSourceEnable) {
    if (renameSourceEnable) {
      return checkAlreadyCompactedBasedOnSourceDirName (fs, dataset);
    } else {
      return checkAlreadyCompactedBasedOnCompletionFile(fs, dataset);
    }
  }


  /**  When renaming source directory strategy is used, a compaction completion means source directories
   *    {@link Dataset#inputPaths()} contains at least one directory which has been renamed to something with
   *    {@link MRCompactor#COMPACTION_RENAME_SOURCE_DIR_SUFFIX}.
   */
  private static boolean checkAlreadyCompactedBasedOnSourceDirName (FileSystem fs, Dataset dataset) {
    try {
      Set<Path> renamedDirs = getDeepestLevelRenamedDirsWithFileExistence(fs, dataset.inputPaths());
      return !renamedDirs.isEmpty();
    } catch (IOException e) {
      LOG.error("Failed to get deepest directories from source", e);
      return false;
    }
  }

 /**
  *  When completion file strategy is used, a compaction completion means there is a file named
  *    {@link MRCompactor#COMPACTION_COMPLETE_FILE_NAME} in its {@link Dataset#outputPath()}.
  */
  private static boolean checkAlreadyCompactedBasedOnCompletionFile(FileSystem fs, Dataset dataset) {
    Path filePath = new Path(dataset.outputPath(), MRCompactor.COMPACTION_COMPLETE_FILE_NAME);
    try {
      return fs.exists(filePath);
    } catch (IOException e) {
      LOG.error("Failed to verify the existence of file " + filePath, e);
      return false;
    }
  }

  public static long readCompactionTimestamp(FileSystem fs, Path compactionOutputPath) throws IOException {
    Path completionFilePath = new Path(compactionOutputPath, COMPACTION_COMPLETE_FILE_NAME);
    try (FSDataInputStream completionFileStream = fs.open(completionFilePath)) {
      return completionFileStream.readLong();
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
              submitVerificationSuccessSlaEvent(result);
              result.dataset().setState(VERIFIED);
              if (jobRunner.isPresent()) {
                jobRunner.get().proceed();
              }
              break;
            case FAILED:
              if (shouldGiveUpVerification()) {
                LOG.info("Completeness verification for dataset " + result.dataset() + " has timed out.");
                submitVerificationSuccessSlaEvent(result);
                result.dataset().setState(GIVEN_UP);
                result.dataset().addThrowable(new RuntimeException(
                    String.format("Completeness verification for dataset %s failed or timed out.", result.dataset())));
              } else {
                LOG.info("Completeness verification for dataset " + result.dataset() + " failed. Will verify again.");
                datasetsToBeVerifiedAgain.add(result.dataset());
              }
              break;
            default:
              throw new IllegalStateException("Unrecognized result status: " + result.status());
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
            submitFailureSlaEvent(dataset, CompactionSlaEventHelper.COMPLETION_VERIFICATION_FAILED_EVENT_NAME);
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

  private void submitCompactionJobsAndWaitForCompletion() {
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
            runCompactionForDataset(dataset, dataset.state() == VERIFIED);
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
      MRCompactorJobRunner jobRunner = getMRCompactorJobRunner(dataset);
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
  private MRCompactorJobRunner getMRCompactorJobRunner(Dataset dataset) {
    try {
      @SuppressWarnings("unchecked")
      Class<? extends MRCompactorJobRunner> cls = (Class<? extends MRCompactorJobRunner>) Class
          .forName(this.state.getProp(COMPACTION_JOB_RUNNER_CLASS, DEFAULT_COMPACTION_JOB_RUNNER_CLASS));
      return cls.getDeclaredConstructor(Dataset.class, FileSystem.class).newInstance(dataset, this.fs);
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
        submitFailureSlaEvent(dataset, CompactionSlaEventHelper.COMPACTION_FAILED_EVENT_NAME);
      }
    }
    if (numDatasetsWithThrowables > 0) {
      throw new RuntimeException(String.format("Failed to process %d datasets.", numDatasetsWithThrowables));
    }
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

  private void shutdownExecutors() {
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
        if (this.verifier.isPresent()) {
          this.verifier.get().closeNow();
        }
      }
    }
  }

  public static void modifyDatasetStateToRecompact (Dataset dataset) {
    // Modify the dataset for recompaction
    LOG.info ("{} changes to recompact mode", dataset.getDatasetName());
    State recompactState = new State();
    recompactState.setProp(MRCompactor.COMPACTION_RECOMPACT_FROM_DEST_PATHS, Boolean.TRUE);
    recompactState.setProp(MRCompactor.COMPACTION_JOB_LATE_DATA_MOVEMENT_TASK, Boolean.FALSE);
    dataset.modifyDatasetForRecompact(recompactState);
    dataset.setState(VERIFIED);
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
          if (jobRunner.getDataset().needToRecompact()) {
            modifyDatasetStateToRecompact (jobRunner.getDataset());
          } else {
            // Set the dataset status to COMPACTION_COMPLETE if compaction is successful.
            jobRunner.getDataset().setState(COMPACTION_COMPLETE);
          }
          if (MRCompactor.this.compactorListener.isPresent()) {
            try {
              MRCompactor.this.compactorListener.get().onDatasetCompactionCompletion(jobRunner.getDataset());
            } catch (Exception e) {
              t = e;
            }
          }
        } else if (jobRunner.getDataset().state() == GIVEN_UP
            && !MRCompactor.this.shouldPublishDataIfCannotVerifyCompl) {

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
      }
      if (t != null) {
        // Compaction job of a dataset has failed with a throwable.
        afterExecuteWithThrowable(jobRunner, t);
      }
    }

    private void afterExecuteWithThrowable(MRCompactorJobRunner jobRunner, Throwable t) {
      jobRunner.getDataset().skip(t);
    }
  }

  /**
   * Submit an event when completeness verification is successful
   */
  private void submitVerificationSuccessSlaEvent(Results.Result result) {
    try {
      CompactionSlaEventHelper.getEventSubmitterBuilder(result.dataset(), Optional.<Job> absent(), this.fs)
      .eventSubmitter(this.eventSubmitter).eventName(CompactionSlaEventHelper.COMPLETION_VERIFICATION_SUCCESS_EVENT_NAME)
      .additionalMetadata(Maps.transformValues(result.verificationContext(), Functions.toStringFunction())).build()
      .submit();
    } catch (Throwable t) {
      LOG.warn("Failed to submit verification success event:" + t, t);
    }
  }

  /**
   * Submit a failure sla event
   */
  private void submitFailureSlaEvent(Dataset dataset, String eventName) {
    try {
      CompactionSlaEventHelper.getEventSubmitterBuilder(dataset, Optional.<Job> absent(), this.fs)
      .eventSubmitter(this.eventSubmitter).eventName(eventName).build().submit();
    } catch (Throwable t) {
      LOG.warn("Failed to submit failure sla event:" + t, t);
    }
  }
}
