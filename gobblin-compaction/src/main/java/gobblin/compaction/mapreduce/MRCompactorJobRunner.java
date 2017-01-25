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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.math3.primes.Primes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

import gobblin.compaction.dataset.Dataset;
import gobblin.compaction.dataset.DatasetHelper;
import gobblin.compaction.event.CompactionSlaEventHelper;
import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.event.EventSubmitter;
import gobblin.util.ExecutorsUtils;
import gobblin.util.FileListUtils;
import gobblin.util.HadoopUtils;
import gobblin.util.RecordCountProvider;
import gobblin.util.WriterUtils;
import gobblin.util.executors.ScalingThreadPoolExecutor;
import gobblin.util.recordcount.LateFileRecordCountProvider;



/**
 * This class is responsible for configuring and running a single MR job.
 * It should be extended by a subclass that properly configures the mapper and reducer related classes.
 *
 * The properties that control the number of reducers are compaction.target.output.file.size and
 * compaction.max.num.reducers. The number of reducers will be the smaller of
 * [total input size] / [compaction.target.output.file.size] + 1 and [compaction.max.num.reducers].
 *
 * If {@value MRCompactor#COMPACTION_JOB_LATE_DATA_MOVEMENT_TASK} is set to true, does not
 * launch an MR job. Instead, just copies the files present in
 * {@value MRCompactor#COMPACTION_JOB_LATE_DATA_FILES} to a 'late' subdirectory within
 * the output directory.
 *
 * @author Ziyang Liu
 */
@SuppressWarnings("deprecation")
public abstract class MRCompactorJobRunner implements Runnable, Comparable<MRCompactorJobRunner> {

  private static final Logger LOG = LoggerFactory.getLogger(MRCompactorJobRunner.class);

  private static final String COMPACTION_JOB_PREFIX = "compaction.job.";

  /**
   * Properties related to the compaction job of a dataset.
   */
  private static final String COMPACTION_JOB_OUTPUT_DIR_PERMISSION = COMPACTION_JOB_PREFIX + "output.dir.permission";
  private static final String COMPACTION_JOB_TARGET_OUTPUT_FILE_SIZE =
      COMPACTION_JOB_PREFIX + "target.output.file.size";
  private static final long DEFAULT_COMPACTION_JOB_TARGET_OUTPUT_FILE_SIZE = 536870912;
  private static final String COMPACTION_JOB_MAX_NUM_REDUCERS = COMPACTION_JOB_PREFIX + "max.num.reducers";
  private static final int DEFAULT_COMPACTION_JOB_MAX_NUM_REDUCERS = 900;
  private static final String COMPACTION_JOB_OVERWRITE_OUTPUT_DIR = COMPACTION_JOB_PREFIX + "overwrite.output.dir";
  private static final boolean DEFAULT_COMPACTION_JOB_OVERWRITE_OUTPUT_DIR = false;
  private static final String COMPACTION_JOB_ABORT_UPON_NEW_DATA = COMPACTION_JOB_PREFIX + "abort.upon.new.data";
  private static final boolean DEFAULT_COMPACTION_JOB_ABORT_UPON_NEW_DATA = false;
  private static final String COMPACTION_COPY_LATE_DATA_THREAD_POOL_SIZE =
      COMPACTION_JOB_PREFIX + "copy.latedata.thread.pool.size";
  private static final int DEFAULT_COMPACTION_COPY_LATE_DATA_THREAD_POOL_SIZE = 5;

  // If true, the MR job will use either 1 reducer or a prime number of reducers.
  private static final String COMPACTION_JOB_USE_PRIME_REDUCERS = COMPACTION_JOB_PREFIX + "use.prime.reducers";
  private static final boolean DEFAULT_COMPACTION_JOB_USE_PRIME_REDUCERS = true;

  private static final String HADOOP_JOB_NAME = "Gobblin MR Compaction";
  private static final long MR_JOB_CHECK_COMPLETE_INTERVAL_MS = 5000;

  public enum Policy {

    // The job runner is permitted to publish the data.
    DO_PUBLISH_DATA,

    // The job runner can proceed with the compaction for now but should not publish the data.
    DO_NOT_PUBLISH_DATA,

    // The job runner should abort asap without publishing data.
    ABORT_ASAP
  }

  public enum Status {
    ABORTED,
    COMMITTED,
    RUNNING
  }

  protected final Dataset dataset;
  protected final FileSystem fs;
  protected final FsPermission perm;
  protected final boolean shouldDeduplicate;
  protected final boolean outputDeduplicated;
  protected final boolean recompactFromDestPaths;
  protected final boolean recompactAllData;
  protected final boolean usePrimeReducers;
  protected final EventSubmitter eventSubmitter;
  private final RecordCountProvider inputRecordCountProvider;
  private final RecordCountProvider outputRecordCountProvider;
  private final LateFileRecordCountProvider lateInputRecordCountProvider;
  private final LateFileRecordCountProvider lateOutputRecordCountProvider;
  private final DatasetHelper datasetHelper;
  private final int copyLateDataThreadPoolSize;

  private volatile Policy policy = Policy.DO_NOT_PUBLISH_DATA;
  private volatile Status status = Status.RUNNING;
  private final Cache<Path, List<Path>> applicablePathCache;

  protected MRCompactorJobRunner(Dataset dataset, FileSystem fs) {
    this.dataset = dataset;
    this.fs = fs;
    this.perm = HadoopUtils.deserializeFsPermission(this.dataset.jobProps(), COMPACTION_JOB_OUTPUT_DIR_PERMISSION,
        FsPermission.getDefault());
    this.recompactFromDestPaths = this.dataset.jobProps().getPropAsBoolean(
        MRCompactor.COMPACTION_RECOMPACT_FROM_DEST_PATHS, MRCompactor.DEFAULT_COMPACTION_RECOMPACT_FROM_DEST_PATHS);
    this.recompactAllData = this.dataset.jobProps().getPropAsBoolean(
        MRCompactor.COMPACTION_RECOMPACT_ALL_DATA, MRCompactor.DEFAULT_COMPACTION_RECOMPACT_ALL_DATA);
    Preconditions.checkArgument(this.dataset.jobProps().contains(MRCompactor.COMPACTION_SHOULD_DEDUPLICATE),
        String.format("Missing property %s for dataset %s", MRCompactor.COMPACTION_SHOULD_DEDUPLICATE, this.dataset));
    this.shouldDeduplicate = this.dataset.jobProps().getPropAsBoolean(MRCompactor.COMPACTION_SHOULD_DEDUPLICATE);

    this.outputDeduplicated = this.dataset.jobProps().getPropAsBoolean(MRCompactor.COMPACTION_OUTPUT_DEDUPLICATED,
        MRCompactor.DEFAULT_COMPACTION_OUTPUT_DEDUPLICATED);

    this.usePrimeReducers = this.dataset.jobProps().getPropAsBoolean(COMPACTION_JOB_USE_PRIME_REDUCERS,
        DEFAULT_COMPACTION_JOB_USE_PRIME_REDUCERS);

    this.eventSubmitter = new EventSubmitter.Builder(
        GobblinMetrics.get(this.dataset.jobProps().getProp(ConfigurationKeys.JOB_NAME_KEY)).getMetricContext(),
        MRCompactor.COMPACTION_TRACKING_EVENTS_NAMESPACE).build();

    this.copyLateDataThreadPoolSize = this.dataset.jobProps().getPropAsInt(COMPACTION_COPY_LATE_DATA_THREAD_POOL_SIZE,
        DEFAULT_COMPACTION_COPY_LATE_DATA_THREAD_POOL_SIZE);

    try {
      this.inputRecordCountProvider = (RecordCountProvider) Class
          .forName(this.dataset.jobProps().getProp(MRCompactor.COMPACTION_INPUT_RECORD_COUNT_PROVIDER,
              MRCompactor.DEFAULT_COMPACTION_INPUT_RECORD_COUNT_PROVIDER))
          .newInstance();
      this.outputRecordCountProvider = (RecordCountProvider) Class
          .forName(this.dataset.jobProps().getProp(MRCompactor.COMPACTION_OUTPUT_RECORD_COUNT_PROVIDER,
              MRCompactor.DEFAULT_COMPACTION_OUTPUT_RECORD_COUNT_PROVIDER))
          .newInstance();
      this.lateInputRecordCountProvider = new LateFileRecordCountProvider(this.inputRecordCountProvider);
      this.lateOutputRecordCountProvider = new LateFileRecordCountProvider(this.outputRecordCountProvider);
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate RecordCountProvider", e);
    }

    this.applicablePathCache = CacheBuilder.newBuilder().maximumSize(2000).build();
    this.datasetHelper = new DatasetHelper(this.dataset, this.fs, this.getApplicableFileExtensions());

  }

  @Override
  public void run() {
    Configuration conf = HadoopUtils.getConfFromState(this.dataset.jobProps());

    // Turn on mapreduce output compression by default
    if (conf.get("mapreduce.output.fileoutputformat.compress") == null && conf.get("mapred.output.compress") == null) {
      conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
    }

    // Disable delegation token cancellation by default
    if (conf.get("mapreduce.job.complete.cancel.delegation.tokens") == null) {
      conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);
    }

    try {
      DateTime compactionTimestamp = getCompactionTimestamp();
      LOG.info("MR Compaction Job Timestamp " + compactionTimestamp.getMillis());
      if (this.dataset.jobProps().getPropAsBoolean(MRCompactor.COMPACTION_JOB_LATE_DATA_MOVEMENT_TASK, false)) {
        List<Path> newLateFilePaths = Lists.newArrayList();
        for (String filePathString : this.dataset.jobProps()
            .getPropAsList(MRCompactor.COMPACTION_JOB_LATE_DATA_FILES)) {
          if (FilenameUtils.isExtension(filePathString, getApplicableFileExtensions())) {
            newLateFilePaths.add(new Path(filePathString));
          }
        }

        Path lateDataOutputPath = this.outputDeduplicated ? this.dataset.outputLatePath() : this.dataset.outputPath();
        LOG.info(String.format("Copying %d late data files to %s", newLateFilePaths.size(), lateDataOutputPath));
        if (this.outputDeduplicated) {
          if (!this.fs.exists(lateDataOutputPath)) {
            if (!this.fs.mkdirs(lateDataOutputPath)) {
              throw new RuntimeException(
                  String.format("Failed to create late data output directory: %s.", lateDataOutputPath.toString()));
            }
          }
        }
        this.copyDataFiles(lateDataOutputPath, newLateFilePaths);
        if (this.outputDeduplicated) {
          dataset.checkIfNeedToRecompact (datasetHelper);
        }
        this.status = Status.COMMITTED;
      } else {
        if (this.fs.exists(this.dataset.outputPath()) && !canOverwriteOutputDir()) {
          LOG.warn(String.format("Output paths %s exists. Will not compact %s.", this.dataset.outputPath(),
              this.dataset.inputPaths()));
          this.status = Status.COMMITTED;
          return;
        }
        addJars(conf);
        Job job = Job.getInstance(conf);
        this.configureJob(job);
        this.submitAndWait(job);
        if (shouldPublishData(compactionTimestamp)) {
          if (!this.recompactAllData && this.recompactFromDestPaths) {
            // append new files without deleting output directory
            addFilesInTmpPathToOutputPath();
            // clean up late data from outputLateDirectory, which has been set to inputPath
            deleteFilesByPaths(this.dataset.inputPaths());
          } else {
            moveTmpPathToOutputPath();
            if (this.recompactFromDestPaths) {
              deleteFilesByPaths(this.dataset.additionalInputPaths());
            }
          }
          submitSlaEvent(job);
          LOG.info("Successfully published data for input folder " + this.dataset.inputPaths());
          this.status = Status.COMMITTED;
        } else {
          LOG.info("Data not published for input folder " + this.dataset.inputPaths() + " due to incompleteness");
          this.status = Status.ABORTED;
          return;
        }
      }
      this.markOutputDirAsCompleted(compactionTimestamp);
      this.submitRecordsCountsEvent();
    } catch (Throwable t) {
      throw Throwables.propagate(t);
    }
  }


  /**
   * For regular compactions, compaction timestamp is the time the compaction job starts.
   *
   * If this is a recompaction from output paths, the compaction timestamp will remain the same as previously
   * persisted compaction time. This is because such a recompaction doesn't consume input data, so next time,
   * whether a file in the input folder is considered late file should still be based on the previous compaction
   * timestamp.
   */
  private DateTime getCompactionTimestamp() throws IOException {
    DateTimeZone timeZone = DateTimeZone.forID(
        this.dataset.jobProps().getProp(MRCompactor.COMPACTION_TIMEZONE, MRCompactor.DEFAULT_COMPACTION_TIMEZONE));

    if (!this.recompactFromDestPaths) {
      return new DateTime(timeZone);
    }

    Set<Path> inputPaths = getInputPaths();
    long maxTimestamp = Long.MIN_VALUE;
    for (FileStatus status : FileListUtils.listFilesRecursively(this.fs, inputPaths)) {
      maxTimestamp = Math.max(maxTimestamp, status.getModificationTime());
    }
    return maxTimestamp == Long.MIN_VALUE ? new DateTime(timeZone) : new DateTime(maxTimestamp, timeZone);
  }

  private void copyDataFiles(final Path outputDirectory, List<Path> inputFilePaths) throws IOException {
    ExecutorService executor = ScalingThreadPoolExecutor.newScalingThreadPool(0, this.copyLateDataThreadPoolSize, 100,
        ExecutorsUtils.newThreadFactory(Optional.of(LOG), Optional.of(this.dataset.getName() + "-copy-data")));

    List<Future<?>> futures = Lists.newArrayList();
    for (final Path filePath : inputFilePaths) {
      Future<Void> future = executor.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          Path convertedFilePath = MRCompactorJobRunner.this.outputRecordCountProvider.convertPath(
              LateFileRecordCountProvider.restoreFilePath(filePath),
              MRCompactorJobRunner.this.inputRecordCountProvider);
          String targetFileName = convertedFilePath.getName();
          Path outPath = MRCompactorJobRunner.this.lateOutputRecordCountProvider.constructLateFilePath(targetFileName,
              MRCompactorJobRunner.this.fs, outputDirectory);
          HadoopUtils.copyPath(MRCompactorJobRunner.this.fs, filePath, MRCompactorJobRunner.this.fs, outPath,
              MRCompactorJobRunner.this.fs.getConf());
          LOG.debug(String.format("Copied %s to %s.", filePath, outPath));
          return null;
        }
      });
      futures.add(future);
    }
    try {
      for (Future<?> future : futures) {
        future.get();
      }
    } catch (ExecutionException | InterruptedException e) {
      throw new IOException("Failed to copy file.", e);
    } finally {
      ExecutorsUtils.shutdownExecutorService(executor, Optional.of(LOG));
    }
  }

  private boolean canOverwriteOutputDir() {
    return this.dataset.jobProps().getPropAsBoolean(COMPACTION_JOB_OVERWRITE_OUTPUT_DIR,
        DEFAULT_COMPACTION_JOB_OVERWRITE_OUTPUT_DIR) || this.recompactFromDestPaths;
  }

  private void addJars(Configuration conf) throws IOException {
    if (!this.dataset.jobProps().contains(MRCompactor.COMPACTION_JARS)) {
      return;
    }
    Path jarFileDir = new Path(this.dataset.jobProps().getProp(MRCompactor.COMPACTION_JARS));
    for (FileStatus status : this.fs.listStatus(jarFileDir)) {
      DistributedCache.addFileToClassPath(status.getPath(), conf, this.fs);
    }
  }

  protected void configureJob(Job job) throws IOException {
    job.setJobName(HADOOP_JOB_NAME);
    configureInputAndOutputPaths(job);
    configureMapper(job);
    configureReducer(job);
    if (!this.shouldDeduplicate) {
      job.setNumReduceTasks(0);
    }
  }

  private void configureInputAndOutputPaths(Job job) throws IOException {
    for (Path inputPath : getInputPaths()) {
      FileInputFormat.addInputPath(job, inputPath);
    }

    //MR output path must not exist when MR job starts, so delete if exists.
    this.fs.delete(this.dataset.outputTmpPath(), true);
    FileOutputFormat.setOutputPath(job, this.dataset.outputTmpPath());
  }

  private Set<Path> getInputPaths() {
    return ImmutableSet.<Path> builder().addAll(this.dataset.inputPaths()).addAll(this.dataset.additionalInputPaths())
        .build();
  }

  public Dataset getDataset() {
    return this.dataset;
  }

  protected void configureMapper(Job job) {
    setInputFormatClass(job);
    setMapperClass(job);
    setMapOutputKeyClass(job);
    setMapOutputValueClass(job);
  }

  protected void configureReducer(Job job) throws IOException {
    setOutputFormatClass(job);
    setReducerClass(job);
    setOutputKeyClass(job);
    setOutputValueClass(job);
    setNumberOfReducers(job);
  }

  protected abstract void setInputFormatClass(Job job);

  protected abstract void setMapperClass(Job job);

  protected abstract void setMapOutputKeyClass(Job job);

  protected abstract void setMapOutputValueClass(Job job);

  protected abstract void setOutputFormatClass(Job job);

  protected abstract void setReducerClass(Job job);

  protected abstract void setOutputKeyClass(Job job);

  protected abstract void setOutputValueClass(Job job);

  protected abstract Collection<String> getApplicableFileExtensions();

  protected void setNumberOfReducers(Job job) throws IOException {
    long inputSize = getInputSize();
    long targetFileSize = getTargetFileSize();
    int numReducers = Math.min(Ints.checkedCast(inputSize / targetFileSize) + 1, getMaxNumReducers());
    if (this.usePrimeReducers && numReducers != 1) {
      numReducers = Primes.nextPrime(numReducers);
    }
    job.setNumReduceTasks(numReducers);
  }

  private long getInputSize() throws IOException {
    long inputSize = 0;
    for (Path inputPath : this.getInputPaths()) {
      inputSize += this.fs.getContentSummary(inputPath).getLength();
    }
    return inputSize;
  }

  private long getTargetFileSize() {
    return this.dataset.jobProps().getPropAsLong(COMPACTION_JOB_TARGET_OUTPUT_FILE_SIZE,
        DEFAULT_COMPACTION_JOB_TARGET_OUTPUT_FILE_SIZE);
  }

  private int getMaxNumReducers() {
    return this.dataset.jobProps().getPropAsInt(COMPACTION_JOB_MAX_NUM_REDUCERS,
        DEFAULT_COMPACTION_JOB_MAX_NUM_REDUCERS);
  }

  private void submitAndWait(Job job) throws ClassNotFoundException, IOException, InterruptedException {
    job.submit();
    MRCompactor.addRunningHadoopJob(this.dataset, job);
    LOG.info(String.format("MR job submitted for dataset %s, input %s, url: %s", this.dataset, getInputPaths(),
        job.getTrackingURL()));
    while (!job.isComplete()) {
      if (this.policy == Policy.ABORT_ASAP) {
        LOG.info(String.format(
            "MR job for dataset %s, input %s killed due to input data incompleteness." + " Will try again later",
            this.dataset, getInputPaths()));
        job.killJob();
        return;
      }
      Thread.sleep(MR_JOB_CHECK_COMPLETE_INTERVAL_MS);
    }
    if (!job.isSuccessful()) {
      throw new RuntimeException(String.format("MR job failed for topic %s, input %s, url: %s", this.dataset,
          getInputPaths(), job.getTrackingURL()));
    }
  }

  /**
   * Data should be published if: (1) this.policy == {@link Policy#DO_PUBLISH_DATA}; (2) either
   * compaction.abort.upon.new.data=false, or no new data is found in the input folder since jobStartTime.
   */
  private boolean shouldPublishData(DateTime jobStartTime) throws IOException {
    if (this.policy != Policy.DO_PUBLISH_DATA) {
      return false;
    }
    if (!this.dataset.jobProps().getPropAsBoolean(COMPACTION_JOB_ABORT_UPON_NEW_DATA,
        DEFAULT_COMPACTION_JOB_ABORT_UPON_NEW_DATA)) {
      return true;
    }
    for (Path inputPath : getInputPaths()) {
      if (findNewDataSinceCompactionStarted(inputPath, jobStartTime)) {
        return false;
      }
    }
    return true;
  }

  private boolean findNewDataSinceCompactionStarted(Path inputPath, DateTime jobStartTime) throws IOException {
    for (FileStatus fstat : FileListUtils.listFilesRecursively(this.fs, inputPath)) {
      DateTime fileModificationTime = new DateTime(fstat.getModificationTime());
      if (fileModificationTime.isAfter(jobStartTime)) {
        LOG.info(String.format("Found new file %s in input folder %s after compaction started. Will abort compaction.",
            fstat.getPath(), inputPath));
        return true;
      }
    }
    return false;
  }

  private void markOutputDirAsCompleted(DateTime jobStartTime) throws IOException {
    Path completionFilePath = new Path(this.dataset.outputPath(), MRCompactor.COMPACTION_COMPLETE_FILE_NAME);
    try (FSDataOutputStream completionFileStream = this.fs.create(completionFilePath)) {
      completionFileStream.writeLong(jobStartTime.getMillis());
    }
  }

  private void moveTmpPathToOutputPath() throws IOException {
    LOG.info(String.format("Moving %s to %s", this.dataset.outputTmpPath(), this.dataset.outputPath()));

    this.fs.delete(this.dataset.outputPath(), true);

    WriterUtils.mkdirsWithRecursivePermission(this.fs, this.dataset.outputPath().getParent(), this.perm);
    if (!this.fs.rename(this.dataset.outputTmpPath(), this.dataset.outputPath())) {
      throw new IOException(
          String.format("Unable to move %s to %s", this.dataset.outputTmpPath(), this.dataset.outputPath()));
    }
  }

  private void addFilesInTmpPathToOutputPath () throws IOException {
    List<Path> paths = this.getApplicableFilePaths(this.dataset.outputTmpPath());
    for (Path path: paths) {
      String fileName = path.getName();
      LOG.info(String.format("Adding %s to %s", path.toString(), this.dataset.outputPath()));
      Path outPath = MRCompactorJobRunner.this.lateOutputRecordCountProvider.constructLateFilePath(fileName,
          MRCompactorJobRunner.this.fs, this.dataset.outputPath());

      if (!this.fs.rename(path, outPath)) {
        throw new IOException(
            String.format("Unable to move %s to %s", path.toString(), outPath.toString()));
      }
    }
  }


  private void deleteFilesByPaths(Set<Path> paths) throws IOException {
    for (Path path : paths) {
      HadoopUtils.deletePathAndEmptyAncestors(this.fs, path, true);
    }
  }

  /**
   * Tell the {@link MRCompactorJobRunner} that it can go ahead and publish the data.
   */
  public void proceed() {
    this.policy = Policy.DO_PUBLISH_DATA;
  }

  public void abort() {
    this.policy = Policy.ABORT_ASAP;
  }

  /**
   * The status of the MRCompactorJobRunner.
   * @return RUNNING, COMMITTED or ABORTED.
   */
  public Status status() {
    return this.status;
  }

  @Override
  public int compareTo(MRCompactorJobRunner o) {
    return Double.compare(o.dataset.priority(), this.dataset.priority());
  }

  /**
   * Get the list of file {@link Path}s in the given dataDir, which satisfy the extension requirements
   *  of {@link #getApplicableFileExtensions()}.
   */
  private List<Path> getApplicableFilePaths(final Path dataDir) throws IOException {
    try {
      return applicablePathCache.get(dataDir, new Callable<List<Path>>() {

        @Override
        public List<Path> call() throws Exception {
          if (!MRCompactorJobRunner.this.fs.exists(dataDir)) {
            return Lists.newArrayList();
          }
          List<Path> paths = Lists.newArrayList();
          for (FileStatus fileStatus : FileListUtils.listFilesRecursively(MRCompactorJobRunner.this.fs, dataDir,
              new PathFilter() {
            @Override
            public boolean accept(Path path) {
              for (String validExtention : getApplicableFileExtensions()) {
                if (path.getName().endsWith(validExtention)) {
                  return true;
                }
              }
              return false;
            }
          })) {
            paths.add(fileStatus.getPath());
          }
          return paths;
        }
      });
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
  }

  /**
   * Submit an event when compaction MR job completes
   */
  private void submitSlaEvent(Job job) {
    try {
      CompactionSlaEventHelper
          .getEventSubmitterBuilder(this.dataset, Optional.of(job), this.fs)
          .eventSubmitter(this.eventSubmitter)
          .eventName(CompactionSlaEventHelper.COMPACTION_COMPLETED_EVENT_NAME)
          .additionalMetadata(
              CompactionSlaEventHelper.LATE_RECORD_COUNT,
              Long.toString(this.lateOutputRecordCountProvider.getRecordCount(this.getApplicableFilePaths(this.dataset
                  .outputLatePath()))))
          .additionalMetadata(
              CompactionSlaEventHelper.REGULAR_RECORD_COUNT,
              Long.toString(this.outputRecordCountProvider.getRecordCount(this.getApplicableFilePaths(this.dataset
                  .outputPath()))))
          .additionalMetadata(CompactionSlaEventHelper.RECOMPATED_METADATA_NAME,
              Boolean.toString(this.dataset.needToRecompact())).build().submit();
    } catch (Throwable e) {
      LOG.warn("Failed to submit compcation completed event:" + e, e);
    }
  }

  /**
   * Submit an event reporting late record counts and non-late record counts.
   */
  private void submitRecordsCountsEvent() {
    long lateOutputRecordCount = this.datasetHelper.getLateOutputRecordCount();
    long outputRecordCount = this.datasetHelper.getOutputRecordCount();

    try {
      CompactionSlaEventHelper
          .getEventSubmitterBuilder(this.dataset, Optional.<Job> absent(), this.fs)
          .eventSubmitter(this.eventSubmitter)
          .eventName(CompactionSlaEventHelper.COMPACTION_RECORD_COUNT_EVENT)
          .additionalMetadata(CompactionSlaEventHelper.DATASET_OUTPUT_PATH, this.dataset.outputPath().toString())
          .additionalMetadata(
              CompactionSlaEventHelper.LATE_RECORD_COUNT,
              Long.toString(lateOutputRecordCount))
          .additionalMetadata(
              CompactionSlaEventHelper.REGULAR_RECORD_COUNT,
              Long.toString(outputRecordCount))
          .additionalMetadata(CompactionSlaEventHelper.NEED_RECOMPACT, Boolean.toString(this.dataset.needToRecompact()))
          .build().submit();
    } catch (Throwable e) {
      LOG.warn("Failed to submit late event count:" + e, e);
    }
  }
}
