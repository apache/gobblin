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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
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
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import com.google.common.primitives.Ints;

import gobblin.compaction.Dataset;
import gobblin.compaction.event.CompactionSlaEventHelper;
import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.event.EventSubmitter;
import gobblin.metrics.event.sla.SlaEventSubmitter;
import gobblin.util.FileListUtils;
import gobblin.util.HadoopUtils;
import gobblin.util.RecordCountProvider;
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
 * @author ziliu
 */
@SuppressWarnings("deprecation")
public abstract class MRCompactorJobRunner implements Runnable, Comparable<MRCompactorJobRunner> {

  private static final Logger LOG = LoggerFactory.getLogger(MRCompactorJobRunner.class);

  private static final String COMPACTION_JOB_PREFIX = "compaction.job.";

  private static final String LATE_RECORD_COUNTS_EVENT = "LateRecordCounts";
  private static final String NEW_LATE_RECORD_COUNTS = "newLateRecordCounts";
  private static final String CUMULATIVE_LATE_RECORD_COUNTS = "cumulativeLateRecordCounts";

  /**
   * Properties related to the compaction job of a dataset.
   */
  private static final String COMPACTION_JOB_OUTPUT_DIR_PERMISSION = COMPACTION_JOB_PREFIX + "output.dir.permission";
  private static final String COMPACTION_JOB_TARGET_OUTPUT_FILE_SIZE =
      COMPACTION_JOB_PREFIX + "target.output.file.size";
  private static final long DEFAULT_COMPACTION_JOB_TARGET_OUTPUT_FILE_SIZE = 268435456;
  private static final String COMPACTION_JOB_MAX_NUM_REDUCERS = COMPACTION_JOB_PREFIX + "max.num.reducers";
  private static final int DEFAULT_COMPACTION_JOB_MAX_NUM_REDUCERS = 900;
  private static final String COMPACTION_JOB_OVERWRITE_OUTPUT_DIR = COMPACTION_JOB_PREFIX + "overwrite.output.dir";
  private static final boolean DEFAULT_COMPACTION_JOB_OVERWRITE_OUTPUT_DIR = false;
  private static final String COMPACTION_JOB_ABORT_UPON_NEW_DATA = COMPACTION_JOB_PREFIX + "abort.upon.new.data";
  private static final boolean DEFAULT_COMPACTION_JOB_ABORT_UPON_NEW_DATA = false;

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
    RUNNING;
  }

  protected final Dataset dataset;
  protected final FileSystem fs;
  protected final FsPermission perm;
  protected final boolean deduplicate;
  protected final boolean recompactFromDestPaths;
  protected final EventSubmitter eventSubmitter;
  private final RecordCountProvider inputRecordCountProvider;
  private final RecordCountProvider outputRecordCountProvider;
  private final LateFileRecordCountProvider lateInputRecordCountProvider;
  private final LateFileRecordCountProvider lateOutputRecordCountProvider;

  private volatile Policy policy = Policy.DO_NOT_PUBLISH_DATA;
  private volatile Status status = Status.RUNNING;

  protected MRCompactorJobRunner(Dataset dataset, FileSystem fs, Double priority) {
    this.dataset = dataset;
    this.fs = fs;
    this.perm = HadoopUtils.deserializeFsPermission(this.dataset.jobProps(), COMPACTION_JOB_OUTPUT_DIR_PERMISSION,
        FsPermission.getDefault());
    this.recompactFromDestPaths = this.dataset.jobProps().getPropAsBoolean(
        MRCompactor.COMPACTION_RECOMPACT_FROM_DEST_PATHS, MRCompactor.DEFAULT_COMPACTION_RECOMPACT_FROM_DEST_PATHS);
    this.deduplicate = this.dataset.jobProps().getPropAsBoolean(MRCompactor.COMPACTION_DEDUPLICATE,
        MRCompactor.DEFAULT_COMPACTION_DEDUPLICATE);
    this.eventSubmitter = new EventSubmitter.Builder(
        GobblinMetrics.get(this.dataset.jobProps().getProp(ConfigurationKeys.JOB_NAME_KEY)).getMetricContext(),
        MRCompactor.COMPACTION_TRACKING_EVENTS_NAMESPACE).build();

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
  }

  @Override
  public void run() {
    Configuration conf = HadoopUtils.getConfFromState(this.dataset.jobProps());

    try {
      DateTime compactionTimestamp = getCompactionTimestamp();
      if (this.dataset.jobProps().getPropAsBoolean(MRCompactor.COMPACTION_JOB_LATE_DATA_MOVEMENT_TASK, false)) {
        List<Path> newLateFilePaths = Lists.newArrayList();
        for (String filePathString : this.dataset.jobProps()
            .getPropAsList(MRCompactor.COMPACTION_JOB_LATE_DATA_FILES)) {
          if (FilenameUtils.isExtension(filePathString, getApplicableFileExtensions())) {
            newLateFilePaths.add(new Path(filePathString));
          }
        }

        Path lateDataOutputPath = this.deduplicate ? this.dataset.outputLatePath() : this.dataset.outputPath();
        LOG.info(String.format("Copying %d late data files to %s", newLateFilePaths.size(), lateDataOutputPath));
        if (this.deduplicate) {
          if (!fs.exists(lateDataOutputPath)) {
            if (!fs.mkdirs(lateDataOutputPath)) {
              throw new RuntimeException(
                  String.format("Failed to create late data output directory: %s.", lateDataOutputPath.toString()));
            }
          }
        }
        this.submitLateRecordCountsEvent(newLateFilePaths, lateDataOutputPath);
        this.copyDataFiles(lateDataOutputPath, newLateFilePaths);
        this.status = Status.COMMITTED;
      } else {
        if (this.fs.exists(this.dataset.outputPath()) && !canOverwriteOutputDir()) {
          LOG.warn(String.format("Output path %s exists. Will not compact %s.", this.dataset.outputPath(),
              this.dataset.inputPath()));
          this.status = Status.COMMITTED;
          return;
        }
        addJars(conf);
        Job job = Job.getInstance(conf);
        this.configureJob(job);
        this.submitAndWait(job);
        if (shouldPublishData(compactionTimestamp)) {
          moveTmpPathToOutputPath();
          if (this.recompactFromDestPaths) {
            deleteAdditionalInputPaths();
          }
          submitSlaEvent(job);
          LOG.info("Successfully published data for input folder " + this.dataset.inputPath());
          this.status = Status.COMMITTED;
        } else {
          LOG.info("Data not published for input folder " + this.dataset.inputPath() + " due to incompleteness");
          this.status = Status.ABORTED;
          return;
        }
      }
      this.markOutputDirAsCompleted(compactionTimestamp);
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
    } else {
      List<Path> inputPaths = Lists.newArrayList(this.dataset.inputPath());
      inputPaths.addAll(this.dataset.additionalInputPaths());
      long maxTimestamp = Long.MIN_VALUE;
      for (FileStatus status : FileListUtils.listFilesRecursively(this.fs, inputPaths)) {
        maxTimestamp = Math.max(maxTimestamp, status.getModificationTime());
      }
      return maxTimestamp == Long.MIN_VALUE ? new DateTime(timeZone) : new DateTime(maxTimestamp, timeZone);
    }
  }

  private void copyDataFiles(Path outputDirectory, List<Path> inputFilePaths) throws IOException {
    for (Path filePath : inputFilePaths) {
      Path convertedFilePath = this.outputRecordCountProvider
          .convertPath(this.lateInputRecordCountProvider.restoreFilePath(filePath), this.inputRecordCountProvider);
      String targetFileName = convertedFilePath.getName();
      Path outPath = this.lateOutputRecordCountProvider.constructLateFilePath(targetFileName, this.fs, outputDirectory);
      HadoopUtils.copyPath(this.fs, filePath, outPath);
      LOG.info(String.format("Copied %s to %s.", filePath, outPath));
    }
  }

  private boolean canOverwriteOutputDir() {
    return this.dataset.jobProps().getPropAsBoolean(COMPACTION_JOB_OVERWRITE_OUTPUT_DIR,
        DEFAULT_COMPACTION_JOB_OVERWRITE_OUTPUT_DIR);
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
    if (!this.deduplicate) {
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

  private List<Path> getInputPaths() {
    List<Path> inputPaths = Lists.newArrayList(this.dataset.inputPath());
    inputPaths.addAll(this.dataset.additionalInputPaths());
    return inputPaths;
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
    job.setNumReduceTasks(Math.min(Ints.checkedCast(inputSize / targetFileSize) + 1, getMaxNumReducers()));
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
    LOG.info(String.format("MR job submitted for topic %s, input %s, url: %s", this.dataset.topic(), getInputPaths(),
        job.getTrackingURL()));
    while (!job.isComplete()) {
      if (this.policy == Policy.ABORT_ASAP) {
        LOG.info(String.format(
            "MR job for topic %s, input %s killed due to input data incompleteness." + " Will try again later",
            this.dataset.topic(), getInputPaths()));
        job.killJob();
        return;
      }
      Thread.sleep(MR_JOB_CHECK_COMPLETE_INTERVAL_MS);
    }
    if (!job.isSuccessful()) {
      throw new RuntimeException(String.format("MR job failed for topic %s, input %s, url: %s", this.dataset.topic(),
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
    Closer closer = Closer.create();
    try {
      FSDataOutputStream completionFileStream = closer.register(this.fs.create(completionFilePath));
      completionFileStream.writeLong(jobStartTime.getMillis());
    } catch (Throwable e) {
      throw closer.rethrow(e);
    } finally {
      closer.close();
    }
  }

  private void moveTmpPathToOutputPath() throws IOException {
    LOG.info(String.format("Moving %s to %s", this.dataset.outputTmpPath(), this.dataset.outputPath()));
    this.fs.delete(this.dataset.outputPath(), true);
    this.fs.mkdirs(this.dataset.outputPath().getParent(), this.perm);
    if (!this.fs.rename(this.dataset.outputTmpPath(), this.dataset.outputPath())) {
      throw new IOException(
          String.format("Unable to move %s to %s", this.dataset.outputTmpPath(), this.dataset.outputPath()));
    }
  }

  private void deleteAdditionalInputPaths() throws IOException {
    for (Path path : this.dataset.additionalInputPaths()) {
      HadoopUtils.deletePathAndEmptyAncestors(this.fs, path, true);
    }
  }

  private void submitSlaEvent(Job job) {
    CompactionSlaEventHelper.populateState(this.dataset, Optional.of(job), fs);
    new SlaEventSubmitter(this.eventSubmitter, "CompactionCompleted", this.dataset.jobProps().getProperties()).submit();
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

  private List<Path> getCumulativeLateFilePaths(Path lateDataDir) throws FileNotFoundException, IOException {
    if (!this.fs.exists(lateDataDir)) {
      return Lists.newArrayList();
    }
    List<Path> paths = Lists.newArrayList();
    for (FileStatus fileStatus : FileListUtils.listFilesRecursively(fs, lateDataDir, new PathFilter() {
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

  /**
   * Submit an event reporting new and cumulative late record counts.
   */
  private void submitLateRecordCountsEvent(List<Path> newLateFilePaths, Path lateDataDir) {
    try {
      Map<String, String> eventMetadataMap = Maps.newHashMap();
      long newLateRecordCount = this.lateInputRecordCountProvider.getRecordCount(newLateFilePaths);
      eventMetadataMap.put(NEW_LATE_RECORD_COUNTS, Long.toString(newLateRecordCount));
      eventMetadataMap.put(CUMULATIVE_LATE_RECORD_COUNTS, Long.toString(newLateRecordCount
          + this.lateOutputRecordCountProvider.getRecordCount(this.getCumulativeLateFilePaths(lateDataDir))));

      LOG.info("Submitting late event counts: " + eventMetadataMap);
      this.eventSubmitter.submit(LATE_RECORD_COUNTS_EVENT, eventMetadataMap);
    } catch (Exception e) {
      LOG.error("Failed to submit late event count:" + e, e);
    }
  }
}
