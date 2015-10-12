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
import java.util.Random;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
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

import lombok.AllArgsConstructor;

import gobblin.compaction.Dataset;
import gobblin.compaction.event.CompactionSlaEventHelper;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.event.EventSubmitter;
import gobblin.metrics.event.sla.SlaEventSubmitter;
import gobblin.util.FileListUtils;
import gobblin.util.HadoopUtils;
import gobblin.util.RecordCountProvider;


/**
 * This class is responsible for configuring and running a single MR job.
 * It should be extended by a subclass that properly configures the mapper and reducer related classes.
 *
 * The properties that control the number of reducers are compaction.target.output.file.size and
 * compaction.max.num.reducers. The number of reducers will be the smaller of
 * [total input size] / [compaction.target.output.file.size] + 1 and [compaction.max.num.reducers].
 *
 * If {@value ConfigurationKeys#COMPACTION_JOB_LATE_DATA_MOVEMENT_TASK} is set to true, does not
 * launch an MR job. Instead, just copies the files present in
 * {@value ConfigurationKeys#COMPACTION_JOB_LATE_DATA_FILES} to a 'late' subdirectory within
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
  private static final String COMPACTION_JOB_TARGET_OUTPUT_FILE_SIZE = COMPACTION_JOB_PREFIX
      + "target.output.file.size";
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
  protected final State jobProps;
  protected final String topic;
  protected final Path inputPath;
  protected final Path tmpPath;
  protected final Path outputPath;
  protected final FileSystem fs;
  protected final FsPermission perm;
  protected final boolean deduplicate;
  protected final double priority;
  protected final EventSubmitter eventSubmitter;
  private final LateFileRecordCountProvider recordCountProvider;

  private volatile Policy policy = Policy.DO_NOT_PUBLISH_DATA;
  private volatile Status status = Status.RUNNING;

  protected MRCompactorJobRunner(Dataset dataset, FileSystem fs, Double priority) {
    this.dataset = dataset;
    this.jobProps = dataset.jobProps();
    this.topic = jobProps.getProp(MRCompactor.COMPACTION_TOPIC);
    this.inputPath = new Path(jobProps.getProp(MRCompactor.COMPACTION_JOB_INPUT_DIR));
    this.outputPath = new Path(jobProps.getProp(MRCompactor.COMPACTION_JOB_DEST_DIR));
    this.tmpPath = new Path(jobProps.getProp(MRCompactor.COMPACTION_JOB_TMP_DIR));
    this.fs = fs;
    this.perm =
        HadoopUtils.deserializeFsPermission(this.jobProps, COMPACTION_JOB_OUTPUT_DIR_PERMISSION,
            FsPermission.getDefault());
    this.deduplicate =
        this.jobProps.getPropAsBoolean(MRCompactor.COMPACTION_DEDUPLICATE, MRCompactor.DEFAULT_COMPACTION_DEDUPLICATE);
    this.priority = priority;
    this.eventSubmitter =
        new EventSubmitter.Builder(GobblinMetrics.get(this.jobProps.getProp(ConfigurationKeys.JOB_NAME_KEY))
            .getMetricContext(), MRCompactor.COMPACTION_TRACKING_EVENTS_NAMESPACE).build();

    try {
      this.recordCountProvider =
          new LateFileRecordCountProvider((RecordCountProvider) Class.forName(
              this.jobProps.getProp(MRCompactor.COMPACTION_RECORD_COUNT_PROVIDER,
                  MRCompactor.DEFAULT_COMPACTION_RECORD_COUNT_PROVIDER)).newInstance());
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate record count provider: " + e, e);
    }
  }

  @Override
  public void run() {
    Configuration conf = HadoopUtils.getConfFromState(this.jobProps);
    DateTime jobStartTime =
        new DateTime(DateTimeZone.forID(this.jobProps.getProp(MRCompactor.COMPACTION_TIMEZONE,
            MRCompactor.DEFAULT_COMPACTION_TIMEZONE)));
    try {
      if (this.jobProps.getPropAsBoolean(MRCompactor.COMPACTION_JOB_LATE_DATA_MOVEMENT_TASK, false)) {
        List<Path> newLateFilePaths = Lists.newArrayList();
        for (String filePathString : this.jobProps.getPropAsList(MRCompactor.COMPACTION_JOB_LATE_DATA_FILES)) {
          if (FilenameUtils.isExtension(filePathString, getApplicableFileExtensions())) {
            newLateFilePaths.add(new Path(filePathString));
          }
        }
        Path lateDataOutputPath = this.outputPath;
        Path lateDataDir = new Path(this.jobProps.getProp(MRCompactor.COMPACTION_JOB_DEST_LATE_DIR));
        if (this.deduplicate) {
          // Update the late data output path.
          lateDataOutputPath = lateDataDir;
          LOG.info(String.format("Will copy to late path %s since deduplication has been done. ", lateDataOutputPath));
          if (!fs.exists(lateDataOutputPath)) {
            if (!fs.mkdirs(lateDataOutputPath)) {
              throw new RuntimeException(String.format("Failed to create late data output directory: %s.",
                  lateDataOutputPath.toString()));
            }
          }
        }
        this.submitLateRecordCountsEvent(newLateFilePaths, lateDataDir);
        this.copyDataFiles(lateDataOutputPath, newLateFilePaths, conf);
        this.status = Status.COMMITTED;
      } else {
        if (this.fs.exists(this.outputPath) && !canOverwriteOutputDir()) {
          LOG.warn(String.format("Output path %s exists. Will not compact %s.", this.outputPath, this.inputPath));
          this.status = Status.COMMITTED;
          return;
        }
        addJars(conf);
        Job job = Job.getInstance(conf);
        this.configureJob(job);
        this.submitAndWait(job);
        if (shouldPublishData(jobStartTime)) {
          this.moveTmpPathToOutputPath();
          this.submitSlaEvent(job);
          LOG.info("Successfully published data for input folder " + this.inputPath);
          this.status = Status.COMMITTED;
        } else {
          LOG.info("Data not published for input folder " + this.inputPath + " due to incompleteness");
          this.status = Status.ABORTED;
          return;
        }
      }
      this.markOutputDirAsCompleted(jobStartTime);
    } catch (Throwable t) {
      throw Throwables.propagate(t);
    }
  }

  private void copyDataFiles(Path outputDirectory, List<Path> inputFilePaths, Configuration conf) throws IOException {
    for (Path filePath : inputFilePaths) {
      String fileName = filePath.getName();
      Path outPath;
      do {
        outPath = this.recordCountProvider.constructLateFilePath(fileName, this.fs, outputDirectory);
      } while (this.fs.exists(outPath));

      if (FileUtil.copy(this.fs, filePath, this.fs, outPath, false, conf)) {
        LOG.info(String.format("Copied %s to %s.", filePath, outPath));
      } else {
        LOG.warn(String.format("Failed to copy %s to %s.", filePath, outPath));
      }
    }
  }

  private boolean canOverwriteOutputDir() {
    return this.jobProps.getPropAsBoolean(COMPACTION_JOB_OVERWRITE_OUTPUT_DIR,
        DEFAULT_COMPACTION_JOB_OVERWRITE_OUTPUT_DIR);
  }

  private void addJars(Configuration conf) throws IOException {
    if (!this.jobProps.contains(MRCompactor.COMPACTION_JARS)) {
      return;
    }
    Path jarFileDir = new Path(this.jobProps.getProp(MRCompactor.COMPACTION_JARS));
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
    FileInputFormat.addInputPath(job, getInputPath());

    //MR output path must not exist when MR job starts, so delete if exists.
    this.fs.delete(getTmpPath(), true);
    FileOutputFormat.setOutputPath(job, getTmpPath());
  }

  private Path getInputPath() {
    return new Path(this.jobProps.getProp(MRCompactor.COMPACTION_JOB_INPUT_DIR));
  }

  private Path getTmpPath() {
    return new Path(this.jobProps.getProp(MRCompactor.COMPACTION_JOB_TMP_DIR));
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
    return this.fs.getContentSummary(this.inputPath).getLength();
  }

  private long getTargetFileSize() {
    return this.jobProps.getPropAsLong(COMPACTION_JOB_TARGET_OUTPUT_FILE_SIZE,
        DEFAULT_COMPACTION_JOB_TARGET_OUTPUT_FILE_SIZE);
  }

  private int getMaxNumReducers() {
    return this.jobProps.getPropAsInt(COMPACTION_JOB_MAX_NUM_REDUCERS, DEFAULT_COMPACTION_JOB_MAX_NUM_REDUCERS);
  }

  private void submitAndWait(Job job) throws ClassNotFoundException, IOException, InterruptedException {
    job.submit();
    MRCompactor.addRunningHadoopJob(this.dataset, job);
    LOG.info(String.format("MR job submitted for topic %s, input %s, url: %s", this.topic, this.inputPath,
        job.getTrackingURL()));
    while (!job.isComplete()) {
      if (this.policy == Policy.ABORT_ASAP) {
        LOG.info(String.format("MR job for topic %s, input %s killed due to input data incompleteness."
            + " Will try again later", this.topic, this.inputPath));
        job.killJob();
        return;
      }
      Thread.sleep(MR_JOB_CHECK_COMPLETE_INTERVAL_MS);
    }
    if (!job.isSuccessful()) {
      throw new RuntimeException(String.format("MR job failed for topic %s, input %s, url: %s", this.topic,
          this.inputPath, job.getTrackingURL()));
    }
  }

  /**
   * Data should be published if: (1) this.policy == GO; (2) either compaction.abort.upon.new.data=false,
   * or no new data is found in the input folder since jobStartTime.
   */
  private boolean shouldPublishData(DateTime jobStartTime) throws IOException {
    if (this.policy != Policy.DO_PUBLISH_DATA) {
      return false;
    }
    if (!this.jobProps.getPropAsBoolean(COMPACTION_JOB_ABORT_UPON_NEW_DATA, DEFAULT_COMPACTION_JOB_ABORT_UPON_NEW_DATA)) {
      return true;
    }
    return !findNewDataSinceCompactionStarted(jobStartTime);
  }

  private boolean findNewDataSinceCompactionStarted(DateTime jobStartTime) throws IOException {
    for (FileStatus fstat : HadoopUtils.listStatusRecursive(this.fs, this.inputPath)) {
      DateTime fileModificationTime = new DateTime(fstat.getModificationTime());
      if (fileModificationTime.isAfter(jobStartTime)) {
        LOG.info(String.format("Found new file %s in input folder %s after compaction started. Will abort compaction.",
            fstat.getPath(), this.inputPath));
        return true;
      }
    }
    return false;
  }

  private void markOutputDirAsCompleted(DateTime jobStartTime) throws IOException {
    Path completionFilePath = new Path(this.outputPath, MRCompactor.COMPACTION_COMPLETE_FILE_NAME);
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
    LOG.info(String.format("Moving %s to %s", this.tmpPath, this.outputPath));
    this.fs.delete(this.outputPath, true);
    this.fs.mkdirs(this.outputPath.getParent(), this.perm);
    if (!this.fs.rename(this.tmpPath, this.outputPath)) {
      throw new IOException(String.format("Unable to move %s to %s", this.tmpPath, this.outputPath));
    }
  }

  private void submitSlaEvent(Job job) {
    CompactionSlaEventHelper.populateState(jobProps, Optional.of(job), fs);
    new SlaEventSubmitter(this.eventSubmitter, "CompactionCompleted", jobProps.getProperties()).submit();
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
    return Double.compare(o.priority, this.priority);
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
      long newLateRecordCount = this.recordCountProvider.getRecordCount(newLateFilePaths);
      eventMetadataMap.put(NEW_LATE_RECORD_COUNTS, Long.toString(newLateRecordCount));
      eventMetadataMap.put(
          CUMULATIVE_LATE_RECORD_COUNTS,
          Long.toString(newLateRecordCount
              + this.recordCountProvider.getRecordCount(this.getCumulativeLateFilePaths(lateDataDir))));

      LOG.info("Submitting late event counts: " + eventMetadataMap);
      this.eventSubmitter.submit(LATE_RECORD_COUNTS_EVENT, eventMetadataMap);
    } catch (Exception e) {
      LOG.error("Failed to submit late event count:" + e, e);
    }
  }

  @AllArgsConstructor
  public static class LateFileRecordCountProvider implements RecordCountProvider {
    private static final String SEPARATOR = ".";
    private static final String LATE_COMPONENT = ".late";
    private static final String EMPTY_STRING = "";

    private RecordCountProvider recordCountProviderWithoutSuffix;

    /**
     * Construct filename for a late file. If the file does not exists in the output dir, retain the original name.
     * Otherwise, append a LATE_COMPONENT{RandomInteger} to the original file name.
     * For example, if file "part1.123.avro" exists in dir "/a/b/", the returned path will be "/a/b/part1.123.late12345.avro".
     */
    public Path constructLateFilePath(String originalFilename, FileSystem fs, Path outputDir) throws IOException {
      if (!fs.exists(new Path(outputDir, originalFilename))) {
        return new Path(outputDir, originalFilename);
      } else {
        return constructLateFilePath(
            FilenameUtils.getBaseName(originalFilename) + LATE_COMPONENT + new Random().nextInt(Integer.MAX_VALUE)
                + SEPARATOR + FilenameUtils.getExtension(originalFilename), fs, outputDir);
      }
    }

    /**
     * Get record count from a given filename (possibly having LATE_COMPONENT{RandomInteger}), using the original {@link FileNameWithRecordCountFormat}.
     */
    @Override
    public long getRecordCount(Path filepath) {
      return this.recordCountProviderWithoutSuffix.getRecordCount(new Path(filepath.getName().replaceAll(
          Pattern.quote(LATE_COMPONENT) + "[\\d]*", EMPTY_STRING)));
    }

    /**
     * Get record count for a list of paths.
     */
    public long getRecordCount(Collection<Path> paths) {
      long count = 0;
      for (Path path : paths) {
        count += getRecordCount(path);
      }
      return count;
    }
  }
}
