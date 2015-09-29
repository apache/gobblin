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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.primitives.Ints;

import gobblin.compaction.event.CompactionSlaEventHelper;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.event.EventSubmitter;
import gobblin.metrics.event.sla.SlaEventSubmitter;
import gobblin.util.HadoopUtils;

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
public abstract class MRCompactorJobRunner implements Callable<Void> {

  private static final Logger LOG = LoggerFactory.getLogger(MRCompactorJobRunner.class);
  private static final String HADOOP_JOB_NAME = "Gobblin MR Compaction";

  protected final State jobProps;
  protected final String topic;
  protected final Path inputPath;
  protected final Path tmpPath;
  protected final Path outputPath;
  protected final FileSystem fs;
  protected final FsPermission perm;
  protected final boolean deduplicate;
  protected final EventSubmitter eventSubmitter;

  protected MRCompactorJobRunner(State jobProps, FileSystem fs) {
    this.jobProps = jobProps;
    this.topic = jobProps.getProp(ConfigurationKeys.COMPACTION_TOPIC);
    this.inputPath = new Path(jobProps.getProp(ConfigurationKeys.COMPACTION_JOB_INPUT_DIR));
    this.outputPath = new Path(jobProps.getProp(ConfigurationKeys.COMPACTION_JOB_DEST_DIR));
    this.tmpPath = new Path(jobProps.getProp(ConfigurationKeys.COMPACTION_JOB_TMP_DIR));
    this.fs = fs;
    this.perm = HadoopUtils.deserializeFsPermission(this.jobProps, ConfigurationKeys.COMPACTION_OUTPUT_DIR_PERMISSION,
        FsPermission.getDefault());
    this.deduplicate = this.jobProps.getPropAsBoolean(ConfigurationKeys.COMPACTION_DEDUPLICATE,
        ConfigurationKeys.DEFAULT_COMPACTION_DEDUPLICATE);
    this.eventSubmitter =
        new EventSubmitter.Builder(GobblinMetrics.get(this.jobProps.getProp(ConfigurationKeys.JOB_NAME_KEY))
            .getMetricContext(), ConfigurationKeys.COMPACTION_TRACKING_EVENTS_NAMESPACE).build();
  }

  @Override
  public Void call() throws IOException, ClassNotFoundException, InterruptedException {

    Configuration conf = HadoopUtils.getConfFromState(this.jobProps);
    DateTime jobStartTime = new DateTime(DateTimeZone.forID(
        this.jobProps.getProp(ConfigurationKeys.COMPACTION_TIMEZONE, ConfigurationKeys.DEFAULT_COMPACTION_TIMEZONE)));

    if (this.jobProps.getPropAsBoolean(ConfigurationKeys.COMPACTION_JOB_LATE_DATA_MOVEMENT_TASK, false)) {
      List<Path> lateFilePaths = Lists.newArrayList();
      for (String filePathString : this.jobProps.getPropAsList(ConfigurationKeys.COMPACTION_JOB_LATE_DATA_FILES)) {
        if (FilenameUtils.isExtension(filePathString, getApplicableFileExtensions())) {
          lateFilePaths.add(new Path(filePathString));
        }
      }
      Path lateDataOutputPath = this.deduplicate
          ? new Path(this.outputPath, ConfigurationKeys.COMPACTION_LATE_FILES_DIRECTORY) : this.outputPath;
      this.copyDataFiles(lateDataOutputPath, lateFilePaths, conf);
    } else {
      if (this.fs.exists(this.outputPath) && !canOverwriteOutputDir()) {
        LOG.warn(String.format("Output path %s exists. Will not compact %s.", this.outputPath, this.inputPath));
        return null;
      }
      addJars(conf);
      Job job = Job.getInstance(conf);
      this.configureJob(job);
      this.submitAndWait(job);
      this.moveTmpPathToOutputPath();
      this.submitSlaEvent(job);

    }
    this.markOutputDirAsCompleted(jobStartTime);

    return null;
  }

  private void copyDataFiles(Path outputDirectory, List<Path> inputFilePaths, Configuration conf) throws IOException {
    for (Path filePath : inputFilePaths) {
      String fileName = filePath.getName();
      Path outPath;
      int fileSuffix = 0;
      do {
        outPath = new Path(outputDirectory, String.format("%s.%d.%s", FilenameUtils.getBaseName(fileName), fileSuffix++,
            FilenameUtils.getExtension(fileName)));
      } while (this.fs.exists(outPath));

      if (FileUtil.copy(this.fs, filePath, this.fs, outPath, false, conf)) {
        LOG.info(String.format("Copied %s to %s.", filePath, outPath));
      } else {
        LOG.warn(String.format("Failed to copy %s to %s.", filePath, outPath));
      }
    }
  }

  private boolean canOverwriteOutputDir() {
    return this.jobProps.getPropAsBoolean(ConfigurationKeys.COMPACTION_OVERWRITE_OUTPUT_DIR,
        ConfigurationKeys.DEFAULT_COMPACTION_OVERWRITE_OUTPUT_DIR);
  }

  private void addJars(Configuration conf) throws IOException {
    if (!this.jobProps.contains(ConfigurationKeys.COMPACTION_JARS)) {
      return;
    }
    Path jarFileDir = new Path(this.jobProps.getProp(ConfigurationKeys.COMPACTION_JARS));
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
    return new Path(this.jobProps.getProp(ConfigurationKeys.COMPACTION_JOB_INPUT_DIR));
  }

  private Path getTmpPath() {
    return new Path(this.jobProps.getProp(ConfigurationKeys.COMPACTION_JOB_TMP_DIR));
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
    return this.jobProps.getPropAsLong(ConfigurationKeys.COMPACTION_TARGET_OUTPUT_FILE_SIZE,
        ConfigurationKeys.DEFAULT_COMPACTION_TARGET_OUTPUT_FILE_SIZE);
  }

  private int getMaxNumReducers() {
    return this.jobProps.getPropAsInt(ConfigurationKeys.COMPACTION_MAX_NUM_REDUCERS,
        ConfigurationKeys.DEFAULT_COMPACTION_MAX_NUM_REDUCERS);
  }

  private void submitAndWait(Job job) throws ClassNotFoundException, IOException, InterruptedException {
    job.submit();
    MRCompactor.addRunningHadoopJob(job);
    LOG.info(String.format("MR job submitted for topic %s, input %s, url: %s", this.topic, this.inputPath,
        job.getTrackingURL()));
    job.waitForCompletion(false);
    if (!job.isSuccessful()) {
      throw new RuntimeException(String.format("MR job failed for topic %s, input %s, url: %s", this.topic,
          this.inputPath, job.getTrackingURL()));
    }
  }

  private void markOutputDirAsCompleted(DateTime jobStartTime) throws IOException {
    Path completionFilePath = new Path(this.outputPath, ConfigurationKeys.COMPACTION_COMPLETE_FILE_NAME);
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

    CompactionSlaEventHelper.populateState(jobProps, job, fs);
    new SlaEventSubmitter(eventSubmitter, "CompactionCompleted", jobProps.getProperties()).submit();

  }
}
