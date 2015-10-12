/*
 *
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
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import gobblin.compaction.Dataset;
import gobblin.configuration.State;
import gobblin.util.FileListUtils;


/**
 * This class creates the following properties for a single MapReduce job for compaction:
 * compaction.topic, compaction.job.input.dir, compaction.job.dest.dir, compaction.job.dest.dir.
 *
 * @author ziliu
 */
public class MRCompactorJobPropCreator {
  private static final Logger LOG = LoggerFactory.getLogger(MRCompactorJobPropCreator.class);

  @SuppressWarnings("unchecked")
  static class Builder<T extends Builder<?>> {

    String topic;
    double priority = Dataset.DEFAULT_PRIORITY;
    Path topicInputDir;
    Path topicInputLateDir;
    Path topicOutputDir;
    Path topicOutputLateDir;
    Path topicTmpOutputDir;
    FileSystem fs;
    State state;

    T withTopic(String topic) {
      this.topic = topic;
      return (T) this;
    }

    T withPriority(double priority) {
      this.priority = priority;
      return (T) this;
    }

    T withTopicInputDir(Path topicInputDir) {
      this.topicInputDir = topicInputDir;
      return (T) this;
    }

    T withTopicInputLateDir(Path topicInputLateDir) {
      this.topicInputLateDir = topicInputLateDir;
      return (T) this;
    }

    T withTopicOutputDir(Path topicOutputDir) {
      this.topicOutputDir = topicOutputDir;
      return (T) this;
    }

    T withTopicOutputLateDir(Path topicOutputLateDir) {
      this.topicOutputLateDir = topicOutputLateDir;
      return (T) this;
    }

    T withTopicTmpOutputDir(Path topicTmpOutputDir) {
      this.topicTmpOutputDir = topicTmpOutputDir;
      return (T) this;
    }

    T withFileSystem(FileSystem fs) {
      this.fs = fs;
      return (T) this;
    }

    T withState(State state) {
      this.state = new State();
      this.state.addAll(state);
      return (T) this;
    }

    MRCompactorJobPropCreator build() {
      return new MRCompactorJobPropCreator(this);
    }
  }

  protected final String topic;
  protected final double priority;
  protected final Path topicInputDir;
  protected final Path topicInputLateDir;
  protected final Path topicOutputDir;
  protected final Path topicOutputLateDir;
  protected final Path topicTmpOutputDir;
  protected final FileSystem fs;
  protected final State state;
  protected final boolean deduplicate;

  // Whether we should recompact the input folders if new data files are found in the input folders.
  protected final boolean recompactFromInputPaths;

  // Whether we should recompact the output folders if there are late data files in the output '_late' folder.
  // If this is set to true, input folders of the datasets will be ignored. The output folders and the
  // output '_late' folders will be used as input to compaction jobs.
  protected final boolean recompactFromOutputPaths;

  protected MRCompactorJobPropCreator(Builder<?> builder) {
    this.topic = builder.topic;
    this.priority = builder.priority;
    this.topicInputDir = builder.topicInputDir;
    this.topicInputLateDir = builder.topicInputLateDir;
    this.topicOutputDir = builder.topicOutputDir;
    this.topicOutputLateDir = builder.topicOutputLateDir;
    this.topicTmpOutputDir = builder.topicTmpOutputDir;
    this.fs = builder.fs;
    this.state = builder.state;
    this.deduplicate =
        this.state.getPropAsBoolean(MRCompactor.COMPACTION_DEDUPLICATE, MRCompactor.DEFAULT_COMPACTION_DEDUPLICATE);
    this.recompactFromInputPaths =
        this.state.getPropAsBoolean(MRCompactor.COMPACTION_RECOMPACT_FROM_INPUT_FOR_LATE_DATA,
            MRCompactor.DEFAULT_COMPACTION_RECOMPACT_FROM_INPUT_FOR_LATE_DATA);
    this.recompactFromOutputPaths = this.state.getPropAsBoolean(MRCompactor.COMPACTION_RECOMPACT_FROM_DEST_PATHS,
        MRCompactor.DEFAULT_COMPACTION_RECOMPACT_FROM_DEST_PATHS);
  }

  protected List<Dataset> createJobProps() throws IOException {
    if (!this.fs.exists(this.topicInputDir)) {
      LOG.warn("Input folder " + this.topicInputDir + " does not exist. Skipping topic " + this.topic);
      return ImmutableList.<Dataset> of();
    }

    Dataset dataset = new Dataset.Builder().withTopic(this.topic).withPriority(this.priority)
        .withInputPath(this.recompactFromOutputPaths ? this.topicOutputDir : this.topicInputDir)
        .withInputLatePath(this.recompactFromOutputPaths ? this.topicOutputLateDir : this.topicInputLateDir)
        .withOutputPath(this.topicOutputDir).withOutputLatePath(this.topicOutputLateDir)
        .withOutputTmpPath(this.topicTmpOutputDir).build();

    Optional<Dataset> datasetWithJobProps = createJobProps(dataset);
    if (datasetWithJobProps.isPresent()) {
      return ImmutableList.<Dataset> of(datasetWithJobProps.get());
    } else {
      return ImmutableList.<Dataset> of();
    }
  }

  /**
   * Create MR job properties for a {@link Dataset}.
   */
  protected Optional<Dataset> createJobProps(Dataset dataset) throws IOException {
    if (this.recompactFromOutputPaths
        && (!this.fs.exists(dataset.inputLatePath()) || this.fs.listStatus(dataset.inputLatePath()).length == 0)) {
      LOG.info(String.format("Skipping recompaction for %s since there is no late data in %s", dataset.inputPath(),
          dataset.inputLatePath()));
      return Optional.<Dataset> absent();
    }

    State jobProps = new State();
    jobProps.addAll(this.state);
    jobProps.setProp(MRCompactor.COMPACTION_ENABLE_SUCCESS_FILE, false);
    jobProps.setProp(MRCompactor.COMPACTION_DEDUPLICATE, this.deduplicate);

    if (this.recompactFromOutputPaths || !MRCompactor.datasetAlreadyCompacted(this.fs, dataset)) {
      addInputLateFilesForFirstTimeCompaction(jobProps, dataset);
    } else {
      List<Path> newDataFiles = getNewDataInFolder(dataset.inputPath(), dataset.outputPath());
      newDataFiles.addAll(getNewDataInFolder(dataset.inputLatePath(), dataset.outputPath()));
      if (newDataFiles.isEmpty()) {
        return Optional.<Dataset> absent();
      }
      addJobPropsForCompactedFolder(jobProps, dataset);
    }

    LOG.info(String.format("Created MR job properties for input %s and output %s.", dataset.inputPath(),
        dataset.outputPath()));
    dataset.setJobProps(jobProps);
    return Optional.of(dataset);
  }

  private void addInputLateFilesForFirstTimeCompaction(State jobProps, Dataset dataset) throws IOException {
    if (this.fs.exists(dataset.inputLatePath()) && this.fs.listStatus(dataset.inputLatePath()).length > 0) {
      dataset.addAdditionalInputPath(dataset.inputLatePath());
      jobProps.setProp(MRCompactor.COMPACTION_DEDUPLICATE, true);
    }
  }

  private void addJobPropsForCompactedFolder(State jobProps, Dataset dataset) throws IOException {
    if (this.recompactFromInputPaths) {
      LOG.info(String.format("Will recompact for %s.", dataset.outputPath()));
      addInputLateFilesForFirstTimeCompaction(jobProps, dataset);
    } else {
      List<Path> newDataFiles = getNewDataInFolder(dataset.inputPath(), dataset.outputPath());
      List<Path> newDataFilesInLatePath = getNewDataInFolder(dataset.inputLatePath(), dataset.outputPath());
      newDataFiles.addAll(newDataFilesInLatePath);

      if (!newDataFilesInLatePath.isEmpty()) {
        dataset.addAdditionalInputPath(dataset.inputLatePath());
      }

      LOG.info(String.format("Will copy %d new data files for %s", newDataFiles.size(), dataset.outputPath()));
      jobProps.setProp(MRCompactor.COMPACTION_JOB_LATE_DATA_MOVEMENT_TASK, true);
      jobProps.setProp(MRCompactor.COMPACTION_JOB_LATE_DATA_FILES, Joiner.on(",").join(newDataFiles));
    }
  }

  /**
   * Create MR job properties for a {@link Dataset} and partition.
   */
  protected Optional<Dataset> createJobProps(Dataset dataset, String partition) throws IOException {
    Optional<Dataset> datasetWithJobProps = createJobProps(dataset);
    if (datasetWithJobProps.isPresent()) {
      datasetWithJobProps.get().jobProps().setProp(MRCompactor.COMPACTION_JOB_DEST_PARTITION, partition);
      return datasetWithJobProps;
    } else {
      return Optional.<Dataset> absent();
    }
  }

  /**
   * Check if inputFolder contains any files which have modification times which are more
   * recent than the last compaction time as stored within outputFolder; return any files
   * which do. An empty list will be returned if all files are older than the last compaction time.
   */
  private List<Path> getNewDataInFolder(Path inputFolder, Path outputFolder) throws IOException {
    List<Path> newFiles = Lists.newArrayList();

    if (!this.fs.exists(inputFolder) || !this.fs.exists(outputFolder)) {
      return newFiles;
    }

    DateTime lastCompactionTime = new DateTime(MRCompactor.readCompactionTimestamp(this.fs, outputFolder));
    for (FileStatus fstat : FileListUtils.listFilesRecursively(this.fs, inputFolder)) {
      DateTime fileModificationTime = new DateTime(fstat.getModificationTime());
      if (fileModificationTime.isAfter(lastCompactionTime)) {
        newFiles.add(fstat.getPath());
      }
    }
    if (!newFiles.isEmpty()) {
      LOG.info(String.format("Found %d new files within folder %s which are more recent than the previous "
          + "compaction start time of %s.", newFiles.size(), inputFolder, lastCompactionTime));
    }

    return newFiles;
  }
}
