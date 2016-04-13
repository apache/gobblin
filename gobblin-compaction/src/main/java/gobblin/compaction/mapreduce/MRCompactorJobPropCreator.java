/*
 *
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

package gobblin.compaction.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

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

import gobblin.compaction.dataset.Dataset;
import gobblin.compaction.event.CompactionSlaEventHelper;
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

  static class Builder {

    Dataset dataset;
    FileSystem fs;
    State state;
    double lateDataThresholdForRecompact;

    Builder withDataset(Dataset dataset) {
      this.dataset = dataset;
      return this;
    }

    Builder withFileSystem(FileSystem fs) {
      this.fs = fs;
      return this;
    }

    Builder withState(State state) {
      this.state = new State();
      this.state.addAll(state);
      return this;
    }

    Builder withLateDataThresholdForRecompact(double thresholdForRecompact) {
      this.lateDataThresholdForRecompact = thresholdForRecompact;
      return this;
    }

    MRCompactorJobPropCreator build() {
      return new MRCompactorJobPropCreator(this);
    }
  }

  protected final Dataset dataset;
  protected final FileSystem fs;
  protected final State state;
  protected final boolean inputDeduplicated;
  protected final boolean outputDeduplicated;
  protected final double lateDataThresholdForRecompact;

  // Whether we should recompact the input folders if new data files are found in the input folders.
  protected final boolean recompactFromInputPaths;

  // Whether we should recompact the output folders if there are late data files in the output '_late' folder.
  // If this is set to true, input folders of the datasets will be ignored. The output folders and the
  // output '_late' folders will be used as input to compaction jobs.
  protected final boolean recompactFromOutputPaths;

  private MRCompactorJobPropCreator(Builder builder) {
    this.dataset = builder.dataset;
    this.fs = builder.fs;
    this.state = builder.state;
    this.lateDataThresholdForRecompact = builder.lateDataThresholdForRecompact;
    this.inputDeduplicated = this.state.getPropAsBoolean(MRCompactor.COMPACTION_INPUT_DEDUPLICATED,
        MRCompactor.DEFAULT_COMPACTION_INPUT_DEDUPLICATED);
    this.outputDeduplicated = this.state.getPropAsBoolean(MRCompactor.COMPACTION_OUTPUT_DEDUPLICATED,
        MRCompactor.DEFAULT_COMPACTION_OUTPUT_DEDUPLICATED);
    this.recompactFromInputPaths =
        this.state.getPropAsBoolean(MRCompactor.COMPACTION_RECOMPACT_FROM_INPUT_FOR_LATE_DATA,
            MRCompactor.DEFAULT_COMPACTION_RECOMPACT_FROM_INPUT_FOR_LATE_DATA);
    this.recompactFromOutputPaths = this.state.getPropAsBoolean(MRCompactor.COMPACTION_RECOMPACT_FROM_DEST_PATHS,
        MRCompactor.DEFAULT_COMPACTION_RECOMPACT_FROM_DEST_PATHS);
  }

  protected List<Dataset> createJobProps() throws IOException {
    if (!this.fs.exists(this.dataset.inputPath())) {
      LOG.warn("Input folder " + this.dataset.inputPath() + " does not exist. Skipping dataset " + this.dataset);
      return ImmutableList.<Dataset> of();
    }
    Optional<Dataset> datasetWithJobProps = createJobProps(this.dataset);
    if (datasetWithJobProps.isPresent()) {
      setCompactionSLATimestamp(datasetWithJobProps.get());
      return ImmutableList.<Dataset> of(datasetWithJobProps.get());
    }
    return ImmutableList.<Dataset> of();
  }

  private void setCompactionSLATimestamp(Dataset dataset) {
    // Set up SLA timestamp only if this dataset will be compacted and MRCompactor.COMPACTION_INPUT_PATH_TIME is present.
    if ((this.recompactFromOutputPaths || !MRCompactor.datasetAlreadyCompacted(this.fs, dataset))
        && dataset.jobProps().contains(MRCompactor.COMPACTION_INPUT_PATH_TIME)) {
      long timeInMills = dataset.jobProps().getPropAsLong(MRCompactor.COMPACTION_INPUT_PATH_TIME);
      // Set the upstream time to partition + 1 day. E.g. for 2015/10/13 the upstream time is midnight of 2015/10/14
      CompactionSlaEventHelper.setUpstreamTimeStamp(this.state,
          timeInMills + TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS));
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
    jobProps.setProp(MRCompactor.COMPACTION_INPUT_DEDUPLICATED, this.inputDeduplicated);
    jobProps.setProp(MRCompactor.COMPACTION_OUTPUT_DEDUPLICATED, this.outputDeduplicated);
    jobProps.setProp(MRCompactor.COMPACTION_SHOULD_DEDUPLICATE, !this.inputDeduplicated && this.outputDeduplicated);

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
      if (this.outputDeduplicated) {

        // If input contains late data (i.e., input data is not deduplicated) and output data should be deduplicated,
        // run a deduping compaction instead of non-deduping compaction.
        jobProps.setProp(MRCompactor.COMPACTION_SHOULD_DEDUPLICATE, true);
      }
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

  /**
   * Create a {@link Dataset} with the given {@link Throwable}. This {@link Dataset} will be skipped by setting
   * its state to {@link Dataset.DatasetState#COMPACTION_COMPLETE}, and the {@link Throwable} will be added to
   * the {@link Dataset}.
   */
  public Dataset createFailedJobProps(Throwable t) {
    this.dataset.setJobProps(this.state);
    this.dataset.skip(t);
    return this.dataset;
  }
}
