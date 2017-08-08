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

package org.apache.gobblin.compaction.mapreduce;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import org.apache.gobblin.compaction.dataset.DatasetHelper;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.gobblin.compaction.dataset.Dataset;
import org.apache.gobblin.compaction.event.CompactionSlaEventHelper;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.FileListUtils;


/**
 * This class creates the following properties for a single MapReduce job for compaction:
 * compaction.topic, compaction.job.input.dir, compaction.job.dest.dir, compaction.job.dest.dir.
 *
 * @author Ziyang Liu
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
  protected final boolean renameSourceDirEnabled;
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
    this.renameSourceDirEnabled = this.state.getPropAsBoolean(MRCompactor.COMPACTION_RENAME_SOURCE_DIR_ENABLED,
            MRCompactor.DEFAULT_COMPACTION_RENAME_SOURCE_DIR_ENABLED);
  }

  protected List<Dataset> createJobProps() throws IOException {

    if (Iterables.tryFind(this.dataset.inputPaths(), new Predicate<Path>() {
      public boolean apply(Path input) {
        try {
          return MRCompactorJobPropCreator.this.fs.exists(input);
        } catch (IOException e) {
          MRCompactorJobPropCreator.LOG.error(String.format("Failed to check if %s exits", new Object[] { input }), e);
        }
        return false;
      }
    }).isPresent()) {
      Optional<Dataset> datasetWithJobProps = createJobProps(this.dataset);
      if (datasetWithJobProps.isPresent()) {
        setCompactionSLATimestamp((Dataset) datasetWithJobProps.get());
        return ImmutableList.of(datasetWithJobProps.get());
      }
      return ImmutableList.of();
    }
    LOG.warn("Input folders " + this.dataset.inputPaths() + " do not exist. Skipping dataset " + this.dataset);
    return ImmutableList.of();
  }

  private void setCompactionSLATimestamp(Dataset dataset) {
    // Set up SLA timestamp only if this dataset will be compacted and MRCompactor.COMPACTION_INPUT_PATH_TIME is present.
    if ((this.recompactFromOutputPaths || !MRCompactor.datasetAlreadyCompacted(this.fs, dataset, this.renameSourceDirEnabled))
        && dataset.jobProps().contains(MRCompactor.COMPACTION_INPUT_PATH_TIME)) {
      long timeInMills = dataset.jobProps().getPropAsLong(MRCompactor.COMPACTION_INPUT_PATH_TIME);
      // Set the upstream time to partition + 1 day. E.g. for 2015/10/13 the upstream time is midnight of 2015/10/14
      CompactionSlaEventHelper.setUpstreamTimeStamp(this.state,
          timeInMills + TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS));
    }
  }

  private boolean latePathsFound(Dataset dataset) throws IOException, FileNotFoundException {
    for (Path lateInputPath : dataset.inputLatePaths()) {
      if ((this.fs.exists(lateInputPath)) && (this.fs.listStatus(lateInputPath).length > 0)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Create MR job properties for a {@link Dataset}.
   */
  protected Optional<Dataset> createJobProps(Dataset dataset) throws IOException {
    if (this.recompactFromOutputPaths && (!latePathsFound(dataset))) {
      LOG.info(String.format("Skipping recompaction for %s since there is no late data in %s",
          new Object[] { dataset.inputPaths(), dataset.inputLatePaths() }));
      return Optional.absent();
    }

    State jobProps = new State();
    jobProps.addAll(this.state);
    jobProps.setProp(MRCompactor.COMPACTION_ENABLE_SUCCESS_FILE, false);
    jobProps.setProp(MRCompactor.COMPACTION_INPUT_DEDUPLICATED, this.inputDeduplicated);
    jobProps.setProp(MRCompactor.COMPACTION_OUTPUT_DEDUPLICATED, this.outputDeduplicated);
    jobProps.setProp(MRCompactor.COMPACTION_SHOULD_DEDUPLICATE, !this.inputDeduplicated && this.outputDeduplicated);

    if (this.recompactFromOutputPaths || !MRCompactor.datasetAlreadyCompacted(this.fs, dataset, renameSourceDirEnabled)) {
      if (renameSourceDirEnabled) {
        Set<Path> newUnrenamedDirs = MRCompactor.getDeepestLevelUnrenamedDirsWithFileExistence(this.fs, dataset.inputPaths());
        if (getAllFilePathsRecursively(newUnrenamedDirs).isEmpty()) {
          return Optional.absent();
        }
        LOG.info ("[{}] has unprocessed directories for first time compaction: {}", dataset.getDatasetName(), newUnrenamedDirs);
        dataset.overwriteInputPaths(newUnrenamedDirs);
        dataset.setRenamePaths(newUnrenamedDirs);
      } else {
        addInputLateFilesForFirstTimeCompaction(jobProps, dataset);
      }

      LOG.info(String.format("Created MR job properties for input %s and output %s.", dataset.inputPaths(),
              dataset.outputPath()));
      dataset.setJobProps(jobProps);
      return Optional.of(dataset);
    } else {
      return obtainDatasetWithJobProps (jobProps, dataset);
    }
  }

  private void addInputLateFilesForFirstTimeCompaction(State jobProps, Dataset dataset) throws IOException {
    if ((latePathsFound(dataset)) && (this.outputDeduplicated)) {
      dataset.addAdditionalInputPaths(dataset.inputLatePaths());
      if (this.outputDeduplicated) {
        // If input contains late data (i.e., input data is not deduplicated) and output data should be deduplicated,
        // run a deduping compaction instead of non-deduping compaction.
        jobProps.setProp(MRCompactor.COMPACTION_SHOULD_DEDUPLICATE, true);
      }
    }
  }

  private Set<Path> getAllFilePathsRecursively (Set<Path> paths) throws  IOException{
    Set<Path> allPaths = Sets.newHashSet();
    for (FileStatus fileStatus : FileListUtils.listFilesRecursively(fs, paths)) {
      allPaths.add(fileStatus.getPath());
    }

    return allPaths;
  }

  private Optional<Dataset> obtainDatasetWithJobProps (State jobProps, Dataset dataset) throws IOException {
    if (this.recompactFromInputPaths) {
      LOG.info(String.format("Will recompact for %s.", dataset.outputPath()));
      addInputLateFilesForFirstTimeCompaction(jobProps, dataset);
    } else {
      Set<Path> newDataFiles = new HashSet<>();

      do {
        if (renameSourceDirEnabled) {
          Set<Path> newUnrenamedDirs = MRCompactor.getDeepestLevelUnrenamedDirsWithFileExistence(this.fs, dataset.inputPaths());
          if (newUnrenamedDirs.isEmpty()) {
            LOG.info("[{}] doesn't have unprocessed directories", dataset.getDatasetName());
            break;
          }
          Set<Path> allFiles = getAllFilePathsRecursively(newUnrenamedDirs);
          if (allFiles.isEmpty()) {
            LOG.info("[{}] has unprocessed directories but all empty: {}", dataset.getDatasetName(), newUnrenamedDirs);
            break;
          }

          dataset.setRenamePaths(newUnrenamedDirs);
          newDataFiles.addAll(allFiles);
          LOG.info("[{}] has unprocessed directories: {}", dataset.getDatasetName(), newUnrenamedDirs);
        } else {
          newDataFiles = getNewDataInFolder(dataset.inputPaths(), dataset.outputPath());
          Set<Path> newDataFilesInLatePath = getNewDataInFolder(dataset.inputLatePaths(), dataset.outputPath());
          newDataFiles.addAll(newDataFilesInLatePath);
          if (newDataFiles.isEmpty()) {
            break;
          }
          if (!newDataFilesInLatePath.isEmpty()) {
            dataset.addAdditionalInputPaths(dataset.inputLatePaths());
          }
        }
      } while (false);

      if (newDataFiles.isEmpty()) {
        // Although no new data come in, it is needed to check if late directory has remaining data for two reasons:
        // 1) Previous compaction job may move data to the late directory but haven't compacted them before a job is killed or timed out.
        // 2) Provide a chance to look at if late data has been existed too long, so the recompact condition will be set.
        // When late data exists and it is required to move, we modify the dataset state to recompact state so only
        // re-compaction flow will run.
        if (isOutputLateDataExists (dataset)) {
          LOG.info ("{} don't have new data, but previous late data still remains, check if it requires to move", dataset.getDatasetName());
          dataset.setJobProps(jobProps);
          dataset.checkIfNeedToRecompact(new DatasetHelper(dataset, this.fs, Lists.newArrayList("avro")));
          if (dataset.needToRecompact()) {
            MRCompactor.modifyDatasetStateToRecompact (dataset);
          } else {
            return Optional.absent();
          }
        } else {
          return Optional.absent();
        }
      } else {
        LOG.info(String.format("Will copy %d new data files for %s", newDataFiles.size(), dataset.outputPath()));
        jobProps.setProp(MRCompactor.COMPACTION_JOB_LATE_DATA_MOVEMENT_TASK, true);
        jobProps.setProp(MRCompactor.COMPACTION_JOB_LATE_DATA_FILES, Joiner.on(",").join(newDataFiles));
      }
    }

    dataset.setJobProps(jobProps);
    return Optional.of(dataset);
  }

  private boolean isOutputLateDataExists (Dataset dataset) throws IOException {
    if (!this.fs.exists(dataset.outputLatePath())) {
      return false;
    }
    return this.fs.listStatus(dataset.outputLatePath()).length > 0;
  }

  private Set<Path> getNewDataInFolder(Set<Path> inputFolders, Path outputFolder) throws IOException {
    Set<Path> paths = Sets.newHashSet();
    for (Path inputFolder : inputFolders) {
      paths.addAll(getNewDataInFolder(inputFolder, outputFolder));
    }
    return paths;
  }

  /**
   * Check if inputFolder contains any files which have modification times which are more
   * recent than the last compaction time as stored within outputFolder; return any files
   * which do. An empty list will be returned if all files are older than the last compaction time.
   */
  private Set<Path> getNewDataInFolder(Path inputFolder, Path outputFolder) throws IOException {
    Set<Path> newFiles = Sets.newHashSet();

    if (!this.fs.exists(inputFolder) || !this.fs.exists(outputFolder)) {
      return newFiles;
    }

    DateTime lastCompactionTime = new DateTime(MRCompactor.readCompactionTimestamp(this.fs, outputFolder));
    for (FileStatus fstat : FileListUtils.listFilesRecursively(this.fs, inputFolder)) {
      DateTime fileModificationTime = new DateTime(fstat.getModificationTime());
      if (fileModificationTime.isAfter(lastCompactionTime)) {
        LOG.info ("[" + fileModificationTime.getMillis() + "] " + fstat.getPath() + " is after " + lastCompactionTime.getMillis());
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
