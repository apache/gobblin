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

package org.apache.gobblin.compaction.action;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.compaction.event.CompactionSlaEventHelper;
import org.apache.gobblin.compaction.mapreduce.CompactionJobConfigurator;
import org.apache.gobblin.compaction.mapreduce.MRCompactor;
import org.apache.gobblin.compaction.mapreduce.MRCompactorJobRunner;
import org.apache.gobblin.compaction.mapreduce.RecordKeyDedupReducerBase;
import org.apache.gobblin.compaction.mapreduce.RecordKeyMapperBase;
import org.apache.gobblin.compaction.parser.CompactionPathParser;
import org.apache.gobblin.compaction.source.CompactionSource;
import org.apache.gobblin.compaction.verify.InputRecordCountHelper;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.dataset.FileSystemDataset;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.WriterUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;

import static org.apache.gobblin.compaction.event.CompactionSlaEventHelper.*;


/**
 * A type of post action {@link CompactionCompleteAction} which focus on the file operations
 *
 */
@Slf4j
@AllArgsConstructor
public class CompactionCompleteFileOperationAction implements CompactionCompleteAction<FileSystemDataset> {

  public final static String COMPACTION_DIRECTORY_FORMAT = "/compaction_%s";
  protected WorkUnitState state;
  private CompactionJobConfigurator configurator;
  private InputRecordCountHelper helper;
  private EventSubmitter eventSubmitter;
  private FileSystem fs;

  public CompactionCompleteFileOperationAction(State state, CompactionJobConfigurator configurator, InputRecordCountHelper helper) {
    if (!(state instanceof WorkUnitState)) {
      throw new UnsupportedOperationException(this.getClass().getName() + " only supports workunit state");
    }
    this.state = (WorkUnitState) state;
    this.helper = helper;
    this.configurator = configurator;
    this.fs = configurator.getFs();
  }

  public CompactionCompleteFileOperationAction(State state, CompactionJobConfigurator configurator) {
    this(state, configurator, new InputRecordCountHelper(state));
  }

  /**
   * Replace or append the destination folder with new files from map-reduce job
   * Create a record count file containing the number of records that have been processed .
   */
  public void onCompactionJobComplete(FileSystemDataset dataset) throws IOException {
    if (dataset.isVirtual()) {
      return;
    }

    if (configurator != null && configurator.isJobCreated()) {
      CompactionPathParser.CompactionParserResult result = new CompactionPathParser(state).parse(dataset);
      Path tmpPath = configurator.getMrOutputPath();
      Path dstPath = new Path(result.getDstAbsoluteDir());

      // this is append delta mode due to the compaction rename source dir mode being enabled
      boolean appendDeltaOutput = this.state.getPropAsBoolean(MRCompactor.COMPACTION_RENAME_SOURCE_DIR_ENABLED,
          MRCompactor.DEFAULT_COMPACTION_RENAME_SOURCE_DIR_ENABLED);

      Job job = this.configurator.getConfiguredJob();

      long newTotalRecords = 0;
      long oldTotalRecords = helper.readRecordCount(new Path(result.getDstAbsoluteDir()));
      long executionCount = helper.readExecutionCount(new Path(result.getDstAbsoluteDir()));

      List<Path> goodPaths = CompactionJobConfigurator.getGoodFiles(job, tmpPath, this.fs,
          ImmutableList.of(configurator.getFileExtension()));
      HashSet<Path> outputFiles = new HashSet<>();
      if (appendDeltaOutput) {
        FsPermission permission =
            HadoopUtils.deserializeFsPermission(this.state, MRCompactorJobRunner.COMPACTION_JOB_OUTPUT_DIR_PERMISSION,
                FsPermission.getDefault());
        WriterUtils.mkdirsWithRecursivePermission(this.fs, dstPath, permission);
        // append files under mr output to destination
        for (Path filePath : goodPaths) {
          String fileName = filePath.getName();
          log.info(String.format("Adding %s to %s", filePath.toString(), dstPath));
          Path outPath = new Path(dstPath, fileName);

          if (!this.fs.rename(filePath, outPath)) {
            throw new IOException(String.format("Unable to move %s to %s", filePath.toString(), outPath.toString()));
          }
        }

        // Obtain record count from input file names.
        // We don't get record count from map-reduce counter because in the next run, the threshold (delta record)
        // calculation is based on the input file names. By pre-defining which input folders are involved in the
        // MR execution, it is easy to track how many files are involved in MR so far, thus calculating the number of total records
        // (all previous run + current run) is possible.
        newTotalRecords = this.configurator.getFileNameRecordCount();
      } else {
        if (state.getPropAsBoolean(ConfigurationKeys.RECOMPACTION_WRITE_TO_NEW_FOLDER, false)) {
          Path oldFilePath =
              PathUtils.mergePaths(dstPath, new Path(String.format(COMPACTION_DIRECTORY_FORMAT, executionCount)));
          dstPath =
              PathUtils.mergePaths(dstPath, new Path(String.format(COMPACTION_DIRECTORY_FORMAT, executionCount + 1)));
          this.configurator.getOldFiles().add(this.fs.makeQualified(oldFilePath).toString());
          //Write to a new path, no need to delete the old path
        } else {
          this.configurator.getOldFiles().add(this.fs.makeQualified(dstPath).toString());
          this.fs.delete(dstPath, true);
        }
        FsPermission permission =
            HadoopUtils.deserializeFsPermission(this.state, MRCompactorJobRunner.COMPACTION_JOB_OUTPUT_DIR_PERMISSION,
                FsPermission.getDefault());

        WriterUtils.mkdirsWithRecursivePermission(this.fs, dstPath.getParent(), permission);
        if (!this.fs.rename(tmpPath, dstPath)) {
          throw new IOException(String.format("Unable to move %s to %s", tmpPath, dstPath));
        }

        // Obtain record count from map reduce job counter
        // We don't get record count from file name because tracking which files are actually involved in the MR execution can
        // be hard. This is due to new minutely data is rolled up to hourly folder but from daily compaction perspective we are not
        // able to tell which file are newly added (because we simply pass all hourly folders to MR job instead of individual files).
        Counter counter = job.getCounters().findCounter(RecordKeyMapperBase.EVENT_COUNTER.RECORD_COUNT);
        newTotalRecords = counter.getValue();
      }
      final Path finalDstPath = dstPath;
      goodPaths.stream().forEach(p -> {
        String fileName = p.getName();
        outputFiles.add(new Path(finalDstPath, fileName));
      });
      this.configurator.setDstNewFiles(outputFiles);

      State compactionState = helper.loadState(new Path(result.getDstAbsoluteDir()));
      if (executionCount != 0) {
        compactionState.setProp(CompactionSlaEventHelper.RECORD_COUNT_TOTAL + Long.toString(executionCount),
            Long.toString(helper.readRecordCount(new Path(result.getDstAbsoluteDir()))));
        compactionState.setProp(CompactionSlaEventHelper.EXEC_COUNT_TOTAL + Long.toString(executionCount),
            Long.toString(executionCount));
        compactionState.setProp(DUPLICATE_COUNT_TOTAL + Long.toString(executionCount),
            compactionState.getProp(DUPLICATE_COUNT_TOTAL, "null"));
      }
      if (state.getPropAsBoolean(ConfigurationKeys.GOBBLIN_METADATA_CHANGE_EVENT_ENABLED, false)) {
        //GMCE enabled, set the key to be false to indicate that GMCE has not been sent yet
        compactionState.setProp(CompactionGMCEPublishingAction.GMCE_EMITTED_KEY, false);
      }
      compactionState.setProp(CompactionSlaEventHelper.RECORD_COUNT_TOTAL, Long.toString(newTotalRecords));
      compactionState.setProp(CompactionSlaEventHelper.EXEC_COUNT_TOTAL, Long.toString(executionCount + 1));
      compactionState.setProp(CompactionSlaEventHelper.MR_JOB_ID,
          this.configurator.getConfiguredJob().getJobID().toString());
      compactionState.setProp(DUPLICATE_COUNT_TOTAL,
          job.getCounters().findCounter(RecordKeyDedupReducerBase.EVENT_COUNTER.DEDUPED).getValue());
      compactionState.setProp(CompactionSlaEventHelper.LAST_RUN_START_TIME,
          this.state.getProp(CompactionSource.COMPACTION_INIT_TIME));
      helper.saveState(new Path(result.getDstAbsoluteDir()), compactionState);
      log.info("duplicated records count for " + dstPath + " : " + compactionState.getProp(DUPLICATE_COUNT_TOTAL));

      log.info("Updating record count from {} to {} in {} [{}]", oldTotalRecords, newTotalRecords, dstPath,
          executionCount + 1);

      // submit events for record count
      if (eventSubmitter != null) {
        Map<String, String> eventMetadataMap =
            ImmutableMap.of(CompactionSlaEventHelper.DATASET_URN, dataset.datasetURN(),
                CompactionSlaEventHelper.RECORD_COUNT_TOTAL, Long.toString(newTotalRecords),
                CompactionSlaEventHelper.PREV_RECORD_COUNT_TOTAL, Long.toString(oldTotalRecords),
                CompactionSlaEventHelper.EXEC_COUNT_TOTAL, Long.toString(executionCount + 1),
                CompactionSlaEventHelper.MR_JOB_ID, this.configurator.getConfiguredJob().getJobID().toString());
        this.eventSubmitter.submit(CompactionSlaEventHelper.COMPACTION_RECORD_COUNT_EVENT, eventMetadataMap);
      }
    }
  }

  public void addEventSubmitter(EventSubmitter eventSubmitter) {
    this.eventSubmitter = eventSubmitter;
  }

  public String getName() {
    return CompactionCompleteFileOperationAction.class.getName();
  }
}
