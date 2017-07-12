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

package gobblin.compaction.action;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import gobblin.compaction.dataset.DatasetHelper;
import gobblin.compaction.event.CompactionSlaEventHelper;
import gobblin.compaction.mapreduce.CompactionAvroJobConfigurator;
import gobblin.compaction.mapreduce.MRCompactor;
import gobblin.compaction.mapreduce.MRCompactorJobRunner;
import gobblin.compaction.mapreduce.avro.AvroKeyMapper;
import gobblin.compaction.parser.CompactionPathParser;
import gobblin.compaction.verify.InputRecordCountHelper;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.dataset.FileSystemDataset;
import gobblin.metrics.event.EventSubmitter;
import gobblin.util.HadoopUtils;
import gobblin.util.WriterUtils;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.List;
import java.util.Map;


/**
 * A type of post action {@link CompactionCompleteAction} which focus on the file operations
 */
@Slf4j
@AllArgsConstructor
public class CompactionCompleteFileOperationAction implements CompactionCompleteAction<FileSystemDataset> {

  protected WorkUnitState state;
  private CompactionAvroJobConfigurator configurator;
  private InputRecordCountHelper helper;
  private EventSubmitter eventSubmitter;
  private FileSystem fs;

  public CompactionCompleteFileOperationAction (State state, CompactionAvroJobConfigurator configurator) {
    if (!(state instanceof WorkUnitState)) {
      throw new UnsupportedOperationException(this.getClass().getName() + " only supports workunit state");
    }
    this.state = (WorkUnitState) state;
    this.helper = new InputRecordCountHelper(state);
    this.configurator = configurator;
    this.fs = configurator.getFs();
  }

  /**
   * Replace or append the destination folder with new avro files from map-reduce job
   * Create a record count file containing the number of records that have been processed .
   */
  public void onCompactionJobComplete (FileSystemDataset dataset) throws IOException {
    if (configurator != null && configurator.isJobCreated()) {
      CompactionPathParser.CompactionParserResult result = new CompactionPathParser(state).parse(dataset);
      Path tmpPath = configurator.getMrOutputPath();
      Path dstPath = new Path (result.getDstAbsoluteDir());

      // this is append delta mode due to the compaction rename source dir mode being enabled
      boolean appendDeltaOutput = this.state.getPropAsBoolean(MRCompactor.COMPACTION_RENAME_SOURCE_DIR_ENABLED,
              MRCompactor.DEFAULT_COMPACTION_RENAME_SOURCE_DIR_ENABLED);

      // Obtain record count from input file names
      // We are not getting record count from map-reduce counter because in next run, the threshold (delta record)
      // calculation is based on the input file names.
      long newTotalRecords = 0;
      long oldTotalRecords = InputRecordCountHelper.readRecordCount (helper.getFs(), new Path (result.getDstAbsoluteDir()));

      if (appendDeltaOutput) {
        FsPermission permission = HadoopUtils.deserializeFsPermission(this.state,
                MRCompactorJobRunner.COMPACTION_JOB_OUTPUT_DIR_PERMISSION,
                FsPermission.getDefault());
        WriterUtils.mkdirsWithRecursivePermission(this.fs, dstPath, permission);
        // append files under mr output to destination
        List<Path> paths = DatasetHelper.getApplicableFilePaths(fs, tmpPath, Lists.newArrayList("avro"));
        for (Path path: paths) {
          String fileName = path.getName();
          log.info(String.format("Adding %s to %s", path.toString(), dstPath));
          Path outPath = new Path (dstPath, fileName);

          if (!this.fs.rename(path, outPath)) {
            throw new IOException(
                    String.format("Unable to move %s to %s", path.toString(), outPath.toString()));
          }
        }

        newTotalRecords = this.configurator.getFileNameRecordCount();
      } else {
        this.fs.delete(dstPath, true);
        FsPermission permission = HadoopUtils.deserializeFsPermission(this.state,
                MRCompactorJobRunner.COMPACTION_JOB_OUTPUT_DIR_PERMISSION,
                FsPermission.getDefault());

        WriterUtils.mkdirsWithRecursivePermission(this.fs, dstPath.getParent(), permission);
        if (!this.fs.rename(tmpPath, dstPath)) {
          throw new IOException(
                  String.format("Unable to move %s to %s", tmpPath, dstPath));
        }

        // get record count from map reduce job counter
        Job job = this.configurator.getConfiguredJob();
        Counter counter = job.getCounters().findCounter(AvroKeyMapper.EVENT_COUNTER.RECORD_COUNT);
        newTotalRecords = counter.getValue();
      }

      // submit events for record count
      if (eventSubmitter != null) {
        Map<String, String> eventMetadataMap = ImmutableMap.of(CompactionSlaEventHelper.DATASET_URN, dataset.datasetURN(),
            CompactionSlaEventHelper.RECORD_COUNT_TOTAL, Long.toString(newTotalRecords));
        this.eventSubmitter.submit(CompactionSlaEventHelper.COMPACTION_RECORD_COUNT_EVENT, eventMetadataMap);
      }

      InputRecordCountHelper.writeRecordCount (helper.getFs(), new Path (result.getDstAbsoluteDir()), newTotalRecords);
      log.info("Updating record count from {} to {} in {} ", oldTotalRecords, newTotalRecords, dstPath);
    }
  }

  public void addEventSubmitter(EventSubmitter eventSubmitter) {
    this.eventSubmitter = eventSubmitter;
  }

  public String getName () {
    return CompactionCompleteFileOperationAction.class.getName();
  }
}
