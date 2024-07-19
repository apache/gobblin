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

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.joda.time.DateTimeUtils;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.gobblin.compaction.mapreduce.CompactionJobConfigurator;
import org.apache.gobblin.compaction.mapreduce.CompactionOrcJobConfigurator;
import org.apache.gobblin.compaction.mapreduce.MRCompactor;
import org.apache.gobblin.compaction.source.CompactionSource;
import org.apache.gobblin.compaction.verify.InputRecordCountHelper;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.dataset.FileSystemDataset;
import org.apache.gobblin.metrics.event.EventSubmitter;


/**
 * Unit tests for the {@link org.apache.gobblin.compaction.action.CompactionCompleteFileOperationAction} class.
 */
public class CompactionCompleteFileOperationActionTest {

  /**
   * Test the deletion invocation under various conditions during CompactionCompleteFileOperationAction.
   * <ul>
   *   <li>For recompaction writing to a new directory, ensure the directory is deleted if it exists.</li>
   *   <li>For recompaction overwriting the previous directory, ensure the directory is deleted before renaming.</li>
   *   <li>In append mode, ensure that deletion is never explicitly performed.</li>
   * </ul>
   * @throws IOException
   */
  @Test
  public void testDeletionDuringCompaction()
      throws IOException {

    // Initialize mocks
    final WorkUnitState wus = Mockito.spy(new WorkUnitState());
    final CompactionJobConfigurator cjc = Mockito.mock(CompactionOrcJobConfigurator.class);
    final InputRecordCountHelper irch = Mockito.mock(InputRecordCountHelper.class);
    final EventSubmitter es = Mockito.mock(EventSubmitter.class);
    final FileSystem fs = Mockito.mock(FileSystem.class);
    final FileSystemDataset fsd = Mockito.mock(FileSystemDataset.class);
    final Job job = Mockito.mock(Job.class);
    final FileStatus fileStatus = Mockito.mock(FileStatus.class);
    final Counters jobCounters = Mockito.mock(Counters.class);

    // Initialize variables
    final Path tmpFile = new Path("/tmp/somePath/someFile.orc");
    final Path oldCompactionPath = new Path("/base/datasetName/daily/2024/01/01/compaction_1");
    final Path newCompactionPath = new Path("/base/datasetName/daily/2024/01/01/compaction_2");
    final String hourlyInputPathStr = "/base/datasetName/hourly/2024/01/01";
    final Path dailyOutputPath = new Path("/base/datasetName/daily/2024/01/01");
    final Counter recordCounter = new GenericCounter();
    recordCounter.setValue(99);
    final JobID jobId = new JobID("someId", 12345);

    // Configure Mocks
    Mockito.doReturn(job).when(cjc).getConfiguredJob();
    Mockito.doReturn(false).when(fsd).isVirtual();
    Mockito.doReturn(true).when(cjc).isJobCreated();
    Mockito.doReturn(tmpFile).when(cjc).getMrOutputPath();
    Mockito.doReturn(100L).when(irch).readRecordCount(Mockito.any());
    Mockito.doReturn(1L).when(irch).readExecutionCount(Mockito.any());
    Mockito.doReturn("orc").when(cjc).getFileExtension();
    Mockito.doReturn(true).when(fs).exists(Mockito.any());
    Mockito.doReturn(fileStatus).when(fs).getFileStatus(tmpFile);
    Mockito.doReturn(true).when(fs).rename(Mockito.any(Path.class), Mockito.any(Path.class));
    Mockito.doReturn(false).when(fileStatus).isDirectory();
    Mockito.doReturn(tmpFile).when(fileStatus).getPath();
    Mockito.doReturn(oldCompactionPath).when(fs).makeQualified(oldCompactionPath);
    Mockito.doReturn(newCompactionPath).when(fs).makeQualified(newCompactionPath);
    Mockito.doReturn(dailyOutputPath).when(fs).makeQualified(dailyOutputPath);
    Mockito.doReturn(jobCounters).when(job).getCounters();
    Mockito.doReturn(jobId).when(job).getJobID();
    Mockito.doReturn(recordCounter).when(jobCounters).findCounter(Mockito.any());
    Mockito.doReturn(new State()).when(irch).loadState(Mockito.any());
    Mockito.doReturn(hourlyInputPathStr).when(fsd).datasetURN();

    // Configure WorkUnitState
    wus.setProp(MRCompactor.COMPACTION_INPUT_DIR, "/base");
    wus.setProp(MRCompactor.COMPACTION_INPUT_SUBDIR, "hourly");
    wus.setProp(MRCompactor.COMPACTION_DEST_DIR, "/base");
    wus.setProp(MRCompactor.COMPACTION_DEST_SUBDIR, "daily");
    wus.setProp(MRCompactor.COMPACTION_DEST_SUBDIR, "daily");
    wus.setProp(MRCompactor.COMPACTION_DEST_SUBDIR, "daily");
    wus.setProp(CompactionSource.COMPACTION_INIT_TIME, DateTimeUtils.currentTimeMillis());

    CompactionCompleteFileOperationAction compactionCompleteFileOperationAction =
        new CompactionCompleteFileOperationAction(wus, cjc, irch, es, fs);

    // When recompaction should write to a fresh directory
    wus.setProp(MRCompactor.COMPACTION_RENAME_SOURCE_DIR_ENABLED, false);
    wus.setProp(ConfigurationKeys.RECOMPACTION_WRITE_TO_NEW_FOLDER, true);
    compactionCompleteFileOperationAction.onCompactionJobComplete(fsd);
    Mockito.verify(fs, Mockito.times(1)).delete(newCompactionPath, true);
    Mockito.clearInvocations(fs);

    // When recompaction should write overwrite to the directory
    wus.setProp(MRCompactor.COMPACTION_RENAME_SOURCE_DIR_ENABLED, false);
    wus.setProp(ConfigurationKeys.RECOMPACTION_WRITE_TO_NEW_FOLDER, false);
    compactionCompleteFileOperationAction.onCompactionJobComplete(fsd);
    Mockito.verify(fs, Mockito.times(1)).delete(dailyOutputPath, true);
    Mockito.clearInvocations(fs);

    // When compaction should append
    wus.setProp(MRCompactor.COMPACTION_RENAME_SOURCE_DIR_ENABLED, true);
    compactionCompleteFileOperationAction.onCompactionJobComplete(fsd);
    Mockito.verify(fs, Mockito.never()).delete(newCompactionPath, true);
  }
}
