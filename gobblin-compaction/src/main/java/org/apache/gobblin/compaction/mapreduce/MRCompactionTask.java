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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter;

import com.google.common.collect.ImmutableMap;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.compaction.action.CompactionCompleteAction;
import org.apache.gobblin.compaction.event.CompactionSlaEventHelper;
import org.apache.gobblin.compaction.suite.CompactionSuite;
import org.apache.gobblin.compaction.suite.CompactionSuiteUtils;
import org.apache.gobblin.compaction.verify.CompactionVerifier;
import org.apache.gobblin.dataset.Dataset;
import org.apache.gobblin.dataset.FileSystemDataset;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.runtime.mapreduce.MRTask;



/**
 * Customized task of type {@link MRTask}, which runs MR job to compact dataset.
 * The job creation is delegated to {@link CompactionSuite#createJob(Dataset)}
 * After job is created, {@link MRCompactionTask#run()} is invoked and after compaction is finished.
 * a callback {@link CompactionSuite#getCompactionCompleteActions()} will be invoked
 */
@Slf4j
public class MRCompactionTask extends MRTask {

  public static final String RECORD_COUNT = "counter.recordCount";
  public static final String FILE_COUNT = "counter.fileCount";
  public static final String BYTE_COUNT = "counter.byteCount";

  protected final CompactionSuite suite;
  protected final Dataset dataset;
  protected final EventSubmitter eventSubmitter;
  /**
   * Constructor
   */
  public MRCompactionTask(TaskContext taskContext) throws IOException {
    super(taskContext);
    this.suite = CompactionSuiteUtils.getCompactionSuiteFactory (taskContext.getTaskState()).
            createSuite(taskContext.getTaskState());
    this.dataset = this.suite.load(taskContext.getTaskState());
    this.eventSubmitter = new EventSubmitter.Builder(this.metricContext, MRCompactor.COMPACTION_TRACKING_EVENTS_NAMESPACE)
        .addMetadata(additionalEventMetadata()).build();
  }

  /**
   * Below three steps are performed for a compaction task:
   * Do verifications before a map-reduce job is launched.
   * Start a map-reduce job and wait until it is finished
   * Do post-actions after map-reduce job is finished
   */
  @Override
  public void run() {
    List<CompactionVerifier> verifiers = this.suite.getMapReduceVerifiers();
    for (CompactionVerifier verifier : verifiers) {
      if (!verifier.verify(dataset).isSuccessful()) {
        log.error("Verification {} for {} is not passed.", verifier.getName(), dataset.getUrn());
        this.onMRTaskComplete (false, new IOException("Compaction verification for MR is failed"));
        return;
      }
    }

    if (dataset instanceof FileSystemDataset
        && ((FileSystemDataset)dataset).isVirtual()) {
      log.info("A trivial compaction job as there is no physical data for {}."
          + "Will trigger a success complete directly", dataset.getUrn());
      this.onMRTaskComplete(true, null);
      return;
    }

    super.run();
  }

  public void onMRTaskComplete (boolean isSuccess, Throwable throwable) {
    if (isSuccess) {
      try {
        setCounterInfo(taskContext.getTaskState());

        List<CompactionCompleteAction> actions = this.suite.getCompactionCompleteActions();
        for (CompactionCompleteAction action: actions) {
          action.addEventSubmitter(eventSubmitter);
          action.onCompactionJobComplete(dataset);
        }
        submitEvent(CompactionSlaEventHelper.COMPACTION_COMPLETED_EVENT_NAME);
        super.onMRTaskComplete(true, null);
      } catch (IOException e) {
        submitEvent(CompactionSlaEventHelper.COMPACTION_FAILED_EVENT_NAME);
        super.onMRTaskComplete(false, e);
      }
    } else {
      submitEvent(CompactionSlaEventHelper.COMPACTION_FAILED_EVENT_NAME);
      super.onMRTaskComplete(false, throwable);
    }
  }

  private void setCounterInfo(TaskState taskState)
      throws IOException {

    if (mrJob == null) {
      return;
    }

    long recordCount = getCounterValue(mrJob, RecordKeyDedupReducerBase.EVENT_COUNTER.RECORD_COUNT);
    if (recordCount == 0) {
      // map only job
      recordCount = getCounterValue(mrJob, RecordKeyMapperBase.EVENT_COUNTER.RECORD_COUNT);
    }
    taskState.setProp(RECORD_COUNT, recordCount);
    taskState.setProp(FILE_COUNT, getCounterValue(mrJob, CompactorOutputCommitter.EVENT_COUNTER.OUTPUT_FILE_COUNT));
    taskState.setProp(BYTE_COUNT, getCounterValue(mrJob, FileOutputFormatCounter.BYTES_WRITTEN));
  }

  private long getCounterValue(Job job, Enum<?> key)
      throws IOException {
    return job.getCounters().findCounter(key).getValue();
  }

  private void submitEvent(String eventName) {
    Map<String, String> eventMetadataMap = ImmutableMap.of(CompactionSlaEventHelper.DATASET_URN, this.dataset.datasetURN());
    this.eventSubmitter.submit(eventName, eventMetadataMap);
  }

  /**
   * Create a map-reduce job
   * The real job configuration is delegated to {@link CompactionSuite#createJob(Dataset)}
   *
   * @return a map-reduce job
   */
  protected Job createJob() throws IOException {
    return this.suite.createJob(dataset);
  }
}
