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

package gobblin.compaction.event;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import gobblin.compaction.dataset.Dataset;
import gobblin.compaction.mapreduce.MRCompactor;
import gobblin.compaction.mapreduce.avro.AvroKeyDedupReducer;
import gobblin.compaction.mapreduce.avro.AvroKeyMapper;
import gobblin.configuration.State;
import gobblin.metrics.event.sla.SlaEventKeys;
import gobblin.metrics.event.sla.SlaEventSubmitter;
import gobblin.metrics.event.sla.SlaEventSubmitter.SlaEventSubmitterBuilder;


/**
 * Helper class to build compaction sla event metadata.
 */
public class CompactionSlaEventHelper {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionSlaEventHelper.class);
  public static final String RECOMPATED_METADATA_NAME = "recompacted";
  public static final String DATASET_URN = "datasetUrn";
  public static final String DATASET_OUTPUT_PATH = "datasetOutputPath";
  public static final String LATE_RECORD_COUNT = "lateRecordCount";
  public static final String REGULAR_RECORD_COUNT = "regularRecordCount";
  public static final String NEED_RECOMPACT = "needRecompact";
  public static final String RECORD_COUNT_TOTAL = "recordCountTotal";
  public static final String HIVE_REGISTRATION_PATHS = "hiveRegistrationPaths";
  public static final String RENAME_DIR_PATHS = "renameDirPaths";

  public static final String COMPACTION_COMPLETED_EVENT_NAME = "CompactionCompleted";
  public static final String COMPACTION_FAILED_EVENT_NAME = "CompactionFailed";
  public static final String COMPLETION_VERIFICATION_FAILED_EVENT_NAME = "CompletenessCannotBeVerified";
  public static final String COMPLETION_VERIFICATION_SUCCESS_EVENT_NAME = "CompletenessVerified";
  public static final String COMPACTION_RECORD_COUNT_EVENT = "CompactionRecordCounts";
  public static final String COMPACTION_HIVE_REGISTRATION_EVENT = "CompactionHiveRegistration";
  public static final String COMPACTION_MARK_DIR_EVENT = "CompactionMarkDirComplete";

  /**
   * Get an {@link SlaEventSubmitterBuilder} that has dataset urn, partition, record count, previous publish timestamp
   * and dedupe status set.
   * The caller MUST set eventSubmitter, eventname before submitting.
   */
  public static SlaEventSubmitterBuilder getEventSubmitterBuilder(Dataset dataset, Optional<Job> job, FileSystem fs) {
    SlaEventSubmitterBuilder builder =
        SlaEventSubmitter.builder().datasetUrn(dataset.getUrn())
            .partition(dataset.jobProps().getProp(MRCompactor.COMPACTION_JOB_DEST_PARTITION, ""))
            .dedupeStatus(getOutputDedupeStatus(dataset.jobProps()));

    long previousPublishTime = getPreviousPublishTime(dataset, fs);
    long upstreamTime = dataset.jobProps().getPropAsLong(SlaEventKeys.UPSTREAM_TS_IN_MILLI_SECS_KEY, -1l);
    long recordCount = getRecordCount(job);

    // Previous publish only exists when this is a recompact job
    if (previousPublishTime != -1l) {
      builder.previousPublishTimestamp(Long.toString(previousPublishTime));
    }
    // Upstream time is the logical time represented by the compaction input directory
    if (upstreamTime != -1l) {
      builder.upstreamTimestamp(Long.toString(upstreamTime));
    }
    if (recordCount != -1l) {
      builder.recordCount(Long.toString(recordCount));
    }
    return builder;
  }

  /**
   * {@link Deprecated} use {@link #getEventSubmitterBuilder(Dataset, Optional, FileSystem)}
   */
  @Deprecated
  public static void populateState(Dataset dataset, Optional<Job> job, FileSystem fs) {
    dataset.jobProps().setProp(SlaEventKeys.DATASET_URN_KEY, dataset.getUrn());
    dataset.jobProps().setProp(SlaEventKeys.PARTITION_KEY,
        dataset.jobProps().getProp(MRCompactor.COMPACTION_JOB_DEST_PARTITION, ""));
    dataset.jobProps().setProp(SlaEventKeys.DEDUPE_STATUS_KEY, getOutputDedupeStatus(dataset.jobProps()));
    dataset.jobProps().setProp(SlaEventKeys.PREVIOUS_PUBLISH_TS_IN_MILLI_SECS_KEY, getPreviousPublishTime(dataset, fs));
    dataset.jobProps().setProp(SlaEventKeys.RECORD_COUNT_KEY, getRecordCount(job));
  }

  public static void setUpstreamTimeStamp(State state, long time) {
    state.setProp(SlaEventKeys.UPSTREAM_TS_IN_MILLI_SECS_KEY, Long.toString(time));
  }

  private static long getPreviousPublishTime(Dataset dataset, FileSystem fs) {
    Path compactionCompletePath = new Path(dataset.outputPath(), MRCompactor.COMPACTION_COMPLETE_FILE_NAME);
    try {
      return fs.getFileStatus(compactionCompletePath).getModificationTime();
    } catch (IOException e) {
      LOG.debug("Failed to get previous publish time.", e);
    }
    return -1l;
  }

  private static String getOutputDedupeStatus(State state) {
    return state.getPropAsBoolean(MRCompactor.COMPACTION_OUTPUT_DEDUPLICATED,
        MRCompactor.DEFAULT_COMPACTION_OUTPUT_DEDUPLICATED) ? DedupeStatus.DEDUPED.toString()
        : DedupeStatus.NOT_DEDUPED.toString();
  }

  private static long getRecordCount(Optional<Job> job) {

    if (!job.isPresent()) {
      return -1l;
    }

    Counters counters = null;
    try {
      counters = job.get().getCounters();
    } catch (IOException e) {
      LOG.debug("Failed to get job counters. Record count will not be set. ", e);
      return -1l;
    }

    Counter recordCounter = counters.findCounter(AvroKeyDedupReducer.EVENT_COUNTER.RECORD_COUNT);

    if (recordCounter != null && recordCounter.getValue() != 0) {
      return recordCounter.getValue();
    }

    recordCounter = counters.findCounter(AvroKeyMapper.EVENT_COUNTER.RECORD_COUNT);

    if (recordCounter != null && recordCounter.getValue() != 0) {
      return recordCounter.getValue();
    }

    LOG.debug("Non zero record count not found in both mapper and reducer counters");

    return -1l;
  }
}
