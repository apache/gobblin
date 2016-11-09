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
  public static final String COMPACTION_COMPLETED_EVENT_NAME = "CompactionCompleted";
  public static final String COMPACTION_FAILED_EVENT_NAME = "CompactionFailed";
  public static final String COMPLETION_VERIFICATION_FAILED_EVENT_NAME = "CompletenessCannotBeVerified";
  public static final String COMPLETION_VERIFICATION_SUCCESS_EVENT_NAME = "CompletenessVerified";

  /**
   * Get an {@link SlaEventSubmitterBuilder} that has dataset urn, partition, record count, previous publish timestamp
   * and dedupe status set.
   * The caller MUST set eventSubmitter, eventname before submitting.
   *
   */
  public static SlaEventSubmitterBuilder getEventSubmitterBuilder(Dataset dataset, Optional<Job> job, FileSystem fs) {
    return SlaEventSubmitter
        .builder()
        .datasetUrn(dataset.getUrn())
        .partition(dataset.jobProps().getProp(MRCompactor.COMPACTION_JOB_DEST_PARTITION, ""))
        .dedupeStatus(getOutputDedupeStatus(dataset.jobProps()))
        .previousPublishTimestamp(Long.toString(getPreviousPublishTime(dataset, fs)))
        .upstreamTimestamp(dataset.jobProps().getProp(SlaEventKeys.UPSTREAM_TS_IN_MILLI_SECS_KEY, Long.toString(-1l)))
        .recordCount(Long.toString(getRecordCount(job)));
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
