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

package gobblin.compaction.event;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import gobblin.compaction.mapreduce.MRCompactor;
import gobblin.compaction.mapreduce.avro.AvroKeyDedupReducer;
import gobblin.configuration.State;
import gobblin.metrics.event.sla.SlaEventKeys;


/**
 * Helper class to populate sla event metadata in state.
 */
public class CompactionSlaEventHelper {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionSlaEventHelper.class);

  public static void populateState(State state, Optional<Job> job, FileSystem fs) {
    setDatasetUrn(state);
    setPartition(state);
    setDedupeStatus(state);
    setPreviousPublishTime(state, fs);
    if (job.isPresent()) {
      setRecordCount(state, job.get());
    }
    setUpstreamTimeStamp(state, fs);
  }

  private static void setDatasetUrn(State state) {
    state.setProp(SlaEventKeys.DATASET_URN_KEY,
        new Path(state.getProp(MRCompactor.COMPACTION_DEST_DIR), state.getProp(MRCompactor.COMPACTION_TOPIC))
            .toString());
  }

  private static void setPartition(State state) {
    state.setProp(SlaEventKeys.PARTITION_KEY, state.getProp(MRCompactor.COMPACTION_JOB_DEST_PARTITION));
  }

  private static void setUpstreamTimeStamp(State state, FileSystem fs) {

    String inputDirectory = state.getProp(MRCompactor.COMPACTION_JOB_INPUT_DIR);
    try {
      FileStatus fileStatus = fs.getFileStatus(new Path(inputDirectory));
      state.setProp(SlaEventKeys.UPSTREAM_TS_IN_MILLI_SECS_KEY, Long.toString(fileStatus.getModificationTime()));
    } catch (IOException e) {
      LOG.debug("Failed to get upstream time.", e);
    }
  }

  private static void setPreviousPublishTime(State state, FileSystem fs) {

    Path compactionCompletePath =
        new Path(state.getProp(MRCompactor.COMPACTION_JOB_DEST_DIR), MRCompactor.COMPACTION_COMPLETE_FILE_NAME);

    try {
      FileStatus fileStatus = fs.getFileStatus(compactionCompletePath);
      state.setProp(SlaEventKeys.PREVIOUS_PUBLISH_TS_IN_MILLI_SECS_KEY,
          Long.toString(fileStatus.getModificationTime()));
    } catch (IOException e) {
      LOG.debug("Failed to get previous publish time.", e);
    }
  }

  private static void setDedupeStatus(State state) {
    if (state.getPropAsBoolean(MRCompactor.COMPACTION_DEDUPLICATE, MRCompactor.DEFAULT_COMPACTION_DEDUPLICATE)) {
      state.setProp(SlaEventKeys.DEDUPE_STATUS_KEY, DedupeStatus.DEDUPED);

    } else {
      state.setProp(SlaEventKeys.DEDUPE_STATUS_KEY, DedupeStatus.NOT_DEDUPED);
    }
  }

  private static void setRecordCount(State state, Job job) {

    Counters counters = null;
    try {
      counters = job.getCounters();
    } catch (IOException e) {
      LOG.debug("Failed to get job counters. Record count will not be set. ", e);
      return;
    }

    if (counters != null) {
      Counter recordCounter = counters.findCounter(AvroKeyDedupReducer.EVENT_COUNTER.RECORD_COUNT);
      if (recordCounter != null) {
        state.setProp(SlaEventKeys.RECORD_COUNT_KEY, Long.toString(recordCounter.getValue()));
      }
    }
  }

}
