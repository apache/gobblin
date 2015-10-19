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

import gobblin.compaction.Dataset;
import gobblin.compaction.mapreduce.MRCompactor;
import gobblin.compaction.mapreduce.avro.AvroKeyDedupReducer;
import gobblin.compaction.mapreduce.avro.AvroKeyMapper;
import gobblin.configuration.State;
import gobblin.metrics.event.sla.SlaEventKeys;


/**
 * Helper class to populate sla event metadata in state.
 */
public class CompactionSlaEventHelper {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionSlaEventHelper.class);

  public static void populateState(Dataset dataset, Optional<Job> job, FileSystem fs) {
    setDatasetUrn(dataset);
    setPartition(dataset);
    setOutputDedupeStatus(dataset.jobProps());
    setPreviousPublishTime(dataset, fs);
    if (job.isPresent()) {
      setRecordCount(dataset.jobProps(), job.get());
    }
  }

  public static void setUpstreamTimeStamp(State state, long time) {
    state.setProp(SlaEventKeys.UPSTREAM_TS_IN_MILLI_SECS_KEY, Long.toString(time));
  }

  private static void setDatasetUrn(Dataset dataset) {
    dataset.jobProps().setProp(SlaEventKeys.DATASET_URN_KEY,
        new Path(dataset.jobProps().getProp(MRCompactor.COMPACTION_DEST_DIR), dataset.topic()).toString());
  }

  private static void setPartition(Dataset dataset) {
    dataset.jobProps().setProp(SlaEventKeys.PARTITION_KEY,
        dataset.jobProps().getProp(MRCompactor.COMPACTION_JOB_DEST_PARTITION, ""));
  }

  private static void setPreviousPublishTime(Dataset dataset, FileSystem fs) {

    Path compactionCompletePath = new Path(dataset.outputPath(), MRCompactor.COMPACTION_COMPLETE_FILE_NAME);

    try {
      FileStatus fileStatus = fs.getFileStatus(compactionCompletePath);
      dataset.jobProps().setProp(SlaEventKeys.PREVIOUS_PUBLISH_TS_IN_MILLI_SECS_KEY,
          Long.toString(fileStatus.getModificationTime()));
    } catch (IOException e) {
      LOG.debug("Failed to get previous publish time.", e);
    }
  }

  private static void setOutputDedupeStatus(State state) {
    if (state.getPropAsBoolean(MRCompactor.COMPACTION_OUTPUT_DEDUPLICATED,
        MRCompactor.DEFAULT_COMPACTION_OUTPUT_DEDUPLICATED)) {
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
      LOG.info("Failed to get job counters. Record count will not be set. ", e);
      return;
    }

    Counter recordCounter = counters.findCounter(AvroKeyDedupReducer.EVENT_COUNTER.RECORD_COUNT);

    if (recordCounter != null & recordCounter.getValue() != 0) {
      state.setProp(SlaEventKeys.RECORD_COUNT_KEY, Long.toString(recordCounter.getValue()));
      return;
    }

    recordCounter = counters.findCounter(AvroKeyMapper.EVENT_COUNTER.RECORD_COUNT);

    if (recordCounter != null & recordCounter.getValue() != 0) {
      state.setProp(SlaEventKeys.RECORD_COUNT_KEY, Long.toString(recordCounter.getValue()));
      return;
    }

    LOG.info("Non zero record count not found in both mapper and reducer counters");

  }

}
