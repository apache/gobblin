/*
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

import java.util.Map;

import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;
import lombok.AllArgsConstructor;

import gobblin.compaction.Dataset;
import gobblin.metrics.event.EventSubmitter;


/**
 * Provide interfaces to submit events of compaction output record count.
 */
@AllArgsConstructor
@Slf4j
public class CompactionRecordCountEvent {

  public static final String COMPACTION_RECORD_COUNT_EVENT = "CompactionRecordCounts";
  public static final String DATASET_OUTPUT_PATH = "DatasetOutputPath";
  public static final String LATE_RECORD_COUNT = "LateRecordCount";
  public static final String REGULAR_RECORD_COUNT = "RegularRecordCount";

  private final Dataset dataset;
  private final long outputRecordCount;
  private final long lateOutputRecordCount;
  private final EventSubmitter eventSubmitter;

  /**
   * Build event metadata map, which contains dataset output path, late record count, and non-late record count.
   */
  private Map<String, String> buildRecordCountEventMetaData() {
    Map<String, String> eventMetadataMap = Maps.newHashMap();
    eventMetadataMap.put(DATASET_OUTPUT_PATH, dataset.outputPath().toString());
    eventMetadataMap.put(LATE_RECORD_COUNT, Long.toString(lateOutputRecordCount));
    eventMetadataMap.put(REGULAR_RECORD_COUNT, Long.toString(outputRecordCount));
    log.info("Compaction record count event metadata: " + eventMetadataMap);
    return eventMetadataMap;
  }

  public void submit() {
    this.eventSubmitter.submit(COMPACTION_RECORD_COUNT_EVENT, this.buildRecordCountEventMetaData());
  }
}
