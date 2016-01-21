/*
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
package gobblin.data.management.copy.publisher;

import java.io.IOException;

import com.google.common.collect.ImmutableMap;

import gobblin.configuration.WorkUnitState;
import gobblin.data.management.copy.CopySource;
import gobblin.data.management.copy.CopyableFile;
import gobblin.metrics.event.EventSubmitter;
import gobblin.metrics.event.sla.SlaEventKeys;
import gobblin.metrics.event.sla.SlaEventSubmitter;


/**
 * Helper class to submit events for copy job
 */
public class CopyEventSubmitterHelper {

  private static final String DATASET_PUBLISHED_EVENT_NAME = "DatasetPublished";
  private static final String DATASET_PUBLISHED_FAILED_EVENT_NAME = "DatasetPublishFailed";
  private static final String FILE_PUBLISHED_EVENT_NAME = "FilePublished";
  public static final String DATASET_ROOT_METADATA_NAME = "datasetUrn";
  public static final String DATASET_TARGET_ROOT_METADATA_NAME = "datasetTargetRoot";
  public static final String TARGET_PATH = "TargetPath";
  public static final String SOURCE_PATH = "SourcePath";
  public static final String SIZE_IN_BYTES = "SizeInBytes";

  static void submitSuccessfulDatasetPublish(EventSubmitter eventSubmitter, CopyableFile.DatasetAndPartition
      datasetAndPartition, String originTimestamp, String upstreamTimestamp) {
    SlaEventSubmitter.builder().eventSubmitter(eventSubmitter).eventName(DATASET_PUBLISHED_EVENT_NAME)
        .datasetUrn(datasetAndPartition.getDataset().getDatasetRoot().toString())
        .partition(datasetAndPartition.getPartition())
        .originTimestamp(originTimestamp)
        .upstreamTimestamp(upstreamTimestamp)
        .additionalMetadata(DATASET_TARGET_ROOT_METADATA_NAME,
            datasetAndPartition.getDataset().getDatasetTargetRoot().toString()).build()
        .submit();
  }

  static void submitFailedDatasetPublish(EventSubmitter eventSubmitter,
      CopyableFile.DatasetAndPartition datasetAndPartition) {
    eventSubmitter.submit(DATASET_PUBLISHED_FAILED_EVENT_NAME, ImmutableMap.of(DATASET_ROOT_METADATA_NAME,
        datasetAndPartition.getDataset().getDatasetRoot().toString(), DATASET_TARGET_ROOT_METADATA_NAME,
        datasetAndPartition.getDataset().getDatasetTargetRoot().toString()));

  }

  /**
   * Submit an sla event when a {@link CopyableFile} is published. The <code>workUnitState</code> passed should have the
   * required {@link SlaEventKeys} set.
   *
   * @see {@link SlaEventSubmitter#submit()} for all the required {@link SlaEventKeys} to be set
   *
   * @param eventSubmitter
   * @param workUnitState
   */
  static void submitSuccessfulFilePublish(EventSubmitter eventSubmitter, WorkUnitState workUnitState) throws
      IOException {
    String datasetUrn = workUnitState.getProp(SlaEventKeys.DATASET_URN_KEY);
    String partition = workUnitState.getProp(SlaEventKeys.PARTITION_KEY);
    String completenessPercentage = workUnitState.getProp(SlaEventKeys.COMPLETENESS_PERCENTAGE_KEY);
    String recordCount = workUnitState.getProp(SlaEventKeys.RECORD_COUNT_KEY);
    String previousPublishTimestamp = workUnitState.getProp(SlaEventKeys.PREVIOUS_PUBLISH_TS_IN_MILLI_SECS_KEY);
    String dedupeStatus = workUnitState.getProp(SlaEventKeys.DEDUPE_STATUS_KEY);
    CopyableFile copyableFile = CopySource.deserializeCopyableFile(workUnitState);
    SlaEventSubmitter.builder().eventSubmitter(eventSubmitter).eventName(FILE_PUBLISHED_EVENT_NAME)
        .datasetUrn(datasetUrn).partition(partition).originTimestamp(Long.toString(copyableFile.getOriginTimestamp()))
        .upstreamTimestamp(Long.toString(copyableFile.getUpstreamTimestamp()))
        .completenessPercentage(completenessPercentage).recordCount(recordCount)
        .previousPublishTimestamp(previousPublishTimestamp).dedupeStatus(dedupeStatus)
        .additionalMetadata(TARGET_PATH, copyableFile.getDestination().toString())
        .additionalMetadata(SOURCE_PATH, copyableFile.getOrigin().getPath().toString())
        .additionalMetadata(SIZE_IN_BYTES, Long.toString(copyableFile.getOrigin().getLen()))
        .build().submit();
  }
}
