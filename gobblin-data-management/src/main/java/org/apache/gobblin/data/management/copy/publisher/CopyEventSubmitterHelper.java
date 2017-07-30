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
package gobblin.data.management.copy.publisher;

import com.google.common.collect.ImmutableMap;

import gobblin.configuration.WorkUnitState;
import gobblin.data.management.copy.CopyEntity;
import gobblin.data.management.copy.CopyableFile;
import gobblin.metrics.event.EventSubmitter;
import gobblin.metrics.event.sla.SlaEventKeys;
import gobblin.metrics.event.sla.SlaEventSubmitter;

import java.util.Map;

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

  static void submitSuccessfulDatasetPublish(EventSubmitter eventSubmitter,
      CopyEntity.DatasetAndPartition datasetAndPartition, String originTimestamp, String upstreamTimestamp,
      Map<String, String> additionalMetadata) {
        SlaEventSubmitter.builder().eventSubmitter(eventSubmitter).eventName(DATASET_PUBLISHED_EVENT_NAME)
        .datasetUrn(datasetAndPartition.getDataset().getDatasetURN()).partition(datasetAndPartition.getPartition())
            .originTimestamp(originTimestamp).upstreamTimestamp(upstreamTimestamp).additionalMetadata(additionalMetadata)
            .build().submit();
  }

  static void submitFailedDatasetPublish(EventSubmitter eventSubmitter,
      CopyEntity.DatasetAndPartition datasetAndPartition) {
    eventSubmitter.submit(DATASET_PUBLISHED_FAILED_EVENT_NAME,
        ImmutableMap.of(DATASET_ROOT_METADATA_NAME, datasetAndPartition.getDataset().getDatasetURN()));
  }

  /**
   * Submit an sla event when a {@link gobblin.data.management.copy.CopyableFile} is published. The <code>workUnitState</code> passed should have the
   * required {@link SlaEventKeys} set.
   *
   * @see SlaEventSubmitter#submit()
   *
   * @param eventSubmitter
   * @param workUnitState
   */
  static void submitSuccessfulFilePublish(EventSubmitter eventSubmitter, CopyableFile cf, WorkUnitState workUnitState) {
    String datasetUrn = workUnitState.getProp(SlaEventKeys.DATASET_URN_KEY);
    String partition = workUnitState.getProp(SlaEventKeys.PARTITION_KEY);
    String completenessPercentage = workUnitState.getProp(SlaEventKeys.COMPLETENESS_PERCENTAGE_KEY);
    String recordCount = workUnitState.getProp(SlaEventKeys.RECORD_COUNT_KEY);
    String previousPublishTimestamp = workUnitState.getProp(SlaEventKeys.PREVIOUS_PUBLISH_TS_IN_MILLI_SECS_KEY);
    String dedupeStatus = workUnitState.getProp(SlaEventKeys.DEDUPE_STATUS_KEY);
    SlaEventSubmitter.builder().eventSubmitter(eventSubmitter).eventName(FILE_PUBLISHED_EVENT_NAME)
        .datasetUrn(datasetUrn).partition(partition).originTimestamp(Long.toString(cf.getOriginTimestamp()))
        .upstreamTimestamp(Long.toString(cf.getUpstreamTimestamp())).completenessPercentage(completenessPercentage)
        .recordCount(recordCount).previousPublishTimestamp(previousPublishTimestamp).dedupeStatus(dedupeStatus)
        .additionalMetadata(TARGET_PATH, cf.getDestination().toString())
        .additionalMetadata(SOURCE_PATH, cf.getOrigin().getPath().toString())
        .additionalMetadata(SIZE_IN_BYTES, Long.toString(cf.getOrigin().getLen())).build().submit();
  }
}
