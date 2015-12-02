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
package gobblin.data.management.copy.publisher;

import com.google.common.collect.ImmutableMap;

import gobblin.configuration.WorkUnitState;
import gobblin.data.management.copy.CopyableDataset;
import gobblin.data.management.copy.CopyableDatasetMetadata;
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

  static void submitSuccessfulDatasetPublish(EventSubmitter eventSubmitter, CopyableDatasetMetadata copyableDataset) {
    SlaEventSubmitter.builder().eventSubmitter(eventSubmitter).eventName(DATASET_PUBLISHED_EVENT_NAME)
        .datasetUrn(copyableDataset.getDatasetRoot().toString()).partition(copyableDataset.getDatasetRoot().toString())
        .additionalMetadata(DATASET_TARGET_ROOT_METADATA_NAME, copyableDataset.getDatasetTargetRoot().toString()).build()
        .submit();
  }

  static void submitFailedDatasetPublish(EventSubmitter eventSubmitter, CopyableDatasetMetadata copyableDataset) {
    eventSubmitter.submit(DATASET_PUBLISHED_FAILED_EVENT_NAME, ImmutableMap.of(DATASET_ROOT_METADATA_NAME,
        copyableDataset.getDatasetRoot().toString(), DATASET_TARGET_ROOT_METADATA_NAME, copyableDataset
            .getDatasetTargetRoot().toString()));

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
  static void submitSuccessfulFilePublish(EventSubmitter eventSubmitter, WorkUnitState workUnitState) {
    new SlaEventSubmitter(eventSubmitter, FILE_PUBLISHED_EVENT_NAME, workUnitState.getProperties()).submit();
  }

}
