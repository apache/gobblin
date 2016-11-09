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

package gobblin.data.management.retention;

import gobblin.data.management.retention.dataset.CleanableDataset;

/**
 * Holds event names and constants used in events submitted by a retention job.
 */
class RetentionEvents {

  /**
   * This event is submitted when {@link CleanableDataset#clean()} throws an exception
   */
  static class CleanFailed {
    static final String EVENT_NAME = "CleanFailed";
    /**
     * Value for this key will be a stacktrace of any exception caused while deleting a dataset
     */
    static final String FAILURE_CONTEXT_METADATA_KEY = "failureContext";
  }
  static final String NAMESPACE = "gobblin.data.management.retention";
  static final String DATASET_URN_METADATA_KEY = "datasetUrn";
}
