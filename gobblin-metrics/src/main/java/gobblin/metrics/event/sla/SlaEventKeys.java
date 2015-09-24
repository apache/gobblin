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

package gobblin.metrics.event.sla;

public class SlaEventKeys {

  static final String EVENT_GOBBLIN_STATE_PREFIX = "event.sla.";
  public static final String DATASET_URN_KEY = EVENT_GOBBLIN_STATE_PREFIX + "datasetUrn";
  public static final String PARTITION_KEY = EVENT_GOBBLIN_STATE_PREFIX + "partition";
  public static final String ORIGIN_TS_IN_MILLI_SECS_KEY = EVENT_GOBBLIN_STATE_PREFIX + "originTimestamp";
  public static final String UPSTREAM_TS_IN_MILLI_SECS_KEY = EVENT_GOBBLIN_STATE_PREFIX + "upstreamTimestamp";

  public static final String COMPLETENESS_PERCENTAGE_KEY = EVENT_GOBBLIN_STATE_PREFIX + "completenessPercentage";
  public static final String DEDUPE_STATUS_KEY = EVENT_GOBBLIN_STATE_PREFIX + "dedupeStatus";
  public static final String RECORD_COUNT_KEY = EVENT_GOBBLIN_STATE_PREFIX + "recordCount";
  public static final String PREVIOUS_PUBLISH_TS_IN_MILLI_SECS_KEY = EVENT_GOBBLIN_STATE_PREFIX + "previousPublishTs";

}
