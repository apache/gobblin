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
  public static final String IS_FIRST_PUBLISH = EVENT_GOBBLIN_STATE_PREFIX + "isFirstPublish";

  public static final String EVENT_ADDITIONAL_METADATA_PREFIX = EVENT_GOBBLIN_STATE_PREFIX + "additionalMetadata.";

  public static final String SOURCE_URI = "sourceCluster";
  public static final String DESTINATION_URI = "destinationCluster";
}
