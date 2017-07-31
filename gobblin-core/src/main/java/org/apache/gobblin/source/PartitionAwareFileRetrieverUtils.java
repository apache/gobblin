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
package org.apache.gobblin.source;

import org.joda.time.Duration;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.DatePartitionType;

import static org.apache.gobblin.source.PartitionedFileSourceBase.DATE_PARTITIONED_SOURCE_PARTITION_LEAD_TIME;
import static org.apache.gobblin.source.PartitionedFileSourceBase.DATE_PARTITIONED_SOURCE_PARTITION_LEAD_TIME_GRANULARITY;
import static org.apache.gobblin.source.PartitionedFileSourceBase.DEFAULT_DATE_PARTITIONED_SOURCE_PARTITION_LEAD_TIME_GRANULARITY;
import static org.apache.gobblin.source.PartitionedFileSourceBase.DEFAULT_PARTITIONED_SOURCE_PARTITION_LEAD_TIME;


/**
 * Utility functions for parsing configuration parameters commonly used by {@link PartitionAwareFileRetriever}
 * objects.
 */
public class PartitionAwareFileRetrieverUtils {
  /**
   * Retrieve the lead time duration from the LEAD_TIME and LEAD_TIME granularity config settings.
   */
  public static Duration getLeadTimeDurationFromConfig(State state) {
    String leadTimeProp = state.getProp(DATE_PARTITIONED_SOURCE_PARTITION_LEAD_TIME);
    if (leadTimeProp == null || leadTimeProp.length() == 0) {
      return DEFAULT_PARTITIONED_SOURCE_PARTITION_LEAD_TIME;
    }

    int leadTime = Integer.parseInt(leadTimeProp);

    DatePartitionType leadTimeGranularity = DEFAULT_DATE_PARTITIONED_SOURCE_PARTITION_LEAD_TIME_GRANULARITY;

    String leadTimeGranularityProp = state.getProp(DATE_PARTITIONED_SOURCE_PARTITION_LEAD_TIME_GRANULARITY);
    if (leadTimeGranularityProp != null) {
      leadTimeGranularity = DatePartitionType.valueOf(leadTimeGranularityProp);
    }

    return new Duration(leadTime * leadTimeGranularity.getUnitMilliseconds());
  }
}
