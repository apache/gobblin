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

import java.util.Optional;

import org.joda.time.DateTimeFieldType;
import org.joda.time.Duration;
import org.joda.time.chrono.ISOChronology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG = LoggerFactory.getLogger(PartitionAwareFileRetrieverUtils.class);
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

  /**
   * Calculates the lookback time duration based on the provided lookback time string.
   *
   * @param lookBackTime the lookback time string, which should include a numeric value followed by a time unit character.
   *                     For example, "5d" for 5 days or "10h" for 10 hours.
   * @return an {@link Optional} containing the {@link Duration} if the lookback time is valid, or
   *         an empty {@link Optional} if the lookback time is invalid or cannot be parsed.
   */
  public static Optional<Duration> getLookbackTimeDuration(String lookBackTime) {
    try {
      DateTimeFieldType lookBackTimeGranularity = DatePartitionType.getLowestIntervalUnit(lookBackTime);
      if (lookBackTimeGranularity != null) {
        long lookBackTimeGranularityInMillis =
            lookBackTimeGranularity.getDurationType().getField(ISOChronology.getInstance()).getUnitMillis();
        long lookBack = Long.parseLong(lookBackTime.substring(0, lookBackTime.length() - 1));
        return Optional.of(new Duration(lookBack * lookBackTimeGranularityInMillis));
      }
      LOG.warn("There is no valid time granularity for lookback time: {}", lookBackTime);
      return Optional.empty();
    } catch(NumberFormatException ex) {
      LOG.warn("Exception Caught while parsing lookback time", ex);
      return Optional.empty();
    }
  }
}
