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
package gobblin.data.management.conversion.hive.events;

import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.event.sla.SlaEventKeys;
import gobblin.source.workunit.WorkUnit;

/**
 * Utilities to set event metadata into {@link WorkUnit}s
 */
public class EventWorkunitUtils {

  /**
   * Set SLA event metadata in the workunit. The publisher will use this metadta to publish sla events
   */
  public static void setTableSlaEventMetadata(WorkUnit state, Table table, long updateTime, long lowWatermark) {
    state.setProp(SlaEventKeys.DATASET_URN_KEY, state.getProp(ConfigurationKeys.DATASET_URN_KEY));
    state.setProp(SlaEventKeys.PARTITION_KEY, table.getCompleteName());
    state.setProp(SlaEventKeys.UPSTREAM_TS_IN_MILLI_SECS_KEY, String.valueOf(updateTime));

    // Time when the workunit was created
    state.setProp(SlaEventKeys.ORIGIN_TS_IN_MILLI_SECS_KEY, System.currentTimeMillis());
    state.setProp(SlaEventKeys.PREVIOUS_PUBLISH_TS_IN_MILLI_SECS_KEY, lowWatermark);
  }

  /**
   * Set SLA event metadata in the workunit. The publisher will use this metadta to publish sla events
   */
  public static void setPartitionSlaEventMetadata(WorkUnit state, Table table, Partition partition, long updateTime, long lowWatermark) {
    state.setProp(SlaEventKeys.DATASET_URN_KEY, state.getProp(ConfigurationKeys.DATASET_URN_KEY));
    state.setProp(SlaEventKeys.PARTITION_KEY, partition.getName());
    state.setProp(SlaEventKeys.UPSTREAM_TS_IN_MILLI_SECS_KEY, String.valueOf(updateTime));

    // Time when the workunit was created
    state.setProp(SlaEventKeys.ORIGIN_TS_IN_MILLI_SECS_KEY, System.currentTimeMillis());
    state.setProp(SlaEventKeys.PREVIOUS_PUBLISH_TS_IN_MILLI_SECS_KEY, lowWatermark);
  }
}
