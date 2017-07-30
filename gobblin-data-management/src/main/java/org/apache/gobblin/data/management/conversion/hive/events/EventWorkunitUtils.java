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
package gobblin.data.management.conversion.hive.events;

import java.util.List;

import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.metrics.event.sla.SlaEventKeys;
import gobblin.source.extractor.extract.LongWatermark;
import gobblin.source.workunit.WorkUnit;

/**
 * Utilities to set event metadata into {@link WorkUnit}s
 */
public class EventWorkunitUtils {

  public static final String IS_WATERMARK_WORKUNIT_KEY = "hive.source.watermark.isWatermarkWorkUnit";

  /**
   * Set SLA event metadata in the workunit. The publisher will use this metadta to publish sla events
   */
  public static void setTableSlaEventMetadata(WorkUnit state, Table table, long updateTime, long lowWatermark,
      long beginGetWorkunitsTime) {
    state.setProp(SlaEventKeys.DATASET_URN_KEY, state.getProp(ConfigurationKeys.DATASET_URN_KEY));
    state.setProp(SlaEventKeys.PARTITION_KEY, table.getCompleteName());
    state.setProp(SlaEventKeys.UPSTREAM_TS_IN_MILLI_SECS_KEY, String.valueOf(updateTime));

    // Time when the workunit was created
    state.setProp(SlaEventKeys.ORIGIN_TS_IN_MILLI_SECS_KEY, System.currentTimeMillis());
    state.setProp(EventConstants.WORK_UNIT_CREATE_TIME, state.getProp(SlaEventKeys.ORIGIN_TS_IN_MILLI_SECS_KEY));
    state.setProp(EventConstants.BEGIN_GET_WORKUNITS_TIME, beginGetWorkunitsTime);
    state.setProp(SlaEventKeys.PREVIOUS_PUBLISH_TS_IN_MILLI_SECS_KEY, lowWatermark);
  }

  /**
   * Set SLA event metadata in the workunit. The publisher will use this metadta to publish sla events
   */
  public static void setPartitionSlaEventMetadata(WorkUnit state, Table table, Partition partition, long updateTime,
      long lowWatermark, long beginGetWorkunitsTime) {
    state.setProp(SlaEventKeys.DATASET_URN_KEY, state.getProp(ConfigurationKeys.DATASET_URN_KEY));
    state.setProp(SlaEventKeys.PARTITION_KEY, partition.getName());
    state.setProp(SlaEventKeys.UPSTREAM_TS_IN_MILLI_SECS_KEY, String.valueOf(updateTime));

    // Time when the workunit was created
    state.setProp(SlaEventKeys.ORIGIN_TS_IN_MILLI_SECS_KEY, System.currentTimeMillis());
    state.setProp(EventConstants.WORK_UNIT_CREATE_TIME, state.getProp(SlaEventKeys.ORIGIN_TS_IN_MILLI_SECS_KEY));
    state.setProp(SlaEventKeys.PREVIOUS_PUBLISH_TS_IN_MILLI_SECS_KEY, lowWatermark);
    state.setProp(EventConstants.BEGIN_GET_WORKUNITS_TIME, beginGetWorkunitsTime);

    state.setProp(EventConstants.SOURCE_DATA_LOCATION, partition.getDataLocation());
  }

  /**
   * Set number of schema evolution DDLs as Sla event metadata
   */
  public static void setEvolutionMetadata(State state, List<String> evolutionDDLs) {
    state.setProp(EventConstants.SCHEMA_EVOLUTION_DDLS_NUM, evolutionDDLs == null ? 0 : evolutionDDLs.size());
  }

  public static void setBeginDDLBuildTimeMetadata(State state, long time) {
    state.setProp(EventConstants.BEGIN_DDL_BUILD_TIME, Long.toString(time));
  }

  public static void setEndDDLBuildTimeMetadata(State state, long time) {
    state.setProp(EventConstants.END_DDL_BUILD_TIME, Long.toString(time));
  }

  public static void setBeginConversionDDLExecuteTimeMetadata(State state, long time) {
    state.setProp(EventConstants.BEGIN_CONVERSION_DDL_EXECUTE_TIME, Long.toString(time));
  }

  public static void setEndConversionDDLExecuteTimeMetadata(State state, long time) {
    state.setProp(EventConstants.END_CONVERSION_DDL_EXECUTE_TIME, Long.toString(time));
  }

  public static void setBeginPublishDDLExecuteTimeMetadata(State state, long time) {
    state.setProp(EventConstants.BEGIN_PUBLISH_DDL_EXECUTE_TIME, Long.toString(time));
  }

  public static void setEndPublishDDLExecuteTimeMetadata(State state, long time) {
    state.setProp(EventConstants.END_PUBLISH_DDL_EXECUTE_TIME, Long.toString(time));
  }

  /**
   * Sets metadata to indicate whether this is the first time this table or partition is being published.
   * @param wus to set if this is first publish for this table or partition
   */
  public static void setIsFirstPublishMetadata(WorkUnitState wus) {
    if (!Boolean.valueOf(wus.getPropAsBoolean(IS_WATERMARK_WORKUNIT_KEY))) {
      LongWatermark previousWatermark = wus.getWorkunit().getLowWatermark(LongWatermark.class);
      wus.setProp(SlaEventKeys.IS_FIRST_PUBLISH, (null == previousWatermark || previousWatermark.getValue() == 0));
    }
  }
}
