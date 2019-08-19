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
package org.apache.gobblin.eventhub.source;

import avro.shaded.com.google.common.collect.Maps;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.microsoft.azure.eventhubs.EventData;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.extract.AbstractSource;
import org.apache.gobblin.source.extractor.extract.kafka.MultiLongWatermark;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;


/**
 * A {@link org.apache.gobblin.source.Source} implementation for MS EventHub source.
 */
public class EventhubSource extends AbstractSource<Void, EventData> {

    public static final Logger LOG = LoggerFactory.getLogger(EventhubSource.class);

    public static final String WORK_UNIT_STATE_VERSION_KEY = "source.eventhub.workUnitState.version";
    public static final String CURRENT_PARTITION_KEY = "source.eventhub.current.partition";
    public static final Integer CURRENT_WORK_UNIT_STATE_VERSION = 1;
    public static final String PARTITIONS_TO_PULL = "source.eventhub.partitions";

    @Override
    public List<WorkUnit> getWorkunits(SourceState state) {
        List<WorkUnit> workUnits = new ArrayList<>();

        workUnits.addAll(generateWorkUnits(state));

        LOG.info("Total number of workunits for the current run: " + workUnits.size());
        List<WorkUnit> previousWorkUnits = this.getPreviousWorkUnitsForRetry(state);
        LOG.info("Total number of incomplete tasks from the previous run: " + previousWorkUnits.size());
        workUnits.addAll(previousWorkUnits);

        int numOfMultiWorkunits =
                state.getPropAsInt(ConfigurationKeys.MR_JOB_MAX_MAPPERS_KEY, ConfigurationKeys.DEFAULT_MR_JOB_MAX_MAPPERS);

        return pack(workUnits, numOfMultiWorkunits);
    }

    private static List<WorkUnit> pack(List<WorkUnit> workUnits, int numOfMultiWorkunits) {
        Preconditions.checkArgument(numOfMultiWorkunits > 0);

        if (workUnits.size() <= numOfMultiWorkunits) {
            return workUnits;
        }
        List<WorkUnit> result = Lists.newArrayListWithCapacity(numOfMultiWorkunits);
        for (int i = 0; i < numOfMultiWorkunits; i++) {
            result.add(MultiWorkUnit.createEmpty());
        }
        for (int i = 0; i < workUnits.size(); i++) {
            ((MultiWorkUnit) result.get(i % numOfMultiWorkunits)).addWorkUnit(workUnits.get(i));
        }
        return result;
    }

    protected List<WorkUnit> generateWorkUnits(SourceState state) {
        state.setProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY, "offset");
        state.setProp(ConfigurationKeys.SOURCE_QUERYBASED_WATERMARK_TYPE, "simple");
        state.setProp(ConfigurationKeys.SOURCE_QUERYBASED_EXTRACT_TYPE, "snapshot");

        List<String> partitionsToPull = state.getPropAsList(PARTITIONS_TO_PULL);
        LOG.info(String.format("Will pull %d partitions: %s", partitionsToPull.size(), partitionsToPull));

        Map<String,Long> previousWatermark = getPreviousWatermark(state, partitionsToPull);

        List<WorkUnit> workUnits = new ArrayList<>();

        String tableName = state.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY);
        String nameSpaceName = state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY);
        Extract.TableType tableType =
                Extract.TableType.valueOf(state.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY).toUpperCase());

        LOG.info("Create extract output with table name is " + tableName);
        Extract extract = createExtract(tableType, nameSpaceName, tableName);

        for (String partition : partitionsToPull) {
            WorkUnit workunit = WorkUnit.create(extract);
            workunit.setProp(WORK_UNIT_STATE_VERSION_KEY, 1);
            workunit.setProp(CURRENT_PARTITION_KEY, partition);
            workunit.setLowWaterMark(previousWatermark.get(partition));
            workUnits.add(workunit);
        }

        return workUnits;
    }

    private static final Gson gson = new Gson();

    static Map<String,Long> getPreviousWatermark(SourceState state, List<String> partitionsToPull) {
        WorkUnitState.WorkingState previousWorkingState;
        Map<String,Long> previousWatermark = Maps.newHashMapWithExpectedSize(partitionsToPull.size());
        MultiLongWatermark watermark;
        String currentPartition;
        for (WorkUnitState previousWus : state.getPreviousWorkUnitStates()) {
            previousWorkingState = previousWus.getWorkingState();
            if (previousWorkingState == WorkUnitState.WorkingState.SUCCESSFUL || previousWorkingState == WorkUnitState.WorkingState.COMMITTED) {
                watermark = previousWus.getActualHighWatermark(MultiLongWatermark.class);
            } else {
                watermark = previousWus.getWorkunit().getLowWatermark(MultiLongWatermark.class);
            }
            if (watermark != null) {
                Preconditions.checkArgument(
                        partitionsToPull.size() == watermark.size(),
                        String.format("Num of partitions doesn't match number of watermarks: partitions=%s, watermarks=%s",
                                partitionsToPull, watermark));
                for (int i = 0; i < watermark.size(); i++) {
                    currentPartition = partitionsToPull.get(i);
                    previousWatermark.put(currentPartition,
                            Math.max(
                                    watermark.get(i),
                                    previousWatermark.getOrDefault(currentPartition, ConfigurationKeys.DEFAULT_WATERMARK_VALUE)));
                }
            }
        }
        LOG.info(String.format("Low watermarks for %s partitions retrieved from previous states: %s", partitionsToPull.size(), previousWatermark.toString()));
        return previousWatermark;
    }

    @Override
    public EventhubExtractor getExtractor(WorkUnitState workUnitState) throws IOException {
        return new EventhubExtractor(workUnitState);
    }

    @Override
    public void shutdown(SourceState sourceState) {

    }

}

