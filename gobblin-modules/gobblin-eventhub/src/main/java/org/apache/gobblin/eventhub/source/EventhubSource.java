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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.microsoft.azure.eventhubs.EventData;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.extract.AbstractSource;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.source.extractor.partition.Partition;
import org.apache.gobblin.source.extractor.partition.Partitioner;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;


/**
 * Created by Jan on 23-Jan-19.
 */
public class EventhubSource extends AbstractSource<Void, EventData> {

    public static final Logger LOG = LoggerFactory.getLogger(EventhubSource.class);

    public static final String WORK_UNIT_STATE_VERSION_KEY = "source.querybased.workUnitState.version";
    public static final Integer CURRENT_WORK_UNIT_STATE_VERSION = 1;

    @Override
    public List<WorkUnit> getWorkunits(SourceState state) {
        List<WorkUnit> workUnits = new ArrayList<>();

        Long previousWatermark = getPreviousWatermark(state);
        if (previousWatermark == null) {
            previousWatermark = ConfigurationKeys.DEFAULT_WATERMARK_VALUE;
        }

        // for now, enforce single work unit to simplify watermark handling:
        if (state.getPropAsInt(ConfigurationKeys.SOURCE_MAX_NUMBER_OF_PARTITIONS, 1) != 1) {
            LOG.error("Unsupported number of partitions, resetting to 1.");
        }
        state.setProp(ConfigurationKeys.SOURCE_MAX_NUMBER_OF_PARTITIONS, 1);
        workUnits.addAll(generateWorkUnits(state, previousWatermark));

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

    protected List<WorkUnit> generateWorkUnits(SourceState state, long previousWatermark) {
        List<WorkUnit> workUnits = new ArrayList<>();

        state.setProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY, "offset");
        state.setProp(ConfigurationKeys.SOURCE_QUERYBASED_WATERMARK_TYPE, "simple");
        state.setProp(ConfigurationKeys.SOURCE_QUERYBASED_EXTRACT_TYPE, "snapshot");
        List<Partition> partitions = new Partitioner(state).getPartitionList(previousWatermark);
        Collections.sort(partitions, Partitioner.ascendingComparator);

        String tableName = state.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY);
        String nameSpaceName = state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY);
        Extract.TableType tableType =
                Extract.TableType.valueOf(state.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY).toUpperCase());

        LOG.info("Create extract output with table name is " + tableName);
        Extract extract = createExtract(tableType, nameSpaceName, tableName);

        for (Partition partition : partitions) {
            WorkUnit workunit = WorkUnit.create(extract);
            workunit.setProp(WORK_UNIT_STATE_VERSION_KEY, 1);
            partition.serialize(workunit);
            workUnits.add(workunit);
        }

        return workUnits;
    }

    private static final Gson gson = new Gson();

    static Long getPreviousWatermark(SourceState state) {
        long res = ConfigurationKeys.DEFAULT_WATERMARK_VALUE;
        WorkUnitState.WorkingState previousWorkingState;
        LongWatermark watermark;
        for (WorkUnitState previousWus : state.getPreviousWorkUnitStates()) {
            previousWorkingState = previousWus.getWorkingState();
            if (previousWorkingState == WorkUnitState.WorkingState.SUCCESSFUL || previousWorkingState == WorkUnitState.WorkingState.COMMITTED) {
                watermark = previousWus.getActualHighWatermark(LongWatermark.class);
            } else {
                watermark = previousWus.getWorkunit().getLowWatermark(LongWatermark.class);
            }
            if (watermark != null) {
                res = Math.max(res, watermark.getValue());
            }
        }
        LOG.info(String.format("Low watermark retrieved from previous states: %d", res));
        return res;
    }

    @Override
    public EventhubExtractor getExtractor(WorkUnitState workUnitState) throws IOException {
        return new EventhubExtractor(workUnitState);
    }

    @Override
    public void shutdown(SourceState sourceState) {

    }

}

