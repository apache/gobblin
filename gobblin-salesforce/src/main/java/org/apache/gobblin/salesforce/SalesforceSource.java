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

package org.apache.gobblin.salesforce;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.math.DoubleMath;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.dataset.DatasetConstants;
import org.apache.gobblin.dataset.DatasetDescriptor;
import org.apache.gobblin.metrics.event.lineage.LineageInfo;
import org.apache.gobblin.salesforce.SalesforceExtractor.BatchIdAndResultId;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.exception.ExtractPrepareException;
import org.apache.gobblin.source.extractor.exception.RestApiConnectionException;
import org.apache.gobblin.source.extractor.exception.RestApiProcessingException;
import org.apache.gobblin.source.extractor.extract.Command;
import org.apache.gobblin.source.extractor.extract.CommandOutput;
import org.apache.gobblin.source.extractor.extract.QueryBasedSource;
import org.apache.gobblin.source.extractor.extract.restapi.RestApiConnector;
import org.apache.gobblin.source.extractor.partition.Partition;
import org.apache.gobblin.source.extractor.partition.Partitioner;
import org.apache.gobblin.source.extractor.utils.Utils;
import org.apache.gobblin.source.extractor.watermark.Predicate;
import org.apache.gobblin.source.extractor.watermark.WatermarkType;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;

import static org.apache.gobblin.configuration.ConfigurationKeys.*;
import static org.apache.gobblin.salesforce.SalesforceConfigurationKeys.*;

/**
 * An implementation of {@link QueryBasedSource} for salesforce data sources.
 */
@Slf4j
public class SalesforceSource extends QueryBasedSource<JsonArray, JsonElement> {
  public static final String USE_ALL_OBJECTS = "use.all.objects";
  public static final boolean DEFAULT_USE_ALL_OBJECTS = false;

  @VisibleForTesting
  static final String ENABLE_DYNAMIC_PROBING = "salesforce.enableDynamicProbing";
  static final String MIN_TARGET_PARTITION_SIZE = "salesforce.minTargetPartitionSize";
  static final int DEFAULT_MIN_TARGET_PARTITION_SIZE = 250000;

  @VisibleForTesting
  static final String ENABLE_DYNAMIC_PARTITIONING = "salesforce.enableDynamicPartitioning";
  @VisibleForTesting
  static final String EARLY_STOP_TOTAL_RECORDS_LIMIT = "salesforce.earlyStopTotalRecordsLimit";
  private static final long DEFAULT_EARLY_STOP_TOTAL_RECORDS_LIMIT = DEFAULT_MIN_TARGET_PARTITION_SIZE * 4;

  static final String SECONDS_FORMAT = "yyyy-MM-dd-HH:mm:ss";

  private boolean isEarlyStopped = false;
  protected SalesforceConnector salesforceConnector = null;

  private SalesforceHistogramService salesforceHistogramService;

  public SalesforceSource() {
    this.lineageInfo = Optional.absent();
  }

  @VisibleForTesting
  SalesforceSource(LineageInfo lineageInfo) {
    this.lineageInfo = Optional.fromNullable(lineageInfo);
  }

  @VisibleForTesting
  SalesforceSource(SalesforceHistogramService salesforceHistogramService) {
    this.lineageInfo = Optional.absent();
    this.salesforceHistogramService = salesforceHistogramService;
  }

  @Override
  public Extractor<JsonArray, JsonElement> getExtractor(WorkUnitState state) throws IOException {
    try {
      return new SalesforceExtractor(state).build();
    } catch (ExtractPrepareException e) {
      log.error("Failed to prepare extractor", e);
      throw new IOException(e);
    }
  }

  @Override
  public boolean isEarlyStopped() {
    return isEarlyStopped;
  }

  @Override
  protected void addLineageSourceInfo(SourceState sourceState, SourceEntity entity, WorkUnit workUnit) {
    DatasetDescriptor source =
        new DatasetDescriptor(DatasetConstants.PLATFORM_SALESFORCE, entity.getSourceEntityName());
    if (lineageInfo.isPresent()) {
      lineageInfo.get().setSource(source, workUnit);
    }
  }
  @Override
  protected List<WorkUnit> generateWorkUnits(SourceEntity sourceEntity, SourceState state, long previousWatermark) {
    SalesforceConnector connector = getConnector(state);

    SfConfig sfConfig = new SfConfig(state.getProperties());
    if (salesforceHistogramService == null) {
      salesforceHistogramService = new SalesforceHistogramService(sfConfig, connector);
    }

    List<WorkUnit> workUnits;
    String partitionType = state.getProp(SALESFORCE_PARTITION_TYPE, "");
    if (partitionType.equals("PK_CHUNKING")) {
      // pk-chunking only supports start-time by source.querybased.start.value, and does not support end-time.
      // always ingest data later than or equal source.querybased.start.value.
      // we should only pk chunking based work units only in case of snapshot/full ingestion
      workUnits = generateWorkUnitsPkChunking(sourceEntity, state, previousWatermark);
    } else {
      workUnits = generateWorkUnitsHelper(sourceEntity, state, previousWatermark);
    }
    log.info("====Generated {} workUnit(s)====", workUnits.size());
    if (sfConfig.partitionOnly) {
      log.info("It is partitionOnly mode, return blank workUnit list");
      return new ArrayList<>();
    } else {
      return workUnits;
    }
  }

  /**
   * generate workUnit for pk chunking
   */
  private List<WorkUnit> generateWorkUnitsPkChunking(SourceEntity sourceEntity, SourceState state, long previousWatermark) {
    SalesforceExtractor.ResultFileIdsStruct resultFileIdsStruct = executeQueryWithPkChunking(state, previousWatermark);
    return createWorkUnits(sourceEntity, state, resultFileIdsStruct);
  }

  private SalesforceExtractor.ResultFileIdsStruct executeQueryWithPkChunking(
      SourceState sourceState,
      long previousWatermark
  ) throws RuntimeException {
    State state = new State(sourceState);
    WorkUnit workUnit = WorkUnit.createEmpty();
    WorkUnitState workUnitState = new WorkUnitState(workUnit, state);
    workUnitState.setId("Execute pk-chunking");
    SalesforceExtractor salesforceExtractor = null;
    try {
      salesforceExtractor = (SalesforceExtractor) this.getExtractor(workUnitState);
      Partitioner partitioner = new Partitioner(sourceState);
      if (isEarlyStopEnabled(state) && partitioner.isFullDump()) {
        throw new UnsupportedOperationException("Early stop mode cannot work with full dump mode.");
      }
      Partition partition = partitioner.getGlobalPartition(previousWatermark);
      String condition = "";
      Date startDate = Utils.toDate(partition.getLowWatermark(), Partitioner.WATERMARKTIMEFORMAT);
      String field = sourceState.getProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY);
      // pk-chunking only supports start-time by source.querybased.start.value, and does not support end-time.
      // always ingest data later than or equal source.querybased.start.value.
      // we should only pk chunking based work units only in case of snapshot/full ingestion
      if (startDate != null && field != null) {
        String lowWatermarkDate = Utils.dateToString(startDate, SalesforceExtractor.SALESFORCE_TIMESTAMP_FORMAT);
        condition = field + " >= " + lowWatermarkDate;
      }
      Predicate predicate = new Predicate(null, 0, condition, "", null);
      List<Predicate> predicateList = Arrays.asList(predicate);
      String entity = sourceState.getProp(ConfigurationKeys.SOURCE_ENTITY);

      if (state.contains(BULK_TEST_JOB_ID)) {
        String jobId = state.getProp(BULK_TEST_JOB_ID, "");
        log.info("---Skip query, fetching result files directly for [jobId={}]", jobId);
        String batchIdListStr = state.getProp(BULK_TEST_BATCH_ID_LIST);
        return salesforceExtractor.getQueryResultIdsPkChunkingFetchOnly(jobId, batchIdListStr);
      } else {
        log.info("---Pk Chunking query submit.");
        return salesforceExtractor.getQueryResultIdsPkChunking(entity, predicateList);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if(salesforceExtractor != null) {
        try {
          // Only close connection here since we don't want to update the high watermark for the workUnitState here
          salesforceExtractor.closeConnection();
        } catch (Exception e) {
          log.error("Failed to close the extractor connections", e);
        }
      }
    }
  }

  /**
   *  Create work units by taking a bulkJobId.
   *  The work units won't contain a query in this case. Instead they will contain a BulkJobId and a list of `batchId:resultId`
   *  So in extractor, the work to do is just to fetch the resultSet files.
   */
  private List<WorkUnit> createWorkUnits(
      SourceEntity sourceEntity,
      SourceState state,
      SalesforceExtractor.ResultFileIdsStruct resultFileIdsStruct
  ) {
    String nameSpaceName = state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY);
    Extract.TableType tableType = Extract.TableType.valueOf(state.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY).toUpperCase());
    String outputTableName = sourceEntity.getDestTableName();
    Extract extract = createExtract(tableType, nameSpaceName, outputTableName);

    List<WorkUnit> workUnits = Lists.newArrayList();
    int partitionNumber = state.getPropAsInt(SOURCE_MAX_NUMBER_OF_PARTITIONS, 1);
    List<BatchIdAndResultId> batchResultIds = resultFileIdsStruct.getBatchIdAndResultIdList();
    int total = batchResultIds.size();

    // size of every partition should be: math.ceil(total/partitionNumber), use simpler way: (total+partitionNumber-1)/partitionNumber
    int sizeOfPartition = (total + partitionNumber - 1) / partitionNumber;
    List<List<BatchIdAndResultId>> partitionedResultIds = Lists.partition(batchResultIds, sizeOfPartition);
    log.info("----partition strategy: max-parti={}, size={}, actual-parti={}, total={}", partitionNumber, sizeOfPartition, partitionedResultIds.size(), total);

    for (List<BatchIdAndResultId> resultIds : partitionedResultIds) {
      WorkUnit workunit = new WorkUnit(extract);
      String bulkJobId = resultFileIdsStruct.getJobId();
      workunit.setProp(PK_CHUNKING_JOB_ID, bulkJobId);
      String resultIdStr = resultIds.stream().map(x -> x.getBatchId() + ":" + x.getResultId()).collect(Collectors.joining(","));
      workunit.setProp(PK_CHUNKING_BATCH_RESULT_ID_PAIRS, resultIdStr);
      workunit.setProp(ConfigurationKeys.SOURCE_ENTITY, sourceEntity.getSourceEntityName());
      workunit.setProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, sourceEntity.getDestTableName());
      workunit.setProp(WORK_UNIT_STATE_VERSION_KEY, CURRENT_WORK_UNIT_STATE_VERSION);
      addLineageSourceInfo(state, sourceEntity, workunit);
      workUnits.add(workunit);
    }
    return workUnits;
  }

  /**
   * Generates {@link WorkUnit}s based on a bunch of config values like max number of partitions, early stop,
   * dynamic partitioning, dynamic probing, etc.
   */
  @VisibleForTesting
  List<WorkUnit> generateWorkUnitsHelper(SourceEntity sourceEntity, SourceState state, long previousWatermark) {
    boolean isSoftDeletePullDisabled = state.getPropAsBoolean(SOURCE_QUERYBASED_SALESFORCE_IS_SOFT_DELETES_PULL_DISABLED, false);
    log.info("disable soft delete pull: " + isSoftDeletePullDisabled);
    WatermarkType watermarkType = WatermarkType.valueOf(
        state.getProp(ConfigurationKeys.SOURCE_QUERYBASED_WATERMARK_TYPE, ConfigurationKeys.DEFAULT_WATERMARK_TYPE)
            .toUpperCase());
    String watermarkColumn = state.getProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY);

    int maxPartitions = state.getPropAsInt(SOURCE_MAX_NUMBER_OF_PARTITIONS,
        ConfigurationKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS);
    int minTargetPartitionSize = state.getPropAsInt(MIN_TARGET_PARTITION_SIZE, DEFAULT_MIN_TARGET_PARTITION_SIZE);

    // Only support time related watermark
    if (watermarkType == WatermarkType.SIMPLE
        || Strings.isNullOrEmpty(watermarkColumn)
        || !state.getPropAsBoolean(ENABLE_DYNAMIC_PARTITIONING)) {
      List<WorkUnit> workUnits = super.generateWorkUnits(sourceEntity, state, previousWatermark);
      workUnits.forEach(workUnit ->
          workUnit.setProp(SOURCE_QUERYBASED_SALESFORCE_IS_SOFT_DELETES_PULL_DISABLED, isSoftDeletePullDisabled));
      return workUnits;
    }

    Partitioner partitioner = new Partitioner(state);
    if (isEarlyStopEnabled(state) && partitioner.isFullDump()) {
      throw new UnsupportedOperationException("Early stop mode cannot work with full dump mode.");
    }

    Partition partition = partitioner.getGlobalPartition(previousWatermark);
    Histogram histogram =
        salesforceHistogramService.getHistogram(sourceEntity.getSourceEntityName(), watermarkColumn, state, partition);

    // we should look if the count is too big, cut off early if count exceeds the limit, or bucket size is too large

    Histogram histogramAdjust;

    // TODO: we should consider move this logic into getRefinedHistogram so that we can early terminate the search
    if (isEarlyStopEnabled(state)) {
      histogramAdjust = new Histogram();
      for (HistogramGroup group : histogram.getGroups()) {
        histogramAdjust.add(group);
        long earlyStopRecordLimit = state.getPropAsLong(EARLY_STOP_TOTAL_RECORDS_LIMIT, DEFAULT_EARLY_STOP_TOTAL_RECORDS_LIMIT);
        if (histogramAdjust.getTotalRecordCount() > earlyStopRecordLimit) {
          break;
        }
      }
    } else {
      histogramAdjust = histogram;
    }

    long expectedHighWatermark = partition.getHighWatermark();
    if (histogramAdjust.getGroups().size() < histogram.getGroups().size()) {
      HistogramGroup lastPlusOne = histogram.get(histogramAdjust.getGroups().size());
      long earlyStopHighWatermark = Long.parseLong(Utils.toDateTimeFormat(lastPlusOne.getKey(), SECONDS_FORMAT, Partitioner.WATERMARKTIMEFORMAT));
      log.info("Job {} will be stopped earlier. [LW : {}, early-stop HW : {}, expected HW : {}]",
          state.getProp(ConfigurationKeys.JOB_NAME_KEY), partition.getLowWatermark(), earlyStopHighWatermark, expectedHighWatermark);
      this.isEarlyStopped = true;
      expectedHighWatermark = earlyStopHighWatermark;
    } else {
      log.info("Job {} will be finished in a single run. [LW : {}, expected HW : {}]",
          state.getProp(ConfigurationKeys.JOB_NAME_KEY), partition.getLowWatermark(), expectedHighWatermark);
    }

    String specifiedPartitions = generateSpecifiedPartitions(histogramAdjust, minTargetPartitionSize, maxPartitions,
        partition.getLowWatermark(), expectedHighWatermark);
    state.setProp(Partitioner.HAS_USER_SPECIFIED_PARTITIONS, true);
    state.setProp(Partitioner.USER_SPECIFIED_PARTITIONS, specifiedPartitions);
    state.setProp(Partitioner.IS_EARLY_STOPPED, isEarlyStopped);

    List<WorkUnit> workUnits = super.generateWorkUnits(sourceEntity, state, previousWatermark);
    workUnits.forEach(workUnit ->
        workUnit.setProp(SOURCE_QUERYBASED_SALESFORCE_IS_SOFT_DELETES_PULL_DISABLED, isSoftDeletePullDisabled));
    return workUnits;
  }

  private boolean isEarlyStopEnabled(State state) {
    return state.getPropAsBoolean(ConfigurationKeys.SOURCE_EARLY_STOP_ENABLED, ConfigurationKeys.DEFAULT_SOURCE_EARLY_STOP_ENABLED);
  }

  @VisibleForTesting
  String generateSpecifiedPartitions(Histogram histogram, int minTargetPartitionSize, int maxPartitions, long lowWatermark,
      long expectedHighWatermark) {
    int interval = computeTargetPartitionSize(histogram, minTargetPartitionSize, maxPartitions);
    int totalGroups = histogram.getGroups().size();

    log.info("Histogram total record count: " + histogram.getTotalRecordCount());
    log.info("Histogram total groups: " + totalGroups);
    log.info("maxPartitions: " + maxPartitions);
    log.info("interval: " + interval);

    List<HistogramGroup> groups = histogram.getGroups();
    List<String> partitionPoints = new ArrayList<>();
    DescriptiveStatistics statistics = new DescriptiveStatistics();

    int count = 0;
    HistogramGroup group;
    Iterator<HistogramGroup> it = groups.iterator();

    while (it.hasNext()) {
      group = it.next();
      if (count == 0) {
        // Add a new partition point;
        partitionPoints.add(Utils.toDateTimeFormat(group.getKey(), SECONDS_FORMAT, Partitioner.WATERMARKTIMEFORMAT));
      }

      /**
       * Using greedy algorithm by keep adding group until it exceeds the interval size (x2)
       * Proof: Assuming nth group violates 2 x interval size, then all groups from 0th to (n-1)th, plus nth group,
       * will have total size larger or equal to interval x 2. Hence, we are saturating all intervals (with original size)
       * without leaving any unused space in between. We could choose x3,x4... but it is not space efficient.
       */
      if (count != 0 && count + group.getCount() >= 2 * interval) {
        // Summarize current group
        statistics.addValue(count);
        // A step-in start
        partitionPoints.add(Utils.toDateTimeFormat(group.getKey(), SECONDS_FORMAT, Partitioner.WATERMARKTIMEFORMAT));
        count = group.getCount();
      } else {
        // Add group into current partition
        count += group.getCount();
      }

      if (count >= interval) {
        // Summarize current group
        statistics.addValue(count);
        // A fresh start next time
        count = 0;
      }
    }

    if (partitionPoints.isEmpty()) {
      throw new RuntimeException("Unexpected empty partition list");
    }

    if (count > 0) {
      // Summarize last group
      statistics.addValue(count);
    }

    // Add global high watermark as last point
    partitionPoints.add(Long.toString(expectedHighWatermark));

    log.info("Dynamic partitioning statistics: ");
    log.info("data: " + Arrays.toString(statistics.getValues()));
    log.info(statistics.toString());
    String specifiedPartitions = Joiner.on(",").join(partitionPoints);
    log.info("Calculated specified partitions: " + specifiedPartitions);
    return specifiedPartitions;
  }

  /**
   * Compute the target partition size.
   */
  private int computeTargetPartitionSize(Histogram histogram, int minTargetPartitionSize, int maxPartitions) {
    return Math.max(minTargetPartitionSize,
        DoubleMath.roundToInt((double) histogram.getTotalRecordCount() / maxPartitions, RoundingMode.CEILING));
  }

  protected Set<SourceEntity> getSourceEntities(State state) {
    if (!state.getPropAsBoolean(USE_ALL_OBJECTS, DEFAULT_USE_ALL_OBJECTS)) {
      return super.getSourceEntities(state);
    }

    SalesforceConnector connector = getConnector(state);
    try {
      if (!connector.connect()) {
        throw new RuntimeException("Failed to connect.");
      }
    } catch (RestApiConnectionException e) {
      throw new RuntimeException("Failed to connect.", e);
    }

    List<Command> commands = RestApiConnector.constructGetCommand(connector.getFullUri("/sobjects"));
    try {
      CommandOutput<?, ?> response = connector.getResponse(commands);
      Iterator<String> itr = (Iterator<String>) response.getResults().values().iterator();
      if (itr.hasNext()) {
        String next = itr.next();
        return getSourceEntities(next);
      }
      throw new RuntimeException("Unable to retrieve source entities");
    } catch (RestApiProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

  private static Set<SourceEntity> getSourceEntities(String response) {
    Set<SourceEntity> result = Sets.newHashSet();
    JsonObject jsonObject = new Gson().fromJson(response, JsonObject.class).getAsJsonObject();
    JsonArray array = jsonObject.getAsJsonArray("sobjects");
    for (JsonElement element : array) {
      String sourceEntityName = element.getAsJsonObject().get("name").getAsString();
      result.add(SourceEntity.fromSourceEntityName(sourceEntityName));
    }
    return result;
  }

  protected SalesforceConnector getConnector(State state) {
    if (this.salesforceConnector == null) {
      this.salesforceConnector = new SalesforceConnector(state);
    }
    return this.salesforceConnector;
  }
}
