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

import com.google.common.collect.Lists;
import java.io.IOException;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.stream.Collectors;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.math.DoubleMath;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.dataset.DatasetConstants;
import org.apache.gobblin.dataset.DatasetDescriptor;
import org.apache.gobblin.metrics.event.lineage.LineageInfo;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.exception.ExtractPrepareException;
import org.apache.gobblin.source.extractor.exception.RestApiClientException;
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

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static org.apache.gobblin.configuration.ConfigurationKeys.*;
import static org.apache.gobblin.salesforce.SalesforceConfigurationKeys.*;
import org.apache.gobblin.salesforce.SalesforceExtractor.JobIdAndBatchIdResultIdList;
import org.apache.gobblin.salesforce.SalesforceExtractor.BatchIdAndResultId;

/**
 * An implementation of {@link QueryBasedSource} for salesforce data sources.
 */
@Slf4j
public class SalesforceSource extends QueryBasedSource<JsonArray, JsonElement> {
  public static final String USE_ALL_OBJECTS = "use.all.objects";
  public static final boolean DEFAULT_USE_ALL_OBJECTS = false;

  private static final String ENABLE_DYNAMIC_PROBING = "salesforce.enableDynamicProbing";
  private static final String DYNAMIC_PROBING_LIMIT = "salesforce.dynamicProbingLimit";
  private static final int DEFAULT_DYNAMIC_PROBING_LIMIT = 1000;
  private static final String MIN_TARGET_PARTITION_SIZE = "salesforce.minTargetPartitionSize";
  private static final int DEFAULT_MIN_TARGET_PARTITION_SIZE = 250000;
  // this is used to generate histogram buckets smaller than the target partition size to allow for more even
  // packing of the generated partitions
  private static final String PROBE_TARGET_RATIO = "salesforce.probeTargetRatio";
  private static final double DEFAULT_PROBE_TARGET_RATIO = 0.60;
  private static final int MIN_SPLIT_TIME_MILLIS = 1000;

  private static final String DAY_PARTITION_QUERY_TEMPLATE =
      "SELECT count(${column}) cnt, DAY_ONLY(${column}) time FROM ${table} " + "WHERE ${column} ${greater} ${start}"
          + " AND ${column} ${less} ${end} GROUP BY DAY_ONLY(${column}) ORDER BY DAY_ONLY(${column})";
  private static final String PROBE_PARTITION_QUERY_TEMPLATE = "SELECT count(${column}) cnt FROM ${table} "
      + "WHERE ${column} ${greater} ${start} AND ${column} ${less} ${end}";

  private static final String ENABLE_DYNAMIC_PARTITIONING = "salesforce.enableDynamicPartitioning";
  private static final String EARLY_STOP_TOTAL_RECORDS_LIMIT = "salesforce.earlyStopTotalRecordsLimit";
  private static final long DEFAULT_EARLY_STOP_TOTAL_RECORDS_LIMIT = DEFAULT_MIN_TARGET_PARTITION_SIZE * 4;

  private static final String SECONDS_FORMAT = "yyyy-MM-dd-HH:mm:ss";
  private static final String ZERO_TIME_SUFFIX = "-00:00:00";

  private static final Gson GSON = new Gson();
  private boolean isEarlyStopped = false;
  protected SalesforceConnector salesforceConnector = null;

  public SalesforceSource() {
    this.lineageInfo = Optional.absent();
  }

  @VisibleForTesting
  SalesforceSource(LineageInfo lineageInfo) {
    this.lineageInfo = Optional.fromNullable(lineageInfo);
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
    List<WorkUnit> workUnits = null;
    String partitionType = state.getProp(SALESFORCE_PARTITION_TYPE, "");
    if (partitionType.equals("PK_CHUNKING")) {
      // pk-chunking only supports start-time by source.querybased.start.value, and does not support end-time.
      // always ingest data later than or equal source.querybased.start.value.
      // we should only pk chunking based work units only in case of snapshot/full ingestion
      workUnits = generateWorkUnitsPkChunking(sourceEntity, state, previousWatermark);
    } else {
      workUnits = generateWorkUnitsStrategy(sourceEntity, state, previousWatermark);
    }
    log.info("====Generated {} workUnit(s)====", workUnits.size());
    return workUnits;
  }

  /**
   * generate workUnit for pk chunking
   */
  private List<WorkUnit> generateWorkUnitsPkChunking(SourceEntity sourceEntity, SourceState state, long previousWatermark) {
    JobIdAndBatchIdResultIdList jobIdAndBatchIdResultIdList = executeQueryWithPkChunking(state, previousWatermark);
    return createWorkUnits(sourceEntity, state, jobIdAndBatchIdResultIdList);
  }

  private JobIdAndBatchIdResultIdList executeQueryWithPkChunking(
      SourceState sourceState,
      long previousWatermark
  ) throws RuntimeException {
    State state = new State(sourceState);
    WorkUnit workUnit = WorkUnit.createEmpty();
    WorkUnitState workUnitState = new WorkUnitState(workUnit, state);
    workUnitState.setId("Execute pk-chunking");
    try {
      SalesforceExtractor salesforceExtractor = (SalesforceExtractor) this.getExtractor(workUnitState);
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

      if (state.contains(PK_CHUNKING_TEST_JOB_ID)) {
        String jobId = state.getProp(PK_CHUNKING_TEST_JOB_ID, "");
        log.info("---Skip query, fetching result files directly for [jobId={}]", jobId);
        String batchIdListStr = state.getProp(PK_CHUNKING_TEST_BATCH_ID_LIST);
        return salesforceExtractor.getQueryResultIdsPkChunkingFetchOnly(jobId, batchIdListStr);
      } else {
        log.info("---Pk Chunking query submit.");
        return salesforceExtractor.getQueryResultIdsPkChunking(entity, predicateList);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
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
      JobIdAndBatchIdResultIdList jobIdAndBatchIdResultIdList
  ) {
    String nameSpaceName = state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY);
    Extract.TableType tableType = Extract.TableType.valueOf(state.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY).toUpperCase());
    String outputTableName = sourceEntity.getDestTableName();
    Extract extract = createExtract(tableType, nameSpaceName, outputTableName);

    List<WorkUnit> workUnits = Lists.newArrayList();
    int partitionNumber = state.getPropAsInt(SOURCE_MAX_NUMBER_OF_PARTITIONS, 1);
    List<BatchIdAndResultId> batchResultIds = jobIdAndBatchIdResultIdList.getBatchIdAndResultIdList();
    int total = batchResultIds.size();

    // size of every partition should be: math.ceil(total/partitionNumber), use simpler way: (total+partitionNumber-1)/partitionNumber
    int sizeOfPartition = (total + partitionNumber - 1) / partitionNumber;
    List<List<BatchIdAndResultId>> partitionedResultIds = Lists.partition(batchResultIds, sizeOfPartition);
    log.info("----partition strategy: max-parti={}, size={}, actual-parti={}, total={}", partitionNumber, sizeOfPartition, partitionedResultIds.size(), total);

    for (List<BatchIdAndResultId> resultIds : partitionedResultIds) {
      WorkUnit workunit = new WorkUnit(extract);
      String bulkJobId = jobIdAndBatchIdResultIdList.getJobId();
      workunit.setProp(PK_CHUNKING_JOB_ID, bulkJobId);
      String resultIdStr = resultIds.stream().map(x -> x.getBatchId() + ":" + x.getResultId()).collect(Collectors.joining(","));
      workunit.setProp(PK_CHUNKING_BATCH_RESULT_IDS, resultIdStr);
      workunit.setProp(ConfigurationKeys.SOURCE_ENTITY, sourceEntity.getSourceEntityName());
      workunit.setProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, sourceEntity.getDestTableName());
      workunit.setProp(WORK_UNIT_STATE_VERSION_KEY, CURRENT_WORK_UNIT_STATE_VERSION);
      addLineageSourceInfo(state, sourceEntity, workunit);
      workUnits.add(workunit);
    }
    return workUnits;
  }

  /**
   *
   */
  private List<WorkUnit> generateWorkUnitsStrategy(SourceEntity sourceEntity, SourceState state, long previousWatermark) {
    WatermarkType watermarkType = WatermarkType.valueOf(
        state.getProp(ConfigurationKeys.SOURCE_QUERYBASED_WATERMARK_TYPE, ConfigurationKeys.DEFAULT_WATERMARK_TYPE)
            .toUpperCase());
    String watermarkColumn = state.getProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY);

    int maxPartitions = state.getPropAsInt(SOURCE_MAX_NUMBER_OF_PARTITIONS,
        ConfigurationKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS);
    int minTargetPartitionSize = state.getPropAsInt(MIN_TARGET_PARTITION_SIZE, DEFAULT_MIN_TARGET_PARTITION_SIZE);

    // Only support time related watermark
    if (watermarkType == WatermarkType.SIMPLE || Strings.isNullOrEmpty(watermarkColumn) || !state.getPropAsBoolean(
        ENABLE_DYNAMIC_PARTITIONING) || maxPartitions <= 1) {
      return super.generateWorkUnits(sourceEntity, state, previousWatermark);
    }

    Partitioner partitioner = new Partitioner(state);
    if (isEarlyStopEnabled(state) && partitioner.isFullDump()) {
      throw new UnsupportedOperationException("Early stop mode cannot work with full dump mode.");
    }

    Partition partition = partitioner.getGlobalPartition(previousWatermark);
    Histogram histogram = getHistogram(sourceEntity.getSourceEntityName(), watermarkColumn, state, partition);

    // we should look if the count is too big, cut off early if count exceeds the limit, or bucket size is too large

    Histogram histogramAdjust;

    // TODO: we should consider move this logic into getRefinedHistogram so that we can early terminate the search
    if (isEarlyStopEnabled(state)) {
      histogramAdjust = new Histogram();
      for (HistogramGroup group : histogram.getGroups()) {
        histogramAdjust.add(group);
        if (histogramAdjust.getTotalRecordCount() > state
            .getPropAsLong(EARLY_STOP_TOTAL_RECORDS_LIMIT, DEFAULT_EARLY_STOP_TOTAL_RECORDS_LIMIT)) {
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
    Boolean disableSoft = state.getPropAsBoolean(SOURCE_QUERYBASED_SALESFORCE_IS_SOFT_DELETES_PULL_DISABLED, false);
    workUnits.stream().forEach(x -> x.setProp(SOURCE_QUERYBASED_SALESFORCE_IS_SOFT_DELETES_PULL_DISABLED, disableSoft));
    return workUnits;
  }

  private boolean isEarlyStopEnabled(State state) {
    return state.getPropAsBoolean(ConfigurationKeys.SOURCE_EARLY_STOP_ENABLED, ConfigurationKeys.DEFAULT_SOURCE_EARLY_STOP_ENABLED);
  }

  String generateSpecifiedPartitions(Histogram histogram, int minTargetPartitionSize, int maxPartitions, long lowWatermark,
      long expectedHighWatermark) {
    int interval = computeTargetPartitionSize(histogram, minTargetPartitionSize, maxPartitions);
    int totalGroups = histogram.getGroups().size();

    log.info("Histogram total record count: " + histogram.totalRecordCount);
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
      if (count != 0 && count + group.count >= 2 * interval) {
        // Summarize current group
        statistics.addValue(count);
        // A step-in start
        partitionPoints.add(Utils.toDateTimeFormat(group.getKey(), SECONDS_FORMAT, Partitioner.WATERMARKTIMEFORMAT));
        count = group.count;
      } else {
        // Add group into current partition
        count += group.count;
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
        DoubleMath.roundToInt((double) histogram.totalRecordCount / maxPartitions, RoundingMode.CEILING));
  }

  /**
   * Get a {@link JsonArray} containing the query results
   */
  private JsonArray getRecordsForQuery(SalesforceConnector connector, String query) {
    try {
      String soqlQuery = SalesforceExtractor.getSoqlUrl(query);
      List<Command> commands = RestApiConnector.constructGetCommand(connector.getFullUri(soqlQuery));
      CommandOutput<?, ?> response = connector.getResponse(commands);

      String output;
      Iterator<String> itr = (Iterator<String>) response.getResults().values().iterator();
      if (itr.hasNext()) {
        output = itr.next();
      } else {
        throw new DataRecordException("Failed to get data from salesforce; REST response has no output");
      }

      return GSON.fromJson(output, JsonObject.class).getAsJsonArray("records");
    } catch (RestApiClientException | RestApiProcessingException | DataRecordException e) {
      throw new RuntimeException("Fail to get data from salesforce", e);
    }
  }

  /**
   * Get the row count for a time range
   */
  private int getCountForRange(TableCountProbingContext probingContext, StrSubstitutor sub,
      Map<String, String> subValues, long startTime, long endTime) {
    String startTimeStr = Utils.dateToString(new Date(startTime), SalesforceExtractor.SALESFORCE_TIMESTAMP_FORMAT);
    String endTimeStr = Utils.dateToString(new Date(endTime), SalesforceExtractor.SALESFORCE_TIMESTAMP_FORMAT);

    subValues.put("start", startTimeStr);
    subValues.put("end", endTimeStr);

    String query = sub.replace(PROBE_PARTITION_QUERY_TEMPLATE);

    log.debug("Count query: " + query);
    probingContext.probeCount++;

    JsonArray records = getRecordsForQuery(probingContext.connector, query);
    Iterator<JsonElement> elements = records.iterator();
    JsonObject element = elements.next().getAsJsonObject();

    return element.get("cnt").getAsInt();
  }

  /**
   * Split a histogram bucket along the midpoint if it is larger than the bucket size limit.
   */
  private void getHistogramRecursively(TableCountProbingContext probingContext, Histogram histogram, StrSubstitutor sub,
      Map<String, String> values, int count, long startEpoch, long endEpoch) {
    long midpointEpoch = startEpoch + (endEpoch - startEpoch) / 2;

    // don't split further if small, above the probe limit, or less than 1 second difference between the midpoint and start
    if (count <= probingContext.bucketSizeLimit
        || probingContext.probeCount > probingContext.probeLimit
        || (midpointEpoch - startEpoch < MIN_SPLIT_TIME_MILLIS)) {
      histogram.add(new HistogramGroup(Utils.epochToDate(startEpoch, SECONDS_FORMAT), count));
      return;
    }

    int countLeft = getCountForRange(probingContext, sub, values, startEpoch, midpointEpoch);

    getHistogramRecursively(probingContext, histogram, sub, values, countLeft, startEpoch, midpointEpoch);
    log.debug("Count {} for left partition {} to {}", countLeft, startEpoch, midpointEpoch);

    int countRight = count - countLeft;

    getHistogramRecursively(probingContext, histogram, sub, values, countRight, midpointEpoch, endEpoch);
    log.debug("Count {} for right partition {} to {}", countRight, midpointEpoch, endEpoch);
  }

  /**
   * Get a histogram for the time range by probing to break down large buckets. Use count instead of
   * querying if it is non-negative.
   */
  private Histogram getHistogramByProbing(TableCountProbingContext probingContext, int count, long startEpoch,
      long endEpoch) {
    Histogram histogram = new Histogram();

    Map<String, String> values = new HashMap<>();
    values.put("table", probingContext.entity);
    values.put("column", probingContext.watermarkColumn);
    values.put("greater", ">=");
    values.put("less", "<");
    StrSubstitutor sub = new StrSubstitutor(values);

    getHistogramRecursively(probingContext, histogram, sub, values, count, startEpoch, endEpoch);

    return histogram;
  }

  /**
   * Refine the histogram by probing to split large buckets
   * @return the refined histogram
   */
  private Histogram getRefinedHistogram(SalesforceConnector connector, String entity, String watermarkColumn,
      SourceState state, Partition partition, Histogram histogram) {
    final int maxPartitions = state.getPropAsInt(SOURCE_MAX_NUMBER_OF_PARTITIONS, DEFAULT_MAX_NUMBER_OF_PARTITIONS);
    final int probeLimit = state.getPropAsInt(DYNAMIC_PROBING_LIMIT, DEFAULT_DYNAMIC_PROBING_LIMIT);
    final int minTargetPartitionSize = state.getPropAsInt(MIN_TARGET_PARTITION_SIZE, DEFAULT_MIN_TARGET_PARTITION_SIZE);
    final Histogram outputHistogram = new Histogram();
    final double probeTargetRatio = state.getPropAsDouble(PROBE_TARGET_RATIO, DEFAULT_PROBE_TARGET_RATIO);
    final int bucketSizeLimit =
        (int) (probeTargetRatio * computeTargetPartitionSize(histogram, minTargetPartitionSize, maxPartitions));

    log.info("Refining histogram with bucket size limit {}.", bucketSizeLimit);

    HistogramGroup currentGroup;
    HistogramGroup nextGroup;
    final TableCountProbingContext probingContext =
        new TableCountProbingContext(connector, entity, watermarkColumn, bucketSizeLimit, probeLimit);

    if (histogram.getGroups().isEmpty()) {
      return outputHistogram;
    }

    // make a copy of the histogram list and add a dummy entry at the end to avoid special processing of the last group
    List<HistogramGroup> list = new ArrayList(histogram.getGroups());
    Date hwmDate = Utils.toDate(partition.getHighWatermark(), Partitioner.WATERMARKTIMEFORMAT);
    list.add(new HistogramGroup(Utils.epochToDate(hwmDate.getTime(), SECONDS_FORMAT), 0));

    for (int i = 0; i < list.size() - 1; i++) {
      currentGroup = list.get(i);
      nextGroup = list.get(i + 1);

      // split the group if it is larger than the bucket size limit
      if (currentGroup.count > bucketSizeLimit) {
        long startEpoch = Utils.toDate(currentGroup.getKey(), SECONDS_FORMAT).getTime();
        long endEpoch = Utils.toDate(nextGroup.getKey(), SECONDS_FORMAT).getTime();

        outputHistogram.add(getHistogramByProbing(probingContext, currentGroup.count, startEpoch, endEpoch));
      } else {
        outputHistogram.add(currentGroup);
      }
    }

    log.info("Executed {} probes for refining the histogram.", probingContext.probeCount);

    // if the probe limit has been reached then print a warning
    if (probingContext.probeCount >= probingContext.probeLimit) {
      log.warn("Reached the probe limit");
    }

    return outputHistogram;
  }

  /**
   * Get a histogram with day granularity buckets.
   */
  private Histogram getHistogramByDayBucketing(SalesforceConnector connector, String entity, String watermarkColumn,
      Partition partition) {
    Histogram histogram = new Histogram();

    Calendar calendar = new GregorianCalendar();
    Date startDate = Utils.toDate(partition.getLowWatermark(), Partitioner.WATERMARKTIMEFORMAT);
    calendar.setTime(startDate);
    int startYear = calendar.get(Calendar.YEAR);
    String lowWatermarkDate = Utils.dateToString(startDate, SalesforceExtractor.SALESFORCE_TIMESTAMP_FORMAT);

    Date endDate = Utils.toDate(partition.getHighWatermark(), Partitioner.WATERMARKTIMEFORMAT);
    calendar.setTime(endDate);
    int endYear = calendar.get(Calendar.YEAR);
    String highWatermarkDate = Utils.dateToString(endDate, SalesforceExtractor.SALESFORCE_TIMESTAMP_FORMAT);

    Map<String, String> values = new HashMap<>();
    values.put("table", entity);
    values.put("column", watermarkColumn);
    StrSubstitutor sub = new StrSubstitutor(values);

    for (int year = startYear; year <= endYear; year++) {
      if (year == startYear) {
        values.put("start", lowWatermarkDate);
        values.put("greater", partition.isLowWatermarkInclusive() ? ">=" : ">");
      } else {
        values.put("start", getDateString(year));
        values.put("greater", ">=");
      }

      if (year == endYear) {
        values.put("end", highWatermarkDate);
        values.put("less", partition.isHighWatermarkInclusive() ? "<=" : "<");
      } else {
        values.put("end", getDateString(year + 1));
        values.put("less", "<");
      }

      String query = sub.replace(DAY_PARTITION_QUERY_TEMPLATE);
      log.info("Histogram query: " + query);

      histogram.add(parseDayBucketingHistogram(getRecordsForQuery(connector, query)));
    }

    return histogram;
  }

  protected SalesforceConnector getConnector(State state) {
    if (this.salesforceConnector == null) {
      this.salesforceConnector = new SalesforceConnector(state);
    }
    return this.salesforceConnector;
  }

  /**
   * Generate the histogram
   */
  private Histogram getHistogram(String entity, String watermarkColumn, SourceState state,
      Partition partition) {
    SalesforceConnector connector = getConnector(state);

    try {
      if (!connector.connect()) {
        throw new RuntimeException("Failed to connect.");
      }
    } catch (RestApiConnectionException e) {
      throw new RuntimeException("Failed to connect.", e);
    }

    Histogram histogram = getHistogramByDayBucketing(connector, entity, watermarkColumn, partition);

    // exchange the first histogram group key with the global low watermark to ensure that the low watermark is captured
    // in the range of generated partitions
    HistogramGroup firstGroup = histogram.get(0);
    Date lwmDate = Utils.toDate(partition.getLowWatermark(), Partitioner.WATERMARKTIMEFORMAT);
    histogram.getGroups().set(0, new HistogramGroup(Utils.epochToDate(lwmDate.getTime(), SECONDS_FORMAT),
        firstGroup.getCount()));

    // refine the histogram
    if (state.getPropAsBoolean(ENABLE_DYNAMIC_PROBING)) {
      histogram = getRefinedHistogram(connector, entity, watermarkColumn, state, partition, histogram);
    }

    return histogram;
  }

  private String getDateString(int year) {
    Calendar calendar = new GregorianCalendar();
    calendar.clear();
    calendar.set(Calendar.YEAR, year);
    return Utils.dateToString(calendar.getTime(), SalesforceExtractor.SALESFORCE_TIMESTAMP_FORMAT);
  }

  /**
   * Parse the query results into a {@link Histogram}
   */
  private Histogram parseDayBucketingHistogram(JsonArray records) {
    log.info("Parse day-based histogram");

    Histogram histogram = new Histogram();

    Iterator<JsonElement> elements = records.iterator();
    JsonObject element;

    while (elements.hasNext()) {
      element = elements.next().getAsJsonObject();
      String time = element.get("time").getAsString() + ZERO_TIME_SUFFIX;
      int count = element.get("cnt").getAsInt();

      histogram.add(new HistogramGroup(time, count));
    }

    return histogram;
  }

  @AllArgsConstructor
  static class HistogramGroup {
    @Getter
    private final String key;
    @Getter
    private final int count;

    @Override
    public String toString() {
      return key + ":" + count;
    }
  }

  static class Histogram {
    @Getter
    private long totalRecordCount;
    @Getter
    private List<HistogramGroup> groups;

    Histogram() {
      totalRecordCount = 0;
      groups = new ArrayList<>();
    }

    void add(HistogramGroup group) {
      groups.add(group);
      totalRecordCount += group.count;
    }

    void add(Histogram histogram) {
      groups.addAll(histogram.getGroups());
      totalRecordCount += histogram.totalRecordCount;
    }

    HistogramGroup get(int idx) {
      return this.groups.get(idx);
    }

    @Override
    public String toString() {
      return groups.toString();
    }
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

  /**
   * Context for probing the table for row counts of a time range
   */
  @RequiredArgsConstructor
  private static class TableCountProbingContext {
    private final SalesforceConnector connector;
    private final String entity;
    private final String watermarkColumn;
    private final int bucketSizeLimit;
    private final int probeLimit;

    private int probeCount = 0;
  }
}

