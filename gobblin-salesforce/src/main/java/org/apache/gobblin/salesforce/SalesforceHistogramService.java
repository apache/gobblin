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

import com.google.common.math.DoubleMath;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.exception.RestApiClientException;
import org.apache.gobblin.source.extractor.exception.RestApiConnectionException;
import org.apache.gobblin.source.extractor.exception.RestApiProcessingException;
import org.apache.gobblin.source.extractor.extract.Command;
import org.apache.gobblin.source.extractor.extract.CommandOutput;
import org.apache.gobblin.source.extractor.extract.restapi.RestApiConnector;
import org.apache.gobblin.source.extractor.partition.Partition;
import org.apache.gobblin.source.extractor.partition.Partitioner;
import org.apache.gobblin.source.extractor.utils.Utils;

import static org.apache.gobblin.configuration.ConfigurationKeys.*;


/**
 * This class encapsulates everything related to histogram calculation for Salesforce. A histogram here refers to a
 * mapping of number of records to be fetched by time intervals.
 */
@Slf4j
public class SalesforceHistogramService {
  private static final int MIN_SPLIT_TIME_MILLIS = 1000;
  private static final String ZERO_TIME_SUFFIX = "-00:00:00";
  private static final Gson GSON = new Gson();
  // this is used to generate histogram buckets smaller than the target partition size to allow for more even
  // packing of the generated partitions
  private static final String PROBE_TARGET_RATIO = "salesforce.probeTargetRatio";
  private static final double DEFAULT_PROBE_TARGET_RATIO = 0.60;
  private static final String DYNAMIC_PROBING_LIMIT = "salesforce.dynamicProbingLimit";
  private static final int DEFAULT_DYNAMIC_PROBING_LIMIT = 1000;

  private static final String DAY_PARTITION_QUERY_TEMPLATE =
      "SELECT count(${column}) cnt, DAY_ONLY(${column}) time FROM ${table} " + "WHERE ${column} ${greater} ${start}"
          + " AND ${column} ${less} ${end} GROUP BY DAY_ONLY(${column}) ORDER BY DAY_ONLY(${column})";
  private static final String PROBE_PARTITION_QUERY_TEMPLATE = "SELECT count(${column}) cnt FROM ${table} "
      + "WHERE ${column} ${greater} ${start} AND ${column} ${less} ${end}";

  protected SalesforceConnector salesforceConnector;
  private final SfConfig sfConfig;

  SalesforceHistogramService(SfConfig sfConfig, SalesforceConnector connector) {
    this.sfConfig = sfConfig;
    salesforceConnector = connector;
  }

  /**
   * Generate the histogram
   */
  Histogram getHistogram(String entity, String watermarkColumn, SourceState state,
      Partition partition) {

    try {
      if (!salesforceConnector.connect()) {
        throw new RuntimeException("Failed to connect.");
      }
    } catch (RestApiConnectionException e) {
      throw new RuntimeException("Failed to connect.", e);
    }

    Histogram histogram = getHistogramByDayBucketing(salesforceConnector, entity, watermarkColumn, partition);

    // exchange the first histogram group key with the global low watermark to ensure that the low watermark is captured
    // in the range of generated partitions
    HistogramGroup firstGroup = histogram.get(0);
    Date lwmDate = Utils.toDate(partition.getLowWatermark(), Partitioner.WATERMARKTIMEFORMAT);
    histogram.getGroups().set(0, new HistogramGroup(Utils.epochToDate(lwmDate.getTime(), SalesforceSource.SECONDS_FORMAT),
        firstGroup.getCount()));

    // refine the histogram
    if (state.getPropAsBoolean(SalesforceSource.ENABLE_DYNAMIC_PROBING)) {
      histogram = getRefinedHistogram(salesforceConnector, entity, watermarkColumn, state, partition, histogram);
    }

    return histogram;
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

  /**
   * Refine the histogram by probing to split large buckets
   * @return the refined histogram
   */
  private Histogram getRefinedHistogram(SalesforceConnector connector, String entity, String watermarkColumn,
      SourceState state, Partition partition, Histogram histogram) {
    final int maxPartitions = state.getPropAsInt(SOURCE_MAX_NUMBER_OF_PARTITIONS, DEFAULT_MAX_NUMBER_OF_PARTITIONS);
    final int probeLimit = state.getPropAsInt(
        DYNAMIC_PROBING_LIMIT, DEFAULT_DYNAMIC_PROBING_LIMIT);
    final int minTargetPartitionSize = state.getPropAsInt(
        SalesforceSource.MIN_TARGET_PARTITION_SIZE, SalesforceSource.DEFAULT_MIN_TARGET_PARTITION_SIZE);
    final Histogram outputHistogram = new Histogram();
    final double probeTargetRatio = state.getPropAsDouble(
        PROBE_TARGET_RATIO, DEFAULT_PROBE_TARGET_RATIO);
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
    List<HistogramGroup> list = new ArrayList<>(histogram.getGroups());
    Date hwmDate = Utils.toDate(partition.getHighWatermark(), Partitioner.WATERMARKTIMEFORMAT);
    list.add(new HistogramGroup(Utils.epochToDate(hwmDate.getTime(), SalesforceSource.SECONDS_FORMAT), 0));

    for (int i = 0; i < list.size() - 1; i++) {
      currentGroup = list.get(i);
      nextGroup = list.get(i + 1);

      // split the group if it is larger than the bucket size limit
      if (currentGroup.getCount() > bucketSizeLimit) {
        long startEpoch = Utils.toDate(currentGroup.getKey(), SalesforceSource.SECONDS_FORMAT).getTime();
        long endEpoch = Utils.toDate(nextGroup.getKey(), SalesforceSource.SECONDS_FORMAT).getTime();

        outputHistogram.add(getHistogramByProbing(probingContext, currentGroup.getCount(), startEpoch, endEpoch));
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
      histogram.add(new HistogramGroup(Utils.epochToDate(startEpoch, SalesforceSource.SECONDS_FORMAT), count));
      return;
    }

    int countLeft = getCountForRange(probingContext, sub, values, startEpoch, midpointEpoch);

    getHistogramRecursively(probingContext, histogram, sub, values, countLeft, startEpoch, midpointEpoch);
    log.info("Count {} for left partition {} to {}", countLeft, startEpoch, midpointEpoch);

    int countRight = count - countLeft;

    getHistogramRecursively(probingContext, histogram, sub, values, countRight, midpointEpoch, endEpoch);
    log.info("Count {} for right partition {} to {}", countRight, midpointEpoch, endEpoch);
  }


  /**
   * Get a {@link JsonArray} containing the query results
   */
  @SneakyThrows
  private JsonArray getRecordsForQuery(SalesforceConnector connector, String query) {
    RestApiProcessingException exception = null;
    for (int i = 0; i < sfConfig.restApiRetryLimit + 1; i++) {
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
      } catch (RestApiClientException | DataRecordException e) {
        throw new RuntimeException("Fail to get data from salesforce", e);
      } catch (RestApiProcessingException e) {
        exception = e;
        log.info("Caught RestApiProcessingException, retrying({}) rest query: {}", i+1, query);
        Thread.sleep(sfConfig.restApiRetryInterval);
      }
    }
    throw new RuntimeException("Fail to get data from salesforce", exception);
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

    log.info("Count query: {}", query);
    probingContext.probeCount++;

    JsonArray records = getRecordsForQuery(probingContext.connector, query);
    Iterator<JsonElement> elements = records.iterator();
    JsonObject element = elements.next().getAsJsonObject();

    return element.get("cnt").getAsInt();
  }

  /**
   * Compute the target partition size.
   */
  private int computeTargetPartitionSize(Histogram histogram, int minTargetPartitionSize, int maxPartitions) {
    return Math.max(minTargetPartitionSize,
        DoubleMath.roundToInt((double) histogram.getTotalRecordCount() / maxPartitions, RoundingMode.CEILING));
  }

  /**
   * Context for probing the table for row counts of a time range
   */
  @RequiredArgsConstructor
  public static class TableCountProbingContext {
    private final SalesforceConnector connector;
    private final String entity;
    private final String watermarkColumn;
    private final int bucketSizeLimit;
    private final int probeLimit;

    private int probeCount = 0;
  }
}
