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

import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import com.google.common.base.Joiner;
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
import org.apache.gobblin.source.extractor.watermark.WatermarkType;
import org.apache.gobblin.source.workunit.WorkUnit;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * An implementation of {@link QueryBasedSource} for salesforce data sources.
 */
@Slf4j
public class SalesforceSource extends QueryBasedSource<JsonArray, JsonElement> {

  public static final String USE_ALL_OBJECTS = "use.all.objects";
  public static final boolean DEFAULT_USE_ALL_OBJECTS = false;

  private static final String ENABLE_DYNAMIC_PARTITIONING = "salesforce.enableDynamicPartitioning";
  private static final String DAY_PARTITION_QUERY_TEMPLATE = "SELECT count(${column}) cnt, DAY_ONLY(${column}) time FROM ${table} "
      + "WHERE ${column} ${greater} ${start} AND ${column} ${less} ${end} GROUP BY DAY_ONLY(${column}) ORDER BY DAY_ONLY(${column})";
  private static final String DAY_FORMAT = "yyyy-MM-dd";

  private static final Gson GSON = new Gson();

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
  protected List<WorkUnit> generateWorkUnits(SourceEntity sourceEntity, SourceState state, long previousWatermark) {
    WatermarkType watermarkType = WatermarkType.valueOf(
        state.getProp(ConfigurationKeys.SOURCE_QUERYBASED_WATERMARK_TYPE, ConfigurationKeys.DEFAULT_WATERMARK_TYPE)
            .toUpperCase());
    String watermarkColumn = state.getProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY);

    int maxPartitions = state.getPropAsInt(ConfigurationKeys.SOURCE_MAX_NUMBER_OF_PARTITIONS,
        ConfigurationKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS);

    // Only support time related watermark
    if (watermarkType == WatermarkType.SIMPLE || Strings.isNullOrEmpty(watermarkColumn)
        || !state.getPropAsBoolean(ENABLE_DYNAMIC_PARTITIONING) || maxPartitions <= 1) {
      return super.generateWorkUnits(sourceEntity, state, previousWatermark);
    }

    Partition partition = new Partitioner(state).getGlobalPartition(previousWatermark);
    Histogram histogram = getHistogram(sourceEntity.getSourceEntityName(), watermarkColumn, state, partition);

    String specifiedPartitions = generateSpecifiedPartitions(histogram, maxPartitions, partition.getHighWatermark());
    state.setProp(Partitioner.HAS_USER_SPECIFIED_PARTITIONS, true);
    state.setProp(Partitioner.USER_SPECIFIED_PARTITIONS, specifiedPartitions);

    return super.generateWorkUnits(sourceEntity, state, previousWatermark);
  }

  String generateSpecifiedPartitions(Histogram histogram, int maxPartitions, long expectedHighWatermark) {
    long interval = DoubleMath.roundToLong((double) histogram.totalRecordCount / maxPartitions, RoundingMode.CEILING);
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
        partitionPoints.add(Utils.toDateTimeFormat(group.getKey(), DAY_FORMAT, Partitioner.WATERMARKTIMEFORMAT));
      }

      // Move the candidate to a new bucket if the attempted total is 2x of interval
      if (count != 0 && count + group.count >= 2 * interval) {
        // Summarize current group
        statistics.addValue(count);
        // A step-in start
        partitionPoints.add(Utils.toDateTimeFormat(group.getKey(), DAY_FORMAT, Partitioner.WATERMARKTIMEFORMAT));
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

    // If the last group is used as the last partition point
    if (count == 0) {
      // Exchange the last partition point with global high watermark
      partitionPoints.set(partitionPoints.size() - 1, Long.toString(expectedHighWatermark));
    } else {
      // Summarize last group
      statistics.addValue(count);
      // Add global high watermark as last point
      partitionPoints.add(Long.toString(expectedHighWatermark));
    }

    log.info("Dynamic partitioning statistics: ");
    log.info("data: " + Arrays.toString(statistics.getValues()));
    log.info(statistics.toString());
    String specifiedPartitions = Joiner.on(",").join(partitionPoints);
    log.info("Calculated specified partitions: " + specifiedPartitions);
    return specifiedPartitions;
  }

  private Histogram getHistogram(String entity, String watermarkColumn, SourceState state,
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

    SalesforceConnector connector = new SalesforceConnector(state);
    try {
      if (!connector.connect()) {
        throw new RuntimeException("Failed to connect.");
      }
    } catch (RestApiConnectionException e) {
      throw new RuntimeException("Failed to connect.", e);
    }

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

      try {
        String soqlQuery = SalesforceExtractor.getSoqlUrl(query);
        List<Command> commands = RestApiConnector.constructGetCommand(connector.getFullUri(soqlQuery));
        CommandOutput<?, ?> response = connector.getResponse(commands);
        histogram.add(parseHistogram(response));
      } catch (RestApiClientException | RestApiProcessingException | DataRecordException e) {
        throw new RuntimeException("Fail to get data of year " + year + " from salesforce", e);
      }
    }

    return histogram;
  }

  private String getDateString(int year) {
    Calendar calendar = new GregorianCalendar();
    calendar.clear();
    calendar.set(Calendar.YEAR, year);
    return Utils.dateToString(calendar.getTime(), SalesforceExtractor.SALESFORCE_TIMESTAMP_FORMAT);
  }

  private Histogram parseHistogram(CommandOutput<?, ?> response) throws DataRecordException {
    log.info("Parse histogram");
    Histogram histogram = new Histogram();

    String output;
    Iterator<String> itr = (Iterator<String>) response.getResults().values().iterator();
    if (itr.hasNext()) {
      output = itr.next();
    } else {
      throw new DataRecordException("Failed to get data from salesforce; REST response has no output");
    }
    JsonArray records = GSON.fromJson(output, JsonObject.class).getAsJsonArray("records");
    Iterator<JsonElement> elements = records.iterator();
    JsonObject element;
    while (elements.hasNext()) {
      element = elements.next().getAsJsonObject();
      histogram.add(new HistogramGroup(element.get("time").getAsString(), element.get("cnt").getAsInt()));
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

    @Override
    public String toString() {
      return groups.toString();
    }
  }

  protected Set<SourceEntity> getSourceEntities(State state) {
    if (!state.getPropAsBoolean(USE_ALL_OBJECTS, DEFAULT_USE_ALL_OBJECTS)) {
      return super.getSourceEntities(state);
    }

    SalesforceConnector connector = new SalesforceConnector(state);
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

}
