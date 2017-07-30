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

package gobblin.ingestion.google.webmaster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import gobblin.annotation.Alpha;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.avro.JsonElementConversionFactory;
import gobblin.ingestion.google.util.SchemaUtil;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.QueryBasedSource;
import gobblin.source.workunit.WorkUnit;


/**
 * Google Webmaster API enables you to download data from Google Search Console for search analytics of the verified sites. See more here https://developers.google.com/webmaster-tools/. Configure the Google Webmaster Source for starting a daily job to download search analytics data. This gobblin job partitions the whole task into sub-tasks for each day. Each sub-task is handled by a GoogleWebmasterExtractor for that date, and each GoogleWebmasterExtractor holds a queue of GoogleWebmasterExtractorIterators, each of which does the query task for each filter(Currently, only the country filter is supported.) on that date.
 *
 * The minimum unit of querying range is date. Change the range by configuring "source.querybased.start.value" and "source.querybased.end.value". Note that the analytics data for Google Search Console has a delay or 3 days. So cap your configuration of "source.querybased.append.max.watermark.limit" by "CURRENTDATE-3". See the documentation details of each configuration in the GoogleWebMasterSource fields.
 *
 */
@Alpha
abstract class GoogleWebMasterSource extends QueryBasedSource<String, String[]> {
  public static final String SOURCE_GOOGLE_WEBMASTER_PREFIX = "source.google_webmasters.";
  /**
   * Must Provide.
   * Provide the property site URL whose google search analytics data you want to download
   */
  public static final String KEY_PROPERTY = SOURCE_GOOGLE_WEBMASTER_PREFIX + "property_urls";
  /**
   * Optional. Default to false.
   * Determine whether to add source property as the last column to your configured schema
   */
  public static final String KEY_INCLUDE_SOURCE_PROPERTY = SOURCE_GOOGLE_WEBMASTER_PREFIX + "source_property.include";
  /**
   * Optional. Default to "Source".
   * Determine the column name for the additional source property origin column if included
   */
  public static final String KEY_SOURCE_PROPERTY_COLUMN_NAME =
      SOURCE_GOOGLE_WEBMASTER_PREFIX + "source_property.column_name";
  /**
   * The filters that will be passed to all your API requests.
   * Filter format is [GoogleWebmasterFilter.Dimension].[DimensionValue]
   * Currently, this filter operator is "EQUALS" and only Country dimension is supported. Will extend this feature according to more use cases in the futher.
   */
  public static final String KEY_REQUEST_FILTERS = SOURCE_GOOGLE_WEBMASTER_PREFIX + "request.filters";
  /**
   * Must Provide.
   *
   * Allowed dimensions can be found in the enum GoogleWebmasterFilter.Dimension
   */
  public static final String KEY_REQUEST_DIMENSIONS = SOURCE_GOOGLE_WEBMASTER_PREFIX + "request.dimensions";
  /**
   * Must Provide.
   *
   * Allowed metrics can be found in the enum GoogleWebmasterDataFetcher.Metric
   */
  public static final String KEY_REQUEST_METRICS = SOURCE_GOOGLE_WEBMASTER_PREFIX + "request.metrics";
  /**
   * Optional: Default to 5000, which is the maximum allowed.
   *
   * The response row limit when you ask for pages. Set it to 5000 when you want to get all pages, which might be larger than 5000.
   */
  public static final String KEY_REQUEST_PAGE_LIMIT = SOURCE_GOOGLE_WEBMASTER_PREFIX + "request.page_limit";
  /**
   * Optional: Default to String.empty
   * Hot start this service with pre-set pages. Once this is set, the service will ignore KEY_REQUEST_PAGE_LIMIT, and won't get all pages, but use the pre-set pages instead.
   */
  public static final String KEY_REQUEST_HOT_START = SOURCE_GOOGLE_WEBMASTER_PREFIX + "request.hot_start";
  /**
   * Optional: Default to 5000, which is the maximum allowed.
   *
   * The response row limit when you ask for queries.
   */
  public static final String KEY_REQUEST_QUERY_LIMIT = SOURCE_GOOGLE_WEBMASTER_PREFIX + "request.query_limit";
  public static final String TUNING = SOURCE_GOOGLE_WEBMASTER_PREFIX + "request.tuning.";

  // ===============================================
  // =========   GET QUERIES TUNING BEGIN ==========
  // ===============================================
  public static final String QUERIES_TUNING = TUNING + "get_queries.";
  /**
   * Optional. Default to 120 minutes.
   * Set the time out in minutes for each round while getting queries.
   */
  public static final String KEY_QUERIES_TUNING_TIME_OUT = QUERIES_TUNING + "time_out";
  /**
   * Optional. Default to 40.
   * Tune the maximum rounds of retries while getting queries.
   */
  public static final String KEY_QUERIES_TUNING_RETRIES = QUERIES_TUNING + "max_reties";
  /**
   * Optional. Default to 250 millisecond.
   * Tune the cool down time between each round of retry.
   */
  public static final String KEY_QUERIES_TUNING_COOL_DOWN = QUERIES_TUNING + "cool_down_time";
  /**
   * Optional. Default to 2.25 batches per second.
   * Tune the speed of API requests.
   */
  public static final String KEY_QUERIES_TUNING_BATCHES_PER_SECOND = QUERIES_TUNING + "batches_per_second";
  /**
   * Optional. Default to 2.
   * Tune the size of a batch. Batch API calls together to reduce the number of HTTP connections.
   * Note: A set of n requests batched together counts toward your usage limit as n requests, not as one request. The batch request is taken apart into a set of requests before processing.
   * Read more at https://developers.google.com/webmaster-tools/v3/how-tos/batch
   */
  public static final String KEY_QUERIES_TUNING_BATCH_SIZE = QUERIES_TUNING + "batch_size";
  /**
   * Optional. Default to 500.
   * Set the group size for UrlTriePrefixGrouper
   */
  public static final String KEY_QUERIES_TUNING_GROUP_SIZE = QUERIES_TUNING + "trie_group_size";

  /**
   * Optional. Default to false.
   * Choose whether to apply the trie based algorithm while getting all queries.
   *
   * If set to true, you also need to set page_limit to 5000 indicating that you want to get all pages because trie based algorithm won't give you expected results if you just need a subset of all pages.
   */
  public static final String KEY_REQUEST_TUNING_ALGORITHM = QUERIES_TUNING + "apply_trie";
  // =============================================
  // =========   GET QUERIES TUNING END ==========
  // =============================================

  // =============================================
  // =========   GET PAGES TUNING BEGIN ==========
  // =============================================
  public static final String PAGES_TUNING = TUNING + "get_pages.";
  /**
   * Optional. Default to 5.0.
   * Tune the speed of API requests while getting all pages.
   */
  public static final String KEY_PAGES_TUNING_REQUESTS_PER_SECOND = PAGES_TUNING + "requests_per_second";
  /**
   * Optional. Default to 120.
   * Tune the number of maximum retries while getting all pages. Consider the following affecting factors while setting this number:
   * 1. the length of shared prefix path may be very long
   * 2. the Quota Exceeded exception
   */
  public static final String KEY_PAGES_TUNING_MAX_RETRIES = PAGES_TUNING + "max_retries";
  /**
   * Optional. Default to 2 minutes.
   * Set the time out in minutes while getting all pages.
   */
  public static final String KEY_PAGES_TUNING_TIME_OUT = PAGES_TUNING + "time_out";
  // =============================================
  // =========   GET PAGES TUNING END ============
  // =============================================

  private final static Splitter splitter = Splitter.on(",").omitEmptyStrings().trimResults();
  public static final boolean DEFAULT_INCLUDE_SOURCE_PROPERTY = false;
  public static final String DEFAULT_SOURCE_PROPERTY_COLUMN_NAME = "Source";

  @Override
  public Extractor<String, String[]> getExtractor(WorkUnitState state)
      throws IOException {
    List<GoogleWebmasterFilter.Dimension> requestedDimensions = getRequestedDimensions(state);
    List<GoogleWebmasterDataFetcher.Metric> requestedMetrics = getRequestedMetrics(state);

    WorkUnit workunit = state.getWorkunit();
    String schema = workunit.getProp(ConfigurationKeys.SOURCE_SCHEMA);

    JsonArray schemaJson = new JsonParser().parse(schema).getAsJsonArray();
    Map<String, Integer> columnPositionMap = new HashMap<>();
    for (int i = 0; i < schemaJson.size(); ++i) {
      JsonElement jsonElement = schemaJson.get(i);
      String columnName = jsonElement.getAsJsonObject().get("columnName").getAsString().toUpperCase();
      columnPositionMap.put(columnName, i);
    }

    if (workunit.getPropAsBoolean(GoogleWebMasterSource.KEY_INCLUDE_SOURCE_PROPERTY, DEFAULT_INCLUDE_SOURCE_PROPERTY)) {
      String columnName = workunit.getProp(KEY_SOURCE_PROPERTY_COLUMN_NAME, DEFAULT_SOURCE_PROPERTY_COLUMN_NAME);
      schemaJson.add(SchemaUtil.createColumnJson(columnName, false, JsonElementConversionFactory.Type.STRING));
    }

    validateFilters(state.getProp(GoogleWebMasterSource.KEY_REQUEST_FILTERS));
    validateRequests(columnPositionMap, requestedDimensions, requestedMetrics);
    return createExtractor(state, columnPositionMap, requestedDimensions, requestedMetrics, schemaJson);
  }

  abstract GoogleWebmasterExtractor createExtractor(WorkUnitState state, Map<String, Integer> columnPositionMap,
      List<GoogleWebmasterFilter.Dimension> requestedDimensions,
      List<GoogleWebmasterDataFetcher.Metric> requestedMetrics, JsonArray schemaJson)
      throws IOException;

  private void validateFilters(String filters) {
    String countryPrefix = "COUNTRY.";

    for (String filter : splitter.split(filters)) {
      if (filter.toUpperCase().startsWith(countryPrefix)) {
        GoogleWebmasterFilter.validateCountryCode(filter.substring(countryPrefix.length()));
      }
    }
  }

  private void validateRequests(Map<String, Integer> columnPositionMap,
      List<GoogleWebmasterFilter.Dimension> requestedDimensions,
      List<GoogleWebmasterDataFetcher.Metric> requestedMetrics) {
    for (GoogleWebmasterFilter.Dimension dimension : requestedDimensions) {
      Preconditions.checkState(columnPositionMap.containsKey(dimension.toString()),
          "Your requested dimension must exist in the source.schema.");
    }
    for (GoogleWebmasterDataFetcher.Metric metric : requestedMetrics) {
      Preconditions.checkState(columnPositionMap.containsKey(metric.toString()),
          "Your requested metric must exist in the source.schema.");
    }
  }

  private List<GoogleWebmasterFilter.Dimension> getRequestedDimensions(WorkUnitState wuState) {
    List<GoogleWebmasterFilter.Dimension> dimensions = new ArrayList<>();
    String dimensionsString = wuState.getProp(GoogleWebMasterSource.KEY_REQUEST_DIMENSIONS);
    for (String dim : splitter.split(dimensionsString)) {
      dimensions.add(GoogleWebmasterFilter.Dimension.valueOf(dim.toUpperCase()));
    }
    return dimensions;
  }

  private List<GoogleWebmasterDataFetcher.Metric> getRequestedMetrics(WorkUnitState wuState) {
    List<GoogleWebmasterDataFetcher.Metric> metrics = new ArrayList<>();
    String metricsString = wuState.getProp(GoogleWebMasterSource.KEY_REQUEST_METRICS);
    for (String metric : splitter.split(metricsString)) {
      metrics.add(GoogleWebmasterDataFetcher.Metric.valueOf(metric.toUpperCase()));
    }
    return metrics;
  }
}
