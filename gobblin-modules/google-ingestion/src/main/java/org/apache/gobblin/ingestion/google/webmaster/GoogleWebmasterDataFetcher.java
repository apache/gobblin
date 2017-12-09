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

package org.apache.gobblin.ingestion.google.webmaster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.services.webmasters.model.ApiDataRow;
import com.google.api.services.webmasters.model.ApiDimensionFilter;
import com.google.api.services.webmasters.model.SearchAnalyticsQueryResponse;


/**
 * GoogleWebmasterDataFetcher implements the logic to download all query data(e.g. page, query, clicks, impressions, CTR, position) for a given date and country.
 *
 * The user of this GoogleWebmasterDataFetcher should follow these steps:
 * 1. Call getAllPages to get all pages based on your filters.
 * 2. For each page we get at last step, call performSearchAnalyticsQuery to get query data. If query data is not available, then this page won't be included in the return value.
 *
 * Note:
 * Due to the limitation of the Google API -- each API request can return at most 5000 rows of records, you need to take additional steps to fetch all data that the Google Search Console keeps.
 *
 * There is a rule to check whether Google Search Console keeps more than 5000 rows of data based on your request. The rule is that if you request for 5000 rows, but the response has less than 5000 rows; then in this case you've got all data that the Google service provides. If you request for 5000 rows, and the API returns you 5000 rows, there is a high chance that the Google service has more data matching your query, but due to the limitation of the API, only first 5000 rows returned.
 *
 */
public abstract class GoogleWebmasterDataFetcher {
  public abstract String getSiteProperty();

  enum Metric {
    CLICKS, IMPRESSIONS, CTR, POSITION
  }

  /**
   * Results are composed of [[requestedDimension list], clicks, impressions, ctr, position]
   * @param rowLimit row limit for this API call
   * @param requestedDimensions a list of dimension requests. The dimension values can be found at the first part of the return value
   * @param filters filters of your request
   */
  public abstract List<String[]> performSearchAnalyticsQuery(String startDate, String endDate, int rowLimit,
      List<GoogleWebmasterFilter.Dimension> requestedDimensions, List<Metric> requestedMetrics,
      Collection<ApiDimensionFilter> filters)
      throws IOException;

  /**
   * Call API in batches
   */
  public abstract void performSearchAnalyticsQueryInBatch(List<ProducerJob> jobs,
      List<ArrayList<ApiDimensionFilter>> filterList,
      List<JsonBatchCallback<SearchAnalyticsQueryResponse>> callbackList,
      List<GoogleWebmasterFilter.Dimension> requestedDimensions, int rowLimit)
      throws IOException;

  /**
   * Return all pages given (date, country) filter
   * @param country country code string
   * @param rowLimit this is mostly for testing purpose. In order to get all pages, set this to the API row limit, which is 5000
   */
  public abstract Collection<ProducerJob> getAllPages(String startDate, String endDate, String country, int rowLimit)
      throws IOException;

  public static List<String[]> convertResponse(List<Metric> requestedMetrics, SearchAnalyticsQueryResponse response) {
    List<ApiDataRow> rows = response.getRows();
    if (rows == null || rows.isEmpty()) {
      return new ArrayList<>();
    }
    int arraySize = rows.get(0).getKeys().size() + requestedMetrics.size();

    List<String[]> ret = new ArrayList<>(rows.size());
    for (ApiDataRow row : rows) {
      List<String> keys = row.getKeys();
      String[] data = new String[arraySize];
      int i = 0;
      for (; i < keys.size(); ++i) {
        data[i] = keys.get(i);
      }

      for (Metric requestedMetric : requestedMetrics) {
        if (requestedMetric == Metric.CLICKS) {
          data[i] = row.getClicks().toString();
        } else if (requestedMetric == Metric.IMPRESSIONS) {
          data[i] = row.getImpressions().toString();
        } else if (requestedMetric == Metric.CTR) {
          data[i] = String.format("%.5f", row.getCtr());
        } else if (requestedMetric == Metric.POSITION) {
          data[i] = String.format("%.2f", row.getPosition());
        } else {
          throw new RuntimeException("Unknown Google Webmaster Metric Type");
        }
        ++i;
      }
      ret.add(data);
    }
    return ret;
  }
}

