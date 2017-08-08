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
import java.util.List;

import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.services.webmasters.Webmasters;
import com.google.api.services.webmasters.model.ApiDimensionFilter;
import com.google.api.services.webmasters.model.ApiDimensionFilterGroup;


/**
 * Provide basic accesses to Google Search Console by utilizing Google Webmaster API.
 * Details about Google Webmaster or Google Search Console API can be found at https://developers.google.com/webmaster-tools/
 */
public abstract class GoogleWebmasterClient {
  public static final int API_ROW_LIMIT = 5000;

  /**
   * Return all pages given all constraints.
   * @param siteProperty your site property string
   * @param startDate date string with format "yyyy-MM-dd"
   * @param endDate date string with format "yyyy-MM-dd"
   * @param country country code string
   * @param rowLimit limit the number of rows returned by the API. The API maximum limit is 5000.
   * @param requestedDimensions requested dimensions of the API call.
   * @param filters a list of filters. Include the country filter if you need it, even though you've provided the country string.
   * @param startRow this is a 0 based index configuration to set the starting row of your API request. Even though the API row limit is 5000, you can send a request starting from row 5000, so you will be able to get data from row 5000 to row 9999.
   * @return Return all pages given all constraints.
   */
  public abstract List<String> getPages(String siteProperty, String startDate, String endDate, String country,
      int rowLimit, List<GoogleWebmasterFilter.Dimension> requestedDimensions, List<ApiDimensionFilter> filters, int startRow)
      throws IOException;

  /**
   * Perform the api call for search analytics query
   * @param siteProperty your site property string
   * @param startDate date string with format "yyyy-MM-dd"
   * @param endDate date string with format "yyyy-MM-dd"
   * @param dimensions your requested dimensions
   * @param filterGroup filters for your API request. Provide your filters in a group, find utility functions in GoogleWebmasterFilter
   * @param rowLimit the row limit for your API response
   * @param startRow this is a 0 based index configuration to set the starting row of your API request. Even though the API row limit is 5000, you can send a request starting from row 5000, so you will be able to get data from row 5000 to row 9999.
   * @return return the response of Google Webmaster API
   */
  public abstract Webmasters.Searchanalytics.Query createSearchAnalyticsQuery(String siteProperty, String startDate,
      String endDate, List<GoogleWebmasterFilter.Dimension> dimensions, ApiDimensionFilterGroup filterGroup, int rowLimit, int startRow)
      throws IOException;

  public abstract BatchRequest createBatch();
}
