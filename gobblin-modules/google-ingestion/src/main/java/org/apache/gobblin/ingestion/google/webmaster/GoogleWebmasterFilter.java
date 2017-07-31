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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;

import com.google.api.services.webmasters.model.ApiDimensionFilter;
import com.google.api.services.webmasters.model.ApiDimensionFilterGroup;


public class GoogleWebmasterFilter {

  //Reference http://www.nationsonline.org/oneworld/country_code_list.htm for a full list of "ISO 3166-1 alpha-3 country code"
  private static HashSet<String> countryCodes;

  static {
    String[] countries = Locale.getISOCountries();
    countryCodes = new HashSet<>(countries.length);
    for (String country : countries) {
      Locale locale = new Locale("", country);
      countryCodes.add(locale.getISO3Country());
    }
  }

  enum Dimension {
    DATE, PAGE, COUNTRY, QUERY, DEVICE, SEARCH_TYPE, SEARCH_APPEARANCE
  }

  enum FilterOperator {
    EQUALS, CONTAINS, NOTCONTAINS
  }

  private static ApiDimensionFilter build(String dimension, String operator, String expression) {
    return new ApiDimensionFilter().setDimension(dimension).setOperator(operator).setExpression(expression);
  }

  static ApiDimensionFilter pageFilter(FilterOperator op, String expression) {
    //Operator string is case insensitive
    return build(Dimension.PAGE.toString(), op.toString(), expression);
  }

  static ApiDimensionFilter countryEqFilter(String country) {
    String countryCode = validateCountryCode(country);
    if (countryCode.equals("ALL")) {
      return null;
    }
    return build(Dimension.COUNTRY.toString(), FilterOperator.EQUALS.toString().toLowerCase(), countryCode);
  }

  static String countryFilterToString(ApiDimensionFilter countryFilter) {
    String country;
    if (countryFilter == null) {
      country = "ALL";
    } else {
      country = countryFilter.getExpression();
    }
    return country;
  }

  static ApiDimensionFilterGroup andGroupFilters(Collection<ApiDimensionFilter> filters) {
    if (filters == null || filters.isEmpty()) {
      return null;
    }
    List<ApiDimensionFilter> filtersList;
    if (filters instanceof List) {
      filtersList = (List<ApiDimensionFilter>) filters;
    } else {
      filtersList = new ArrayList<>(filters);
    }
    return new ApiDimensionFilterGroup().setFilters(filtersList).setGroupType("and");
  }

  static String validateCountryCode(String countryCode) {
    String upper = countryCode.toUpperCase();
    if (upper.equals("ALL") || countryCodes.contains(upper)) {
      return upper;
    }
    throw new RuntimeException(String.format(
        "Unknown country code '%s' in configuration file. Please provide a valid ISO 3166-1 alpha-3 country code. Use 'ALL' if you want to download data without a country filter.",
        countryCode));
  }
}
