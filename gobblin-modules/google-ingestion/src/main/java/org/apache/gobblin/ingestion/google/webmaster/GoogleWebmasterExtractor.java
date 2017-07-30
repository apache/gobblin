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

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.api.services.webmasters.model.ApiDimensionFilter;
import com.google.common.base.Splitter;
import com.google.gson.JsonArray;

import avro.shaded.com.google.common.collect.Iterables;
import lombok.extern.slf4j.Slf4j;

import gobblin.annotation.Alpha;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.LongWatermark;


@Slf4j
@Alpha
public class GoogleWebmasterExtractor implements Extractor<String, String[]> {

  private final static Splitter splitter = Splitter.on(",").omitEmptyStrings().trimResults();
  private final JsonArray _schema;
  private final WorkUnitState _wuState;
  private final DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
  private final boolean _includeSource;
  /**
   * _current is an index that indicates which iterator is under processing.
   */
  private int _current = 0;
  private List<GoogleWebmasterExtractorIterator> _iterators = new ArrayList<>();
  /**
   * Each element keeps a mapping from API response order to output schema order.
   * The array index matches the order of API response.
   * The array values matches the order of output schema.
   */
  private List<int[]> _positionMaps = new ArrayList<>();

  private final DateTime _startDate;
  private final long _expectedHighWaterMark;
  private final DateTime _expectedHighWaterMarkDate;

  public GoogleWebmasterExtractor(GoogleWebmasterClient gscClient, WorkUnitState wuState, long lowWatermark,
      long expectedHighWaterMark, Map<String, Integer> columnPositionMap,
      List<GoogleWebmasterFilter.Dimension> requestedDimensions,
      List<GoogleWebmasterDataFetcher.Metric> requestedMetrics, JsonArray schemaJson)
      throws IOException {
    this(wuState, lowWatermark, expectedHighWaterMark, columnPositionMap, requestedDimensions, requestedMetrics,
        schemaJson,
        createGoogleWebmasterDataFetchers(wuState.getProp(GoogleWebMasterSource.KEY_PROPERTY), gscClient, wuState));
  }

  private static List<GoogleWebmasterDataFetcher> createGoogleWebmasterDataFetchers(String properties,
      GoogleWebmasterClient gscClient, WorkUnitState wuState)
      throws IOException {
    List<GoogleWebmasterDataFetcher> fetchers = new ArrayList<>();
    Iterable<String> props = splitter.split(properties);
    for (String prop : props) {
      fetchers.add(new GoogleWebmasterDataFetcherImpl(prop, gscClient, wuState));
    }
    return fetchers;
  }

  /**
   * For test only
   */
  GoogleWebmasterExtractor(WorkUnitState wuState, long lowWatermark, long expectedHighWaterMark,
      Map<String, Integer> columnPositionMap, List<GoogleWebmasterFilter.Dimension> requestedDimensions,
      List<GoogleWebmasterDataFetcher.Metric> requestedMetrics, JsonArray schemaJson,
      List<GoogleWebmasterDataFetcher> dataFetchers) {
    DateTimeFormatter watermarkFormatter = DateTimeFormat.forPattern("yyyyMMddHHmmss");
    _startDate = watermarkFormatter.parseDateTime(Long.toString(lowWatermark));
    _expectedHighWaterMark = expectedHighWaterMark;
    _expectedHighWaterMarkDate = watermarkFormatter.parseDateTime(Long.toString(expectedHighWaterMark));
    log.info(String.format("Creating GoogleWebmasterExtractor for [%s, %s] for job %s.", _startDate.toString(),
        _expectedHighWaterMarkDate.toString(), wuState.getProp(ConfigurationKeys.SOURCE_ENTITY)));

    _wuState = wuState;
    _schema = schemaJson;
    _includeSource = wuState.getWorkunit().getPropAsBoolean(GoogleWebMasterSource.KEY_INCLUDE_SOURCE_PROPERTY,
        GoogleWebMasterSource.DEFAULT_INCLUDE_SOURCE_PROPERTY);

    Iterable<Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter>> filterGroups = getFilterGroups(wuState);

    for (GoogleWebmasterDataFetcher dataFetcher : dataFetchers) {
      for (Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter> filters : filterGroups) {
        List<GoogleWebmasterFilter.Dimension> actualDimensionRequests = new ArrayList<>(requestedDimensions);
        //Need to remove the dimension from actualDimensionRequests if the filter for that dimension is ALL/Aggregated
        for (Map.Entry<GoogleWebmasterFilter.Dimension, ApiDimensionFilter> filter : filters.entrySet()) {
          if (filter.getValue() == null) {
            actualDimensionRequests.remove(filter.getKey());
          }
        }
        String startDate = dateFormatter.print(_startDate);
        String endDate = dateFormatter.print(_expectedHighWaterMarkDate);
        GoogleWebmasterExtractorIterator iterator =
            new GoogleWebmasterExtractorIterator(dataFetcher, startDate, endDate, actualDimensionRequests,
                requestedMetrics, filters, wuState);
        log.info("Created " + iterator.toString());
        // positionMapping is to address the cases when requested dimensions/metrics order
        // is different from the column order in source.schema
        int[] positionMapping = new int[actualDimensionRequests.size() + requestedMetrics.size()];
        int i = 0;
        for (; i < actualDimensionRequests.size(); ++i) {
          positionMapping[i] = columnPositionMap.get(actualDimensionRequests.get(i).toString());
        }
        for (GoogleWebmasterDataFetcher.Metric requestedMetric : requestedMetrics) {
          positionMapping[i++] = columnPositionMap.get(requestedMetric.toString());
        }
        //One positionMapping is corresponding to one iterator.
        _iterators.add(iterator);
        _positionMaps.add(positionMapping);
      }
    }
  }

  /**
   * Currently, the filter group is just one filter at a time, there is no cross-dimension filters combination.
   * TODO: May need to implement this feature in the future based on use cases.
   */
  private Iterable<Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter>> getFilterGroups(WorkUnitState wuState) {
    List<Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter>> filters = new ArrayList<>();
    for (String filter : splitter.split(wuState.getProp(GoogleWebMasterSource.KEY_REQUEST_FILTERS))) {
      String[] parts = Iterables.toArray(Splitter.on(".").split(filter), String.class);
      String dimString = parts[0].toUpperCase();
      String valueString = parts[1].toUpperCase();

      GoogleWebmasterFilter.Dimension dimension = GoogleWebmasterFilter.Dimension.valueOf(dimString);
      Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter> map = new HashMap<>();

      if (dimension == GoogleWebmasterFilter.Dimension.COUNTRY) {
        map.put(GoogleWebmasterFilter.Dimension.COUNTRY, GoogleWebmasterFilter.countryEqFilter(valueString));
      } else {
        throw new UnsupportedOperationException("Only country filter is supported for now");
      }
      filters.add(map);
    }
    return filters;
  }

  @Override
  public String getSchema()
      throws IOException {
    return _schema.toString();
  }

  @Override
  public String[] readRecord(@Deprecated String[] reuse)
      throws DataRecordException, IOException {
    while (_current < _iterators.size()) {
      GoogleWebmasterExtractorIterator iterator = _iterators.get(_current);
      if (iterator.isFailed()) {
        log.info(String.format("Extractor failed at iterator %d: %s", _current, iterator.toString()));
        // Task retry reuses the same extractor instead of creating a new one.
        // Reinitialize processed iterators and set extractor to restart from the very beginning
        for (int i = 0; i <= _current; ++i) {
          _iterators.set(i, new GoogleWebmasterExtractorIterator(_iterators.get(i)));
        }
        log.info(String.format("Resetting _current index from %d to 0 to restart from the beginning", _current));
        _current = 0;
        iterator = _iterators.get(_current);
      }
      int[] positionMap = _positionMaps.get(_current);
      if (iterator.hasNext()) {
        String[] apiResponse = iterator.next();
        int size = _schema.size();
        String[] record = new String[size];
        for (int i = 0; i < positionMap.length; ++i) {
          record[positionMap[i]] = apiResponse[i];
        }
        //unfilled elements should be nullable.
        if (_includeSource) {
          record[size - 1] = iterator.getProperty();
        }
        return record;
      }
      log.info(iterator.toString() + " finished successfully. ^_^");
      ++_current;
    }
    return null;
  }

  @Override
  public long getExpectedRecordCount() {
    if (_current == _iterators.size()) {
      //Any positive number will be okay.
      //Need to add this because of this commit:
      //76ae45a by ibuenros on 12/20/16 at 11:34AM Query based source will reset to low watermark if previous run did not process any data for that table.
      return 1;
    }
    return 0;
  }

  @Override
  public long getHighWatermark() {
    throw new UnsupportedOperationException("This method has been deprecated!");
  }

  @Override
  public void close()
      throws IOException {
    if (_current == _iterators.size()) {
      log.info(String.format("Successfully finished fetching data from Google Search Console from %s to %s.",
          dateFormatter.print(_startDate), dateFormatter.print(_expectedHighWaterMarkDate)));
      _wuState.setActualHighWatermark(new LongWatermark(_expectedHighWaterMark));
    } else {
      log.error(String.format("Had problems fetching data from Google Search Console from %s to %s.",
          dateFormatter.print(_startDate), dateFormatter.print(_expectedHighWaterMarkDate)));
    }
  }

  /**
   * For test only
   */
  List<GoogleWebmasterExtractorIterator> getIterators() {
    return _iterators;
  }

  /**
   * For test only
   */
  List<int[]> getPositionMaps() {
    return _positionMaps;
  }
}
