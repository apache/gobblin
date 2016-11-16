package gobblin.source.extractor.extract.google.webmaster;

import avro.shaded.com.google.common.collect.Iterables;
import com.google.api.services.webmasters.WebmastersScopes;
import com.google.api.services.webmasters.model.ApiDimensionFilter;
import com.google.common.base.Splitter;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.LongWatermark;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static gobblin.configuration.ConfigurationKeys.*;


public class GoogleWebmasterExtractor implements Extractor<String, String[]> {
  private final static Logger LOG = LoggerFactory.getLogger(GoogleWebmasterExtractor.class);
  private final static Splitter splitter = Splitter.on(",").omitEmptyStrings().trimResults();
  private final String _schema;
  private final WorkUnitState _wuState;
  private final DateTimeFormatter _watermarkFormatter;
  private final int _size;
  private Queue<GoogleWebmasterExtractorIterator> _iterators = new ArrayDeque<>();
  /**
   * Each element keeps a mapping from API response order to output schema order.
   * The array index matches the order of API response.
   * The array values matches the order of output schema.
   */
  private Queue<int[]> _positionMaps = new ArrayDeque<>();
  private final DateTime _currentDate;
  private boolean _successful = false;

  public GoogleWebmasterExtractor(WorkUnitState wuState, Map<String, Integer> columnPositionMap,
      List<GoogleWebmasterFilter.Dimension> requestedDimensions,
      List<GoogleWebmasterDataFetcher.Metric> requestedMetrics) throws IOException {
    this(wuState, columnPositionMap, requestedDimensions, requestedMetrics,
        new GoogleWebmasterDataFetcherImpl(wuState.getProp(GoogleWebMasterSource.KEY_PROPERTY),
            wuState.getPropAsBoolean(GoogleWebMasterSource.KEY_REQUEST_HYPERCRITICAL, false),
            wuState.getProp(GoogleWebMasterSource.KEY_CREDENTIAL_LOCATION),
            wuState.getProp(ConfigurationKeys.SOURCE_ENTITY),
            wuState.getProp(GoogleWebMasterSource.KEY_API_SCOPE, WebmastersScopes.WEBMASTERS_READONLY)));
  }

  public GoogleWebmasterExtractor(WorkUnitState wuState, Map<String, Integer> columnPositionMap,
      List<GoogleWebmasterFilter.Dimension> requestedDimensions,
      List<GoogleWebmasterDataFetcher.Metric> requestedMetrics, GoogleWebmasterDataFetcher dataFetcher) {
    _watermarkFormatter = DateTimeFormat.forPattern("yyyyMMddHHmmss")
        .withZone(DateTimeZone.forID(wuState.getProp(SOURCE_TIMEZONE, DEFAULT_SOURCE_TIMEZONE)));
    _currentDate = _watermarkFormatter.parseDateTime(
        Long.toString(wuState.getWorkunit().getLowWatermark(LongWatermark.class).getValue()));
    String dateString = DateTimeFormat.forPattern("yyyy-MM-dd").print(_currentDate);
    LOG.info("Creating GoogleWebmasterExtractor for " + dateString);
    _schema = wuState.getWorkunit().getProp(ConfigurationKeys.SOURCE_SCHEMA);
    _size = columnPositionMap.size();
    _wuState = wuState;

    Iterable<Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter>> filterGroups = getFilterGroups(wuState);
    int pageLimit =
        wuState.getPropAsInt(GoogleWebMasterSource.KEY_REQUEST_PAGE_LIMIT, GoogleWebmasterClient.API_ROW_LIMIT);
    int queryLimit =
        wuState.getPropAsInt(GoogleWebMasterSource.KEY_REQUEST_QUERY_LIMIT, GoogleWebmasterClient.API_ROW_LIMIT);

    for (Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter> filters : filterGroups) {
      List<GoogleWebmasterFilter.Dimension> actualDimensionRequests = new ArrayList<>(requestedDimensions);
      //Need to remove the dimension from actualDimensionRequests if the filter for that dimension is ALL/Aggregated
      for (Map.Entry<GoogleWebmasterFilter.Dimension, ApiDimensionFilter> filter : filters.entrySet()) {
        if (filter.getValue() == null) {
          actualDimensionRequests.remove(filter.getKey());
        }
      }
      GoogleWebmasterExtractorIterator iterator =
          new GoogleWebmasterExtractorIterator(dataFetcher, dateString, actualDimensionRequests, requestedMetrics,
              filters, pageLimit, queryLimit);
      //positionMapping is to address the problems that requested dimensions/metrics order might be different from the column order in source.schema
      int[] positionMapping = new int[actualDimensionRequests.size() + requestedMetrics.size()];
      int i = 0;
      for (; i < actualDimensionRequests.size(); ++i) {
        positionMapping[i] = columnPositionMap.get(actualDimensionRequests.get(i).toString());
      }
      for (GoogleWebmasterDataFetcher.Metric requestedMetric : requestedMetrics) {
        positionMapping[i++] = columnPositionMap.get(requestedMetric.toString());
      }
      _iterators.add(iterator);
      _positionMaps.add(positionMapping);
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
  public String getSchema() throws IOException {
    return this._schema;
  }

  @Override
  public String[] readRecord(@Deprecated String[] reuse) throws DataRecordException, IOException {
    while (!_iterators.isEmpty()) {
      GoogleWebmasterExtractorIterator iterator = _iterators.peek();
      int[] positionMap = _positionMaps.peek();
      if (iterator.hasNext()) {
        String[] apiResponse = iterator.next();
        String[] record = new String[_size];
        for (int i = 0; i < positionMap.length; ++i) {
          record[positionMap[i]] = apiResponse[i];
        }
        //unfilled elements should be nullable.
        return record;
      }
      _iterators.remove();
      _positionMaps.remove();
    }

//    if (_currentDate.dayOfMonth().get() == 1) {
//      throw new DataRecordException("Fail me!!! at " + _currentDate.toString());
//    }
    _successful = true;
    return null;
  }

  @Override
  public long getExpectedRecordCount() {
    return 0;
  }

  @Override
  public long getHighWatermark() {
    throw new UnsupportedOperationException("This method has been deprecated!");
  }

  @Override
  public void close() throws IOException {
    if (_successful) {
      LOG.info("Successfully finished fetching data from Google Search Console for " + _currentDate.toString());
      _wuState.setActualHighWatermark(
          new LongWatermark(Long.parseLong(_watermarkFormatter.print(_currentDate.plusDays(1)))));
    } else {
      LOG.warn("Had problems fetching all data from Google Search Console for " + _currentDate.toString());
    }
  }

  /**
   * For test only
   */
  Queue<GoogleWebmasterExtractorIterator> getIterators() {
    return _iterators;
  }

  /**
   * For test only
   */
  Queue<int[]> getPositionMaps() {
    return _positionMaps;
  }
}
