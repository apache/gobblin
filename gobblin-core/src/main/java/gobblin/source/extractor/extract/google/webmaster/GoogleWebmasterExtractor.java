package gobblin.source.extractor.extract.google.webmaster;

import avro.shaded.com.google.common.collect.Iterables;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.api.services.webmasters.WebmastersScopes;
import com.google.api.services.webmasters.model.ApiDimensionFilter;
import com.google.common.base.Splitter;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.LongWatermark;
import gobblin.source.extractor.extract.google.GoogleCommon;
import gobblin.source.extractor.extract.google.GoogleCommonKeys;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static gobblin.configuration.ConfigurationKeys.*;
import static gobblin.source.extractor.extract.google.GoogleCommonKeys.*;


public class GoogleWebmasterExtractor implements Extractor<String, String[]> {
  private final static Logger LOG = LoggerFactory.getLogger(GoogleWebmasterExtractor.class);
  private final static Splitter splitter = Splitter.on(",").omitEmptyStrings().trimResults();
  private final String _schema;
  private final WorkUnitState _wuState;
  private final int _size;
  private final static DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
  private final static DateTimeFormatter watermarkFormatter = DateTimeFormat.forPattern("yyyyMMddHHmmss");
  private Queue<GoogleWebmasterExtractorIterator> _iterators = new ArrayDeque<>();
  /**
   * Each element keeps a mapping from API response order to output schema order.
   * The array index matches the order of API response.
   * The array values matches the order of output schema.
   */
  private Queue<int[]> _positionMaps = new ArrayDeque<>();

  private final DateTime _startDate;
  private final DateTime _endDate;
  private boolean _successful = false;

  public GoogleWebmasterExtractor(WorkUnitState wuState, long lowWatermark, long highWatermark,
      Map<String, Integer> columnPositionMap, List<GoogleWebmasterFilter.Dimension> requestedDimensions,
      List<GoogleWebmasterDataFetcher.Metric> requestedMetrics) throws IOException {
    this(wuState, lowWatermark, highWatermark, columnPositionMap, requestedDimensions, requestedMetrics,
        new GoogleWebmasterDataFetcherImpl(wuState.getProp(GoogleWebMasterSource.KEY_PROPERTY), getCredential(wuState),
            wuState.getProp(ConfigurationKeys.SOURCE_ENTITY), getHotStartJobs(wuState)));
  }

  private static List<ProducerJob> getHotStartJobs(WorkUnitState wuState) {
    String hotStartString = wuState.getProp(GoogleWebMasterSource.KEY_REQUEST_HOT_START, "");
    if (!hotStartString.isEmpty()) {
      return SimpleProducerJob.deserialize(hotStartString);
    }
    return new ArrayList<>();
  }

  private static Credential getCredential(WorkUnitState wuState) {
    String scope = wuState.getProp(GoogleCommonKeys.API_SCOPES, WebmastersScopes.WEBMASTERS_READONLY);
    Preconditions.checkArgument(
        Objects.equals(WebmastersScopes.WEBMASTERS_READONLY, scope) || Objects.equals(WebmastersScopes.WEBMASTERS,
            scope), "The scope for WebMaster must either be WEBMASTERS_READONLY or WEBMASTERS");

    String credentialFile = wuState.getProp(SOURCE_CONN_PRIVATE_KEY);
    List<String> scopes = Collections.singletonList(scope);

//    return GoogleCredential.fromStream(new FileInputStream(credentialFile))
//        .createScoped(Collections.singletonList(scope));

    return new GoogleCommon.CredentialBuilder(credentialFile, scopes).fileSystemUri(
        wuState.getProp(PRIVATE_KEY_FILESYSTEM_URI))
        .proxyUrl(wuState.getProp(SOURCE_CONN_USE_PROXY_URL))
        .port(wuState.getProp(SOURCE_CONN_USE_PROXY_PORT))
        .serviceAccountId(wuState.getProp(SOURCE_CONN_USERNAME))
        .build();
  }

  /**
   * For test only
   */
  GoogleWebmasterExtractor(WorkUnitState wuState, long lowWatermark, long highWatermark,
      Map<String, Integer> columnPositionMap, List<GoogleWebmasterFilter.Dimension> requestedDimensions,
      List<GoogleWebmasterDataFetcher.Metric> requestedMetrics, GoogleWebmasterDataFetcher dataFetcher) {
    _startDate = watermarkFormatter.parseDateTime(Long.toString(lowWatermark));
    _endDate = watermarkFormatter.parseDateTime(Long.toString(highWatermark));

    _schema = wuState.getWorkunit().getProp(ConfigurationKeys.SOURCE_SCHEMA);
    _size = columnPositionMap.size();
    _wuState = wuState;

    Iterable<Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter>> filterGroups = getFilterGroups(wuState);

    for (Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter> filters : filterGroups) {
      List<GoogleWebmasterFilter.Dimension> actualDimensionRequests = new ArrayList<>(requestedDimensions);
      //Need to remove the dimension from actualDimensionRequests if the filter for that dimension is ALL/Aggregated
      for (Map.Entry<GoogleWebmasterFilter.Dimension, ApiDimensionFilter> filter : filters.entrySet()) {
        if (filter.getValue() == null) {
          actualDimensionRequests.remove(filter.getKey());
        }
      }
      GoogleWebmasterExtractorIterator iterator =
          new GoogleWebmasterExtractorIterator(dataFetcher, dateFormatter.print(_startDate),
              dateFormatter.print(_endDate), actualDimensionRequests, requestedMetrics, filters, wuState);
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
      LOG.info(String.format("Successfully finished fetching data from Google Search Console from %s to %s.",
          dateFormatter.print(_startDate), dateFormatter.print(_endDate)));
      _wuState.setActualHighWatermark(
          new LongWatermark(Long.parseLong(watermarkFormatter.print(_endDate.plusDays(1)))));
    } else {
      LOG.warn(String.format("Had problems fetching all data from Google Search Console from %s to %s.",
          dateFormatter.print(_startDate), dateFormatter.print(_endDate)));
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
