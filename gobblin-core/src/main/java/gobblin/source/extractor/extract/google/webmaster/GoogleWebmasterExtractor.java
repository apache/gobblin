package gobblin.source.extractor.extract.google.webmaster;

import com.google.api.services.webmasters.model.ApiDimensionFilter;
import com.google.common.base.Splitter;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.LongWatermark;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


//Doesn't implement Iterator<String[]> because I want to throw exception.
class GoogleWebmasterDataIterator {
  private final GoogleWebmasterClient _webmaster;
  private final String _date;
  private final GoogleWebmasterFilter.Country _country;
  private final int _pageLimit;
  private final int _queryLimit;
  private Deque<String> _cachedPages = null;
  private Deque<String[]> _cachedQueries = new LinkedList<>();

  private final Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter> _filterMap = new HashMap<>();
  private final List<GoogleWebmasterFilter.Dimension> _requestedDimensions;

  public GoogleWebmasterDataIterator(GoogleWebmasterClient webmaster, String date,
      GoogleWebmasterFilter.Country country, int pageLimit, int queryLimit) {
    _webmaster = webmaster;
    _date = date;
    _country = country;
    _pageLimit = pageLimit;
    _queryLimit = queryLimit;

    //The requested dimension order must match the order of output schema
    if (_country == GoogleWebmasterFilter.Country.ALL) {
      _requestedDimensions = Arrays.asList(GoogleWebmasterFilter.Dimension.DATE, GoogleWebmasterFilter.Dimension.PAGE,
          GoogleWebmasterFilter.Dimension.QUERY);
    } else {
      _requestedDimensions =
          Arrays.asList(GoogleWebmasterFilter.Dimension.DATE, GoogleWebmasterFilter.Dimension.COUNTRY,
              GoogleWebmasterFilter.Dimension.PAGE, GoogleWebmasterFilter.Dimension.QUERY);
      _filterMap.put(GoogleWebmasterFilter.Dimension.COUNTRY, GoogleWebmasterFilter.countryFilter(_country));
    }
  }

  public boolean hasNext() throws IOException {
    if (_cachedPages == null) {
      _cachedPages = new ArrayDeque<>(_webmaster.getAllPages(_date, _country, _pageLimit));
    }

    if (!_cachedQueries.isEmpty()) {
      return true;
    }

    while (_cachedQueries.isEmpty()) {
      if (_cachedPages.isEmpty()) {
        return false;
      }
      String nextPage = _cachedPages.remove();
      _filterMap.remove(GoogleWebmasterFilter.Dimension.PAGE);
      _filterMap.put(GoogleWebmasterFilter.Dimension.PAGE,
          GoogleWebmasterFilter.pageFilter(GoogleWebmasterFilter.FilterOperator.EQUALS, nextPage));
      List<String[]> response = _webmaster.doQuery(_date, _queryLimit, _requestedDimensions, _filterMap);
      if (_country == GoogleWebmasterFilter.Country.ALL) {
        //TODO: can this be generalized?
        for (String[] r : response) {
          String[] countryIsGlobal = new String[8];
          countryIsGlobal[0] = r[0];
          countryIsGlobal[1] = null; //country is global, set to null
          System.arraycopy(r, 1, countryIsGlobal, 2, r.length - 1);
          _cachedQueries.add(countryIsGlobal);
        }
      } else {
        _cachedQueries = new ArrayDeque<>(response);
      }
    }
    return true;
  }

  public String[] next() throws IOException {
    if (hasNext()) {
      return _cachedQueries.remove();
    }
    return null;
  }
}

public class GoogleWebmasterExtractor implements Extractor<String, String[]> {
  private final static Logger LOG = LoggerFactory.getLogger(GoogleWebmasterExtractor.class);
  private final static Splitter splitter = Splitter.on(",").omitEmptyStrings().trimResults();
  private final String _schema;
  private final WorkUnitState workUnitState;
  private Queue<GoogleWebmasterDataIterator> _iterators = new ArrayDeque<>();

  public GoogleWebmasterExtractor(WorkUnitState workUnitState) {
    this.workUnitState = workUnitState;
    _schema = workUnitState.getWorkunit().getProp(ConfigurationKeys.SOURCE_SCHEMA);

    int pageLimit = Integer.parseInt(workUnitState.getProp(GoogleWebMasterSource.KEY_REQUEST_PAGE_LIMIT));
    int queryLimit = Integer.parseInt(workUnitState.getProp(GoogleWebMasterSource.KEY_REQUEST_QUERY_LIMIT));
    String dateString = workUnitState.getProp(GoogleWebMasterSource.KEY_REQUEST_DATE);

    try {
      String property = workUnitState.getProp(GoogleWebMasterSource.KEY_PROPERTY);
      String credential = workUnitState.getProp(GoogleWebMasterSource.KEY_CREDENTIAL_LOCATION);
      Iterable<String> filters = splitter.split(workUnitState.getProp(GoogleWebMasterSource.KEY_PREDEFINED_FILTERS));
      String appName = workUnitState.getProp(ConfigurationKeys.SOURCE_ENTITY);
      String scope = workUnitState.getProp(GoogleWebMasterSource.KEY_API_SCOPE);
      GoogleWebmasterClientImpl webmaster =
          new GoogleWebmasterClientImpl(property, credential, appName, scope, filters);

      Iterable<String> countries = splitter.split(workUnitState.getProp(GoogleWebMasterSource.KEY_COUNTRIES));
      for (String country : countries) {
        _iterators.add(new GoogleWebmasterDataIterator(webmaster, dateString,
            GoogleWebmasterFilter.Country.valueOf(country.toUpperCase()), pageLimit, queryLimit));
      }
    } catch (IOException e) {
      LOG.error("Failed to create Google webmaster API client: " + e.getMessage());
    }
  }

  @Override
  public String getSchema() throws IOException {
    return this._schema;
  }

  @Override
  public String[] readRecord(@Deprecated String[] reuse) throws DataRecordException, IOException {
    while (!_iterators.isEmpty()) {
      GoogleWebmasterDataIterator iterator = _iterators.peek();
      if (iterator.hasNext()) {
        return iterator.next();
      }
      _iterators.remove();
    }
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
    State previousTableState = workUnitState.getPreviousTableState();
    workUnitState.setActualHighWatermark(
        new LongWatermark(previousTableState.getPropAsInt(ConfigurationKeys.SOURCE_QUERYBASED_END_VALUE, 0) + 100));
  }
}
