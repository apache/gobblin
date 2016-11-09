package gobblin.source.extractor.extract.google.webmaster;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.api.services.webmasters.model.ApiDimensionFilter;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

//Doesn't implement Iterator<String[]> because I want to throw exception.

/**
 *This iterator iterates the data set given all the constraints
 */
class GoogleWebmasterExtractorIterator {
  private final GoogleWebmasterClient _webmaster;
  private final String _date;
  private final int _pageLimit;
  private final int _queryLimit;
  private GoogleWebmasterFilter.Country _country;
  private Deque<String> _cachedPages = null;
  private Deque<String[]> _cachedQueries = new LinkedList<>();

  private final Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter> _filterMap;
  //This is the requested dimensions sent to Google API
  private final List<GoogleWebmasterFilter.Dimension> _requestedDimensions;
  private final List<GoogleWebmasterClient.Metric> _requestedMetrics;

  public GoogleWebmasterExtractorIterator(GoogleWebmasterClient webmaster, String date,
      List<GoogleWebmasterFilter.Dimension> requestedDimensions, List<GoogleWebmasterClient.Metric> requestedMetrics,
      Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter> filterMap, int pageLimit, int queryLimit) {
    Preconditions.checkArgument(!filterMap.containsKey(GoogleWebmasterFilter.Dimension.PAGE),
        "Doesn't support filters for page for the time being. Will implement support later. If page filter is provided, the code won't take the responsibility of get all pages, so it will just return all queries for that page.");

    _webmaster = webmaster;
    _date = date;
    _requestedDimensions = requestedDimensions;
    _requestedMetrics = requestedMetrics;
    _filterMap = filterMap;
    _pageLimit = pageLimit;
    _queryLimit = queryLimit;

    ApiDimensionFilter countryFilter = filterMap.get(GoogleWebmasterFilter.Dimension.COUNTRY);
    if (countryFilter == null) {
      _country = GoogleWebmasterFilter.Country.ALL;
    } else {
      _country = GoogleWebmasterFilter.Country.valueOf(countryFilter.getExpression().toUpperCase());
    }
  }

  /**
   * @return the requested dimensions that is sent to Google API
   */
  public List<GoogleWebmasterFilter.Dimension> getRequestedDimensions() {
    return _requestedDimensions;
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
      List<String[]> response =
          _webmaster.doQuery(_date, _queryLimit, _requestedDimensions, _requestedMetrics, _filterMap);
      _cachedQueries = new ArrayDeque<>(response);
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