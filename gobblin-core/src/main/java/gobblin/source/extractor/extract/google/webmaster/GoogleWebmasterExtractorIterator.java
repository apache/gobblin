package gobblin.source.extractor.extract.google.webmaster;

import com.google.api.services.webmasters.model.ApiDimensionFilter;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


//Doesn't implement Iterator<String[]> because I want to throw exception.
class GoogleWebmasterExtractorIterator {
  private final GoogleWebmasterClient _webmaster;
  private final String _date;
  private final GoogleWebmasterFilter.Country _country;
  private final int _pageLimit;
  private final int _queryLimit;
  private Deque<String> _cachedPages = null;
  private Deque<String[]> _cachedQueries = new LinkedList<>();

  private final Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter> _filterMap = new HashMap<>();
  private final List<GoogleWebmasterFilter.Dimension> _requestedDimensions;

  public GoogleWebmasterExtractorIterator(GoogleWebmasterClient webmaster, String date,
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