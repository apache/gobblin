package gobblin.source.extractor.extract.google.webmaster;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.api.services.webmasters.model.ApiDimensionFilter;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//Doesn't implement Iterator<String[]> because I want to throw exception.

/**
 * This iterator holds a GoogleWebmasterDataFetcher, through which it get all pages. And then for each page, it will get all query data(Clicks, Impressions, CTR, Position). Basically, it will cache all pages got, and for each page, cache the detailed query data, and then iterate through them one by one.
 */
class GoogleWebmasterExtractorIterator {
  private final static Logger LOG = LoggerFactory.getLogger(GoogleWebmasterExtractorIterator.class);
  private final GoogleWebmasterDataFetcher _webmaster;
  private final String _date;
  private final int _pageLimit;
  private final int _queryLimit;
  private final GoogleWebmasterFilter.Country _country;
  private Deque<String> _cachedPages = null;
  private int _totalPages;
  private int _pageCheckPoint;
  private Deque<String[]> _cachedQueries = new ArrayDeque<>();

  private final Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter> _filterMap;
  //This is the requested dimensions sent to Google API
  private final List<GoogleWebmasterFilter.Dimension> _requestedDimensions;
  private final List<GoogleWebmasterDataFetcher.Metric> _requestedMetrics;

  public GoogleWebmasterExtractorIterator(GoogleWebmasterDataFetcher webmaster, String date,
      List<GoogleWebmasterFilter.Dimension> requestedDimensions,
      List<GoogleWebmasterDataFetcher.Metric> requestedMetrics,
      Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter> filterMap, int pageLimit, int queryLimit) {
    Preconditions.checkArgument(!filterMap.containsKey(GoogleWebmasterFilter.Dimension.PAGE),
        "Doesn't support filters for page for the time being. Will implement support later. If page filter is provided, the code won't take the responsibility of get all pages, so it will just return all queries for that page.");

    _webmaster = webmaster;
    _date = date;
    _requestedDimensions = requestedDimensions;
    _requestedMetrics = requestedMetrics;
    _filterMap = new HashMap<>(filterMap);
    _pageLimit = pageLimit;
    _queryLimit = queryLimit;
    _country = GoogleWebmasterFilter.countryFilterToEnum(filterMap.get(GoogleWebmasterFilter.Dimension.COUNTRY));
  }

  public boolean hasNext() throws IOException {
    initialize();

    if (!_cachedQueries.isEmpty()) {
      return true;
    }

    while (_cachedQueries.isEmpty()) {
      if (_cachedPages.isEmpty()) {
        return false;
      }
      String nextPage = _cachedPages.remove();
      if (_cachedPages.size() % _pageCheckPoint == 0) {
        //Report progress every 5%
        LOG.info(String.format("Country-%s iterator progress: %d out of %d left to be processed", _country,
            _cachedPages.size(), _totalPages));
      }

      _filterMap.remove(GoogleWebmasterFilter.Dimension.PAGE);
      _filterMap.put(GoogleWebmasterFilter.Dimension.PAGE,
          GoogleWebmasterFilter.pageFilter(GoogleWebmasterFilter.FilterOperator.EQUALS, nextPage));
      List<String[]> response = _webmaster.doQuery(_date, _queryLimit, _requestedDimensions, _requestedMetrics,
          new ArrayList<>(_filterMap.values()));
      _cachedQueries = new ArrayDeque<>(response);
    }
    return true;
  }

  private void initialize() throws IOException {
    if (_cachedPages == null) {
      _cachedPages = new ArrayDeque<>(_webmaster.getAllPages(_date, _country, _pageLimit));
      _totalPages = _cachedPages.size();
      _pageCheckPoint = Math.max(1, (int) Math.round(Math.ceil(_totalPages / 20.0)));
    }
  }

  public String[] next() throws IOException {
    if (hasNext()) {
      return _cachedQueries.remove();
    }
    return null;
  }

  /**
   * For test only
   */
  GoogleWebmasterFilter.Country getCountry() {
    return _country;
  }
}