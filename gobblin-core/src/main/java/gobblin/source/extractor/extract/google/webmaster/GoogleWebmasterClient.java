package gobblin.source.extractor.extract.google.webmaster;

import com.google.api.services.webmasters.model.ApiDimensionFilter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;


public interface GoogleWebmasterClient {

  enum Metric {
    CLICKS, IMPRESSIONS, CTR, POSITION
  }

  /**
   * Given (date, country), return as many unique pages with clicks as possible. The upper bound is limited by rowLimit.
   */
  Set<String> getAllPages(String date, GoogleWebmasterFilter.Country country, int rowLimit) throws IOException;

  /**
   * Return all fetched pages given the filters. Number of pages is limited by rowLimit
   */
  List<String> getPages(String date, int rowLimit, List<GoogleWebmasterFilter.Dimension> requestedDimensions,
      Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter> filterMap) throws IOException;

  /**
   * The return value is composed of [requestedDimensions, clicks, impressions, ctr, position]
   * @param date
   * @param rowLimit
   * @param requestedDimensions this serves as an list of dimension requests. The first part of the return value will match the requestDimensions
   * @param filterMap filters of your request
   * @return
   * @throws IOException
   */
  List<String[]> doQuery(String date, int rowLimit, List<GoogleWebmasterFilter.Dimension> requestedDimensions,
      List<Metric> requestedMetrics, Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter> filterMap)
      throws IOException;
}
