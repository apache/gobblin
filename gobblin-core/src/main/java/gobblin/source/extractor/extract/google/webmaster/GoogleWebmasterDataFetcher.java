package gobblin.source.extractor.extract.google.webmaster;

import com.google.api.services.webmasters.model.ApiDimensionFilter;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;


/**
 * GoogleWebmasterDataFetcher implements the logic to download all query data(e.g. page, query, clicks, impressions, CTR, position) for a given date and country.
 *
 * The user of this GoogleWebmasterDataFetcher should follow these steps:
 * 1. Call getAllPages to get all pages based on your filters.
 * 2. For each page we get at last step, call doQuery to get query data. If query data is not available, then this page won't be included in the return value.
 *
 * Note:
 * Due to the limitation of the Google API -- each API request can return at most 5000 rows of records, you need to take additional steps to fetch all data that the Google Search Console keeps.
 *
 * There is a rule to check whether Google Search Console keeps more than 5000 rows of data based on your request. The rule is that if you request for 5000 rows, but the response has less than 5000 rows; then in this case you've got all data that the Google service provides. If you request for 5000 rows, and the API returns you 5000 rows, there is a high chance that the Google service has more data matching your query, but due to the limitation of the API, only first 5000 rows returned.
 *
 */
public interface GoogleWebmasterDataFetcher {

  enum Metric {
    CLICKS, IMPRESSIONS, CTR, POSITION
  }

  /**
   * Return all pages given (date, country) filter
   * @param date date string
   * @param country country code string
   * @param rowLimit this is mostly for testing purpose. In order to get all pages, set this to the API row limit, which is 5000
   */
  Set<String> getAllPages(String date, String country, int rowLimit) throws IOException;

  /**
   * @param date date filter
   * @param rowLimit row limit for this API call
   * @param requestedDimensions a list of dimension requests. The dimension values can be found at the first part of the return value
   * @param filters filters of your request
   * @return the response from the API call. The value is composed of [[requestedDimension list], clicks, impressions, ctr, position]
   */
  List<String[]> doQuery(String date, int rowLimit, List<GoogleWebmasterFilter.Dimension> requestedDimensions,
      List<Metric> requestedMetrics, Collection<ApiDimensionFilter> filters) throws IOException;
}

