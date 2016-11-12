package gobblin.source.extractor.extract.google.webmaster;

import com.google.api.services.webmasters.model.ApiDimensionFilter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * The goal of this client, combined with GoogleWebmasterExtractorIterator, is to get as many (Page, Query) pairs with data(Clicks, Impressions, CTR, Position) as possible.
 * Details about Google Webmaster or Google Search Console API can be found at https://developers.google.com/webmaster-tools/
 *
 * The user of this GoogleWebmasterClient should follow these steps:
 * 1. Call getAllPages to get all pages based on your filters.
 * 2. For each page we get at last step, call doQuery to get query data. If query data is not available, then this page won't be included in the return value.
 *
 * Note:
 * Due to the limitation of the Google API -- each API request can return at most 5000 rows of records, you need to take additional steps to fetch all data that the Google Search Console keeps.
 *
 * There is a rule to check whether Google Search Console keeps more than 5000 rows of data based on your request. The rule is that if you request for 5000 rows, but the response has less than 5000 rows; then in this case you've got all data that the Google service provides. If you request for 5000 rows, and the API returns you 5000 rows, there is a high chance that the Google service has more data matching your query, but due to the limitation of the API, only first 5000 rows returned.
 *
 * The standard implementation of this client, GoogleWebmasterClientImpl, has the additional steps that it partition the request into more granular pieces and resend each one in order to get as much data as possible.
 *
 * For example, you asked for all pages for your https://www.linkedin.com/ property and you got more than 5000 pages.
 * Then you can send requests for pages starting with https://www.linkedin.com/topic/, https://www.linkedin.com/jobs/, https://www.linkedin.com/groups/ etc.. If any request has 5000 rows, you can go to a lower level recursively.
 *
 */
public interface GoogleWebmasterClient {

  enum Metric {
    CLICKS, IMPRESSIONS, CTR, POSITION
  }

  /**
   * Given (date, country), return as many unique pages with clicks as possible. The upper bound is limited by rowLimit.
   * If rowLimit == API_ROW_LIMIT_MAX(5000), and the response gives 5000 rows, then this function will create page
   * filters with partitions to send multiple requests to get as many pages as possible.
   *
   * The page filter partitions will get more and more granular if the response keeps hitting API_ROW_LIMIT_MAX(5000).
   */
  Set<String> getAllPages(String date, GoogleWebmasterFilter.Country country, int rowLimit) throws IOException;

  /**
   * @param date date filter
   * @param rowLimit row limit for this API call
   * @param requestedDimensions a list of dimension requests. The dimension values can be found at the first part of the return value
   * @param filterMap filters of your request
   * @return the response from the API call. The value is composed of [[requestedDimension list], clicks, impressions, ctr, position]
   * @throws IOException
   */
  List<String[]> doQuery(String date, int rowLimit, List<GoogleWebmasterFilter.Dimension> requestedDimensions,
      List<Metric> requestedMetrics, Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter> filterMap)
      throws IOException;
}
