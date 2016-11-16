package gobblin.source.extractor.extract.google.webmaster;

import com.google.api.services.webmasters.model.ApiDimensionFilter;
import com.google.api.services.webmasters.model.ApiDimensionFilterGroup;
import com.google.api.services.webmasters.model.SearchAnalyticsQueryResponse;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;


/**
 * Provide access to the basic features of Google Webmaster API.
 * Details about Google Webmaster or Google Search Console API can be found at https://developers.google.com/webmaster-tools/
 */
public abstract class GoogleWebmasterClient {
  public static final int API_ROW_LIMIT = 5000;

  /**
   * Return all pages given all constraints.
   * @param date date string with format "yyyy-MM-dd"
   * @param country country code string
   * @param rowLimit limit the number of rows returned by the API. The API maximum limit is 5000.
   * @param requestedDimensions requested dimensions of the API call.
   * @param filters a list of filters. Include the country filter if you need it, even though you've provided the country string.
   * @param startRow this is a 0 based index configuration to set the starting row of your API request. Even though the API row limit is 5000, you can send a request starting from row 5000, so you will be able to get data from row 5000 to row 9999.
   * @return Return all pages given all constraints.
   */
  public abstract List<String> getPages(String siteProperty, String date, String country, int rowLimit,
      List<GoogleWebmasterFilter.Dimension> requestedDimensions, List<ApiDimensionFilter> filters, int startRow)
      throws IOException;

  public Callable<List<String>> getPagesCallable(final String siteProperty, final String date, final String country,
      final int rowLimit, final List<GoogleWebmasterFilter.Dimension> requestedDimensions,
      final List<ApiDimensionFilter> filters, final int startRow) {
    final GoogleWebmasterClient client = this;
    return new Callable<List<String>>() {
      @Override
      public List<String> call() throws Exception {
        return client.getPages(siteProperty, date, country, rowLimit, requestedDimensions, filters, startRow);
      }
    };
  }

  /**
   * Perform the api call for search analytics query
   * @param siteProperty your site property string
   * @param date date string with format "yyyy-MM-dd"
   * @param dimensions your requested dimensions
   * @param filterGroup filters for your API request. Provide your filters in a group, find utility functions in GoogleWebmasterFilter
   * @param rowLimit the row limit for your API response
   * @param startRow this is a 0 based index configuration to set the starting row of your API request. Even though the API row limit is 5000, you can send a request starting from row 5000, so you will be able to get data from row 5000 to row 9999.
   * @return return the response of Google Webmaster API
   */
  public abstract SearchAnalyticsQueryResponse searchAnalyticsQuery(String siteProperty, String date,
      List<GoogleWebmasterFilter.Dimension> dimensions, ApiDimensionFilterGroup filterGroup, int rowLimit, int startRow)
      throws IOException;

  public Callable<SearchAnalyticsQueryResponse> searchAnalyticsQueryCallable(final String siteProperty,
      final String date, final List<GoogleWebmasterFilter.Dimension> dimensions,
      final ApiDimensionFilterGroup filterGroup, final int rowLimit, final int startRow) {

    final GoogleWebmasterClient client = this;
    return new Callable<SearchAnalyticsQueryResponse>() {
      @Override
      public SearchAnalyticsQueryResponse call() throws Exception {
        return client.searchAnalyticsQuery(siteProperty, date, dimensions, filterGroup, rowLimit, startRow);
      }
    };
  }
}
