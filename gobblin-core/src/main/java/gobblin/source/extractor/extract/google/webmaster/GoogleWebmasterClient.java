package gobblin.source.extractor.extract.google.webmaster;

import com.google.api.services.webmasters.model.ApiDimensionFilter;
import com.google.api.services.webmasters.model.ApiDimensionFilterGroup;
import com.google.api.services.webmasters.model.SearchAnalyticsQueryResponse;
import java.io.IOException;
import java.util.List;


/**
 * Provide access to the basic features of Google Webmaster API.
 * Details about Google Webmaster or Google Search Console API can be found at https://developers.google.com/webmaster-tools/
 */
public interface GoogleWebmasterClient {
  int API_ROW_LIMIT = 5000;

  /**
   * Return all pages given all constraints.
   * @param date date string
   * @param country country enum
   * @param rowLimit limit the number of rows returned by the API. The API maximum limit is 5000.
   * @param requestedDimensions requested dimensions of the API call.
   * @param filters a list of filters. Include the country filter if you need it, even though you've provided the country string.
   * @param startRow this is a 0 based index configuration to set the starting row of your API request. Even though the API row limit is 5000, you can send a request starting from row 5000, so you will be able to get data from row 5000 to row 9999.
   * @return Return all pages given all constraints.
   * @throws IOException
   */
  List<String> getPages(String siteProperty, String date, GoogleWebmasterFilter.Country country, int rowLimit,
      List<GoogleWebmasterFilter.Dimension> requestedDimensions, List<ApiDimensionFilter> filters, int startRow)
      throws IOException;

  /**
   * Perform the api call for search analytics query
   * @param siteProperty
   * @param date
   * @param dimensions
   * @param filterGroup
   * @param rowLimit
   * @param startRow
   * @return
   * @throws IOException
   */
  SearchAnalyticsQueryResponse searchAnalyticsQuery(String siteProperty, String date,
      List<GoogleWebmasterFilter.Dimension> dimensions, ApiDimensionFilterGroup filterGroup, int rowLimit, int startRow)
      throws IOException;
}
