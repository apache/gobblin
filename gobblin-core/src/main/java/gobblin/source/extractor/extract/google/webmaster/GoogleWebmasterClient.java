package gobblin.source.extractor.extract.google.webmaster;

import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.services.webmasters.Webmasters;
import com.google.api.services.webmasters.model.ApiDimensionFilter;
import com.google.api.services.webmasters.model.ApiDimensionFilterGroup;
import gobblin.source.extractor.extract.google.webmaster.GoogleWebmasterFilter.Dimension;
import java.io.IOException;
import java.util.List;


/**
 * Provide basic accesses to Google Search Console by utilizing Google Webmaster API.
 * Details about Google Webmaster or Google Search Console API can be found at https://developers.google.com/webmaster-tools/
 */
public abstract class GoogleWebmasterClient {
  public static final int API_ROW_LIMIT = 5000;

  /**
   * Return all pages given all constraints.
   * @param siteProperty your site property string
   * @param startDate date string with format "yyyy-MM-dd"
   * @param endDate date string with format "yyyy-MM-dd"
   * @param country country code string
   * @param rowLimit limit the number of rows returned by the API. The API maximum limit is 5000.
   * @param requestedDimensions requested dimensions of the API call.
   * @param filters a list of filters. Include the country filter if you need it, even though you've provided the country string.
   * @param startRow this is a 0 based index configuration to set the starting row of your API request. Even though the API row limit is 5000, you can send a request starting from row 5000, so you will be able to get data from row 5000 to row 9999.
   * @return Return all pages given all constraints.
   */
  public abstract List<String> getPages(String siteProperty, String startDate, String endDate, String country,
      int rowLimit, List<Dimension> requestedDimensions, List<ApiDimensionFilter> filters, int startRow)
      throws IOException;

  /**
   * Perform the api call for search analytics query
   * @param siteProperty your site property string
   * @param startDate date string with format "yyyy-MM-dd"
   * @param endDate date string with format "yyyy-MM-dd"
   * @param dimensions your requested dimensions
   * @param filterGroup filters for your API request. Provide your filters in a group, find utility functions in GoogleWebmasterFilter
   * @param rowLimit the row limit for your API response
   * @param startRow this is a 0 based index configuration to set the starting row of your API request. Even though the API row limit is 5000, you can send a request starting from row 5000, so you will be able to get data from row 5000 to row 9999.
   * @return return the response of Google Webmaster API
   */
  public abstract Webmasters.Searchanalytics.Query createSearchAnalyticsQuery(String siteProperty, String startDate,
      String endDate, List<Dimension> dimensions, ApiDimensionFilterGroup filterGroup, int rowLimit, int startRow)
      throws IOException;

  public abstract BatchRequest createBatch();
}
