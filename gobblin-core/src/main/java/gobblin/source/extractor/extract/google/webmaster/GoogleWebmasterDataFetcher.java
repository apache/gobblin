package gobblin.source.extractor.extract.google.webmaster;

import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.services.webmasters.Webmasters;
import com.google.api.services.webmasters.model.ApiDataRow;
import com.google.api.services.webmasters.model.ApiDimensionFilter;
import com.google.api.services.webmasters.model.SearchAnalyticsQueryResponse;
import gobblin.source.extractor.extract.google.webmaster.GoogleWebmasterFilter.Dimension;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;


/**
 * GoogleWebmasterDataFetcher implements the logic to download all query data(e.g. page, query, clicks, impressions, CTR, position) for a given date and country.
 *
 * The user of this GoogleWebmasterDataFetcher should follow these steps:
 * 1. Call getAllPages to get all pages based on your filters.
 * 2. For each page we get at last step, call performSearchAnalyticsQuery to get query data. If query data is not available, then this page won't be included in the return value.
 *
 * Note:
 * Due to the limitation of the Google API -- each API request can return at most 5000 rows of records, you need to take additional steps to fetch all data that the Google Search Console keeps.
 *
 * There is a rule to check whether Google Search Console keeps more than 5000 rows of data based on your request. The rule is that if you request for 5000 rows, but the response has less than 5000 rows; then in this case you've got all data that the Google service provides. If you request for 5000 rows, and the API returns you 5000 rows, there is a high chance that the Google service has more data matching your query, but due to the limitation of the API, only first 5000 rows returned.
 *
 */
public abstract class GoogleWebmasterDataFetcher {

  enum Metric {
    CLICKS, IMPRESSIONS, CTR, POSITION
  }

  /**
   * Return all pages given (date, country) filter
   * @param date date string
   * @param country country code string
   * @param rowLimit this is mostly for testing purpose. In order to get all pages, set this to the API row limit, which is 5000
   */
  public abstract Collection<String> getAllPages(String date, String country, int rowLimit) throws IOException;

  /**
   * @param date date filter
   * @param rowLimit row limit for this API call
   * @param requestedDimensions a list of dimension requests. The dimension values can be found at the first part of the return value
   * @param filters filters of your request
   * @return the response from the API call. The value is composed of [[requestedDimension list], clicks, impressions, ctr, position]
   */
  public abstract List<String[]> performSearchAnalyticsQuery(String date, int rowLimit,
      List<Dimension> requestedDimensions, List<Metric> requestedMetrics, Collection<ApiDimensionFilter> filters)
      throws IOException;

  public abstract BatchRequest createBatch();

  public abstract Webmasters.Searchanalytics.Query createSearchAnalyticsQuery(String date,
      List<Dimension> requestedDimensions, Collection<ApiDimensionFilter> filters, int rowLimit, int startRow)
      throws IOException;

  public static List<String[]> convertResponse(List<Metric> requestedMetrics, SearchAnalyticsQueryResponse response) {
    List<ApiDataRow> rows = response.getRows();
    if (rows == null || rows.isEmpty()) {
      return new ArrayList<>();
    }
    List<String[]> ret = new ArrayList<>(rows.size());
    for (ApiDataRow row : rows) {
      List<String> keys = row.getKeys();
      String[] data = new String[keys.size() + 4];
      int i = 0;
      for (; i < keys.size(); ++i) {
        data[i] = keys.get(i);
      }

      for (Metric requestedMetric : requestedMetrics) {
        if (requestedMetric == Metric.CLICKS) {
          data[i] = row.getClicks().toString();
        } else if (requestedMetric == Metric.IMPRESSIONS) {
          data[i] = row.getImpressions().toString();
        } else if (requestedMetric == Metric.CTR) {
          data[i] = String.format("%.5f", row.getCtr());
        } else if (requestedMetric == Metric.POSITION) {
          data[i] = String.format("%.2f", row.getPosition());
        } else {
          throw new RuntimeException("Unknown Google Webmaster Metric Type");
        }
        ++i;
      }
      ret.add(data);
    }
    return ret;
  }

  public static String getWarningMessage(String date, Collection<ApiDimensionFilter> filters) {
    StringBuilder filterString = new StringBuilder();
    for (ApiDimensionFilter filter : filters) {
      filterString.append(filter.toString());
      filterString.append(" ");
    }
    return String.format(
        "There might be more data based on your query: date - %s, filters - %s. Currently, downloading more than the Google API limit '%d' is not supported.",
        date, filterString.toString(), GoogleWebmasterClient.API_ROW_LIMIT);
  }
}

