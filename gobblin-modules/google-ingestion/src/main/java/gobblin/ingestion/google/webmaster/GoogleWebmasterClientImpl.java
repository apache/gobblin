package gobblin.ingestion.google.webmaster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.api.services.webmasters.Webmasters;
import com.google.api.services.webmasters.model.ApiDataRow;
import com.google.api.services.webmasters.model.ApiDimensionFilter;
import com.google.api.services.webmasters.model.ApiDimensionFilterGroup;
import com.google.api.services.webmasters.model.SearchAnalyticsQueryRequest;
import com.google.api.services.webmasters.model.SearchAnalyticsQueryResponse;

import gobblin.source.extractor.extract.google.GoogleCommon;


public class GoogleWebmasterClientImpl extends GoogleWebmasterClient {

  private final static Logger LOG = LoggerFactory.getLogger(GoogleWebmasterClientImpl.class);

  private final Webmasters.Searchanalytics _analytics;
  private final Webmasters _service;

  public GoogleWebmasterClientImpl(Credential credential, String appName) throws IOException {
    //transport: new NetHttpTransport() or GoogleNetHttpTransport.newTrustedTransport()
    //jsonFactory: new JacksonFactory() or JacksonFactory.getDefaultInstance()
    _service =
        new Webmasters.Builder(credential.getTransport(), GoogleCommon.getJsonFactory(), credential).setApplicationName(
            appName).build();
    _analytics = _service.searchanalytics();
  }

  @Override
  public BatchRequest createBatch() {
    return _service.batch();
  }

  @Override
  public List<String> getPages(String siteProperty, String startDate, String endDate, String country, int rowLimit,
      List<GoogleWebmasterFilter.Dimension> requestedDimensions, List<ApiDimensionFilter> filters, int startRow)
      throws IOException {
    checkRowLimit(rowLimit);
    Preconditions.checkArgument(requestedDimensions.contains(GoogleWebmasterFilter.Dimension.PAGE));

    SearchAnalyticsQueryResponse rspByCountry =
        createSearchAnalyticsQuery(siteProperty, startDate, endDate, requestedDimensions,
            GoogleWebmasterFilter.andGroupFilters(filters), rowLimit, startRow).execute();

    List<ApiDataRow> pageRows = rspByCountry.getRows();
    List<String> pages = new ArrayList<>(rowLimit);
    if (pageRows != null) {
      int pageIndex = requestedDimensions.indexOf(GoogleWebmasterFilter.Dimension.PAGE);
      for (ApiDataRow row : pageRows) {
        pages.add(row.getKeys().get(pageIndex));
      }
    }
    return pages;
  }

  @Override
  public Webmasters.Searchanalytics.Query createSearchAnalyticsQuery(String siteProperty, String startDate,
      String endDate, List<GoogleWebmasterFilter.Dimension> dimensions, ApiDimensionFilterGroup filterGroup,
      int rowLimit, int startRow) throws IOException {
    List<String> dimensionStrings = new ArrayList<>();
    for (GoogleWebmasterFilter.Dimension dimension : dimensions) {
      dimensionStrings.add(dimension.toString().toLowerCase());
    }

    SearchAnalyticsQueryRequest request = new SearchAnalyticsQueryRequest().setStartDate(startDate)
        .setEndDate(endDate)
        .setRowLimit(rowLimit)
        .setDimensions(dimensionStrings)
        .setStartRow(startRow);

    if (filterGroup != null) {
      request.setDimensionFilterGroups(Arrays.asList(filterGroup));
    }

    return _analytics.query(siteProperty, request);
  }

  private static void checkRowLimit(int rowLimit) {
    Preconditions.checkArgument(rowLimit > 0 && rowLimit <= API_ROW_LIMIT,
        "Row limit for Google Search Console API must be within range (0, 5000]");
  }
}
