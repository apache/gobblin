package gobblin.source.extractor.extract.google.webmaster;

import avro.shaded.com.google.common.collect.Lists;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.api.services.webmasters.Webmasters;
import com.google.api.services.webmasters.WebmastersScopes;
import com.google.api.services.webmasters.model.ApiDataRow;
import com.google.api.services.webmasters.model.ApiDimensionFilter;
import com.google.api.services.webmasters.model.ApiDimensionFilterGroup;
import com.google.api.services.webmasters.model.SearchAnalyticsQueryRequest;
import com.google.api.services.webmasters.model.SearchAnalyticsQueryResponse;
import com.google.common.annotations.VisibleForTesting;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GoogleWebmasterClientImpl implements GoogleWebmasterClient {
  public static final int API_ROW_LIMIT_MAX = 5000;

  private final static Logger LOG = LoggerFactory.getLogger(GoogleWebmasterExtractor.class);
  private final Webmasters.Searchanalytics _analytics;
  private final String siteProperty; //Also used as a prefix
  private final List<String> pagePrefixFilters;

  public GoogleWebmasterClientImpl(String siteProperty, String credentialFile, String appName, String scope,
      Iterable<String> pageCheckList) throws IOException {
    //Missing "/" in the end will affect the getPagePrefixes logic
    Preconditions.checkArgument(siteProperty.endsWith("/"), "The site property must end in \"/\"");
    Preconditions.checkArgument(
        Objects.equals(WebmastersScopes.WEBMASTERS_READONLY, scope) || Objects.equals(WebmastersScopes.WEBMASTERS,
            scope), "The scope for WebMaster must either be WEBMASTERS_READONLY or WEBMASTERS");
    this.siteProperty = siteProperty;
    pagePrefixFilters = Lists.newArrayList(pageCheckList);
    GoogleCredential credential =
        GoogleCredential.fromStream(new FileInputStream(credentialFile)).createScoped(Collections.singletonList(scope));

    //GoogleNetHttpTransport.newTrustedTransport(),
    //JacksonFactory.getDefaultInstance(),
    Webmasters service =
        new Webmasters.Builder(new NetHttpTransport(), new JacksonFactory(), credential).setApplicationName(appName)
            .build();
    _analytics = service.searchanalytics();
  }

  private static void checkRowLimit(int rowLimit) {
    Preconditions.checkArgument(rowLimit > 0 && rowLimit <= API_ROW_LIMIT_MAX,
        "Row limit for Google Search Console API must be within range (0, 5000]");
  }

  @Override
  public Set<String> getAllPages(String date, GoogleWebmasterFilter.Country country, int rowLimit) throws IOException {
    checkRowLimit(rowLimit);

    List<GoogleWebmasterFilter.Dimension> dimensions = new ArrayList<>();
    dimensions.add(GoogleWebmasterFilter.Dimension.DATE);
    dimensions.add(GoogleWebmasterFilter.Dimension.PAGE);

    Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter> filterMap = new HashMap<>();
    if (country != GoogleWebmasterFilter.Country.ALL) {
      filterMap.put(GoogleWebmasterFilter.Dimension.COUNTRY, GoogleWebmasterFilter.countryFilter(country));
      dimensions.add(GoogleWebmasterFilter.Dimension.COUNTRY);
    }

    Set<String> uniquePages = new HashSet<>(getPages(date, rowLimit, dimensions, filterMap));
    if (rowLimit < GoogleWebmasterClientImpl.API_ROW_LIMIT_MAX
        || uniquePages.size() < GoogleWebmasterClientImpl.API_ROW_LIMIT_MAX) {
      //If the user requests for rows less than API_ROW_LIMIT_MAX or the response has fewer rows than API_ROW_LIMIT_MAX
      return uniquePages;
    }

    HashSet<String> pageFilters = getPagePrefixes(siteProperty, pagePrefixFilters, uniquePages);
    Deque<String> toProcess = new LinkedList<>();
    toProcess.addAll(pageFilters);

    while (!toProcess.isEmpty()) {
      String filter = toProcess.remove();
      LOG.info("Current page filter is " + filter);
      ApiDimensionFilter pagePrefixFilter =
          GoogleWebmasterFilter.pageFilter(GoogleWebmasterFilter.FilterOperator.CONTAINS, filter);
      Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter> filters = new HashMap<>(filterMap);
      filters.put(GoogleWebmasterFilter.Dimension.PAGE, pagePrefixFilter);
      List<String> pages = getPages(date, rowLimit, dimensions, filters);

      if (pages.size() >= GoogleWebmasterClientImpl.API_ROW_LIMIT_MAX) {
        //If the number of pages is at the LIMIT, we need to create sub-tasks and redo query
        LOG.info(String.format(
            "Number of pages fetched for filter %s reaches the MAX request limit %d. Expanding the filter list...",
            filter, GoogleWebmasterClientImpl.API_ROW_LIMIT_MAX));
        //The page filter is case insensitive, A-Z is not necessary.
        for (char c = 'a'; c <= 'z'; ++c) {
          toProcess.add(filter + c);
        }
        for (int num = 0; num <= 9; ++num) {
          toProcess.add(filter + num);
        }
        toProcess.add(filter + "%");
        toProcess.add(filter + "_");
        toProcess.add(filter + "-");
      }
      uniquePages.addAll(pages);
    }
    LOG.info(String.format("Total number of unique pages found for market-%s on %s is %d", country.toString(), date,
        uniquePages.size()));

    return uniquePages;
  }

  /**
   * Return all pages given the filterMap. Number of pages is limited by rowLimit
   */
  public List<String> getPages(String date, int rowLimit, List<GoogleWebmasterFilter.Dimension> requestedDimensions,
      Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter> filterMap) throws IOException {
    Preconditions.checkArgument(requestedDimensions.contains(GoogleWebmasterFilter.Dimension.PAGE));

    SearchAnalyticsQueryResponse rspByCountry = doQuery(date, requestedDimensions, FilterGroupAnd(filterMap), rowLimit);

    String country;
    ApiDimensionFilter countryFilter = filterMap.get(GoogleWebmasterFilter.Dimension.COUNTRY);
    if (countryFilter == null) {
      country = GoogleWebmasterFilter.Country.ALL.toString();
    } else {
      country = countryFilter.getExpression();
    }

    List<ApiDataRow> pageRows = rspByCountry.getRows();
    List<String> pages = new ArrayList<>(rowLimit);
    if (pageRows != null) {
      LOG.info(String.format("%d pages fetched for market-%s on %s. The last page has %.1f clicks.", pageRows.size(),
          country, date, pageRows.get(pageRows.size() - 1).getClicks()));

      int pageIndex = requestedDimensions.indexOf(GoogleWebmasterFilter.Dimension.PAGE);
      for (ApiDataRow row : pageRows) {
        pages.add(row.getKeys().get(pageIndex));
      }
    } else {
      LOG.info(String.format("0 pages fetched for market-%s on %s.", country, date));
    }
    return pages;
  }

  @VisibleForTesting
  static HashSet<String> getPagePrefixes(String siteProperty, List<String> pagePrefixFilters,
      Collection<String> fetchedPages) {
    //Provide a list of page filters that we must check
    HashSet<String> pageFilters = new HashSet<>();
    for (String filter : pagePrefixFilters) {
      pageFilters.add(siteProperty + filter + "/");
    }

    //Combine the page filters list with fetched pages
    for (String p : fetchedPages) {
      String substring = p.substring(siteProperty.length());
      int nextSlash = substring.indexOf("/");
      if (nextSlash > 0) {
        pageFilters.add(siteProperty + substring.substring(0, nextSlash + 1));
      }
    }
    LOG.info("PageFilter filter prefixes are:");
    for (String s : pageFilters) {
      LOG.info(s);
    }
    LOG.info("End of page filters.");
    return pageFilters;
  }

  @Override
  public List<String[]> doQuery(String date, int rowLimit, List<GoogleWebmasterFilter.Dimension> requestedDimensions,
      List<Metric> requestedMetrics, Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter> filterMap)
      throws IOException {

    SearchAnalyticsQueryResponse response = doQuery(date, requestedDimensions, FilterGroupAnd(filterMap), rowLimit);

    List<ApiDataRow> rows = response.getRows();

    if (rows == null || rows.isEmpty()) {
      LOG.info("doQuery: no pages returned");
      return new ArrayList<>();
    }

    List<String[]> ret = new ArrayList<>(rows.size());
    LOG.info("doQuery: Total number of rows returned:" + rows.size());
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

  /**
   * All pages returned by the API should have clicks by default
   */
  private SearchAnalyticsQueryResponse doQuery(String date, List<GoogleWebmasterFilter.Dimension> dimensions,
      ApiDimensionFilterGroup filterGroup, int rowLimit) throws IOException {
    List<String> dimensionStrings = new ArrayList<>();
    for (GoogleWebmasterFilter.Dimension dimension : dimensions) {
      dimensionStrings.add(dimension.toString().toLowerCase());
    }

    SearchAnalyticsQueryRequest request = new SearchAnalyticsQueryRequest().setStartDate(date)
        .setEndDate(date)
        .setRowLimit(rowLimit)
        .setDimensions(dimensionStrings);

    if (filterGroup != null) {
      request.setDimensionFilterGroups(Arrays.asList(filterGroup));
    }

    return _analytics.query(siteProperty, request).execute();
  }

  private ApiDimensionFilterGroup FilterGroupAnd(Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter> filterMap) {
    if (filterMap == null || filterMap.isEmpty()) {
      return null;
    }

    List<ApiDimensionFilter> filters = new ArrayList<>();
    filters.addAll(filterMap.values());
    return new ApiDimensionFilterGroup().setFilters(filters).setGroupType("and");
  }
}