package gobblin.source.extractor.extract.google.webmaster;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.api.services.webmasters.model.ApiDataRow;
import com.google.api.services.webmasters.model.ApiDimensionFilter;
import com.google.api.services.webmasters.model.SearchAnalyticsQueryResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static gobblin.source.extractor.extract.google.webmaster.GoogleWebmasterFilter.*;


public class GoogleWebmasterDataFetcherImpl implements GoogleWebmasterDataFetcher {

  private final static Logger LOG = LoggerFactory.getLogger(GoogleWebmasterDataFetcherImpl.class);
  private final String _siteProperty; //Also used as a prefix
  private final GoogleWebmasterClient _client;
  private boolean _isHypercritical;

  public GoogleWebmasterDataFetcherImpl(String siteProperty, boolean isHypercritical, String credentialFile,
      String appName, String scope) throws IOException {
    this(siteProperty, isHypercritical, new GoogleWebmasterClientImpl(credentialFile, appName, scope));
  }

  public GoogleWebmasterDataFetcherImpl(String siteProperty, boolean isHypercritical, GoogleWebmasterClient client)
      throws IOException {
    //Missing "/" in the end will affect the getPagePrefixes logic
    Preconditions.checkArgument(siteProperty.endsWith("/"), "The site property must end in \"/\"");
    _siteProperty = siteProperty;
    _isHypercritical = isHypercritical;
    _client = client;
  }

  /**
   * Due to the limitation of the API, we can get a maximum of 5000 rows at a time. Another limitation is that, results are sorted by click count descending. If two rows have the same click count, they are sorted in an arbitrary way. (Read more at https://developers.google.com/webmaster-tools/v3/searchanalytics)
   *
   * In order to get all pages in a given date and country, we need to first figure out what is the total size of the data set. We achieve this by utilizing the feature of configuring the starting row when sending an API request. Keep asking for the maximum number of lines, which is 5000, allowed by the API until it returns fewer than 5000 lines, which indicates that you are at the last block of your data set. So we can sum up all lines to get the total count. But because the order is not guaranteed, only the count is reliable. The actual result will contain duplicates. Then it comes to the next step.
   *
   * In short, the steps of this algorithm are:
   *
   * 1. Request for all pages. If the request is less than 5000 or the response has fewer than 5000 rows, we've got all pages to return. Otherwise, go to next step for special handling.
   * 2. Now we need to know a full list of unique page prefixes/root paths. We will keep asking for the maximum number(5000) of rows until it comes to the last block, which is indicated by response giving less than 5000 rows.
   * 3. Remove duplicated pages got in last step.
   * 4. Parse out the list of keywords from last step.
   * 5. Send another request by adding the filters to exclude any pages/keywords in last step.
   * 6. Repeat steps 4 to 5 until we get a full union list of keywords, or prefixes, or root paths(e.g. www.property.com/path1/, www.property.com/path2/)
   * 7. For each page root path, apply the logic in pagesWithRootPaths to get all pages with that page root path/prefix.
   *    In detail, for example, you want all pages for your https://www.linkedin.com/ property. This property has several page prefixes such as https://www.linkedin.com/topic/, https://www.linkedin.com/jobs/, https://www.linkedin.com/groups/ etc.. For each prefix, you request for all pages starting with that prefix.
   * 8. Union all pages from last step and return.
   *
   */
  @Override
  public Set<String> getAllPages(String date, String country, int rowLimit) throws IOException {
    ApiDimensionFilter countryFilter = GoogleWebmasterFilter.countryEqFilter(country);

    List<GoogleWebmasterFilter.Dimension> requestedDimensions = new ArrayList<>();
    requestedDimensions.add(GoogleWebmasterFilter.Dimension.PAGE);

    List<String> response =
        _client.getPages(_siteProperty, date, country, rowLimit, requestedDimensions, Arrays.asList(countryFilter), 0);
    if (rowLimit < GoogleWebmasterClient.API_ROW_LIMIT || response.size() < 5000) {
      return new HashSet<>(response);
    }

    Triple<Set<String>, Set<String>, Integer> results = getPagePrefixes(date, requestedDimensions, countryFilter);

    //A page w/o root path can be https://www.linkedin.com/no_root_path
    Set<String> pagesNoRoots = results.getLeft();
    //A page with root path can be https://www.linkedin.com/root_path_here/
    Set<String> pagesRoots = results.getMiddle();
    Integer expectedSize = results.getRight();

    LOG.info(String.format("Total number of pages w/o root paths is %d.", pagesNoRoots.size()));
    LOG.info(String.format("Total of %d prefixes found. They are:", pagesRoots.size()));
    for (String prefix : pagesRoots) {
      LOG.info(prefix);
    }

    Set<String> allPages = pagesWithRootPaths(date, requestedDimensions, countryFilter, pagesRoots);
    allPages.addAll(pagesNoRoots);
    int size = allPages.size();
    if (size < expectedSize) {
      String msg =
          String.format("Size of the whole data set is %d, but only able to get %d lines. Date is %s and Market is %s.",
              expectedSize, size, date, country);
      if (_isHypercritical) {
        LOG.error(msg);
        throw new RuntimeException(
            "Cannot get all pages kept by Google Webmaster Service. There seems to be a defect with this algorithm.");
      } else {
        LOG.warn(msg);
      }
    }
    LOG.info(String.format("A total of %d pages fetched for market-%s on %s", allPages.size(), country, date));
    return allPages;
  }

  /**
   * @return return <pages w/o root paths, unique page root paths, size of the data set>
   */
  ImmutableTriple<Set<String>, Set<String>, Integer> getPagePrefixes(String date,
      List<GoogleWebmasterFilter.Dimension> requestedDimensions, ApiDimensionFilter countryFilter) throws IOException {
    String country = GoogleWebmasterFilter.countryFilterToString(countryFilter);
    Set<String> pagesNoRoots = new HashSet<>();  //Accumulating all pages w/o roots
    Set<String> pagesRoots = new HashSet<>(); //Accumulating all pages with roots

    int entireSize = -1;
    List<ApiDimensionFilter> filters = new ArrayList<>();
    filters.add(countryFilter);
    int startRow = 0;
    while (true) {
      LOG.info(String.format("Start fetching from row %d with %d filters...", startRow, filters.size()));

      List<String> responsePages =
          _client.getPages(_siteProperty, date, country, GoogleWebmasterClient.API_ROW_LIMIT, requestedDimensions,
              filters, startRow);
      for (String p : responsePages) {
        //_siteProperty URL ends with "/". And we care about the first slash after property URL string
        int fourthSlashIndex = p.indexOf("/", _siteProperty.length());
        if (fourthSlashIndex == -1) {
          pagesNoRoots.add(p); //Add all pages without the forth slash, it's a page w/o root path
        } else {
          pagesRoots.add(p.substring(0, fourthSlashIndex + 1));
        }
      }

      int fetchedSize = responsePages.size();
      LOG.info("Number of pages fetched is " + fetchedSize);
      startRow += fetchedSize;
      if (fetchedSize < GoogleWebmasterClient.API_ROW_LIMIT) {
        //fetchedSize < rowLimit indicates that we are at the last block of the data set.
        if (entireSize != -1) {
          //only break after we've known the total size of the data set.
          break;
        }

        if (entireSize == -1) {
          LOG.info(String.format("Total size of the data set for market-%s on %s is %d", country, date, startRow));
          entireSize = startRow;
        }

        //update starting row and filters before starting
        startRow = 0;
        filters = new ArrayList<>();
        filters.add(countryFilter);
        for (String p : pagesRoots) {
          filters.add(GoogleWebmasterFilter.pageFilter(GoogleWebmasterFilter.FilterOperator.NOTCONTAINS, p));
        }
        for (String p : pagesNoRoots) {
          if (_siteProperty.equals(p)) {
            //Must NOT add site property to the exclusion list, otherwise no pages will be returned.
            continue;
          }
          filters.add(GoogleWebmasterFilter.pageFilter(GoogleWebmasterFilter.FilterOperator.NOTCONTAINS, p));
        }
        LOG.info(String.format(
            "Starting another round of fetching for all pages. Currently there are %d pages w/o roots, and %d pages with roots",
            pagesNoRoots.size(), pagesRoots.size()));
      }
    }

    return new ImmutableTriple<>(pagesNoRoots, pagesRoots, entireSize);
  }

  /**
   * This method gives you all pages given the page prefixes/keywords/root paths.
   *
   * The detailed steps are as follows:
   *
   * 1. For each page prefix, construct a filter group with two filters. One for country; the other for containing that page prefix.
   * 2. Request for all pages with the filter group. If the count is 5000, go to step 3; otherwise, end of the process, we've already got all pages.
   * 3. Expand current prefix by appending a char from the list [a-z,0-9,%,_,-,/].
   * 4. Perform another request for each new prefix at last step. If the number of pages is 5000, repeat steps 3~4; otherwise, end of the process, we've already got all pages.
   * 5. Union all pages as the return value
   *
   * @param date the date string
   * @param dimensions requested dimensions
   * @param pagePrefixes a pagePrefix must end with "/"
   * @return all pages given the page prefixes/keywords, or page root paths.
   */
  private Set<String> pagesWithRootPaths(String date, List<Dimension> dimensions, ApiDimensionFilter countryFilter,
      Set<String> pagePrefixes) throws IOException {

    for (String prefix : pagePrefixes) {
      Preconditions.checkArgument(prefix.endsWith("/"), "Starting prefix must end with '/'");
    }
    String countryString = countryFilterToString(countryFilter);

    Set<String> uniquePages = new HashSet<>();
    Deque<String> toProcess = new LinkedList<>(pagePrefixes);
    while (!toProcess.isEmpty()) {
      String prefix = toProcess.remove();
      LOG.info("Current page prefix is " + prefix);
      List<ApiDimensionFilter> filters = new LinkedList<>();
      filters.add(countryFilter);
      filters.add(GoogleWebmasterFilter.pageFilter(GoogleWebmasterFilter.FilterOperator.CONTAINS, prefix));

      List<String> pages =
          _client.getPages(_siteProperty, date, countryString, GoogleWebmasterClient.API_ROW_LIMIT, dimensions, filters,
              0);

      if (pages.size() == GoogleWebmasterClient.API_ROW_LIMIT) {
        //If the number of pages is at the LIMIT, we need to create sub-tasks and redo query
        LOG.info(String.format(
            "Number of pages fetched for prefix %s reaches the MAX request limit %d. Expanding the prefix...", prefix,
            GoogleWebmasterClient.API_ROW_LIMIT));
        //The page prefix is case insensitive, A-Z is not necessary.
        for (char c = 'a'; c <= 'z'; ++c) {
          toProcess.add(prefix + c);
        }
        for (int num = 0; num <= 9; ++num) {
          toProcess.add(prefix + num);
        }
        toProcess.add(prefix + "%");
        toProcess.add(prefix + "_");
        toProcess.add(prefix + "-");
        toProcess.add(prefix + "/");
      }
      uniquePages.addAll(pages);
    }
    LOG.info(String.format("Number of pages given prefixes for market-%s on %s is %d", countryString, date,
        uniquePages.size()));

    return uniquePages;
  }

  @Override
  public List<String[]> doQuery(String date, int rowLimit, List<GoogleWebmasterFilter.Dimension> requestedDimensions,
      List<Metric> requestedMetrics, Collection<ApiDimensionFilter> filters) throws IOException {

    SearchAnalyticsQueryResponse response = _client.searchAnalyticsQuery(_siteProperty, date, requestedDimensions,
        GoogleWebmasterFilter.andGroupFilters(filters), rowLimit, 0);

    List<ApiDataRow> rows = response.getRows();

    if (rows == null || rows.isEmpty()) {
      return new ArrayList<>();
    }
    int responseSize = rows.size();
    if (responseSize == GoogleWebmasterClient.API_ROW_LIMIT) {
      StringBuilder filterString = new StringBuilder();
      for (ApiDimensionFilter filter : filters) {
        filterString.append(filter.toString());
        filterString.append(" ");
      }
      LOG.warn(String.format(
          "A total of %d rows returned. There might be more data based on your constraints: date - %s, filters - %s. Current code doesn't download more data beyond 5000 rows.",
          GoogleWebmasterClient.API_ROW_LIMIT, date, filterString.toString()));
    }

    List<String[]> ret = new ArrayList<>(responseSize);
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
}