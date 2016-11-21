package gobblin.source.extractor.extract.google.webmaster;

import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.api.services.webmasters.Webmasters;
import com.google.api.services.webmasters.model.ApiDimensionFilter;
import com.google.api.services.webmasters.model.SearchAnalyticsQueryResponse;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static gobblin.source.extractor.extract.google.webmaster.GoogleWebmasterFilter.*;


public class GoogleWebmasterDataFetcherImpl extends GoogleWebmasterDataFetcher {

  private final static Logger LOG = LoggerFactory.getLogger(GoogleWebmasterDataFetcherImpl.class);
  private final String _siteProperty;
  private final GoogleWebmasterClient _client;

  public GoogleWebmasterDataFetcherImpl(String siteProperty, String credentialFile, String appName, String scope)
      throws IOException {
    this(siteProperty, new GoogleWebmasterClientImpl(credentialFile, appName, scope));
  }

  public GoogleWebmasterDataFetcherImpl(String siteProperty, GoogleWebmasterClient client) throws IOException {
    Preconditions.checkArgument(siteProperty.endsWith("/"), "The site property must end in \"/\"");
    _siteProperty = siteProperty;
    _client = client;
  }

  /**
   * Due to the limitation of the API, we can get a maximum of 5000 rows at a time. Another limitation is that, results are sorted by click count descending. If two rows have the same click count, they are sorted in an arbitrary way. (Read more at https://developers.google.com/webmaster-tools/v3/searchanalytics). So we try to get all pages by partitions, if a partition has 5000 rows returned. We try partition current partition into more granular levels.
   *
   */
  @Override
  public Collection<String> getAllPages(String date, String country, int rowLimit) throws IOException {
    ApiDimensionFilter countryFilter = GoogleWebmasterFilter.countryEqFilter(country);

    List<GoogleWebmasterFilter.Dimension> requestedDimensions = new ArrayList<>();
    requestedDimensions.add(GoogleWebmasterFilter.Dimension.PAGE);

    List<String> response =
        _client.getPages(_siteProperty, date, country, rowLimit, requestedDimensions, Arrays.asList(countryFilter), 0);
    if (rowLimit < GoogleWebmasterClient.API_ROW_LIMIT || response.size() < GoogleWebmasterClient.API_ROW_LIMIT) {
      LOG.info(String.format("A total of %d pages fetched for market-%s on %s", response.size(), country, date));
      return response;
    }

    int expectedSize = getPagesSize(date, country, requestedDimensions, Arrays.asList(countryFilter));
    LOG.info(String.format("Total number of pages is %d for market-%s on date %s", expectedSize,
        GoogleWebmasterFilter.countryFilterToString(countryFilter), date));
    Queue<Pair<String, FilterOperator>> jobs = new ArrayDeque<>();
    expandJobs(jobs, _siteProperty);
    Collection<String> allPages = getPagesAsync(date, requestedDimensions, countryFilter, jobs);
    allPages.add(_siteProperty);
    int actualSize = allPages.size();
    if (actualSize != expectedSize) {
      LOG.warn(String.format("Expected page size is %d, but only able to get %d", expectedSize, actualSize));
    }
    LOG.info(String.format("A total of %d pages fetched for market-%s on %s", actualSize, country, date));
    return allPages;
  }

  private int getPagesSize(final String date, final String country, final List<Dimension> requestedDimensions,
      final List<ApiDimensionFilter> apiDimensionFilters) throws IOException {

    int startRow = 0;
    int count = 4;
    final ExecutorService es = Executors.newFixedThreadPool(count);
    int r = 0;
    while (r < 50) {
      r++;
      List<Future<Integer>> results = new ArrayList<>(count);

      for (int i = 0; i < count; ++i) {
        startRow += GoogleWebmasterClient.API_ROW_LIMIT;
        final int start = startRow;
        Future<Integer> submit = es.submit(new Callable<Integer>() {
          @Override
          public Integer call() {
            LOG.info(String.format("Getting page size from %s...", start));
            while (true) {
              try {
                List<String> pages = _client.getPages(_siteProperty, date, country, GoogleWebmasterClient.API_ROW_LIMIT,
                    requestedDimensions, apiDimensionFilters, start);
                if (pages.size() < GoogleWebmasterClient.API_ROW_LIMIT) {
                  //Figured out the size
                  return pages.size() + start;
                } else {
                  //The size is not determined.
                  return -1;
                }
              } catch (IOException e) {
                LOG.info(String.format("Getting page size from %s failed. Retrying...", start));
              }
              try {
                Thread.sleep(200);
              } catch (InterruptedException e) {
                LOG.error(e.getMessage());
              }
            }
          }
        });
        results.add(submit);
        try {
          Thread.sleep(250);
        } catch (InterruptedException e) {
          LOG.error(e.getMessage());
        }
      }

      for (Future<Integer> result : results) {
        try {
          Integer integer = result.get(2, TimeUnit.MINUTES);
          if (integer > 0) {
            es.shutdownNow();
            return integer;
          }
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        } catch (ExecutionException e) {
          throw new RuntimeException(e);
        } catch (TimeoutException e) {
          throw new RuntimeException(e);
        }
      }
    }
    throw new RuntimeException("Something seems wrong here. Having more than 5000*4*50 pages?");
  }

  /**
   * Get all pages in an async mode.
   */
  private Collection<String> getPagesAsync(String date, List<Dimension> dimensions, ApiDimensionFilter countryFilter,
      Queue<Pair<String, FilterOperator>> toProcess) throws IOException {
    ConcurrentLinkedDeque<String> allPages = new ConcurrentLinkedDeque<>();
    final int retry = 20;
    int r = 0;
    while (r <= retry) {
      ++r;
      LOG.info("Get pages current round: " + r);
      ConcurrentLinkedDeque<Pair<String, FilterOperator>> nextRound = new ConcurrentLinkedDeque<>();
      ExecutorService es = Executors.newCachedThreadPool();
      while (!toProcess.isEmpty()) {
        submitJob(toProcess.poll(), countryFilter, date, dimensions, es, allPages, nextRound);
        try {
          Thread.sleep(200); //Submit 5 jobs per second.
        } catch (InterruptedException ignored) {
        }
      }
      //wait for jobs to finish and start next round if necessary.
      try {
        es.shutdown();
        es.awaitTermination(1, TimeUnit.HOURS);
        Thread.sleep(1000); //sleep for 1 second before starting another round.
      } catch (InterruptedException e) {
        LOG.error(e.getMessage());
      }

      if (nextRound.isEmpty()) {
        break;
      }
      toProcess = nextRound;
    }
    if (r == retry) {
      throw new RuntimeException(
          String.format("Getting all pages reaches the maximum number of retires. Current date is %s, Market is %s.",
              date, GoogleWebmasterFilter.countryFilterToString(countryFilter)));
    }
    return allPages;
  }

  private void submitJob(final Pair<String, FilterOperator> job, final ApiDimensionFilter countryFilter,
      final String date, final List<Dimension> dimensions, ExecutorService es,
      final ConcurrentLinkedDeque<String> allPages,
      final ConcurrentLinkedDeque<Pair<String, FilterOperator>> nextRound) {
    es.submit(new Runnable() {
      @Override
      public void run() {
        String countryString = countryFilterToString(countryFilter);
        List<ApiDimensionFilter> filters = new LinkedList<>();
        filters.add(countryFilter);

        String prefix = job.getLeft();
        FilterOperator operator = job.getRight();
        String jobString = String.format("job(prefix: %s, operator: %s)", prefix, operator);
        filters.add(GoogleWebmasterFilter.pageFilter(operator, prefix));
        List<String> pages;
        try {
          pages = _client.getPages(_siteProperty, date, countryString, GoogleWebmasterClient.API_ROW_LIMIT, dimensions,
              filters, 0);
          LOG.info(
              String.format("%d pages fetched for %s market-%s on %s.", pages.size(), jobString, countryString, date));
        } catch (IOException e) {
          //OnFailure
          LOG.error(jobString + " failed. " + e.getMessage());
          nextRound.add(job);
          return;
        }

        //If the number of pages is at the LIMIT, it must be a "CONTAINS" job.
        //We need to create sub-tasks, and check current page with "EQUALS"
        if (pages.size() == GoogleWebmasterClient.API_ROW_LIMIT) {
          LOG.info(String.format("Expanding the prefix '%s'", prefix));
          expandJobs(nextRound, prefix);
          nextRound.add(Pair.of(prefix, FilterOperator.EQUALS));
        } else {
          //Otherwise, we've done with current job.
          allPages.addAll(pages);
        }
      }
    });
  }

  private void expandJobs(Queue<Pair<String, FilterOperator>> jobs, String prefix) {
    for (String expanded : getUrlPartitions(prefix)) {
      jobs.add(Pair.of(expanded, FilterOperator.CONTAINS));
    }
  }

  /**
   * This doesn't cover all cases but more than 99.9% captured
   */
  private ArrayList<String> getUrlPartitions(String prefix) {
    ArrayList<String> expanded = new ArrayList<>();
    //The page prefix is case insensitive, A-Z is not necessary.
    for (char c = 'a'; c <= 'z'; ++c) {
      expanded.add(prefix + c);
    }
    for (int num = 0; num <= 9; ++num) {
      expanded.add(prefix + num);
    }
    expanded.add(prefix + "-");
    expanded.add(prefix + ".");
    expanded.add(prefix + "_"); //most important
    expanded.add(prefix + "~");

    expanded.add(prefix + "/"); //most important
    expanded.add(prefix + "%"); //most important
    expanded.add(prefix + ":");
    expanded.add(prefix + "?");
    expanded.add(prefix + "#");
    expanded.add(prefix + "@");
    expanded.add(prefix + "!");
    expanded.add(prefix + "$");
    expanded.add(prefix + "&");
    expanded.add(prefix + "+");
    expanded.add(prefix + "=");
    return expanded;
  }

  @Override
  public BatchRequest createBatch() {
    return _client.createBatch();
  }

  @Override
  public Webmasters.Searchanalytics.Query createSearchAnalyticsQuery(String date, List<Dimension> requestedDimensions,
      Collection<ApiDimensionFilter> filters, int rowLimit, int startRow) throws IOException {
    return _client.createSearchAnalyticsQuery(_siteProperty, date, requestedDimensions,
        GoogleWebmasterFilter.andGroupFilters(filters), rowLimit, startRow);
  }

  @Override
  public List<String[]> performSearchAnalyticsQuery(String date, int rowLimit, List<Dimension> requestedDimensions,
      List<Metric> requestedMetrics, Collection<ApiDimensionFilter> filters) throws IOException {
    SearchAnalyticsQueryResponse response =
        createSearchAnalyticsQuery(date, requestedDimensions, filters, rowLimit, 0).execute();

    List<String[]> converted = convertResponse(requestedMetrics, response);
    if (converted.size() == GoogleWebmasterClient.API_ROW_LIMIT) {
      LOG.warn(getWarningMessage(date, filters));
    }
    return converted;
  }
}