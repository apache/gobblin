package gobblin.source.extractor.extract.google.webmaster;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
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
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
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
  private final List<ProducerJob> _jobs;

  public GoogleWebmasterDataFetcherImpl(String siteProperty, Credential credential, String appName,
      List<ProducerJob> jobs) throws IOException {
    this(siteProperty, new GoogleWebmasterClientImpl(credential, appName), jobs);
  }

  /**
   * For test only
   */
  GoogleWebmasterDataFetcherImpl(String siteProperty, GoogleWebmasterClient client, List<ProducerJob> jobs)
      throws IOException {
    Preconditions.checkArgument(siteProperty.endsWith("/"), "The site property must end in \"/\"");
    _siteProperty = siteProperty;
    _client = client;
    _jobs = jobs;
  }

  /**
   * Due to the limitation of the API, we can get a maximum of 5000 rows at a time. Another limitation is that, results are sorted by click count descending. If two rows have the same click count, they are sorted in an arbitrary way. (Read more at https://developers.google.com/webmaster-tools/v3/searchanalytics). So we try to get all pages by partitions, if a partition has 5000 rows returned. We try partition current partition into more granular levels.
   *
   */
  @Override
  public Collection<ProducerJob> getAllPages(String startDate, String endDate, String country, int rowLimit)
      throws IOException {
    if (!_jobs.isEmpty()) {
      LOG.info("Service got hot started.");
      return _jobs;
    }

    ApiDimensionFilter countryFilter = GoogleWebmasterFilter.countryEqFilter(country);

    List<GoogleWebmasterFilter.Dimension> requestedDimensions = new ArrayList<>();
    requestedDimensions.add(GoogleWebmasterFilter.Dimension.PAGE);

    Collection<String> allPages =
        _client.getPages(_siteProperty, startDate, endDate, country, rowLimit, requestedDimensions,
            Arrays.asList(countryFilter), 0);
    int actualSize = allPages.size();

    if (rowLimit < GoogleWebmasterClient.API_ROW_LIMIT || actualSize < GoogleWebmasterClient.API_ROW_LIMIT) {
      LOG.info(String.format("A total of %d pages fetched for country-%s from %s to %s", actualSize, country, startDate,
          endDate));
    } else {
      int expectedSize = getPagesSize(startDate, endDate, country, requestedDimensions, Arrays.asList(countryFilter));
      LOG.info(String.format("Total number of pages is %d for market-%s from %s to %s", expectedSize,
          GoogleWebmasterFilter.countryFilterToString(countryFilter), startDate, endDate));
      Queue<Pair<String, FilterOperator>> jobs = new ArrayDeque<>();
      expandJobs(jobs, _siteProperty);

      allPages = getPages(startDate, endDate, requestedDimensions, countryFilter, jobs);
      allPages.add(_siteProperty);
      actualSize = allPages.size();
      if (actualSize != expectedSize) {
        LOG.warn(
            String.format("Expected page size for country-%s is %d, but only able to get %d", country, expectedSize,
                actualSize));
      }
      LOG.info(String.format("A total of %d pages fetched for country-%s from %s to %s", actualSize, country, startDate,
          endDate));
    }

    ArrayDeque<ProducerJob> jobs = new ArrayDeque<>(actualSize);
    for (String page : allPages) {
      jobs.add(new SimpleProducerJob(page, startDate, endDate));
    }
    return jobs;
  }

  private int getPagesSize(final String startDate, final String endDate, final String country,
      final List<Dimension> requestedDimensions, final List<ApiDimensionFilter> apiDimensionFilters)
      throws IOException {

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
                List<String> pages =
                    _client.getPages(_siteProperty, startDate, endDate, country, GoogleWebmasterClient.API_ROW_LIMIT,
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
  private Collection<String> getPages(String startDate, String endDate, List<Dimension> dimensions,
      ApiDimensionFilter countryFilter, Queue<Pair<String, FilterOperator>> toProcess) throws IOException {
    String country = GoogleWebmasterFilter.countryFilterToString(countryFilter);

    ConcurrentLinkedDeque<String> allPages = new ConcurrentLinkedDeque<>();
    Random random = new Random();
    //we need to retry many times because the path may be very long
    final int retry = 120;
    int r = 0;
    while (r <= retry) {
      ++r;
      LOG.info(String.format("Get pages at round %d with size %d.", r, toProcess.size()));
      ConcurrentLinkedDeque<Pair<String, FilterOperator>> nextRound = new ConcurrentLinkedDeque<>();
      ExecutorService es = Executors.newFixedThreadPool(10, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          Thread t = Executors.defaultThreadFactory().newThread(r);
          t.setDaemon(true);
          return t;
        }
      });

      while (!toProcess.isEmpty()) {
        submitJob(toProcess.poll(), countryFilter, startDate, endDate, dimensions, es, allPages, nextRound);
        try {
          Thread.sleep(275); //Submit roughly 4 jobs per second.
        } catch (InterruptedException ignored) {
        }
      }
      //wait for jobs to finish and start next round if necessary.
      try {
        es.shutdown();
        LOG.info(String.format("Wait for get-all-pages jobs to finish at round %d... Next round now has size %d.", r,
            nextRound.size()));
        boolean terminated = es.awaitTermination(5, TimeUnit.MINUTES);
        if (!terminated) {
          es.shutdownNow();
          LOG.warn(
              String.format("Timed out while getting all pages for country-%s at round %d. Next round now has size %d.",
                  country, r, nextRound.size()));
        }
        //Cool down before next round.
        Thread.sleep(333 + 50 * random.nextInt(r));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      if (nextRound.isEmpty()) {
        break;
      }
      toProcess = nextRound;
    }
    if (r == retry) {
      throw new RuntimeException(
          String.format("Getting all pages reaches the maximum number of retires. Date range: %s ~ %s. Country: %s.",
              startDate, endDate, country));
    }
    return allPages;
  }

  private void submitJob(final Pair<String, FilterOperator> job, final ApiDimensionFilter countryFilter,
      final String startDate, final String endDate, final List<Dimension> dimensions, ExecutorService es,
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
          pages =
              _client.getPages(_siteProperty, startDate, endDate, countryString, GoogleWebmasterClient.API_ROW_LIMIT,
                  dimensions, filters, 0);
          LOG.debug(
              String.format("%d pages fetched for %s market-%s from %s to %s.", pages.size(), jobString, countryString,
                  startDate, endDate));
        } catch (IOException e) {
          //OnFailure
          LOG.debug(jobString + " failed. " + e.getMessage());
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
   * This doesn't cover all cases but more than 99.9% captured.
   *
   * According to the standard (RFC-3986), here are possible characters:
   * unreserved    = ALPHA / DIGIT / "-" / "." / "_" / "~"
   * reserved      = gen-delims / sub-delims
   * gen-delims    = ":" / "/" / "?" / "#" / "[" / "]" / "@"
   * sub-delims    = "!" / "$" / "&" / "'" / "(" / ")" / "*" / "+" / "," / ";" / "="
   *
   *
   * Not included:
   * reserved      = gen-delims / sub-delims
   * gen-delims    = "[" / "]"
   * sub-delims    = "(" / ")" / "," / ";"
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
    expanded.add(prefix + "*");
    expanded.add(prefix + "'");
    expanded.add(prefix + "=");
    return expanded;
  }

  @Override
  public List<String[]> performSearchAnalyticsQuery(String startDate, String endDate, int rowLimit,
      List<Dimension> requestedDimensions, List<Metric> requestedMetrics, Collection<ApiDimensionFilter> filters)
      throws IOException {
    SearchAnalyticsQueryResponse response =
        _client.createSearchAnalyticsQuery(_siteProperty, startDate, endDate, requestedDimensions,
            GoogleWebmasterFilter.andGroupFilters(filters), rowLimit, 0).execute();
    return convertResponse(requestedMetrics, response);
  }

  @Override
  public void performSearchAnalyticsQueryInBatch(List<ProducerJob> jobs, List<ArrayList<ApiDimensionFilter>> filterList,
      List<JsonBatchCallback<SearchAnalyticsQueryResponse>> callbackList, List<Dimension> requestedDimensions,
      int rowLimit) throws IOException {
    BatchRequest batchRequest = _client.createBatch();

    for (int i = 0; i < jobs.size(); ++i) {
      ProducerJob job = jobs.get(i);
      ArrayList<ApiDimensionFilter> filters = filterList.get(i);
      JsonBatchCallback<SearchAnalyticsQueryResponse> callback = callbackList.get(i);
      _client.createSearchAnalyticsQuery(_siteProperty, job.getStartDate(), job.getEndDate(), requestedDimensions,
          GoogleWebmasterFilter.andGroupFilters(filters), rowLimit, 0).queue(batchRequest, callback);
    }

    batchRequest.execute();
  }

  @Override
  public String getSiteProperty() {
    return _siteProperty;
  }
}