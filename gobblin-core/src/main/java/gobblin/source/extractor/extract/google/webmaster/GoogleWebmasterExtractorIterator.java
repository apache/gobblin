package gobblin.source.extractor.extract.google.webmaster;

import avro.shaded.com.google.common.base.Joiner;
import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.api.services.webmasters.model.ApiDimensionFilter;
import com.google.api.services.webmasters.model.SearchAnalyticsQueryResponse;
import gobblin.configuration.WorkUnitState;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//Doesn't implement Iterator<String[]> because I want to throw exception.

/**
 * This iterator holds a GoogleWebmasterDataFetcher, through which it get all pages. And then for each page, it will get all query data(Clicks, Impressions, CTR, Position). Basically, it will cache all pages got, and for each page, cache the detailed query data, and then iterate through them one by one.
 */
class GoogleWebmasterExtractorIterator {

  private final static Logger LOG = LoggerFactory.getLogger(GoogleWebmasterExtractorIterator.class);
  private final int BATCH_SIZE;
  private final int MAX_RETRY_ROUNDS;
  private final int INITIAL_COOL_DOWN;
  private final int COOL_DOWN_STEP;
  private final double REQUESTS_PER_SECOND;
  private final int PAGE_LIMIT;
  private final int QUERY_LIMIT;

  private final GoogleWebmasterDataFetcher _webmaster;
  private final String _date;
  private final String _country;
  private Deque<String> _cachedPages = null;
  private Thread _producerThread;
  private LinkedBlockingDeque<String[]> _cachedQueries = new LinkedBlockingDeque<>(1000);
  private final Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter> _filterMap;
  //This is the requested dimensions sent to Google API
  private final List<GoogleWebmasterFilter.Dimension> _requestedDimensions;
  private final List<GoogleWebmasterDataFetcher.Metric> _requestedMetrics;

  public GoogleWebmasterExtractorIterator(GoogleWebmasterDataFetcher webmaster, String date,
      List<GoogleWebmasterFilter.Dimension> requestedDimensions,
      List<GoogleWebmasterDataFetcher.Metric> requestedMetrics,
      Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter> filterMap, WorkUnitState wuState) {
    Preconditions.checkArgument(!filterMap.containsKey(GoogleWebmasterFilter.Dimension.PAGE),
        "Doesn't support filters for page for the time being. Will implement support later. If page filter is provided, the code won't take the responsibility of get all pages, so it will just return all queries for that page.");

    _webmaster = webmaster;
    _date = date;
    _requestedDimensions = requestedDimensions;
    _requestedMetrics = requestedMetrics;
    _filterMap = filterMap;
    _country = GoogleWebmasterFilter.countryFilterToString(filterMap.get(GoogleWebmasterFilter.Dimension.COUNTRY));

    PAGE_LIMIT =
        wuState.getPropAsInt(GoogleWebMasterSource.KEY_REQUEST_PAGE_LIMIT, GoogleWebmasterClient.API_ROW_LIMIT);
    Preconditions.checkArgument(PAGE_LIMIT >= 1, "Page limit must be at least 1.");

    QUERY_LIMIT =
        wuState.getPropAsInt(GoogleWebMasterSource.KEY_REQUEST_QUERY_LIMIT, GoogleWebmasterClient.API_ROW_LIMIT);
    Preconditions.checkArgument(QUERY_LIMIT >= 1, "Query limit must be at least 1.");

    MAX_RETRY_ROUNDS = wuState.getPropAsInt(GoogleWebMasterSource.KEY_REQUEST_TUNING_RETRIES, 10);
    Preconditions.checkArgument(MAX_RETRY_ROUNDS >= 0, "Retry rounds cannot be negative.");

    INITIAL_COOL_DOWN = wuState.getPropAsInt(GoogleWebMasterSource.KEY_REQUEST_TUNING_INITIAL_COOL_DOWN, 300);
    Preconditions.checkArgument(INITIAL_COOL_DOWN >= 0, "Initial cool down time cannot be negative.");

    COOL_DOWN_STEP = wuState.getPropAsInt(GoogleWebMasterSource.KEY_REQUEST_TUNING_COOL_DOWN_STEP, 50);
    Preconditions.checkArgument(COOL_DOWN_STEP >= 0, "Cool down step time cannot be negative.");

    REQUESTS_PER_SECOND = wuState.getPropAsDouble(GoogleWebMasterSource.KEY_REQUEST_TUNING_REQUESTS_PER_SECOND, 1);
    Preconditions.checkArgument(REQUESTS_PER_SECOND > 0, "Requests per second must be positive.");

    BATCH_SIZE = wuState.getPropAsInt(GoogleWebMasterSource.KEY_REQUEST_TUNING_BATCH_SIZE, 5);
    Preconditions.checkArgument(BATCH_SIZE >= 1, "Batch size must be at least 1.");
  }

  public boolean hasNext() throws IOException {
    initialize();
    if (!_cachedQueries.isEmpty()) {
      return true;
    }
    try {
      String[] next = _cachedQueries.poll(1, TimeUnit.SECONDS);
      while (next == null) {
        if (_producerThread.isAlive()) {
          //Job not done yet. Keep waiting.
          next = _cachedQueries.poll(1, TimeUnit.SECONDS);
        } else {
          LOG.info("Producer job has finished. No more query data in the queue.");
          return false;
        }
      }
      //Must put it back. Implement in this way because LinkedBlockingDeque doesn't support blocking peek.
      _cachedQueries.putFirst(next);
      return true;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void initialize() throws IOException {
    if (_cachedPages == null) {
      //Doesn't need to be a ConcurrentLinkedDeque upon creation, because it will only be read by one thread.
      _cachedPages = new ArrayDeque<>(_webmaster.getAllPages(_date, _country, PAGE_LIMIT));
      //start the response producer
      _producerThread = new Thread(new ResponseProducer(_cachedPages));
      _producerThread.start();
    }
  }

  public String[] next() throws IOException {
    if (hasNext()) {
      return _cachedQueries.remove();
    }
    throw new NoSuchElementException();
  }

  /**
   * For test only
   */
  String getCountry() {
    return _country;
  }

  /**
   * ResponseProducer gets the query data for allPages in an async way.
   * It utilize Executors.newCachedThreadPool to submit API request in a configurable speed.
   * API request speed can be tuned by REQUESTS_PER_SECOND, INITIAL_COOL_DOWN, COOL_DOWN_STEP and MAX_RETRY_ROUNDS.
   * The speed must be controlled because it cannot succeed the Google API quota, which can be found in your Google API Manager.
   * If you send the request too fast, you will get "403 Forbidden - Quota Exceeded" exception. Those pages will be handled by next round of retries.
   */
  private class ResponseProducer implements Runnable {
    private Deque<String> _pagesToProcess;
    //Will report every (100% / reportPartitions), e.g. 20 -> report every 5% done. 10 -> report every 10% done.
    private double reportPartitions = 20;

    public ResponseProducer(Deque<String> pagesToProcess) {
      _pagesToProcess = pagesToProcess;
    }

    @Override
    public void run() {
      int r = 0; //indicates current rounds.

      //check if any seed got adding back.
      while (r <= MAX_RETRY_ROUNDS) {
        LOG.info("Currently at round " + r);
        long requestSleep = (long) Math.ceil(1000 / REQUESTS_PER_SECOND);
        int totalPages = _pagesToProcess.size();
        int pageCheckPoint = Math.max(1, (int) Math.round(Math.ceil(totalPages / reportPartitions)));
        //pagesToRetry needs to be concurrent because multiple threads will write to it.
        ConcurrentLinkedDeque<String> pagesToRetry = new ConcurrentLinkedDeque<>();
        ExecutorService es = Executors.newCachedThreadPool();
        int checkPointCount = 0;
        int sum = 0;
        List<String> pagesBatch = new ArrayList<>(BATCH_SIZE);

        while (!_pagesToProcess.isEmpty()) {
          //This is the only place to poll page from queue. Writing to a new queue is async.
          String page = _pagesToProcess.poll();

          if (++checkPointCount == pageCheckPoint) {
            checkPointCount = 0;
            sum += pageCheckPoint;
            LOG.info(String.format("Country-%s iterator progress: about %d of %d has been processed", _country, sum,
                totalPages));
          }

          if (pagesBatch.size() < BATCH_SIZE) {
            pagesBatch.add(page);
          }

          if (pagesBatch.size() == BATCH_SIZE) {
            submitJob(requestSleep, pagesToRetry, es, pagesBatch);
            pagesBatch = new ArrayList<>(BATCH_SIZE);
          }
        }

        //Send the last batch
        if (!pagesBatch.isEmpty()) {
          submitJob(requestSleep, pagesToRetry, es, pagesBatch);
        }

        try {
          es.shutdown(); //stop accepting new requests
          es.awaitTermination(4, TimeUnit.HOURS); //await for all submitted job to finish
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        if (pagesToRetry.isEmpty()) {
          break;
        }
        ++r;
        _pagesToProcess = pagesToRetry;
        LOG.info(String.format("Starting #%d round of retries of size %d", r, _pagesToProcess.size()));
        try {
          // Cool down before starting another round of retry, Google is already quite upset.
          // As it gets more and more upset, we give it more and more time to cool down.
          Thread.sleep(INITIAL_COOL_DOWN + COOL_DOWN_STEP * r);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      if (r == MAX_RETRY_ROUNDS + 1) {
        LOG.warn(
            "Exceeding the number of maximum retry rounds. There are %d unprocessed pages." + _pagesToProcess.size());
      }
      LOG.info(
          String.format("Terminating current ResponseProducer for %s on %s at retry round %d", _country, _date, r));
    }

    private void submitJob(long requestSleep, ConcurrentLinkedDeque<String> pagesToRetry, ExecutorService es,
        List<String> pagesBatch) {
      try {
        Thread.sleep(requestSleep); //Control the speed of sending API requests
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      //assign to submit to suppress gradlew warnings.
      Future<Void> submit = es.submit(getResponses(pagesBatch, pagesToRetry, _cachedQueries));
    }

    /**
     * Call the API, then
     * OnSuccess: put each record into the responseQueue
     * OnFailure: add current page to pagesToRetry
     */
    private Callable<Void> getResponse(final String page, final ConcurrentLinkedDeque<String> pagesToRetry,
        final LinkedBlockingDeque<String[]> responseQueue) {

      return new Callable<Void>() {
        @Override
        public Void call() throws Exception {

          final ArrayList<ApiDimensionFilter> filters = new ArrayList<>();
          filters.addAll(_filterMap.values());
          filters.add(GoogleWebmasterFilter.pageFilter(GoogleWebmasterFilter.FilterOperator.EQUALS, page));
          List<String[]> results;
          try {
            results =
                _webmaster.performSearchAnalyticsQuery(_date, QUERY_LIMIT, _requestedDimensions, _requestedMetrics,
                    filters);
          } catch (IOException e) {
            onFailure(e.getMessage(), page, pagesToRetry);
            return null;
          }
          onSuccess(page, results, responseQueue);
          return null;
        }
      };
    }

    /**
     * Call the APIs with a batch request
     * OnSuccess: put each record into the responseQueue
     * OnFailure: add current page to pagesToRetry
     */
    private Callable<Void> getResponses(final List<String> pages, final ConcurrentLinkedDeque<String> pagesToRetry,
        final LinkedBlockingDeque<String[]> responseQueue) {
      if (pages == null) {
        LOG.error("How come this is null?");
        throw new RuntimeException("pages is null in thread pool. Won't be seen in main thread.");
      }

      if (pages.size() == 1) {
        return getResponse(pages.get(0), pagesToRetry, responseQueue);
      }
      final ResponseProducer producer = this;

      return new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          BatchRequest batchRequest = _webmaster.createBatch();
          try {
            for (String p : pages) {
              final String page = p;
              final ArrayList<ApiDimensionFilter> filters = new ArrayList<>();
              filters.addAll(_filterMap.values());
              filters.add(GoogleWebmasterFilter.pageFilter(GoogleWebmasterFilter.FilterOperator.EQUALS, page));

              _webmaster.createSearchAnalyticsQuery(_date, _requestedDimensions, filters, QUERY_LIMIT, 0)
                  .queue(batchRequest, new JsonBatchCallback<SearchAnalyticsQueryResponse>() {
                    @Override
                    public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders) throws IOException {
                      producer.onFailure(e.getMessage(), page, pagesToRetry);
                    }

                    @Override
                    public void onSuccess(SearchAnalyticsQueryResponse searchAnalyticsQueryResponse,
                        HttpHeaders responseHeaders) throws IOException {
                      producer.onSuccess(page,
                          GoogleWebmasterDataFetcher.convertResponse(_requestedMetrics, searchAnalyticsQueryResponse),
                          responseQueue);
                    }
                  });
            }
          } catch (IOException e) {
            LOG.warn("Fail creating batch request for pages: " + Joiner.on(",").join(pages));
            for (String page : pages) {
              pagesToRetry.add(page);
            }
            return null;
          }

          batchRequest.execute();
          return null;
        }
      };
    }

    private void onFailure(String errMsg, String page, ConcurrentLinkedDeque<String> pagesToRetry) {
      LOG.debug(
          String.format("OnFailure: adding back for retry: %s.%sReason:%s", page, System.lineSeparator(), errMsg));
      pagesToRetry.add(page);
    }

    private void onSuccess(String page, List<String[]> results, LinkedBlockingDeque<String[]> responseQueue) {
      try {
        for (String[] r : results) {
          responseQueue.put(r);
        }
        LOG.debug(String.format("Fetched %s. Result size %d.", page, results.size()));
      } catch (InterruptedException e) {
        LOG.warn(String.format("Adding to queue gets interrupted. Don't add back failed page - %s", page));
      }
    }
  }
}