package gobblin.source.extractor.extract.google.webmaster;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.api.services.webmasters.model.ApiDimensionFilter;
import gobblin.configuration.WorkUnitState;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
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
  private final int MAX_RETRY_ROUNDS;
  private final int INITIAL_COOL_DOWN;
  private final int COOL_DOWN_STEP;
  private final int REQUESTS_PER_SECOND;
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
    QUERY_LIMIT =
        wuState.getPropAsInt(GoogleWebMasterSource.KEY_REQUEST_QUERY_LIMIT, GoogleWebmasterClient.API_ROW_LIMIT);
    MAX_RETRY_ROUNDS = wuState.getPropAsInt(GoogleWebMasterSource.KEY_REQUEST_TUNING_RETRIES, 5);
    INITIAL_COOL_DOWN = wuState.getPropAsInt(GoogleWebMasterSource.KEY_REQUEST_TUNING_INITIAL_COOL_DOWN, 1000);
    COOL_DOWN_STEP = wuState.getPropAsInt(GoogleWebMasterSource.KEY_REQUEST_TUNING_COOL_DOWN_STEP, 500);
    REQUESTS_PER_SECOND = wuState.getPropAsInt(GoogleWebMasterSource.KEY_REQUEST_TUNING_REQUESTS_PER_SECOND, 5);
  }

  public boolean hasNext() throws IOException {
    initialize();
    if (!_cachedQueries.isEmpty()) {
      return true;
    }
    while (true) {
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
  }

  private void initialize() throws IOException {
    if (_cachedPages == null) {
      //Doesn't need to be a ConcurrentLinkedDeque upon creation.
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
        int totalPages = _pagesToProcess.size();
        int pageCheckPoint = Math.max(1, (int) Math.round(Math.ceil(totalPages / reportPartitions)));

        ConcurrentLinkedDeque<String> pagesToRetry = new ConcurrentLinkedDeque<>();
        ExecutorService es = Executors.newCachedThreadPool();
        int checkPointCount = 0;
        int sum = 0;
        while (!_pagesToProcess.isEmpty()) {
          String page = _pagesToProcess.poll();
          //This check is a MUST because seeds might be empty now even if the outer while loop has checked that it was not empty.
          if (page == null) {
            break;
          }
          if (++checkPointCount == pageCheckPoint) {
            checkPointCount = 0;
            sum += pageCheckPoint;
            LOG.info(String.format("Country-%s iterator progress: about %d of %d has been processed", _country, sum,
                totalPages));
          }

          try {
            Thread.sleep(1000 / REQUESTS_PER_SECOND); //Control the speed of sending API requests
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          Future<Void> submit = es.submit(
              getResponse(page, pagesToRetry, _cachedQueries)); //assign to submit to suppress gradlew warnings.
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

    /**
     * Call the API, then
     * OnSuccess: put each record into the responseQueue
     * OnFailure: add current page to pagesToRetry.
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
            results = _webmaster.doQuery(_date, QUERY_LIMIT, _requestedDimensions, _requestedMetrics, filters);
            //LOG.info(String.format("Fetched %s. Result size %d.", page, results.size()));
          } catch (IOException e) {
            //On failure
            LOG.debug(String.format("Adding back for retry: %s.%s%s", page, System.lineSeparator(), e.getMessage()));
            pagesToRetry.add(page);
            return null;
          }
          //On success
          for (String[] r : results) {
            responseQueue.put(r);
          }
          return null;
        }
      };
    }
  }
}