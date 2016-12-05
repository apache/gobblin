package gobblin.ingestion.google.webmaster;

import avro.shaded.com.google.common.base.Joiner;
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
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//Doesn't implement Iterator<String[]> because I want to throw exception.

/**
 * This iterator holds a GoogleWebmasterDataFetcher, through which it get all pages. And then for each page, it will get all query data(Clicks, Impressions, CTR, Position). Basically, it will cache all pages got, and for each page, cache the detailed query data, and then iterate through them one by one.
 */
class GoogleWebmasterExtractorIterator {

  private final static Logger LOG = LoggerFactory.getLogger(GoogleWebmasterExtractorIterator.class);
  private final int TIME_OUT;
  private final int BATCH_SIZE;
  private final int GROUP_SIZE;
  private final boolean ADVANCED_MODE;
  private final int MAX_RETRY_ROUNDS;
  private final int INITIAL_COOL_DOWN;
  private final int COOL_DOWN_STEP;
  private final double REQUESTS_PER_SECOND;
  private final int PAGE_LIMIT;
  private final int QUERY_LIMIT;

  private final GoogleWebmasterDataFetcher _webmaster;
  private final String _startDate;
  private final String _endDate;
  private final String _country;
  private Thread _producerThread;
  private LinkedBlockingDeque<String[]> _cachedQueries = new LinkedBlockingDeque<>(2000);
  private final Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter> _filterMap;
  //This is the requested dimensions sent to Google API
  private final List<GoogleWebmasterFilter.Dimension> _requestedDimensions;
  private final List<GoogleWebmasterDataFetcher.Metric> _requestedMetrics;

  public GoogleWebmasterExtractorIterator(GoogleWebmasterDataFetcher webmaster, String startDate, String endDate,
      List<GoogleWebmasterFilter.Dimension> requestedDimensions,
      List<GoogleWebmasterDataFetcher.Metric> requestedMetrics,
      Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter> filterMap, WorkUnitState wuState) {
    Preconditions.checkArgument(!filterMap.containsKey(GoogleWebmasterFilter.Dimension.PAGE),
        "Doesn't support filters for page for the time being. Will implement support later. If page filter is provided, the code won't take the responsibility of get all pages, so it will just return all queries for that page.");

    _webmaster = webmaster;
    _startDate = startDate;
    _endDate = endDate;
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

    TIME_OUT = wuState.getPropAsInt(GoogleWebMasterSource.KEY_REQUEST_TIME_OUT, 5);
    Preconditions.checkArgument(TIME_OUT > 0, "Time out must be positive.");

    MAX_RETRY_ROUNDS = wuState.getPropAsInt(GoogleWebMasterSource.KEY_REQUEST_TUNING_RETRIES, 20);
    Preconditions.checkArgument(MAX_RETRY_ROUNDS >= 0, "Retry rounds cannot be negative.");

    INITIAL_COOL_DOWN = wuState.getPropAsInt(GoogleWebMasterSource.KEY_REQUEST_TUNING_INITIAL_COOL_DOWN, 300);
    Preconditions.checkArgument(INITIAL_COOL_DOWN >= 0, "Initial cool down time cannot be negative.");

    COOL_DOWN_STEP = wuState.getPropAsInt(GoogleWebMasterSource.KEY_REQUEST_TUNING_COOL_DOWN_STEP, 50);
    Preconditions.checkArgument(COOL_DOWN_STEP >= 0, "Cool down step time cannot be negative.");

    REQUESTS_PER_SECOND = wuState.getPropAsDouble(GoogleWebMasterSource.KEY_REQUEST_TUNING_REQUESTS_PER_SECOND, 2.25);
    Preconditions.checkArgument(REQUESTS_PER_SECOND > 0, "Requests per second must be positive.");

    BATCH_SIZE = wuState.getPropAsInt(GoogleWebMasterSource.KEY_REQUEST_TUNING_BATCH_SIZE, 2);
    Preconditions.checkArgument(BATCH_SIZE >= 1, "Batch size must be at least 1.");

    GROUP_SIZE = wuState.getPropAsInt(GoogleWebMasterSource.KEY_REQUEST_TUNING_GROUP_SIZE, 500);
    Preconditions.checkArgument(GROUP_SIZE >= 1, "Group size must be at least 1.");

    ADVANCED_MODE = wuState.getPropAsBoolean(GoogleWebMasterSource.KEY_REQUEST_TUNING_ALGORITHM, false);
    if (ADVANCED_MODE) {
      Preconditions.checkArgument(PAGE_LIMIT == GoogleWebmasterClient.API_ROW_LIMIT,
          "Page limit must be set at 5000 if you want to use the advanced algorithm. This indicates that you understand what you are doing.");
    }
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
    if (_producerThread == null) {
      Collection<ProducerJob> allJobs = _webmaster.getAllPages(_startDate, _endDate, _country, PAGE_LIMIT);
      _producerThread = new Thread(new ResponseProducer(allJobs));
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
    private Deque<ProducerJob> _jobsToProcess;
    //Will report every (100% / REPORT_PARTITIONS), e.g. 20 -> report every 5% done. 10 -> report every 10% done.
    private final static double REPORT_PARTITIONS = 20.0;

    ResponseProducer(Collection<ProducerJob> jobs) {
      int size = jobs.size();
      if (size == 0) {
        _jobsToProcess = new ArrayDeque<>();
        return;
      }

      if (ADVANCED_MODE) {
        List<String> pages = new ArrayList<>(size);
        for (ProducerJob job : jobs) {
          pages.add(job.getPage());
        }
        UrlTrie trie = new UrlTrie(_webmaster.getSiteProperty(), pages);
        UrlTriePrefixGrouper grouper = new UrlTriePrefixGrouper(trie, GROUP_SIZE);
        //Doesn't need to be a ConcurrentLinkedDeque, because it will only be read by one thread.
        _jobsToProcess = new ArrayDeque<>(size);
        while (grouper.hasNext()) {
          _jobsToProcess.add(new TrieBasedProducerJob(_startDate, _endDate, grouper.next(), grouper.getGroupSize()));
        }
      } else {
        if (jobs.getClass().equals(ArrayDeque.class)) {
          _jobsToProcess = (ArrayDeque<ProducerJob>) jobs;
        } else {
          //Doesn't need to be a ConcurrentLinkedDeque, because it will only be read by one thread.
          _jobsToProcess = new ArrayDeque<>(jobs);
        }
      }
    }

    @Override
    public void run() {
      Random rand = new Random();
      int r = 0; //indicates current round.

      //check if any seed got adding back.
      while (r <= MAX_RETRY_ROUNDS) {
        int totalPages = 0;
        for (ProducerJob job : _jobsToProcess) {
          totalPages += job.getPagesSize();
        }
        if (r > 0) {
          LOG.info(String.format("Starting #%d round retries of size %d for %s", r, totalPages, _country));
        }

        long requestSleep = (long) Math.ceil(1000 / REQUESTS_PER_SECOND);
        int checkPoint = Math.max(1, (int) Math.round(Math.ceil(totalPages / REPORT_PARTITIONS)));
        //retries needs to be concurrent because multiple threads will write to it.
        ConcurrentLinkedDeque<ProducerJob> retries = new ConcurrentLinkedDeque<>();
        ExecutorService es = Executors.newFixedThreadPool(10, new ThreadFactory() {
          @Override
          public Thread newThread(Runnable r) {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(true);
            return t;
          }
        });

        int checkPointCount = 0;
        int totalProcessed = 0;
        List<ProducerJob> batch = new ArrayList<>(BATCH_SIZE);

        while (!_jobsToProcess.isEmpty()) {
          //This is the only place to poll job from queue. Writing to a new queue is async.
          ProducerJob job = _jobsToProcess.poll();
          checkPointCount += job.getPagesSize();
          if (checkPointCount >= checkPoint) {
            totalProcessed += checkPointCount;
            checkPointCount = 0;
            LOG.info(String.format("ResponseProducer progress: %d of %d processed for %s", totalProcessed, totalPages,
                _country));
          }

          if (batch.size() < BATCH_SIZE) {
            batch.add(job);
          }

          if (batch.size() == BATCH_SIZE) {
            submitJob(requestSleep, retries, es, batch);
            batch = new ArrayList<>(BATCH_SIZE);
          }
        }
        //Send the last batch
        if (!batch.isEmpty()) {
          submitJob(requestSleep, retries, es, batch);
        }
        LOG.info(String.format("Submitted all jobs at round %d.", r));

        try {
          es.shutdown(); //stop accepting new requests
          LOG.info(
              String.format("Wait for download-query-data jobs to finish at round %d... Next round now has size %d.", r,
                  retries.size()));
          boolean terminated = es.awaitTermination(TIME_OUT, TimeUnit.MINUTES);
          if (!terminated) {
            es.shutdownNow();
            LOG.warn(String.format(
                "Timed out while downloading query data for country-%s at round %d. Next round now has size %d.",
                _country, r, retries.size()));
          }
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }

        if (retries.isEmpty()) {
          break; //game over
        }

        ++r;
        _jobsToProcess = retries;
        try {
          //Cool down before starting the next round of retry. Google is already quite upset.
          //As it gets more and more upset, we give it more and more time to cool down.
          Thread.sleep(INITIAL_COOL_DOWN + COOL_DOWN_STEP * rand.nextInt(r));
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      if (r == MAX_RETRY_ROUNDS + 1) {
        LOG.error(String.format("Exceeded maximum retries. There are %d unprocessed jobs.", _jobsToProcess.size()));
        StringBuilder sb = new StringBuilder();
        sb.append("You can add as hot start jobs to continue: ")
            .append(System.lineSeparator())
            .append(System.lineSeparator());
        sb.append(ProducerJob.serialize(_jobsToProcess));
        sb.append(System.lineSeparator());
        LOG.error(sb.toString());
      }
      LOG.info(String.format("ResponseProducer finishes for %s from %s to %s at retry round %d", _country, _startDate,
          _endDate, r));
    }

    private void submitJob(long requestSleep, ConcurrentLinkedDeque<ProducerJob> retries, ExecutorService es,
        List<ProducerJob> batch) {
      try {
        Thread.sleep(requestSleep); //Control the speed of sending API requests
      } catch (InterruptedException ignored) {
      }

      es.submit(getResponses(batch, retries, _cachedQueries));
    }

    /**
     * Call the API, then
     * OnSuccess: put each record into the responseQueue
     * OnFailure: add current job back to retries
     */
    private Runnable getResponse(final ProducerJob job, final ConcurrentLinkedDeque<ProducerJob> retries,
        final LinkedBlockingDeque<String[]> responseQueue) {

      return new Runnable() {
        @Override
        public void run() {
          try {
            final ArrayList<ApiDimensionFilter> filters = new ArrayList<>();
            filters.addAll(_filterMap.values());
            filters.add(GoogleWebmasterFilter.pageFilter(job.getOperator(), job.getPage()));

            List<String[]> results =
                _webmaster.performSearchAnalyticsQuery(job.getStartDate(), job.getEndDate(), QUERY_LIMIT,
                    _requestedDimensions, _requestedMetrics, filters);
            onSuccess(job, results, responseQueue, retries);
          } catch (IOException e) {
            onFailure(e.getMessage(), job, retries);
          }
        }
      };
    }

    /**
     * Call the APIs with a batch request
     * OnSuccess: put each record into the responseQueue
     * OnFailure: add current job to retries
     */
    private Runnable getResponses(final List<ProducerJob> jobs, final ConcurrentLinkedDeque<ProducerJob> retries,
        final LinkedBlockingDeque<String[]> responseQueue) {
      final int size = jobs.size();
      if (size == 1) {
        return getResponse(jobs.get(0), retries, responseQueue);
      }
      final ResponseProducer producer = this;
      return new Runnable() {
        @Override
        public void run() {
          try {
            List<ArrayList<ApiDimensionFilter>> filterList = new ArrayList<>(size);
            List<JsonBatchCallback<SearchAnalyticsQueryResponse>> callbackList = new ArrayList<>(size);
            for (ProducerJob j : jobs) {
              final ProducerJob job = j; //to capture this variable
              final String page = job.getPage();
              final ArrayList<ApiDimensionFilter> filters = new ArrayList<>();
              filters.addAll(_filterMap.values());
              filters.add(GoogleWebmasterFilter.pageFilter(job.getOperator(), page));

              filterList.add(filters);
              callbackList.add(new JsonBatchCallback<SearchAnalyticsQueryResponse>() {
                @Override
                public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders) throws IOException {
                  producer.onFailure(e.getMessage(), job, retries);
                }

                @Override
                public void onSuccess(SearchAnalyticsQueryResponse searchAnalyticsQueryResponse,
                    HttpHeaders responseHeaders) throws IOException {
                  List<String[]> results =
                      GoogleWebmasterDataFetcher.convertResponse(_requestedMetrics, searchAnalyticsQueryResponse);
                  producer.onSuccess(job, results, responseQueue, retries);
                }
              });
              LOG.debug("Ready to submit " + job);
            }
            _webmaster.performSearchAnalyticsQueryInBatch(jobs, filterList, callbackList, _requestedDimensions,
                QUERY_LIMIT);
          } catch (IOException e) {
            LOG.warn("Batch request failed. Jobs: " + Joiner.on(",").join(jobs));
            for (ProducerJob job : jobs) {
              retries.add(job);
            }
          }
        }
      };
    }

    private void onFailure(String errMsg, ProducerJob job, ConcurrentLinkedDeque<ProducerJob> retries) {
      LOG.debug(String.format("OnFailure: will retry job %s.%sReason:%s", job, System.lineSeparator(), errMsg));
      retries.add(job);
    }

    private void onSuccess(ProducerJob job, List<String[]> results, LinkedBlockingDeque<String[]> responseQueue,
        ConcurrentLinkedDeque<ProducerJob> pagesToRetry) {
      int size = results.size();
      if (size == GoogleWebmasterClient.API_ROW_LIMIT) {
        List<? extends ProducerJob> granularJobs = job.partitionJobs();
        if (granularJobs.isEmpty()) {
          //The job is not divisible
          //TODO: 99.99% cases we are good. But what if it happens, what can we do?
          LOG.warn(String.format(
              "There might be more query data for your job %s. Currently, downloading more than the Google API limit '%d' is not supported.",
              job, GoogleWebmasterClient.API_ROW_LIMIT));
        } else {
          LOG.info(String.format("Partition current job %s", job));
          pagesToRetry.addAll(granularJobs);
          return;
        }
      }

      LOG.debug(String.format("Finished %s. Records %d.", job, size));
      try {
        for (String[] r : results) {
          responseQueue.put(r);
        }
      } catch (InterruptedException e) {
        LOG.error(e.getMessage());
        throw new RuntimeException(e);
      }
    }
  }
}