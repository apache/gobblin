/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.ingestion.google.webmaster;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.api.services.webmasters.model.ApiDimensionFilter;
import com.google.api.services.webmasters.model.SearchAnalyticsQueryResponse;
import com.google.common.base.Optional;

import avro.shaded.com.google.common.base.Joiner;
import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.WorkUnitState;
import gobblin.ingestion.google.AsyncIteratorWithDataSink;
import gobblin.ingestion.google.GoggleIngestionConfigurationKeys;
import gobblin.util.ExecutorsUtils;
import gobblin.util.limiter.RateBasedLimiter;


/**
 * This iterator holds a GoogleWebmasterDataFetcher, through which it get all pages. And then for each page, it will get all query data(Clicks, Impressions, CTR, Position). Basically, it will cache all pages got, and for each page, cache the detailed query data, and then iterate through them one by one.
 */
@Slf4j
class GoogleWebmasterExtractorIterator extends AsyncIteratorWithDataSink<String[]> {
  private final RateBasedLimiter LIMITER;
  private final int ROUND_TIME_OUT;
  private final int BATCH_SIZE;
  private final int TRIE_GROUP_SIZE;
  private final boolean APPLY_TRIE_ALGO;
  private final int MAX_RETRY_ROUNDS;
  private final int ROUND_COOL_DOWN;

  private final int PAGE_LIMIT;
  private final int QUERY_LIMIT;

  private final GoogleWebmasterDataFetcher _webmaster;
  private final String _startDate;
  private final String _endDate;
  private final String _country;
  private final Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter> _filterMap;
  //This is the requested dimensions sent to Google API
  private final List<GoogleWebmasterFilter.Dimension> _requestedDimensions;
  private final List<GoogleWebmasterDataFetcher.Metric> _requestedMetrics;
  private final WorkUnitState _wuState;
  private boolean _failed = false;

  public GoogleWebmasterExtractorIterator(GoogleWebmasterExtractorIterator iterator) {
    this(iterator._webmaster, iterator._startDate, iterator._endDate, iterator._requestedDimensions,
        iterator._requestedMetrics, iterator._filterMap, iterator._wuState);
  }

  public GoogleWebmasterExtractorIterator(GoogleWebmasterDataFetcher webmaster, String startDate, String endDate,
      List<GoogleWebmasterFilter.Dimension> requestedDimensions,
      List<GoogleWebmasterDataFetcher.Metric> requestedMetrics,
      Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter> filterMap, WorkUnitState wuState) {

    super(wuState.getPropAsInt(GoggleIngestionConfigurationKeys.SOURCE_ASYNC_ITERATOR_BLOCKING_QUEUE_SIZE, 2000),
        wuState.getPropAsInt(GoggleIngestionConfigurationKeys.SOURCE_ASYNC_ITERATOR_POLL_BLOCKING_TIME, 1));
    _wuState = wuState;
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

    ROUND_TIME_OUT = wuState.getPropAsInt(GoogleWebMasterSource.KEY_QUERIES_TUNING_TIME_OUT, 120);
    Preconditions.checkArgument(ROUND_TIME_OUT > 0, "Time out must be positive.");

    MAX_RETRY_ROUNDS = wuState.getPropAsInt(GoogleWebMasterSource.KEY_QUERIES_TUNING_RETRIES, 40);
    Preconditions.checkArgument(MAX_RETRY_ROUNDS >= 0, "Retry rounds cannot be negative.");

    ROUND_COOL_DOWN = wuState.getPropAsInt(GoogleWebMasterSource.KEY_QUERIES_TUNING_COOL_DOWN, 250);
    Preconditions.checkArgument(ROUND_COOL_DOWN >= 0, "Initial cool down time cannot be negative.");

    // QPS limit can be found at
    // https://developers.google.com/webmaster-tools/search-console-api-original/v3/limits
    // Setting the default QPS to be 2 batches per second with a batch of size 2.
    // So the default QPS is set at 2*2=4.
    double batchesPerSecond = wuState.getPropAsDouble(GoogleWebMasterSource.KEY_QUERIES_TUNING_BATCHES_PER_SECOND, 2);
    Preconditions.checkArgument(batchesPerSecond > 0, "Requests per second must be positive.");

    BATCH_SIZE = wuState.getPropAsInt(GoogleWebMasterSource.KEY_QUERIES_TUNING_BATCH_SIZE, 2);
    Preconditions.checkArgument(BATCH_SIZE >= 1, "Batch size must be at least 1.");

    LIMITER = new RateBasedLimiter(batchesPerSecond, TimeUnit.SECONDS);

    TRIE_GROUP_SIZE = wuState.getPropAsInt(GoogleWebMasterSource.KEY_QUERIES_TUNING_GROUP_SIZE, 500);
    Preconditions.checkArgument(TRIE_GROUP_SIZE >= 1, "Group size must be at least 1.");

    APPLY_TRIE_ALGO = wuState.getPropAsBoolean(GoogleWebMasterSource.KEY_REQUEST_TUNING_ALGORITHM, false);
    if (APPLY_TRIE_ALGO) {
      Preconditions.checkArgument(PAGE_LIMIT == GoogleWebmasterClient.API_ROW_LIMIT,
          "Page limit must be set at 5000 if you want to use the advanced algorithm. This indicates that you understand what you are doing.");
    }
  }

  @Override
  protected Runnable getProducerRunnable() {
    try {
      log.info("Start getting all pages for " + this.toString());
      Collection<ProducerJob> allJobs = _webmaster.getAllPages(_startDate, _endDate, _country, PAGE_LIMIT);
      return new ResponseProducer(allJobs);
    } catch (Exception e) {
      log.info(this.toString() + " failed while creating a ResponseProducer", e);
      _failed = true;
      sleepBeforeRetry();
      throw new RuntimeException(e);
    }
  }

  public boolean isFailed() {
    return _failed;
  }

  public String getCountry() {
    return _country;
  }

  public String getProperty() {
    return _webmaster.getSiteProperty();
  }

  @Override
  public String toString() {
    return String
        .format("GoogleWebmasterExtractorIterator{property=%s, startDate=%s, endDate=%s, country=%s}", getProperty(),
            _startDate, _endDate, _country);
  }

  private static void sleepBeforeRetry() {
    try {
      log.info("Sleep 20 seconds before task level retry");
      //Sleep 30 seconds before restarting, we need to set this because:
      // 1. Gobblin sleeps for 0 seconds at the first retry.
      // 2. Gobblin doesn't sleep between subsequent tasks.
      //See the quote limit at https://developers.google.com/webmaster-tools/search-console-api-original/v3/limits
      Thread.sleep(20000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * ResponseProducer gets the query data for allPages in an async way.
   * It utilize Executors.newCachedThreadPool to submit API request in a configurable speed.
   * API request speed can be tuned by BATCHES_PER_SECOND, ROUND_COOL_DOWN, COOL_DOWN_STEP and MAX_RETRY_ROUNDS.
   * The speed must be controlled because it cannot succeed the Google API quota, which can be found in your Google API Manager.
   * If you send the request too fast, you will get "403 Forbidden - Quota Exceeded" exception. Those pages will be handled by next round of retries.
   */
  private class ResponseProducer implements Runnable {
    private Deque<ProducerJob> _jobsToProcess;

    ResponseProducer(Collection<ProducerJob> jobs) {
      int size = jobs.size();
      if (size == 0) {
        _jobsToProcess = new ArrayDeque<>();
        return;
      }

      if (APPLY_TRIE_ALGO) {
        List<String> pages = new ArrayList<>(size);
        for (ProducerJob job : jobs) {
          pages.add(job.getPage());
        }
        UrlTrie trie = new UrlTrie(_webmaster.getSiteProperty(), pages);
        UrlTriePrefixGrouper grouper = new UrlTriePrefixGrouper(trie, TRIE_GROUP_SIZE);
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
      int r = 0; //indicates current round.

      try {
        //check if any seed got adding back.
        while (r <= MAX_RETRY_ROUNDS) {
          int totalPages = 0;
          for (ProducerJob job : _jobsToProcess) {
            totalPages += job.getPagesSize();
          }
          if (r > 0) {
            log.info(String.format("Starting #%d round retries of size %d for %s", r, totalPages, _country));
          }
          ProgressReporter reporter = new ProgressReporter(log, totalPages);

          //retries needs to be concurrent because multiple threads will write to it.
          ConcurrentLinkedDeque<ProducerJob> retries = new ConcurrentLinkedDeque<>();
          ExecutorService es = Executors.newFixedThreadPool(10,
              ExecutorsUtils.newDaemonThreadFactory(Optional.of(log), Optional.of(this.getClass().getSimpleName())));

          List<ProducerJob> batch = new ArrayList<>(BATCH_SIZE);

          while (!_jobsToProcess.isEmpty()) {
            //This is the only place to poll job from queue. Writing to a new queue is async.
            ProducerJob job = _jobsToProcess.poll();
            if (batch.size() < BATCH_SIZE) {
              batch.add(job);
            }
            if (batch.size() == BATCH_SIZE) {
              es.submit(getResponses(batch, retries, _dataSink, reporter));
              batch = new ArrayList<>(BATCH_SIZE);
            }
          }
          //Send the last batch
          if (!batch.isEmpty()) {
            es.submit(getResponses(batch, retries, _dataSink, reporter));
          }
          log.info(String.format("Submitted all jobs at round %d.", r));

          es.shutdown(); //stop accepting new requests
          boolean terminated = es.awaitTermination(ROUND_TIME_OUT, TimeUnit.MINUTES);
          if (!terminated) {
            es.shutdownNow();
            throw new RuntimeException(String.format(
                "Timed out while downloading query data for country-%s at round %d. Next round now has size %d.",
                _country, r, retries.size()));
          }

          if (retries.isEmpty()) {
            break; //game over
          }

          ++r;
          _jobsToProcess = retries;
          //Cool down before starting the next round of retry
          Thread.sleep(ROUND_COOL_DOWN);
        }

        if (r == MAX_RETRY_ROUNDS + 1) {
          log.error(String.format("Exceeded maximum retries. There are %d unprocessed jobs.", _jobsToProcess.size()));
          StringBuilder sb = new StringBuilder();
          sb.append("You can add as hot start jobs to continue: ").append(System.lineSeparator())
              .append(System.lineSeparator());
          sb.append(ProducerJob.serialize(_jobsToProcess));
          sb.append(System.lineSeparator());
          log.error(sb.toString());
        }
        log.info(String
            .format("ResponseProducer finishes for %s from %s to %s at retry round %d", _country, _startDate, _endDate,
                r));
      } catch (InterruptedException e) {
        log.info(GoogleWebmasterExtractorIterator.this.toString() + " failed while executing the ResponseProducer");
        _failed = true;
        sleepBeforeRetry();
        throw new RuntimeException(e);
      }
    }

    /**
     * Call the API, then
     * OnSuccess: put each record into the responseQueue
     * OnFailure: add current job back to retries
     */
    private Runnable getResponse(final ProducerJob job, final ConcurrentLinkedDeque<ProducerJob> retries,
        final LinkedBlockingDeque<String[]> responseQueue, final ProgressReporter reporter) {

      return new Runnable() {
        @Override
        public void run() {
          try {
            final ArrayList<ApiDimensionFilter> filters = new ArrayList<>();
            filters.addAll(_filterMap.values());
            filters.add(GoogleWebmasterFilter.pageFilter(job.getOperator(), job.getPage()));

            LIMITER.acquirePermits(1);
            List<String[]> results = _webmaster
                .performSearchAnalyticsQuery(job.getStartDate(), job.getEndDate(), QUERY_LIMIT, _requestedDimensions,
                    _requestedMetrics, filters);
            onSuccess(job, results, responseQueue, retries);
            reporter.report(job.getPagesSize(), _country);
          } catch (IOException e) {
            onFailure(e.getMessage(), job, retries);
          } catch (InterruptedException e) {
            log.error(String
                .format("Interrupted while trying to get queries for job %s. Current retry size is %d.", job,
                    retries.size()));
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
        final LinkedBlockingDeque<String[]> responseQueue, final ProgressReporter reporter) {
      final int size = jobs.size();
      if (size == 1) {
        return getResponse(jobs.get(0), retries, responseQueue, reporter);
      }
      final ResponseProducer producer = this;
      return new Runnable() {
        @Override
        public void run() {
          try {
            List<ArrayList<ApiDimensionFilter>> filterList = new ArrayList<>(size);
            List<JsonBatchCallback<SearchAnalyticsQueryResponse>> callbackList = new ArrayList<>(size);
            List<String> jobPages = new ArrayList<>();

            for (ProducerJob j : jobs) {
              jobPages.add(j.getPage());
              final ProducerJob job = j; //to capture this variable
              final String page = job.getPage();
              final ArrayList<ApiDimensionFilter> filters = new ArrayList<>();
              filters.addAll(_filterMap.values());
              filters.add(GoogleWebmasterFilter.pageFilter(job.getOperator(), page));

              filterList.add(filters);
              callbackList.add(new JsonBatchCallback<SearchAnalyticsQueryResponse>() {
                @Override
                public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders)
                    throws IOException {
                  producer.onFailure(e.getMessage(), job, retries);
                  log.debug(job.getPage() + " failed");
                }

                @Override
                public void onSuccess(SearchAnalyticsQueryResponse searchAnalyticsQueryResponse,
                    HttpHeaders responseHeaders)
                    throws IOException {
                  List<String[]> results =
                      GoogleWebmasterDataFetcher.convertResponse(_requestedMetrics, searchAnalyticsQueryResponse);
                  producer.onSuccess(job, results, responseQueue, retries);
                  log.debug(job.getPage() + " succeeded");
                }
              });
            }

            log.debug("Submitting jobs: " + Arrays.toString(jobPages.toArray()));
            LIMITER.acquirePermits(1);
            _webmaster
                .performSearchAnalyticsQueryInBatch(jobs, filterList, callbackList, _requestedDimensions, QUERY_LIMIT);
            int processed = 0;
            for (ProducerJob job : jobs) {
              processed += job.getPagesSize();
            }
            reporter.report(processed, _country);
          } catch (IOException e) {
            log.warn("Batch request failed. Jobs: " + Joiner.on(",").join(jobs));
            for (ProducerJob job : jobs) {
              retries.add(job);
            }
          } catch (InterruptedException e) {
            log.error(String.format("Interrupted while trying to get queries for jobs %s. Current retry size is %d.",
                Joiner.on(",").join(jobs), retries.size()));
          }
        }
      };
    }

    private void onFailure(String errMsg, ProducerJob job, ConcurrentLinkedDeque<ProducerJob> retries) {
      log.debug(String.format("OnFailure: will retry job %s.%sReason:%s", job, System.lineSeparator(), errMsg));
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
          log.warn(String.format(
              "There might be more query data for your job %s. Currently, downloading more than the Google API limit '%d' is not supported.",
              job, GoogleWebmasterClient.API_ROW_LIMIT));
        } else {
          log.info(String.format("Partition current job %s", job));
          pagesToRetry.addAll(granularJobs);
          return;
        }
      }

      log.debug(String.format("Finished %s. Current Queue size: %d. Record size: %d.", job, responseQueue.size(), size));
      try {
        for (String[] r : results) {
          responseQueue.put(r);
        }
      } catch (InterruptedException e) {
        log.error(e.getMessage());
        throw new RuntimeException(e);
      }
    }
  }
}

class ProgressReporter {
  private volatile int checkPointCount = 0; //Current check point accumulator
  private volatile int totalProcessed = 0; //Total processed accumulatro
  private final Logger _log;
  private final int _total; //Total number of jobs.
  private final int _checkPoint; //report at every check point

  ProgressReporter(Logger log, int total) {
    this(log, total, 20);
  }

  /**
   * @param total indicate the total size of the job
   * @param frequency indicate the frequency of reporting.
   *                  e.g. If set frequency to 20. Then, the reporter will report 20 times at every 5%.
   */
  private ProgressReporter(Logger log, int total, int frequency) {
    _log = log;
    _total = total;
    _checkPoint = (int) Math.max(1, Math.ceil(1.0 * total / frequency));
  }

  public synchronized void report(int progress, String country) {
    checkPointCount += progress;
    if (checkPointCount >= _checkPoint) {
      totalProcessed += checkPointCount;
      checkPointCount = 0;
      _log.info(String.format("Current progress: %d of %d processed for %s", totalProcessed, _total, country));
    }
  }
}