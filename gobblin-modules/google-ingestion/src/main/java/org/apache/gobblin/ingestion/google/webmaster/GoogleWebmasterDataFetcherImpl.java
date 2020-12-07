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

package org.apache.gobblin.ingestion.google.webmaster;

import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.api.services.webmasters.model.ApiDimensionFilter;
import com.google.api.services.webmasters.model.SearchAnalyticsQueryResponse;
import com.google.common.base.Optional;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.limiter.RateBasedLimiter;

import static org.apache.gobblin.ingestion.google.webmaster.GoogleWebmasterFilter.*;


@Slf4j
public class GoogleWebmasterDataFetcherImpl extends GoogleWebmasterDataFetcher {
  private final double API_REQUESTS_PER_SECOND;
  private final RateBasedLimiter LIMITER;
  private final int PAGES_COUNT_COOLDOWN_TIME; //In seconds
  private final int PAGES_GET_COOLDOWN_TIME; //In seconds
  private final int GET_PAGES_RETRIES;

  private final String _siteProperty;
  private final GoogleWebmasterClient _client;
  private final List<ProducerJob> _jobs;

  GoogleWebmasterDataFetcherImpl(String siteProperty, GoogleWebmasterClient client, State wuState) throws IOException {
    _siteProperty = siteProperty;
    Preconditions.checkArgument(_siteProperty.endsWith("/"), "The site property must end in \"/\"");
    _client = client;
    _jobs = getHotStartJobs(wuState);
    API_REQUESTS_PER_SECOND = wuState.getPropAsDouble(GoogleWebMasterSource.KEY_PAGES_TUNING_REQUESTS_PER_SECOND, 4.5);
    PAGES_COUNT_COOLDOWN_TIME = wuState.getPropAsInt(GoogleWebMasterSource.KEY_PAGES_COUNT_TUNING_COOLDOWN_TIME, 30);
    PAGES_GET_COOLDOWN_TIME = wuState.getPropAsInt(GoogleWebMasterSource.KEY_PAGES_GET_TUNING_COOLDOWN_TIME, 5);
    LIMITER = new RateBasedLimiter(API_REQUESTS_PER_SECOND, TimeUnit.SECONDS);
    GET_PAGES_RETRIES = wuState.getPropAsInt(GoogleWebMasterSource.KEY_PAGES_TUNING_MAX_RETRIES, 120);
  }

  private static List<ProducerJob> getHotStartJobs(State wuState) {
    String hotStartString = wuState.getProp(GoogleWebMasterSource.KEY_REQUEST_HOT_START, "");
    if (!hotStartString.isEmpty()) {
      return SimpleProducerJob.deserialize(hotStartString);
    }
    return new ArrayList<>();
  }

  /**
   * Due to the limitation of the API, we can get a maximum of 5000 rows at a time. Another limitation is that, results are sorted by click count descending. If two rows have the same click count, they are sorted in an arbitrary way. (Read more at https://developers.google.com/webmaster-tools/v3/searchanalytics). So we try to get all pages by partitions, if a partition has 5000 rows returned. We try partition current partition into more granular levels.
   *
   */
  @Override
  public Collection<ProducerJob> getAllPages(String startDate, String endDate, String country, int rowLimit)
      throws IOException {
    log.info("Requested row limit: " + rowLimit);
    if (!_jobs.isEmpty()) {
      log.info("Service got hot started.");
      return _jobs;
    }
    ApiDimensionFilter countryFilter = GoogleWebmasterFilter.countryEqFilter(country);

    List<GoogleWebmasterFilter.Dimension> requestedDimensions = new ArrayList<>();
    requestedDimensions.add(GoogleWebmasterFilter.Dimension.PAGE);
    int expectedSize = -1;
    if (rowLimit >= GoogleWebmasterClient.API_ROW_LIMIT) {
      //expected size only makes sense when the data set size is larger than GoogleWebmasterClient.API_ROW_LIMIT
      expectedSize = getPagesSize(startDate, endDate, country, requestedDimensions, Arrays.asList(countryFilter));
      log.info(String.format("Expected number of pages is %d for market-%s from %s to %s", expectedSize,
          GoogleWebmasterFilter.countryFilterToString(countryFilter), startDate, endDate));
    }

    Queue<Pair<String, FilterOperator>> jobs = new ArrayDeque<>();
    jobs.add(Pair.of(_siteProperty, FilterOperator.CONTAINS));

    Collection<String> allPages = getPages(startDate, endDate, requestedDimensions, countryFilter, jobs,
        Math.min(rowLimit, GoogleWebmasterClient.API_ROW_LIMIT));
    int actualSize = allPages.size();
    log.info(String.format("A total of %d pages fetched for property %s at country-%s from %s to %s", actualSize,
        _siteProperty, country, startDate, endDate));

    if (expectedSize != -1 && actualSize != expectedSize) {
      log.warn(String.format("Expected page size is %d, but only able to get %d", expectedSize, actualSize));
    }

    ArrayDeque<ProducerJob> producerJobs = new ArrayDeque<>(actualSize);
    for (String page : allPages) {
      producerJobs.add(new SimpleProducerJob(page, startDate, endDate));
    }
    return producerJobs;
  }

  /**
   * @return the size of all pages data set
   */
  int getPagesSize(final String startDate, final String endDate, final String country,
      final List<Dimension> requestedDimensions, final List<ApiDimensionFilter> apiDimensionFilters) {
    final ExecutorService es = Executors.newCachedThreadPool(
        ExecutorsUtils.newDaemonThreadFactory(Optional.of(log), Optional.of(this.getClass().getSimpleName())));

    int startRow = 0;
    long groupSize = Math.max(1, Math.round(API_REQUESTS_PER_SECOND));
    List<Future<Integer>> results = new ArrayList<>((int) groupSize);

    int max = -1;
    while (true) {
      for (int i = 0; i < groupSize; ++i) {
        final int start = startRow;
        startRow += GoogleWebmasterClient.API_ROW_LIMIT;

        Future<Integer> submit = es.submit(() -> {
          log.info(String.format("Getting page size from %s...", start));
          String interruptedMsg =
              String.format("Interrupted while trying to get the size of all pages for %s. Current start row is %d.",
                  country, start);
          int r = 0;
          while (r <= GET_PAGES_RETRIES) {
            ++r;
            try {
              LIMITER.acquirePermits(1);
            } catch (InterruptedException e) {
              log.error("RateBasedLimiter: " + interruptedMsg, e);
              return -1;
            }

            try {
              List<String> pages =
                  _client.getPages(_siteProperty, startDate, endDate, country, GoogleWebmasterClient.API_ROW_LIMIT,
                      requestedDimensions, apiDimensionFilters, start);
              if (pages.size() == 0) {
                return 0;
              }
              int totalPages = pages.size() + start;
              log.info(String.format("At least %s pages exist. Continuing...", totalPages));
              return totalPages;
            } catch (IOException e) {
              log.info(String.format("Getting page size from %s failed due to %s. Retrying...", start, e.getMessage()));
              coolDown(r, PAGES_COUNT_COOLDOWN_TIME);
            }
          }
          throw new RuntimeException(String.format(
              "Getting all pages reaches the maximum number of retires %d. Date range: %s ~ %s. Country: %s.",
              GET_PAGES_RETRIES, startDate, endDate, country));
        });
        results.add(submit);
      }

      List<Integer> pagesCount = new ArrayList<>();
      for (Future<Integer> result : results) {
        try {
          pagesCount.add(result.get());
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      }

      if (pagesCount.stream().allMatch(x -> x == 0)) {
        return max;
      }
      max = Math.max(max, Collections.max(pagesCount));
      if (max % GoogleWebmasterClient.API_ROW_LIMIT != 0) {
        return max;
      }

      results.clear();
    }
  }

  private void coolDown(int r, int secondsInterval) {
    int milliSeconds = secondsInterval + (r / 5) * secondsInterval;
    milliSeconds *= 1000;
    log.info(String.format("Sleeping for %s seconds", milliSeconds / 1000));
    try {
      Thread.sleep(milliSeconds);
    } catch (InterruptedException e1) {
      throw new RuntimeException(e1);
    }
  }

  /**
   * Get all pages in an async mode.
   */
  private Collection<String> getPages(String startDate, String endDate, List<Dimension> dimensions,
      ApiDimensionFilter countryFilter, Queue<Pair<String, FilterOperator>> toProcess, int rowLimit) {
    String country = GoogleWebmasterFilter.countryFilterToString(countryFilter);

    ConcurrentLinkedDeque<String> allPages = new ConcurrentLinkedDeque<>();
    int r = 0;
    while (r <= GET_PAGES_RETRIES) {
      ++r;
      log.info(String.format("Get pages at round %d with size %d.", r, toProcess.size()));
      ConcurrentLinkedDeque<Pair<String, FilterOperator>> nextRound = new ConcurrentLinkedDeque<>();
      ExecutorService es = Executors.newFixedThreadPool(10,
          ExecutorsUtils.newDaemonThreadFactory(Optional.of(log), Optional.of(this.getClass().getSimpleName())));

      while (!toProcess.isEmpty()) {
        submitJob(toProcess.poll(), countryFilter, startDate, endDate, dimensions, es, allPages, nextRound, rowLimit);
      }
      //wait for jobs to finish and start next round if necessary.
      try {
        es.shutdown();
        boolean terminated = es.awaitTermination(5, TimeUnit.MINUTES);
        if (!terminated) {
          es.shutdownNow();
          log.warn("Timed out while getting all pages for country-{} at round {}. Next round now has size {}.", country,
              r, nextRound.size());
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      if (nextRound.isEmpty()) {
        break;
      }
      toProcess = nextRound;
      coolDown(r, PAGES_GET_COOLDOWN_TIME);
    }
    if (r == GET_PAGES_RETRIES + 1) {
      throw new RuntimeException(
          String.format("Getting all pages reaches the maximum number of retires %d. Date range: %s ~ %s. Country: %s.",
              GET_PAGES_RETRIES, startDate, endDate, country));
    }
    return allPages;
  }

  private void submitJob(final Pair<String, FilterOperator> job, final ApiDimensionFilter countryFilter,
      final String startDate, final String endDate, final List<Dimension> dimensions, ExecutorService es,
      final ConcurrentLinkedDeque<String> allPages, final ConcurrentLinkedDeque<Pair<String, FilterOperator>> nextRound,
      final int rowLimit) {
    es.submit(new Runnable() {
      @Override
      public void run() {
        try {
          LIMITER.acquirePermits(1);
        } catch (InterruptedException e) {
          throw new RuntimeException("RateBasedLimiter got interrupted.", e);
        }

        String countryString = countryFilterToString(countryFilter);
        List<ApiDimensionFilter> filters = new LinkedList<>();
        filters.add(countryFilter);

        String prefix = job.getLeft();
        FilterOperator operator = job.getRight();
        String jobString = String.format("job(prefix: %s, operator: %s)", prefix, operator);
        filters.add(GoogleWebmasterFilter.pageFilter(operator, prefix));
        List<String> pages;
        try {
          pages = _client.getPages(_siteProperty, startDate, endDate, countryString, rowLimit, dimensions, filters, 0);
          log.debug(
              String.format("%d pages fetched for %s market-%s from %s to %s.", pages.size(), jobString, countryString,
                  startDate, endDate));
        } catch (IOException e) {
          log.debug(String.format("%s failed due to %s. Retrying...", jobString, e.getMessage()));
          nextRound.add(job);
          return;
        }

        //If the number of pages is at the LIMIT, it must be a "CONTAINS" job.
        //We need to create sub-tasks, and check current page with "EQUALS"
        if (pages.size() == GoogleWebmasterClient.API_ROW_LIMIT) {
          log.info(String.format("Expanding the prefix '%s'", prefix));
          nextRound.add(Pair.of(prefix, FilterOperator.EQUALS));
          for (String expanded : getUrlPartitions(prefix)) {
            nextRound.add(Pair.of(expanded, FilterOperator.CONTAINS));
          }
        } else {
          //Otherwise, we've done with current job.
          allPages.addAll(pages);
        }
      }
    });
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