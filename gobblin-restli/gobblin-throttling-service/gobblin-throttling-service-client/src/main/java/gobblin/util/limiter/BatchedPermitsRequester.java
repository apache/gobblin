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

package gobblin.util.limiter;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.callback.Callback;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.RestClient;
import com.linkedin.restli.client.RestLiResponseException;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.HttpStatus;

import gobblin.metrics.MetricContext;
import gobblin.restli.throttling.PermitAllocation;
import gobblin.restli.throttling.PermitRequest;
import gobblin.restli.throttling.PermitsGetRequestBuilder;
import gobblin.restli.throttling.PermitsRequestBuilders;
import gobblin.util.NoopCloseable;

import javax.annotation.concurrent.NotThreadSafe;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * An object that requests batches of permits from an external throttling server. It tries to hide the latency of doing
 * external permit requests by requesting them in batches and preemptively requesting permits before the current ones
 * are exhausted.
 */
@Slf4j
class BatchedPermitsRequester {

  public static final String REST_REQUEST_TIMER = "limiter.restli.restRequestTimer";
  public static final String REST_REQUEST_PERMITS_HISTOGRAM = "limiter.restli.restRequestPermitsHistogram";

  /** These status codes are considered non-retriable. */
  public static final ImmutableSet<Integer> NON_RETRIABLE_ERRORS = ImmutableSet.of(HttpStatus.S_403_FORBIDDEN.getCode(),
      HttpStatus.S_422_UNPROCESSABLE_ENTITY.getCode());
  /** Target frequency at which external requests are performed. */
  public static final long DEFAULT_TARGET_MILLIS_BETWEEN_REQUESTS = 10000;

  /** Maximum number of retries to communicate with the server. */
  protected static final int MAX_RETRIES = 5;

  private static final long RETRY_DELAY_ON_NON_RETRIABLE_EXCEPTION = 60000; // 10 minutes
  private static final double MAX_DEPLETION_RATE = 1e20;

  private final PermitBatchContainer permitBatchContainer;
  private final RestClient restClient;
  private final Lock lock;
  private final Condition newPermitsAvailable;
  private final Semaphore requestSemaphore;
  private final PermitRequest basePermitRequest;
  private final RequestSender requestSender;
  private final Timer restRequestTimer;
  private final Histogram restRequestHistogram;

  private volatile int retries = 0;
  private final RetryStatus retryStatus;
  private final AtomicLong permitsOutstanding;
  private final long targetMillisBetweenRequests;

  @Builder
  private BatchedPermitsRequester(RestClient restClient, String resourceId, String requestorIdentifier,
      long targetMillisBetweenRequests, @VisibleForTesting RequestSender requestSender, MetricContext metricContext) {

    Preconditions.checkNotNull(restClient, "Must provide a Rest client.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(resourceId), "Must provide a resource id.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(requestorIdentifier), "Must provide a requestor identifier.");

    this.permitBatchContainer = new PermitBatchContainer();
    this.restClient = restClient;
    this.lock = new ReentrantLock();
    this.newPermitsAvailable = this.lock.newCondition();
    /** Ensures there is only one in-flight request at a time. */
    this.requestSemaphore = new Semaphore(1);
    /** Number of not-yet-satisfied permits. */
    this.permitsOutstanding = new AtomicLong();
    this.targetMillisBetweenRequests = targetMillisBetweenRequests > 0 ? targetMillisBetweenRequests :
        DEFAULT_TARGET_MILLIS_BETWEEN_REQUESTS;
    this.requestSender = requestSender == null ? new RequestSender(this.restClient) : requestSender;
    this.retryStatus = new RetryStatus();

    this.basePermitRequest = new PermitRequest();
    this.basePermitRequest.setResource(resourceId);
    this.basePermitRequest.setRequestorIdentifier(requestorIdentifier);

    this.restRequestTimer = metricContext == null ? null : metricContext.timer(REST_REQUEST_TIMER);
    this.restRequestHistogram = metricContext == null ? null : metricContext.histogram(REST_REQUEST_PERMITS_HISTOGRAM);
  }

  /**
   * Try to get a number of permits from this requester.
   * @return true if permits were obtained successfully.
   */
  public boolean getPermits(long permits) throws InterruptedException {
    if (permits <= 0) {
      return true;
    }
    this.permitsOutstanding.addAndGet(permits);
    this.lock.lock();
    try {
      while (true) {
        if (this.permitBatchContainer.tryTake(permits)) {
          this.permitsOutstanding.addAndGet(-1 * permits);
          return true;
        }
        if (this.retryStatus.canRetry()) {
          maybeSendNewPermitRequest();
          this.newPermitsAvailable.await();
        } else {
          break;
        }
      }
    } finally {
      this.lock.unlock();
    }
    return false;
  }

  /**
   * Send a new permit request to the server.
   */
  private void maybeSendNewPermitRequest() {
    if (!this.requestSemaphore.tryAcquire() || !this.retryStatus.canRetry()) {
      return;
    }
    try {
      long permits = computeNextPermitRequest();
      if (permits <= 0) {
        this.requestSemaphore.release();
        return;
      }

      PermitRequest permitRequest = this.basePermitRequest.copy();
      permitRequest.setPermits(permits);
      if (BatchedPermitsRequester.this.restRequestHistogram != null) {
        BatchedPermitsRequester.this.restRequestHistogram.update(permits);
      }

      this.requestSender.sendRequest(permitRequest, new AllocationCallback(
          BatchedPermitsRequester.this.restRequestTimer == null ? NoopCloseable.INSTANCE :
              BatchedPermitsRequester.this.restRequestTimer.time()));
    } catch (CloneNotSupportedException cnse) {
      // This should never happen.
      this.requestSemaphore.release();
      throw new RuntimeException(cnse);
    }
  }

  /**
   * @return the number of permits we should request in the next request.
   */
  private long computeNextPermitRequest() {

    long candidatePermits = 0;

    long unsatisfiablePermits = this.permitsOutstanding.get() - this.permitBatchContainer.totalPermits;
    if (unsatisfiablePermits > 0) {
      candidatePermits = unsatisfiablePermits;
    }

    if (this.permitBatchContainer.batches.size() > 1) {
      // If there are multiple batches in the queue, don't create a new request
      return candidatePermits;
    }
    Map.Entry<Long, PermitBatch> firstEntry = this.permitBatchContainer.batches.firstEntry();

    if (firstEntry != null) {
      PermitBatch permitBatch = firstEntry.getValue();
      // If the current batch has more than 20% permits left, don't create a new request
      if ((double) permitBatch.getPermits() / permitBatch.getInitialPermits() > 0.2) {
        return candidatePermits;
      }

      double averageDepletionRate = permitBatch.getAverageDepletionRate();
      long candidatePermitsByDepletion =
          Math.min((long) (averageDepletionRate * this.targetMillisBetweenRequests), 2 * permitBatch.getInitialPermits());
      return Math.max(candidatePermits, candidatePermitsByDepletion);
    } else {
      return candidatePermits;
    }
  }

  /**
   * Callback for Rest request.
   */
  @RequiredArgsConstructor
  private class AllocationCallback implements Callback<Response<PermitAllocation>> {
    private final Closeable timerContext;

    @Override
    public void onError(Throwable exc) {
      BatchedPermitsRequester.this.lock.lock();

      try {
        if (exc instanceof RestLiResponseException) {
          int errorCode = ((RestLiResponseException) exc).getStatus();
          if (NON_RETRIABLE_ERRORS.contains(errorCode)) {
            nonRetriableFail(exc, "Encountered non retriable error.");
          }
        }

        BatchedPermitsRequester.this.retries++;
        if (BatchedPermitsRequester.this.retries >= MAX_RETRIES) {
          nonRetriableFail(exc, "Too many failures trying to communicate with throttling service.");
        } else {
          BatchedPermitsRequester.this.requestSemaphore.release();
          // retry
          maybeSendNewPermitRequest();
        }
      } catch (Throwable t) {
        log.error("Error on batched permits container.", t);
      } finally {
        try {
          this.timerContext.close();
        } catch (IOException ioe) {
          // Do nothing
        }
        BatchedPermitsRequester.this.lock.unlock();
      }
    }

    @Override
    public void onSuccess(Response<PermitAllocation> result) {
      BatchedPermitsRequester.this.retries = 0;
      BatchedPermitsRequester.this.lock.lock();
      try {
        PermitAllocation allocation = result.getEntity();

        if (allocation.getPermits() <= 0) {
          onError(new IllegalStateException("Server returned no permits."));
        }

        BatchedPermitsRequester.this.permitBatchContainer.addPermitAllocation(allocation);
        BatchedPermitsRequester.this.requestSemaphore.release();
        BatchedPermitsRequester.this.newPermitsAvailable.signalAll();
      } finally {
        try {
          this.timerContext.close();
        } catch (IOException ioe) {
          // Do nothing
        }
        BatchedPermitsRequester.this.lock.unlock();
      }
    }

    private void nonRetriableFail(Throwable exc, String msg) {
      BatchedPermitsRequester.this.retryStatus.blockRetries(RETRY_DELAY_ON_NON_RETRIABLE_EXCEPTION, exc);
      BatchedPermitsRequester.this.requestSemaphore.release();
      log.error(msg, exc);

      // Wake up all threads so they can return false
      BatchedPermitsRequester.this.newPermitsAvailable.signalAll();
    }
  }

  /**
   * A batch of permits obtained from the server.
   */
  @NotThreadSafe
  @Getter
  private static class PermitBatch {
    private long permits;
    private final long expiration;

    private final long initialPermits;
    private long firstUseTime;
    private long lastPermitUsedTime;
    private int permitRequests;

    PermitBatch(long permits, long expiration) {
      this.permits = permits;
      this.expiration = expiration;
      this.initialPermits = permits;
    }

    /**
     * Use this number of permits. (Note, this does not check that there are enough permits).
     */
    private void decrementPermits(long value) {
      if (this.firstUseTime == 0) {
        this.firstUseTime = System.currentTimeMillis();
      }
      this.permitRequests++;
      this.permits -= value;
      if (this.permits <= 0) {
        this.lastPermitUsedTime = System.currentTimeMillis();
      }
    }

    /**
     * Get the average rate at which permits in this batch have been used.
     */
    private double getAverageDepletionRate() {
      if (this.firstUseTime == 0) {
        return MAX_DEPLETION_RATE;
      }
      long endTime = this.lastPermitUsedTime > 0 ? this.lastPermitUsedTime : System.currentTimeMillis();
      if (endTime > this.firstUseTime) {
        return (double) (this.initialPermits - this.permits) / (endTime - this.firstUseTime);
      } else {
        return MAX_DEPLETION_RATE;
      }
    }
  }

  /**
   * A container for {@link PermitBatch}es obtained from the server.
   */
  private static class PermitBatchContainer {
    private final TreeMap<Long, PermitBatch> batches = new TreeMap<>();
    private volatile long totalPermits = 0;

    private synchronized boolean tryTake(long permits) {
      purgeExpiredBatches();
      if (this.totalPermits < permits) {
        return false;
      }
      this.totalPermits -= permits;
      Iterator<PermitBatch> batchesIterator = this.batches.values().iterator();
      while (batchesIterator.hasNext()) {
        PermitBatch batch = batchesIterator.next();
        if (batch.getPermits() < permits) {
          permits -= batch.getPermits();
          batchesIterator.remove();
        } else {
          batch.decrementPermits(permits);
          return true;
        }
      }
      // This can only happen if totalPermits is not in sync with the actual batches
      throw new RuntimeException("Total permits was unsynced! This is an error in code.");
    }

    private synchronized void purgeExpiredBatches() {
      long now = System.currentTimeMillis();
      Iterator<PermitBatch> entries = this.batches.subMap(Long.MIN_VALUE, now).values().iterator();
      while (entries.hasNext()) {
        Long permitsExpired = entries.next().getPermits();
        this.totalPermits -= permitsExpired;
        entries.remove();
      }
    }

    private synchronized void addPermitAllocation(PermitAllocation allocation) {
      this.batches.put(allocation.getExpiration(),
          new PermitBatch(allocation.getPermits(), allocation.getExpiration()));
      this.totalPermits += allocation.getPermits();
    }
  }

  /**
   * Sends requests to the Rest server. Allows overriding for testing.
   */
  @AllArgsConstructor
  public static class RequestSender {
    private final RestClient restClient;

    protected void sendRequest(PermitRequest request, Callback<Response<PermitAllocation>> callback) {
      PermitsGetRequestBuilder getBuilder = new PermitsRequestBuilders().get();
      Request<PermitAllocation> fullRequest = getBuilder.id(new ComplexResourceKey<>(request, new EmptyRecord())).build();
      this.restClient.sendRequest(fullRequest, callback);
    }
  }

  /**
   * Stores the retry state of a {@link BatchedPermitsRequester}, e.g. whether it can keep retrying.
   */
  private static class RetryStatus {
    private long retryAt;
    private Throwable exception;

    public boolean canRetry() {
      return System.currentTimeMillis() > this.retryAt;
    }

    public void blockRetries(long millis, Throwable exception) {
      this.exception = exception;
      this.retryAt = System.currentTimeMillis() + millis;
    }
  }
}
