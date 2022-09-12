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

package org.apache.gobblin.util.limiter;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import com.linkedin.common.callback.Callback;
import com.linkedin.data.template.GetMode;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.RestLiResponseException;
import com.linkedin.restli.common.HttpStatus;

import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.restli.throttling.PermitAllocation;
import org.apache.gobblin.restli.throttling.PermitRequest;
import org.apache.gobblin.restli.throttling.ThrottlingProtocolVersion;
import org.apache.gobblin.util.ClosableTimerContext;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.NoopCloseable;
import org.apache.gobblin.util.Sleeper;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
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
  public static final int MAX_GROWTH_REQUEST = 2;
  private static final long GET_PERMITS_MAX_SLEEP_MILLIS = 1000;

  private static final ScheduledExecutorService SCHEDULE_EXECUTOR_SERVICE =
      Executors.newScheduledThreadPool(1, ExecutorsUtils.newDaemonThreadFactory(Optional.of(log),
          Optional.of(BatchedPermitsRequester.class.getName() + "-schedule-%d")));

  @Getter(AccessLevel.PROTECTED) @VisibleForTesting
  private final PermitBatchContainer permitBatchContainer;
  private final Lock lock;
  private final Condition newPermitsAvailable;
  private final Semaphore requestSemaphore;
  private final PermitRequest basePermitRequest;
  private final RequestSender requestSender;
  private final Timer restRequestTimer;
  private final Histogram restRequestHistogram;

  private volatile AtomicInteger retries = new AtomicInteger(0);
  private final RetryStatus retryStatus;
  private final SynchronizedAverager permitsOutstanding;
  private final long targetMillisBetweenRequests;
  private final AtomicLong callbackCounter;
  /** Permit requests will timeout after this many millis. */
  private final long maxTimeout;
  /** Any request larger than this is known to be impossible to satisfy. */
  private long knownUnsatisfiablePermits;

  private volatile AllocationCallback currentCallback;

  @Builder
  private BatchedPermitsRequester(String resourceId, String requestorIdentifier,
      long targetMillisBetweenRequests, RequestSender requestSender, MetricContext metricContext, long maxTimeoutMillis) {

    Preconditions.checkArgument(!Strings.isNullOrEmpty(resourceId), "Must provide a resource id.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(requestorIdentifier), "Must provide a requestor identifier.");

    this.permitBatchContainer = new PermitBatchContainer();
    this.lock = new ReentrantLock();
    this.newPermitsAvailable = this.lock.newCondition();
    /** Ensures there is only one in-flight request at a time. */
    this.requestSemaphore = new Semaphore(1);
    /** Number of not-yet-satisfied permits. */
    this.permitsOutstanding = new SynchronizedAverager();
    this.targetMillisBetweenRequests = targetMillisBetweenRequests > 0 ? targetMillisBetweenRequests :
        DEFAULT_TARGET_MILLIS_BETWEEN_REQUESTS;
    this.requestSender = requestSender;
    this.retryStatus = new RetryStatus();

    this.basePermitRequest = new PermitRequest();
    this.basePermitRequest.setResource(resourceId);
    this.basePermitRequest.setRequestorIdentifier(requestorIdentifier);

    this.restRequestTimer = metricContext == null ? null : metricContext.timer(REST_REQUEST_TIMER);
    this.restRequestHistogram = metricContext == null ? null : metricContext.histogram(REST_REQUEST_PERMITS_HISTOGRAM);
    this.callbackCounter = new AtomicLong();
    this.maxTimeout = maxTimeoutMillis > 0 ? maxTimeoutMillis : 120000;
    this.knownUnsatisfiablePermits = Long.MAX_VALUE;
  }

  /**
   * Try to get a number of permits from this requester.
   * @return true if permits were obtained successfully.
   */
  public boolean getPermits(long permits) throws InterruptedException {
    if (permits <= 0) {
      return true;
    }
    long startTimeNanos = System.nanoTime();
    this.permitsOutstanding.addEntryWithWeight(permits);
    this.lock.lock();
    try {
      while (true) {
        if (permits >= this.knownUnsatisfiablePermits) {
          // We are requesting more permits than the remote policy will ever be able to satisfy, return immediately with no permits
          log.warn(String.format("Server has indicated number of permits is unsatisfiable. "
              + "Permits requested: %d, known unsatisfiable permits: %d ", permits, this.knownUnsatisfiablePermits));
          break;
        }
        if (elapsedMillis(startTimeNanos) > this.maxTimeout) {
          // Max timeout reached, break
          log.warn("Reached timeout waiting for permits. Timeout: " + this.maxTimeout);
          break;
        }
        if (this.permitBatchContainer.tryTake(permits)) {
          this.permitsOutstanding.removeEntryWithWeight(permits);
          return true;
        }
        if (this.retryStatus.canRetryWithinMillis(remainingTime(startTimeNanos, this.maxTimeout))) {
          long callbackCounterSnap = this.callbackCounter.get();
          maybeSendNewPermitRequest();
          if (this.callbackCounter.get() == callbackCounterSnap) {
            // If a callback has happened since we tried to send the new permit request, don't await
            // Since some request senders may be synchronous, we would have missed the notification
            boolean ignore = this.newPermitsAvailable.await(
                Math.min(GET_PERMITS_MAX_SLEEP_MILLIS, remainingTime(startTimeNanos, this.maxTimeout)), TimeUnit.MILLISECONDS);
          }
        } else {
          break;
        }
      }
    } finally {
      this.lock.unlock();
    }
    this.permitsOutstanding.removeEntryWithWeight(permits);
    return false;
  }

  private long remainingTime(long startTimeNanos, long timeout) {
    return Math.max(timeout - elapsedMillis(startTimeNanos), 0);
  }

  private long elapsedMillis(long startTimeNanos) {
    return (System.nanoTime() - startTimeNanos)/1000000;
  }

  /**
   * Send a new permit request to the server.
   */
  private synchronized void maybeSendNewPermitRequest() {
    while (!this.requestSemaphore.tryAcquire()) {
      if (this.currentCallback == null) {
        throw new IllegalStateException("Semaphore is unavailable while callback is null!");
      }
      if (this.currentCallback.elapsedTime() > 30000) {
        // If the previous callback has not returned after 30s, we consider the call lost and try again
        // Note we expect Rest.li to call onError for most failure situations, this logic just handles the edge
        // case were Rest.li fails somehow and we don't want to just hang.
        log.warn("Last request did not return after 30s, considering it lost and retrying.");
        this.currentCallback.clearCallback();
      } else {
        return;
      }
    }
    if (!this.retryStatus.canRetryNow()) {
      clearSemaphore();
      return;
    }
    try {
      long permits = computeNextPermitRequest();
      if (permits <= 0) {
        clearSemaphore();
        return;
      }

      PermitRequest permitRequest = this.basePermitRequest.copy();
      permitRequest.setPermits(permits);
      permitRequest.setMinPermits((long) this.permitsOutstanding.getAverageWeightOrZero());
      permitRequest.setVersion(ThrottlingProtocolVersion.WAIT_ON_CLIENT.ordinal());
      if (BatchedPermitsRequester.this.restRequestHistogram != null) {
        BatchedPermitsRequester.this.restRequestHistogram.update(permits);
      }

      log.debug("Sending permit request " + permitRequest);

      this.currentCallback = new AllocationCallback(
          BatchedPermitsRequester.this.restRequestTimer == null ? NoopCloseable.INSTANCE :
              new ClosableTimerContext(BatchedPermitsRequester.this.restRequestTimer.time()), new Sleeper());
      this.requestSender.sendRequest(permitRequest, currentCallback);
    } catch (CloneNotSupportedException cnse) {
      // This should never happen.
      clearSemaphore();
      throw new RuntimeException(cnse);
    }
  }

  @VisibleForTesting
  synchronized boolean reserveSemaphore() {
    return this.requestSemaphore.tryAcquire();
  }

  private synchronized void clearSemaphore() {
    if (this.requestSemaphore.availablePermits() > 0) {
      throw new IllegalStateException("Semaphore should have 0 permits!");
    }
    BatchedPermitsRequester.this.requestSemaphore.release();
    BatchedPermitsRequester.this.currentCallback = null;
  }

  /**
   * @return the number of permits we should request in the next request.
   */
  private long computeNextPermitRequest() {

    long candidatePermits = 0;

    long unsatisfiablePermits = this.permitsOutstanding.getTotalWeight() - this.permitBatchContainer.totalAvailablePermits;
    if (unsatisfiablePermits > 0) {
      candidatePermits = unsatisfiablePermits;
    }

    if (this.permitBatchContainer.batches.size() > 1) {
      // If there are multiple batches in the queue, don't create a new request
      return candidatePermits;
    }
    PermitBatch firstBatch = Iterables.getFirst(this.permitBatchContainer.batches.values(), null);

    if (firstBatch != null) {
      // If the current batch has more than 20% permits left, don't create a new request
      if ((double) firstBatch.getPermits() / firstBatch.getInitialPermits() > 0.2) {
        return candidatePermits;
      }

      double averageDepletionRate = firstBatch.getAverageDepletionRate();
      long candidatePermitsByDepletion =
          Math.min((long) (averageDepletionRate * this.targetMillisBetweenRequests), MAX_GROWTH_REQUEST *
              firstBatch.getInitialPermits());
      return Math.max(candidatePermits, candidatePermitsByDepletion);
    } else {
      return candidatePermits;
    }
  }

  @VisibleForTesting
  AllocationCallback createAllocationCallback(Sleeper sleeper) {
    return new AllocationCallback(new NoopCloseable(), sleeper);
  }

  /**
   * Callback for Rest request.
   */
  @VisibleForTesting
  class AllocationCallback implements Callback<Response<PermitAllocation>> {
    private final Closeable timerContext;
    private final Sleeper sleeper;
    private final long startTime = System.currentTimeMillis();

    private volatile boolean callbackCleared = false;

    public AllocationCallback(Closeable timerContext, Sleeper sleeper) {
      this.timerContext = timerContext;
      this.sleeper = sleeper;
    }

    @Override
    public void onError(Throwable exc) {
      BatchedPermitsRequester.this.lock.lock();

      try {
        if (exc instanceof RequestSender.NonRetriableException) {
          nonRetriableFail(exc, "Encountered non retriable error. ");
        }
        if (exc instanceof RestLiResponseException) {
          int errorCode = ((RestLiResponseException) exc).getStatus();
          if (NON_RETRIABLE_ERRORS.contains(errorCode)) {
            nonRetriableFail(exc, "Encountered non retriable error. HTTP response code: " + errorCode);
          }
        }

        BatchedPermitsRequester.this.retries.incrementAndGet();

        if (BatchedPermitsRequester.this.retries.get() >= MAX_RETRIES) {
          nonRetriableFail(exc, "Too many failures trying to communicate with throttling service.");
        } else {
          clearCallback();
          // retry
          maybeSendNewPermitRequest();
        }
      } catch (Throwable t) {
        log.error("Error on batched permits container.", t);
      } finally {
        BatchedPermitsRequester.this.lock.unlock();
        try {
          this.timerContext.close();
        } catch (IOException ioe) {
          // Do nothing
        }
      }
    }

    @Override
    public void onSuccess(Response<PermitAllocation> result) {
      BatchedPermitsRequester.this.retries.set(0);
      BatchedPermitsRequester.this.callbackCounter.incrementAndGet();
      BatchedPermitsRequester.this.lock.lock();
      try {
        PermitAllocation allocation = result.getEntity();

        log.debug("Received permit allocation " + allocation);

        Long retryDelay = allocation.getMinRetryDelayMillis(GetMode.NULL);
        if (retryDelay != null) {
          BatchedPermitsRequester.this.retryStatus.blockRetries(retryDelay, null);
        }

        long waitForUse = allocation.getWaitForPermitUseMillis(GetMode.DEFAULT);
        if (waitForUse > 0) {
          this.sleeper.sleep(waitForUse);
        }

        if (allocation.getUnsatisfiablePermits(GetMode.DEFAULT) > 0) {
          BatchedPermitsRequester.this.knownUnsatisfiablePermits = allocation.getUnsatisfiablePermits(GetMode.DEFAULT);
        }

        if (allocation.getPermits() > 0) {
          BatchedPermitsRequester.this.permitBatchContainer.addPermitAllocation(allocation);
        }

        clearCallback();
        if (allocation.getPermits() > 0) {
          BatchedPermitsRequester.this.newPermitsAvailable.signalAll();
        }
      } catch (InterruptedException ie) {
        // Thread was interrupted while waiting for permits to be usable. Permits are not yet usable, so will not
        // add permits to container
      } finally {
        try {
          this.timerContext.close();
        } catch (IOException ioe) {
          // Do nothing
        }
        BatchedPermitsRequester.this.lock.unlock();
      }
    }

    public long elapsedTime() {
      return System.currentTimeMillis() - this.startTime;
    }

    public synchronized void clearCallback() {
      if (this.callbackCleared) {
        return;
      }
      clearSemaphore();
      this.callbackCleared = true;
    }

    private void nonRetriableFail(Throwable exc, String msg) {
      BatchedPermitsRequester.this.retryStatus.blockRetries(RETRY_DELAY_ON_NON_RETRIABLE_EXCEPTION, exc);
      BatchedPermitsRequester.this.callbackCounter.incrementAndGet();
      clearCallback();
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
    private static final AtomicLong NEXT_KEY = new AtomicLong(0);

    private volatile long permits;
    private final long expiration;
    private final long autoIncrementKey;

    private final long initialPermits;
    private long firstUseTime;
    private long lastPermitUsedTime;
    private int permitRequests;

    PermitBatch(long permits, long expiration) {
      this.permits = permits;
      this.expiration = expiration;
      this.initialPermits = permits;
      this.autoIncrementKey = NEXT_KEY.getAndIncrement();
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
  static class PermitBatchContainer {
    private final TreeMultimap<Long, PermitBatch> batches = TreeMultimap.create(Ordering.natural(), new Comparator<PermitBatch>() {
      @Override
      public int compare(PermitBatch o1, PermitBatch o2) {
        return Long.compare(o1.autoIncrementKey, o2.autoIncrementKey);
      }
    });
    @Getter
    private volatile long totalAvailablePermits = 0;

    private synchronized boolean tryTake(long permits) {
      purgeExpiredBatches();
      if (this.totalAvailablePermits < permits) {
        return false;
      }
      this.totalAvailablePermits -= permits;
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
      // This can only happen if totalAvailablePermits is not in sync with the actual batches
      throw new RuntimeException("Total permits was unsynced! This is an error in code.");
    }

    /** Print the state of the container. Useful for debugging. */
    private synchronized void printState(String prefix) {
      StringBuilder builder = new StringBuilder(prefix).append("->");
      builder.append("BatchedPermitsRequester state (").append(hashCode()).append("): ");
      builder.append("TotalPermits: ").append(this.totalAvailablePermits).append(" ");
      builder.append("Batches(").append(this.batches.size()).append("): ");
      for (PermitBatch batch : this.batches.values()) {
        builder.append(batch.getPermits()).append(",");
      }
      log.info(builder.toString());
    }

    private synchronized void purgeExpiredBatches() {
      long now = System.currentTimeMillis();
      purgeBatches(this.batches.asMap().subMap(Long.MIN_VALUE, now).values().iterator());
    }

    private synchronized void purgeAll() {
      purgeBatches(this.batches.asMap().values().iterator());
    }

    private void purgeBatches(Iterator<Collection<PermitBatch>> iterator) {
      while (iterator.hasNext()) {
        Collection<PermitBatch> batches = iterator.next();
        for (PermitBatch batch : batches) {
          Long permitsExpired = batch.getPermits();
          this.totalAvailablePermits -= permitsExpired;
        }
        iterator.remove();
      }
    }

    private synchronized void addPermitAllocation(PermitAllocation allocation) {
      this.batches.put(allocation.getExpiration(),
          new PermitBatch(allocation.getPermits(), allocation.getExpiration()));
      this.totalAvailablePermits += allocation.getPermits();
    }
  }

  private static class SynchronizedAverager {
    private volatile long weight;
    private volatile long entries;

    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT", justification = "All methods updating volatile variables are synchronized")
    public synchronized void addEntryWithWeight(long weight) {
      this.entries++;
      this.weight += weight;
    }

    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT", justification = "All methods updating volatile variables are synchronized")
    public synchronized void removeEntryWithWeight(long weight) {
      if (this.entries == 0) {
        throw new IllegalStateException("Cannot have a negative number of entries.");
      }
      this.entries--;
      this.weight -= weight;
    }

    public synchronized double getAverageWeightOrZero() {
      if (this.entries == 0) {
        return 0;
      }
      return (double) this.weight / this.entries;
    }

    public long getTotalWeight() {
      return this.weight;
    }

    public long getNumEntries() {
      return this.entries;
    }
  }

  /**
   * Stores the retry state of a {@link BatchedPermitsRequester}, e.g. whether it can keep retrying.
   */
  private class RetryStatus {
    private long retryAt;
    @Nullable private Throwable exception;

    public boolean canRetryNow() {
      return canRetryWithinMillis(0);
    }

    public boolean canRetryWithinMillis(long millis) {
      return System.currentTimeMillis() + millis >= this.retryAt;
    }

    public void blockRetries(long millis, Throwable exception) {
      this.exception = exception;
      this.retryAt = System.currentTimeMillis() + millis;
      SCHEDULE_EXECUTOR_SERVICE.schedule(new Runnable() {
        @Override
        public void run() {
          maybeSendNewPermitRequest();
        }
      }, millis, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Clear all stored permits.
   */
  @VisibleForTesting
  public void clearAllStoredPermits() {
    this.getPermitBatchContainer().purgeAll();
  }
}
