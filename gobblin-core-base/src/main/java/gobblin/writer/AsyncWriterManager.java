/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.writer;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.State;
import gobblin.instrumented.Instrumentable;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.MetricContext;
import gobblin.metrics.MetricNames;
import gobblin.metrics.Tag;
import gobblin.util.ConfigUtils;
import gobblin.util.FinalState;


/**
 * A Data Writer to use as a base for writing async writers.
 * This Data Writer wraps writers that implement the {@link AsyncDataWriter} interface
 * and provides the following features:
 * 1. Calculate metrics for: number of records in, attempted, successfully written, failed, latency.
 * 2. Wait for a specified amount of time on commit for all pending writes to complete.
 * 3. Do not proceed if a certain failure threshold is exceeded.
 * 4. Support a fixed number of retries on failure of individual records (TODO: retry strategies)
 * 5. Support a max number of outstanding / unacknowledged writes
 * 6. TODO: Support ordered / unordered write semantics
 *
 *
 */
@Slf4j
public class AsyncWriterManager<D> implements DataWriter<D>, Instrumentable, Closeable, FinalState {
  private static final long MILLIS_TO_NANOS = 1000 * 1000;
  public static final long COMMIT_TIMEOUT_IN_NANOS_DEFAULT = 60000L * MILLIS_TO_NANOS; // 1 minute
  public static final long COMMIT_STEP_WAITTIME_MILLIS_DEFAULT = 500; // 500 ms sleep while waiting for commit
  public static final double FAILURE_ALLOWANCE_DEFAULT = 0.0;
  public static final boolean RETRIES_ENABLED_DEFAULT = true;
  public static final int NUM_RETRIES_DEFAULT = 5;
  public static final int MAX_OUTSTANDING_WRITES_DEFAULT = 1000;

  private final boolean instrumentationEnabled;

  private MetricContext metricContext;
  protected final Closer closer;

  @VisibleForTesting
  Meter recordsAttempted;
  @VisibleForTesting
  Meter recordsIn;
  @VisibleForTesting
  Meter recordsSuccess;
  @VisibleForTesting
  Meter recordsFailed;
  @VisibleForTesting
  Meter bytesWritten;
  @VisibleForTesting
  Optional<Timer> dataWriterTimer;
  private final long commitTimeoutInNanos;
  private final long commitStepWaitTimeMillis;
  private final double failureAllowance;
  private final AsyncDataWriter asyncDataWriter;
  private final WriteCallback writeCallback;
  private final boolean retriesEnabled;
  private final int numRetries;
  private final Optional<LinkedBlockingQueue<Attempt>> retryQueue;
  private final int maxOutstandingWrites;
  private volatile Throwable cachedWriteException = null;


  @Getter
  @AllArgsConstructor
      /**
       * A class to store attempts at writing a record
       **/
  class Attempt {
    D record;
    int attemptNum;
    @Setter
    Throwable prevAttemptFailure;

    public void incAttempt() {
      ++attemptNum;
    }
  };



  @Override
  public void switchMetricContext(List<Tag<?>> tags) {
    this.metricContext = this.closer
        .register(Instrumented.newContextFromReferenceContext(this.metricContext, tags, Optional.<String> absent()));
    regenerateMetrics();
  }

  @Override
  public void switchMetricContext(MetricContext context) {
    this.metricContext = context;
    regenerateMetrics();
  }

  /** Default with no additional tags */
  @Override
  public List<Tag<?>> generateTags(State state) {
    return Lists.newArrayList();
  }

  @Nonnull
  @Override
  public MetricContext getMetricContext() {
    return this.metricContext;
  }

  @Override
  public boolean isInstrumentationEnabled() {
    return this.instrumentationEnabled;
  }

  /**
   * TODO: Figure out what this means for checkpointing.
   * Get final state for this object. By default this returns an empty {@link gobblin.configuration.State}, but
   * concrete subclasses can add information that will be added to the task state.
   * @return Empty {@link gobblin.configuration.State}.
   */
  @Override
  public State getFinalState() {
    return new State();
  }
  /**
   * Generates metrics for the instrumentation of this class.
   */
  protected void regenerateMetrics() {
    // Set up the metrics that are enabled regardless of instrumentation
    this.recordsIn = this.metricContext.meter(MetricNames.DataWriterMetrics.RECORDS_IN_METER);
    this.recordsAttempted = this.metricContext.meter(MetricNames.DataWriterMetrics.RECORDS_ATTEMPTED_METER);
    this.recordsSuccess =
        this.metricContext.meter(MetricNames.DataWriterMetrics.SUCCESSFUL_WRITES_METER);
    this.recordsFailed = this.metricContext.meter(MetricNames.DataWriterMetrics.FAILED_WRITES_METER);
    this.bytesWritten = this.metricContext.meter(MetricNames.DataWriterMetrics.BYTES_WRITTEN_METER);

    if (isInstrumentationEnabled()) {
      this.dataWriterTimer = Optional.of(this.metricContext.timer(MetricNames.DataWriterMetrics.WRITE_TIMER));
    } else {
      this.dataWriterTimer = Optional.absent();
    }
  }


  protected AsyncWriterManager(Config config,
      long commitTimeoutInNanos,
      long commitStepWaitTimeMillis,
      double failureAllowance,
      boolean retriesEnabled,
      int numRetries,
      int maxOutstandingWrites,
      @NonNull AsyncDataWriter asyncDataWriter)
  {
    this.closer = Closer.create();
    State state = ConfigUtils.configToState(config);
    this.instrumentationEnabled = GobblinMetrics.isEnabled(state);
    this.metricContext = this.closer.register(Instrumented.getMetricContext(state, asyncDataWriter.getClass()));

    regenerateMetrics();

    this.commitTimeoutInNanos = commitTimeoutInNanos;
    this.commitStepWaitTimeMillis = commitStepWaitTimeMillis;
    Preconditions.checkArgument((failureAllowance <= 1.0 && failureAllowance >= 0), "Failure Allowance must be a ratio between 0 and 1");
    this.failureAllowance = failureAllowance;
    this.retriesEnabled = retriesEnabled;
    if (this.retriesEnabled)
    {
      retryQueue = Optional.of(new LinkedBlockingQueue<Attempt>());
      this.numRetries = numRetries;
      Thread retryThread = new Thread(new RetryRunner());
      retryThread.setDaemon(true);
      retryThread.setName("AsyncWriterRetryThread");
      retryThread.start();
    }
    else
    {
      retryQueue = Optional.absent();
      this.numRetries = 0;
    }
    this.maxOutstandingWrites = maxOutstandingWrites;
    this.asyncDataWriter = asyncDataWriter;
    this.closer.register(asyncDataWriter);


    this.writeCallback = new WriteCallback() {
      @Override
      public void onSuccess(WriteResponse writeResponse) {
        recordsSuccess.mark();
        log.debug("Received success", writeResponse.getStringResponse());
      }

      @Override
      public void onFailure(Throwable throwable) {
        recordsFailed.mark();
      }
    };
  }

  @Override
  public void write(final D record)
      throws IOException {
    /**
     * A few ways to achieve max concurrency.
     * a) Keep a counter that increments and decrements.
     * Problem: you have to spin lock on the counter on a write... may not be too bad.
     * Dangers: If you don't decrement the counter on corner cases, you may have a false sense of how many outstanding
     * requests you have.
     * b) Use a bounded queue to store call attempts, write the returned futures into it, remove the future on callback success.
     * Problem: More state to keep in memory.
     * How do you deal with retries.. how do you ensure there is no starvation on retries?
     *
     * Doing a) for now.
     */

    maybeThrow();
    int spinNum = 0;
    while (recordsIn.getCount() - (recordsSuccess.getCount() + recordsFailed.getCount()) > maxOutstandingWrites)
    {
      // spin until we have space to write. Sleep 1 millis every spin
      try {
        Thread.sleep(1);
        ++spinNum;
        if (spinNum % 50 == 0) {
          log.info("Spinning due to pending writes, in = " + recordsIn.getCount() +
              ", success = " + recordsSuccess.getCount() + ", failed = " + recordsFailed.getCount() +
              ", maxOutstandingWrites = " + maxOutstandingWrites);
        }
      } catch (InterruptedException e) {
        Throwables.propagate(e);
      }
    }
    recordsIn.mark();
    if (this.retriesEnabled) {
      attemptWrite(new Attempt(record, 1, null));
    } else {
      recordsAttempted.mark();
      this.asyncDataWriter.write(record, writeCallback);
    }
  }

  /**
   * Checks if the current operating metrics would imply that
   * we're out of SLA on failures permitted
   * @return true if any failure would be fatal
   *
   * TODO: Add windowed stats to test for x% failures in y time window
   *
   */
  private boolean isFailureFatal()
  {
    return (failureAllowance == 0.0);
  }


  private void makeNextWriteThrow(Throwable t)
  {
    cachedWriteException = t;
  }

  private void maybeThrow() throws IOException {
    if (cachedWriteException != null)
    {
      throw new IOException("Irrecoverable failure on async write", cachedWriteException);
    }
  }

  private void attemptWrite(final Attempt attempt)
  {
    final long startTime = System.nanoTime();
    this.recordsAttempted.mark();
    this.asyncDataWriter.write(attempt.record, new WriteCallback() {

      @Override
      public void onSuccess(WriteResponse writeResponse) {

        recordsSuccess.mark();
        if (writeResponse.bytesWritten() > 0)
        {
          bytesWritten.mark(writeResponse.bytesWritten());
        }
        if (dataWriterTimer.isPresent()) {
          dataWriterTimer.get().update(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        }
      }

      @Override
      public void onFailure(Throwable exception) {
        if (dataWriterTimer.isPresent()) {
          dataWriterTimer.get().update(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        }
        if (attempt.attemptNum <= numRetries) { // attempts must == numRetries + 1
          attempt.incAttempt();
          attempt.setPrevAttemptFailure(exception);
          retryQueue.get().add(attempt);
        }
        else
        {
          recordsFailed.mark();
          log.debug("Failed to write record : " + attempt.getRecord().toString(), exception);
          // If this failure is fata, set the writer to throw an exception at this point
          if (isFailureFatal())
          {
            makeNextWriteThrow(exception);
          }
        }
      }
    });
  }

  private class RetryRunner implements Runnable {

    private final LinkedBlockingQueue<Attempt> _retryQueue;

    public RetryRunner() {
      Preconditions.checkArgument(retryQueue.isPresent(), "RetryQueue must be present for RetryRunner");
      _retryQueue = retryQueue.get();
    }

    @Override
    public void run() {
      while (true)
      {
        try {
          Attempt attempt = _retryQueue.take();
          if (attempt != null) {
            attemptWrite(attempt);
          }
        } catch (InterruptedException e) {
          log.info("Retry thread interrupted... will exit");
          Throwables.propagate(e);
        }
      }
    }
  }

  @Override
  public void cleanup()
      throws IOException {
    // legacy api ...
  }

  @Override
  public long recordsWritten() {
    return recordsSuccess.getCount();
  }

  @Override
  public long bytesWritten()
      throws IOException {
    return bytesWritten.getCount();
  }

  @Override
  public void close()
      throws IOException {
    log.debug("Close called");
    this.closer.close();
  }

  @Override
  public void commit()
      throws IOException {
    log.debug("Commit called, will wait for commitTimeout : " + commitTimeoutInNanos / MILLIS_TO_NANOS + "ms");
    long commitStartTime = System.nanoTime();
    asyncDataWriter.flush();
    while (((System.nanoTime() - commitStartTime) < commitTimeoutInNanos)  &&
        (recordsIn.getCount() != (recordsSuccess.getCount() + recordsFailed.getCount())))
    {
      log.debug("Commit waiting... records produced: " + recordsIn.getCount() + " written: "
          + recordsSuccess.getCount() + " failed: " + recordsFailed.getCount());
      try {
        Thread.sleep(commitStepWaitTimeMillis);
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while waiting for commit to complete", e);
      }
    }
    log.debug("Commit done waiting");
    long recordsProducedFinal = recordsIn.getCount();
    long recordsWrittenFinal = recordsSuccess.getCount();
    long recordsFailedFinal = recordsFailed.getCount();
    long unacknowledgedWrites  = recordsProducedFinal - recordsWrittenFinal - recordsFailedFinal;
    long totalFailures = unacknowledgedWrites + recordsFailedFinal;
    if (unacknowledgedWrites > 0) // timeout
    {
      log.warn("Timeout waiting for all writes to be acknowledged. Missing " + unacknowledgedWrites
          + " responses out of " + recordsProducedFinal);
    }
    if (totalFailures > 0 && recordsProducedFinal > 0)
    {
      String message = "Commit failed to write " + totalFailures
          + " records (" + recordsFailedFinal + " failed, " + unacknowledgedWrites + " unacknowledged) out of "
          + recordsProducedFinal + " produced.";
      double failureRatio = (double)totalFailures / (double)recordsProducedFinal;
      if (failureRatio > failureAllowance)
      {
        message += "\nAborting because this is greater than the failureAllowance percentage: " + failureAllowance*100.0;
        log.error(message);
        throw new IOException(message);
      }
      else
      {
        message += "\nCommitting because failureRatio percentage: " + (failureRatio * 100.0) +
            " is less than the failureAllowance percentage: " + (failureAllowance * 100.0);
        log.warn(message);
      }
    }
    log.info("Successfully committed " + recordsWrittenFinal + " records.");
  }


  public static AsyncWriterManagerBuilder builder() {
    return new AsyncWriterManagerBuilder();
  }

  public static class AsyncWriterManagerBuilder {
    private Config config = ConfigFactory.empty();
    private long commitTimeoutInNanos = COMMIT_TIMEOUT_IN_NANOS_DEFAULT;
    private long commitStepWaitTimeMillis = COMMIT_STEP_WAITTIME_MILLIS_DEFAULT;
    private double failureAllowance = FAILURE_ALLOWANCE_DEFAULT;
    private boolean retriesEnabled = RETRIES_ENABLED_DEFAULT;
    private int numRetries = NUM_RETRIES_DEFAULT;
    private int maxOutstandingWrites = MAX_OUTSTANDING_WRITES_DEFAULT;
    private AsyncDataWriter asyncDataWriter;

    public AsyncWriterManagerBuilder config(Config config)
    {
      this.config = config;
      return this;
    }

    public AsyncWriterManagerBuilder commitTimeoutInNanos(long commitTimeoutInNanos)
    {
      this.commitTimeoutInNanos = commitTimeoutInNanos;
      return this;
    }

    public AsyncWriterManagerBuilder commitStepWaitTimeInMillis(long commitStepWaitTimeMillis)
    {
      this.commitStepWaitTimeMillis = commitStepWaitTimeMillis;
      return this;
    }

    public AsyncWriterManagerBuilder failureAllowance(double failureAllowance)
    {
      Preconditions.checkArgument((failureAllowance <= 1.0 && failureAllowance >= 0), "Failure Allowance must be a ratio between 0 and 1");
      this.failureAllowance = failureAllowance;
      return this;
    }

    public AsyncWriterManagerBuilder asyncDataWriter(AsyncDataWriter asyncDataWriter)
    {
      this.asyncDataWriter = asyncDataWriter;
      return this;
    }

    public AsyncWriterManagerBuilder retriesEnabled(boolean retriesEnabled)
    {
      this.retriesEnabled = retriesEnabled;
      return this;
    }

    public AsyncWriterManagerBuilder numRetries(int numRetries)
    {
      this.numRetries = numRetries;
      return this;
    }

    public AsyncWriterManagerBuilder maxOutstandingWrites(int maxOutstandingWrites)
    {
      this.maxOutstandingWrites = maxOutstandingWrites;
      return this;
    }

    public AsyncWriterManager build()
    {
      return new AsyncWriterManager(config, commitTimeoutInNanos, commitStepWaitTimeMillis,
          failureAllowance, retriesEnabled, numRetries,
          maxOutstandingWrites,
          asyncDataWriter);
    }

  }

}
