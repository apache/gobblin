/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package gobblin.writer;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import lombok.Getter;
import lombok.Setter;
import gobblin.configuration.State;
import gobblin.instrumented.Instrumentable;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.MetricContext;
import gobblin.metrics.MetricNames;
import gobblin.metrics.Tag;
import gobblin.source.extractor.CheckpointableWatermark;
import gobblin.stream.RecordEnvelope;
import gobblin.util.ConfigUtils;
import gobblin.util.ExecutorsUtils;
import gobblin.util.FinalState;
import gobblin.writer.exception.NonTransientException;


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
public class AsyncWriterManager<D> implements WatermarkAwareWriter<D>, DataWriter<D>, Instrumentable, Closeable, FinalState {
  private static final long MILLIS_TO_NANOS = 1000 * 1000;
  public static final long COMMIT_TIMEOUT_MILLIS_DEFAULT = 60000L; // 1 minute
  public static final long COMMIT_STEP_WAITTIME_MILLIS_DEFAULT = 500; // 500 ms sleep while waiting for commit
  public static final double FAILURE_ALLOWANCE_RATIO_DEFAULT = 0.0;
  public static final boolean RETRIES_ENABLED_DEFAULT = true;
  public static final int NUM_RETRIES_DEFAULT = 5;
  public static final int MIN_RETRY_INTERVAL_MILLIS_DEFAULT = 3;
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
  private final long commitTimeoutMillis;
  private final long commitStepWaitTimeMillis;
  private final double failureAllowanceRatio;
  private final AsyncDataWriter asyncDataWriter;
  private final int numRetries;
  private final int minRetryIntervalMillis;
  private final Optional<ScheduledThreadPoolExecutor> retryThreadPool;
  private final Logger log;
  @VisibleForTesting
  final Optional<LinkedBlockingQueue<Attempt>> retryQueue;
  private final int maxOutstandingWrites;
  private final Semaphore writePermits;
  private volatile Throwable cachedWriteException = null;

  @Override
  public boolean isWatermarkCapable() {
    return true;
  }

  @Override
  public Map<String, CheckpointableWatermark> getCommittableWatermark() {
    throw new UnsupportedOperationException("This writer does not keep track of committed watermarks");
  }

  @Override
  public Map<String, CheckpointableWatermark> getUnacknowledgedWatermark() {
    throw new UnsupportedOperationException("This writer does not keep track of uncommitted watermarks");
  }

  /**
   * A class to store attempts at writing a record
   **/
  @Getter
  class Attempt {
    private final D record;
    private final Ackable ackable;
    private int attemptNum;
    @Setter
    private Throwable prevAttemptFailure; // Any failure
    @Setter
    private long prevAttemptTimestampNanos;

    void incAttempt() {
      ++this.attemptNum;
    }

    Attempt(D record, Ackable ackable) {
      this.record = record;
      this.ackable = ackable;
      this.attemptNum = 1;
      this.prevAttemptFailure = null;
      this.prevAttemptTimestampNanos = -1;
    }
  }

  @Override
  public void switchMetricContext(List<Tag<?>> tags) {
    this.metricContext = this.closer
        .register(Instrumented.newContextFromReferenceContext(this.metricContext, tags, Optional.<String>absent()));
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
    this.recordsSuccess = this.metricContext.meter(MetricNames.DataWriterMetrics.SUCCESSFUL_WRITES_METER);
    this.recordsFailed = this.metricContext.meter(MetricNames.DataWriterMetrics.FAILED_WRITES_METER);
    this.bytesWritten = this.metricContext.meter(MetricNames.DataWriterMetrics.BYTES_WRITTEN_METER);

    if (isInstrumentationEnabled()) {
      this.dataWriterTimer = Optional.<Timer>of(this.metricContext.timer(MetricNames.DataWriterMetrics.WRITE_TIMER));
    } else {
      this.dataWriterTimer = Optional.absent();
    }
  }

  protected AsyncWriterManager(Config config, long commitTimeoutMillis, long commitStepWaitTimeMillis,
      double failureAllowanceRatio, boolean retriesEnabled, int numRetries, int minRetryIntervalMillis,
      int maxOutstandingWrites, AsyncDataWriter asyncDataWriter, Optional<Logger> loggerOptional) {
    Preconditions.checkArgument(commitTimeoutMillis > 0, "Commit timeout must be greater than 0");
    Preconditions.checkArgument(commitStepWaitTimeMillis > 0, "Commit step wait time must be greater than 0");
    Preconditions.checkArgument(commitStepWaitTimeMillis < commitTimeoutMillis, "Commit step wait time must be less "
        + "than commit timeout");
    Preconditions.checkArgument((failureAllowanceRatio <= 1.0 && failureAllowanceRatio >= 0),
        "Failure Allowance must be a ratio between 0 and 1");
    Preconditions.checkArgument(maxOutstandingWrites > 0, "Max outstanding writes must be greater than 0");
    Preconditions.checkNotNull(asyncDataWriter, "Async Data Writer cannot be null");

    this.log = loggerOptional.isPresent()? loggerOptional.get() : LoggerFactory.getLogger(AsyncWriterManager.class);
    this.closer = Closer.create();
    State state = ConfigUtils.configToState(config);
    this.instrumentationEnabled = GobblinMetrics.isEnabled(state);
    this.metricContext = this.closer.register(Instrumented.getMetricContext(state, asyncDataWriter.getClass()));

    regenerateMetrics();

    this.commitTimeoutMillis = commitTimeoutMillis;
    this.commitStepWaitTimeMillis = commitStepWaitTimeMillis;
    this.failureAllowanceRatio = failureAllowanceRatio;
    this.minRetryIntervalMillis = minRetryIntervalMillis;
    if (retriesEnabled) {
      this.numRetries = numRetries;
      this.retryQueue = Optional.of(new LinkedBlockingQueue<Attempt>());
      this.retryThreadPool = Optional.of(new ScheduledThreadPoolExecutor(1,
          ExecutorsUtils.newDaemonThreadFactory(Optional.of(this.log), Optional.of("AsyncWriteManagerRetry-%d"))));
      this.retryThreadPool.get().execute(new RetryRunner());
    } else {
      this.numRetries = 0;
      this.retryQueue = Optional.absent();
      this.retryThreadPool = Optional.absent();
    }
    this.maxOutstandingWrites = maxOutstandingWrites;
    this.writePermits = new Semaphore(maxOutstandingWrites);
    this.asyncDataWriter = asyncDataWriter;
    this.closer.register(asyncDataWriter);
  }

  @Override
  public void writeEnvelope(RecordEnvelope<D> recordEnvelope)
      throws IOException {
    write(recordEnvelope.getRecord(), recordEnvelope);
  }

  @Override
  public void write(final D record)
      throws IOException {
    write(record, Ackable.NoopAckable);
  }


  private void write(final D record, Ackable ackable)
      throws IOException {
    maybeThrow();
    int spinNum = 0;
    try {
      while (!this.writePermits.tryAcquire(100, TimeUnit.MILLISECONDS)) {
        ++spinNum;
        if (spinNum % 50 == 0) {
          log.info("Spinning due to pending writes, in = " + this.recordsIn.getCount() +
              ", success = " + this.recordsSuccess.getCount() + ", failed = " + this.recordsFailed.getCount() +
              ", maxOutstandingWrites = " + this.maxOutstandingWrites);
        }
      }
    } catch (InterruptedException e) {
      Throwables.propagate(e);
    }
    this.recordsIn.mark();
    attemptWrite(new Attempt(record, ackable));
  }


  /**
   * Checks if the current operating metrics would imply that
   * we're out of SLA on failures permitted
   * @return true if any failure would be fatal
   *
   * TODO: Add windowed stats to test for x% failures in y time window
   *
   */
  private boolean isFailureFatal() {
    return (this.failureAllowanceRatio == 0.0);
  }

  private void makeNextWriteThrow(Throwable t) {
    log.error("Will make next write throw", t);
    this.cachedWriteException = t;
  }

  private void maybeThrow() {
    if (this.cachedWriteException != null) {
      throw new NonTransientException("Irrecoverable failure on async write", this.cachedWriteException);
    }
  }

  private void attemptWrite(final Attempt attempt) {
    this.recordsAttempted.mark();
    attempt.setPrevAttemptTimestampNanos(System.nanoTime());
    this.asyncDataWriter.write(attempt.record, new WriteCallback<Object>() {

      @Override
      public void onSuccess(WriteResponse writeResponse) {
        try {
          attempt.ackable.ack();
          AsyncWriterManager.this.recordsSuccess.mark();
          if (writeResponse.bytesWritten() > 0) {
            AsyncWriterManager.this.bytesWritten.mark(writeResponse.bytesWritten());
          }
          if (AsyncWriterManager.this.dataWriterTimer.isPresent()) {
            AsyncWriterManager.this.dataWriterTimer.get()
                .update(System.nanoTime() - attempt.getPrevAttemptTimestampNanos(), TimeUnit.NANOSECONDS);
          }
        } finally {
          AsyncWriterManager.this.writePermits.release();
        }
      }

      @Override
      public void onFailure(Throwable throwable) {
        long currTime = System.nanoTime();
        if (AsyncWriterManager.this.dataWriterTimer.isPresent()) {
          AsyncWriterManager.this.dataWriterTimer.get()
              .update(currTime - attempt.getPrevAttemptTimestampNanos(), TimeUnit.NANOSECONDS);
        }
        if (attempt.attemptNum <= AsyncWriterManager.this.numRetries) { // attempts must == numRetries + 1
          attempt.incAttempt();
          attempt.setPrevAttemptFailure(throwable);
          AsyncWriterManager.this.retryQueue.get().add(attempt);
        } else {
          try {
            AsyncWriterManager.this.recordsFailed.mark();
            log.debug("Failed to write record : {}", attempt.getRecord().toString(), throwable);
            // If this failure is fatal, set the writer to throw an exception at this point
            if (isFailureFatal()) {
              makeNextWriteThrow(throwable);
            } else {
              // since the failure is not fatal, ack the attempt and move forward
              attempt.ackable.ack();
            }
          } finally {
            AsyncWriterManager.this.writePermits.release();
          }
        }
      }
    });
  }

  private class RetryRunner implements Runnable {

    private final LinkedBlockingQueue<Attempt> retryQueue;
    private final long minRetryIntervalNanos;

    public RetryRunner() {
      Preconditions
          .checkArgument(AsyncWriterManager.this.retryQueue.isPresent(), "RetryQueue must be present for RetryRunner");
      this.retryQueue = AsyncWriterManager.this.retryQueue.get();
      this.minRetryIntervalNanos =
          AsyncWriterManager.this.minRetryIntervalMillis * MILLIS_TO_NANOS; // 3 milliseconds in nanos
    }

    private void maybeSleep(long lastAttemptTimestampNanos)
        throws InterruptedException {
      long timeDiff = System.nanoTime() - lastAttemptTimestampNanos;
      long timeToSleep = this.minRetryIntervalNanos - timeDiff;
      if (timeToSleep > 0) {
        Thread.sleep(timeToSleep / MILLIS_TO_NANOS);
      }
    }

    @Override
    public void run() {
      while (true) {
        try {
          Attempt attempt = this.retryQueue.take();
          if (attempt != null) {
            maybeSleep(attempt.getPrevAttemptTimestampNanos());
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
    return this.recordsSuccess.getCount();
  }

  @Override
  public long bytesWritten()
      throws IOException {
    return this.bytesWritten.getCount();
  }

  @Override
  public void close()
      throws IOException {
    log.info("Close called");
    this.closer.close();
    if (this.retryThreadPool.isPresent()) {
      // Shutdown the retry thread pool immediately, no use waiting for in-progress retries
      ExecutorsUtils.shutdownExecutorService(this.retryThreadPool.get(), Optional.of(log), 1, TimeUnit.MILLISECONDS);
    }
    log.info("Successfully done closing");
  }

  @Override
  public void commit()
      throws IOException {
    /**
     * Assuming that commit is called only after all calls to write() have completed.
     * So not taking extra precautions to prevent concurrent calls to write from happening.
     *
     */
    log.info("Commit called, will wait for commitTimeout : {} ms", this.commitTimeoutMillis);
    long commitTimeoutNanos = commitTimeoutMillis * MILLIS_TO_NANOS;
    long commitStartTime = System.nanoTime();
    this.asyncDataWriter.flush();
    while (((System.nanoTime() - commitStartTime) < commitTimeoutNanos) && (this.recordsIn.getCount() != (
        this.recordsSuccess.getCount() + this.recordsFailed.getCount()))) {
      log.debug("Commit waiting... records produced: {}, written: {}, failed: {}", this.recordsIn.getCount(),
          this.recordsSuccess.getCount(), this.recordsFailed.getCount());
      try {
        Thread.sleep(this.commitStepWaitTimeMillis);
      } catch (InterruptedException e) {
        log.info("Interrupted while waiting for commit to complete");
        throw new IOException("Interrupted while waiting for commit to complete", e);
      }
    }
    log.debug("Commit done waiting");
    long recordsProducedFinal = this.recordsIn.getCount();
    long recordsWrittenFinal = this.recordsSuccess.getCount();
    long recordsFailedFinal = this.recordsFailed.getCount();
    long unacknowledgedWrites = recordsProducedFinal - recordsWrittenFinal - recordsFailedFinal;
    long totalFailures = unacknowledgedWrites + recordsFailedFinal;
    if (unacknowledgedWrites > 0) // timeout
    {
      log.warn("Timeout waiting for all writes to be acknowledged. Missing {} responses out of {}",
          unacknowledgedWrites, recordsProducedFinal);
    }
    if (totalFailures > 0 && recordsProducedFinal > 0) {
      log.info("Commit failed to write {} records ({} failed, {} unacknowledged) out of {} produced", totalFailures,
          recordsFailedFinal, unacknowledgedWrites, recordsProducedFinal);
      double failureRatio = (double) totalFailures / (double) recordsProducedFinal;
      if (failureRatio > this.failureAllowanceRatio) {
        log.error("Aborting because this is greater than the failureAllowance percentage: {}",
            this.failureAllowanceRatio * 100.0);
        throw new IOException("Failed to meet failureAllowance SLA", this.cachedWriteException);
      } else {
        log.warn(
            "Committing because the observed failure percentage {} is less than the failureAllowance percentage: {}",
            (failureRatio * 100.0), (this.failureAllowanceRatio * 100.0));
      }
    }

    log.info("Successfully committed {} records.", recordsWrittenFinal);
  }

  public static AsyncWriterManagerBuilder builder() {
    return new AsyncWriterManagerBuilder();
  }

  public static class AsyncWriterManagerBuilder {
    private Config config = ConfigFactory.empty();
    private long commitTimeoutMillis = COMMIT_TIMEOUT_MILLIS_DEFAULT;
    private long commitStepWaitTimeMillis = COMMIT_STEP_WAITTIME_MILLIS_DEFAULT;
    private double failureAllowanceRatio = FAILURE_ALLOWANCE_RATIO_DEFAULT;
    private boolean retriesEnabled = RETRIES_ENABLED_DEFAULT;
    private int numRetries = NUM_RETRIES_DEFAULT;
    private int maxOutstandingWrites = MAX_OUTSTANDING_WRITES_DEFAULT;
    private AsyncDataWriter asyncDataWriter;
    private Optional<Logger> logger = Optional.absent();

    public AsyncWriterManagerBuilder config(Config config) {
      this.config = config;
      return this;
    }

    public AsyncWriterManagerBuilder commitTimeoutMillis(long commitTimeoutMillis) {
      this.commitTimeoutMillis = commitTimeoutMillis;
      return this;
    }

    public AsyncWriterManagerBuilder commitStepWaitTimeInMillis(long commitStepWaitTimeMillis) {
      this.commitStepWaitTimeMillis = commitStepWaitTimeMillis;
      return this;
    }

    public AsyncWriterManagerBuilder failureAllowanceRatio(double failureAllowanceRatio) {
      Preconditions.checkArgument((failureAllowanceRatio <= 1.0 && failureAllowanceRatio >= 0),
          "Failure Allowance must be a ratio between 0 and 1");
      this.failureAllowanceRatio = failureAllowanceRatio;
      return this;
    }

    public AsyncWriterManagerBuilder asyncDataWriter(AsyncDataWriter asyncDataWriter) {
      this.asyncDataWriter = asyncDataWriter;
      return this;
    }

    public AsyncWriterManagerBuilder retriesEnabled(boolean retriesEnabled) {
      this.retriesEnabled = retriesEnabled;
      return this;
    }

    public AsyncWriterManagerBuilder numRetries(int numRetries) {
      this.numRetries = numRetries;
      return this;
    }

    public AsyncWriterManagerBuilder maxOutstandingWrites(int maxOutstandingWrites) {
      this.maxOutstandingWrites = maxOutstandingWrites;
      return this;
    }

    public AsyncWriterManagerBuilder logger(Optional<Logger> logger) {
      this.logger = logger;
      return this;
    }

    public AsyncWriterManager build() {
      return new AsyncWriterManager(this.config, this.commitTimeoutMillis, this.commitStepWaitTimeMillis,
          this.failureAllowanceRatio, this.retriesEnabled, this.numRetries, MIN_RETRY_INTERVAL_MILLIS_DEFAULT,
          // TODO: Make this configurable
          this.maxOutstandingWrites, this.asyncDataWriter, this.logger);
    }
  }
}
