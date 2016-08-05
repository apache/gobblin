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

import gobblin.configuration.State;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.GobblinMetrics;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryListener;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;

/**
 * Retry writer follows decorator pattern that retries on inner writer's failure.
 * @param <D>
 */
public class RetryWriter<D> implements DataWriter<D> {
  private static final Logger LOG = LoggerFactory.getLogger(RetryWriter.class);
  public static final String FAILED_RETRY_WRITES_METER = "gobblin.writer.failed.retry.writes";

  private final DataWriter<D> writer;
  private final Retryer<Void> retryer;
  private boolean isFailure = false;

  public RetryWriter(DataWriter<D> writer, State state) {
    this.writer = writer;
    this.retryer = buildRetryer(state);
  }

  /**
   * Build Retryer.
   * - If Writer implements Retriable, it will use the RetryerBuilder from the writer.
   * - Otherwise, it will use DEFAULT writer builder.
   *
   * - If Gobblin metrics is enabled, it will emit all failure count in to metrics.
   *
   * @param state
   * @return
   */
  private Retryer<Void> buildRetryer(State state) {
    RetryerBuilder<Void> builder = null;
    if (writer instanceof Retriable) {
      builder = ((Retriable) writer).getRetryerBuilder();
    } else {
      builder = getDefaultRetryBuilder();
    }

    if (GobblinMetrics.isEnabled(state)) {
      final Optional<Meter> retryMeter = Optional.of(Instrumented.getMetricContext(state, getClass()).meter(FAILED_RETRY_WRITES_METER));

      builder.withRetryListener(new RetryListener() {
        @Override
        public <V> void onRetry(Attempt<V> attempt) {
          if (attempt.hasException()) {
            LOG.warn("Caught exception. This may be retried.", attempt.getExceptionCause());
            Instrumented.markMeter(retryMeter);
          }
        }
      });
    }
    return builder.build();
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  @Override
  public void write(final D record) throws IOException {
    //Need a Callable interface to be wrapped by Retryer.
    Callable<Void> writeCall = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        writer.write(record);
        return null;
      }
    };

    try {
      this.retryer.wrap(writeCall).call();
    } catch (Exception e) {
      isFailure = true;
      throw new RuntimeException(e);
    }
  }

  @Override
  public void commit() throws IOException {
    if (isFailure) {
      throw new RuntimeException();
    }

    Callable<Void> commitCall = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        writer.commit();
        return null;
      }
    };

    try {
      this.retryer.wrap(commitCall).call();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void cleanup() throws IOException {
    writer.cleanup();
  }

  @Override
  public long recordsWritten() {
    return writer.recordsWritten();
  }

  @Override
  public long bytesWritten() throws IOException {
    return writer.bytesWritten();
  }

  /**
   * @return RetryerBuilder that retries on all exceptions except NonTransientException with exponential back off
   */
  public static RetryerBuilder<Void> getDefaultRetryBuilder() {
    Predicate<Throwable> transients = new Predicate<Throwable>() {
      @Override
      public boolean apply(Throwable t) {
        return !(t instanceof NonTransientException);
      }
    };

    return RetryerBuilder.<Void> newBuilder()
        .retryIfException(transients)
        .withWaitStrategy(WaitStrategies.exponentialWait(500, 10, TimeUnit.SECONDS)) //1, 2, 4, 8, 16 seconds delay
        .withStopStrategy(StopStrategies.stopAfterAttempt(5)); //Total 5 attempts and fail.
  }
}
