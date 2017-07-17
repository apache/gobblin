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
package gobblin.writer;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.github.rholder.retry.RetryerBuilder;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import gobblin.stream.RecordEnvelope;
import gobblin.util.Decorator;
import gobblin.util.limiter.Limiter;
import gobblin.util.limiter.RateBasedLimiter;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.GobblinMetrics;
import gobblin.util.FinalState;

/**
 * Throttle writer follows decorator pattern that throttles inner writer by either QPS or by bytes.
 * @param <D>
 */
public class ThrottleWriter<D> extends WriterWrapper<D> implements Decorator, FinalState, Retriable {
  private static final Logger LOG = LoggerFactory.getLogger(ThrottleWriter.class);
  public static final String WRITER_THROTTLE_TYPE_KEY = "gobblin.writer.throttle_type";
  public static final String WRITER_LIMIT_RATE_LIMIT_KEY = "gobblin.writer.throttle_rate";
  public static final String WRITES_THROTTLED_TIMER = "gobblin.writer.throttled_time";
  public static final String THROTTLED_TIME_KEY = "ThrottledTime";

  private static final String LOCAL_JOB_LAUNCHER_TYPE = "LOCAL";

  public static enum ThrottleType {
    QPS,
    Bytes
  }

  private final State state;
  private final DataWriter<D> writer;
  private final Limiter limiter;
  private final ThrottleType type;
  private final Optional<Timer> throttledTimer;
  private long throttledTime;

  public ThrottleWriter(DataWriter<D> writer, State state) {
    Preconditions.checkNotNull(writer, "DataWriter is required.");
    Preconditions.checkNotNull(state, "State is required.");

    this.state = state;
    this.writer = writer;
    this.type = ThrottleType.valueOf(state.getProp(WRITER_THROTTLE_TYPE_KEY));
    int rateLimit = computeRateLimit(state);
    LOG.info("Rate limit for each writer: " + rateLimit + " " + type);

    this.limiter = new RateBasedLimiter(computeRateLimit(state));
    if (GobblinMetrics.isEnabled(state)) {
      throttledTimer = Optional.<Timer>of(Instrumented.getMetricContext(state, getClass()).timer(WRITES_THROTTLED_TIMER));
    } else {
      throttledTimer = Optional.absent();
    }
  }

  @Override
  public Object getDecoratedObject() {
    return this.writer;
  }

  /**
   * Compute rate limit per executor.
   * Rate limit = Total rate limit / # of parallelism
   *
   * # of parallelism:
   *  - if LOCAL job type Min(# of source partition, thread pool size)
   *  - else Min(# of source partition, # of max mappers)
   *
   * @param state
   * @return
   */
  private int computeRateLimit(State state) {
    String jobLauncherType = state.getProp(ConfigurationKeys.JOB_LAUNCHER_TYPE_KEY, "LOCAL");
    int parallelism = 1;
    if (LOCAL_JOB_LAUNCHER_TYPE.equals(jobLauncherType)) {
      parallelism = state.getPropAsInt(ConfigurationKeys.TASK_EXECUTOR_THREADPOOL_SIZE_KEY,
                                       ConfigurationKeys.DEFAULT_TASK_EXECUTOR_THREADPOOL_SIZE);
    } else {
      parallelism = state.getPropAsInt(ConfigurationKeys.MR_JOB_MAX_MAPPERS_KEY,
                                       ConfigurationKeys.DEFAULT_MR_JOB_MAX_MAPPERS);
    }
    parallelism = Math.min(parallelism, state.getPropAsInt(ConfigurationKeys.SOURCE_MAX_NUMBER_OF_PARTITIONS,
                                                           ConfigurationKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS));
    parallelism = Math.max(parallelism, 1);
    int rateLimit = state.getPropAsInt(WRITER_LIMIT_RATE_LIMIT_KEY) / parallelism;
    rateLimit = Math.max(rateLimit, 1);
    return rateLimit;
  }

  /**
   * Calls inner writer with self throttling.
   * If the throttle type is byte, it applies throttle after write happens.
   * This is because it can figure out written bytes after it's written. It's not ideal but throttling after write should be sufficient for most cases.
   * {@inheritDoc}
   * @see gobblin.writer.DataWriter#write(java.lang.Object)
   */
  @Override
  public void writeEnvelope(RecordEnvelope<D> record) throws IOException {
    try {
      if (ThrottleType.QPS.equals(type)) {
        acquirePermits(1L);
      }
      long beforeWrittenBytes = writer.bytesWritten();
      writer.writeEnvelope(record);

      if (ThrottleType.Bytes.equals(type)) {
        long delta = writer.bytesWritten() - beforeWrittenBytes;
        if (delta < 0) {
          throw new UnsupportedOperationException("Cannot throttle on bytes because "
                                                  + writer.getClass().getSimpleName() + " does not supports bytesWritten");
        }

        if (delta > 0) {
          acquirePermits(delta);
        }
      }
    } catch (InterruptedException e) {
      throw new IOException("Failed while acquiring permits.",e);
    }
  }

  /**
   * Acquire permit along with emitting metrics if enabled.
   * @param permits
   * @throws InterruptedException
   */
  private void acquirePermits(long permits) throws InterruptedException {
    long startMs = System.currentTimeMillis(); //Measure in milliseconds. (Nanoseconds are more precise but expensive and not worth for this case)
    limiter.acquirePermits(permits);
    long permitAcquisitionTime = System.currentTimeMillis() - startMs;
    if (throttledTimer.isPresent()) { // Metrics enabled
      Instrumented.updateTimer(throttledTimer, permitAcquisitionTime, TimeUnit.MILLISECONDS);
    }
    this.throttledTime += permitAcquisitionTime;
  }


  @Override
  public void close() throws IOException {
    writer.close();
  }

  @Override
  public void commit() throws IOException {
    writer.commit();

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

  @Override
  public RetryerBuilder<Void> getRetryerBuilder() {
    if (writer instanceof Retriable) {
      return ((Retriable) writer).getRetryerBuilder();
    }
    return RetryWriter.createRetryBuilder(state);
  }

  @Override
  public State getFinalState() {
    State state = new State();

    if (this.writer instanceof FinalState) {
      state.addAll(((FinalState)this.writer).getFinalState());
    } else {
      LOG.warn("Wrapped writer does not implement FinalState: " + this.writer.getClass());
    }

    state.setProp(THROTTLED_TIME_KEY, this.throttledTime);
    return state;
  }
}
