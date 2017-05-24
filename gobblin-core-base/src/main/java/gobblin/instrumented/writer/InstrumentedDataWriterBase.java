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

package gobblin.instrumented.writer;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.State;
import gobblin.instrumented.Instrumentable;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.MetricContext;
import gobblin.metrics.MetricNames;
import gobblin.metrics.Tag;
import gobblin.util.ExecutorsUtils;
import gobblin.util.FinalState;
import gobblin.writer.DataWriter;


@Slf4j

/**
 * Package-private implementation of instrumentation for {@link gobblin.writer.DataWriter}.
 *
 * @see gobblin.instrumented.writer.InstrumentedDataWriter for extensible class.
 */
abstract class InstrumentedDataWriterBase<D> implements DataWriter<D>, Instrumentable, Closeable, FinalState {

  private final Optional<ScheduledThreadPoolExecutor> writerMetricsUpdater;
  private final boolean instrumentationEnabled;

  private MetricContext metricContext;
  private Optional<Meter> recordsInMeter;
  private Optional<Meter> successfulWritesMeter;
  private Optional<Meter> failedWritesMeter;
  private Optional<Timer> dataWriterTimer;
  private Optional<Meter> recordsWrittenMeter;
  private Optional<Meter> bytesWrittenMeter;

  protected final Closer closer;

  public static final String WRITER_METRICS_UPDATER_INTERVAL = "gobblin.writer.metrics.updater.interval";
  public static final long DEFAULT_WRITER_METRICS_UPDATER_INTERVAL = 30000;

  public InstrumentedDataWriterBase(State state) {
    this(state, Optional.<Class<?>> absent());
  }

  protected InstrumentedDataWriterBase(State state, Optional<Class<?>> classTag) {
    this.closer = Closer.create();
    this.instrumentationEnabled = GobblinMetrics.isEnabled(state);
    this.metricContext = this.closer.register(Instrumented.getMetricContext(state, classTag.or(this.getClass())));

    if (this.instrumentationEnabled) {
      this.writerMetricsUpdater = Optional.of(buildWriterMetricsUpdater());
      scheduleWriterMetricsUpdater(this.writerMetricsUpdater.get(), getWriterMetricsUpdaterInterval(state));
    } else {
      this.writerMetricsUpdater = Optional.absent();
    }

    regenerateMetrics();
  }

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

  /**
   * Generates metrics for the instrumentation of this class.
   */
  protected void regenerateMetrics() {
    if (isInstrumentationEnabled()) {
      this.recordsInMeter = Optional.of(this.metricContext.meter(MetricNames.DataWriterMetrics.RECORDS_IN_METER));
      this.successfulWritesMeter =
          Optional.of(this.metricContext.meter(MetricNames.DataWriterMetrics.SUCCESSFUL_WRITES_METER));
      this.failedWritesMeter = Optional.of(this.metricContext.meter(MetricNames.DataWriterMetrics.FAILED_WRITES_METER));
      setRecordsWrittenMeter(isInstrumentationEnabled());
      setBytesWrittenMeter(isInstrumentationEnabled());
      this.dataWriterTimer = Optional.<Timer>of(this.metricContext.timer(MetricNames.DataWriterMetrics.WRITE_TIMER));
    } else {
      this.recordsInMeter = Optional.absent();
      this.successfulWritesMeter = Optional.absent();
      this.failedWritesMeter = Optional.absent();
      setRecordsWrittenMeter(isInstrumentationEnabled());
      setBytesWrittenMeter(isInstrumentationEnabled());
      this.dataWriterTimer = Optional.absent();
    }
  }

  private synchronized void setRecordsWrittenMeter(boolean isInstrumentationEnabled) {
    if (isInstrumentationEnabled) {
      this.recordsWrittenMeter =
          Optional.of(this.metricContext.meter(MetricNames.DataWriterMetrics.RECORDS_WRITTEN_METER));
    } else {
      this.recordsWrittenMeter = Optional.absent();
    }
  }

  private synchronized void setBytesWrittenMeter(boolean isInstrumentationEnabled) {
    if (isInstrumentationEnabled) {
      this.bytesWrittenMeter = Optional.of(this.metricContext.meter(MetricNames.DataWriterMetrics.BYTES_WRITTEN_METER));
    } else {
      this.bytesWrittenMeter = Optional.absent();
    }
  }

  /** Default with no additional tags */
  @Override
  public List<Tag<?>> generateTags(State state) {
    return Lists.newArrayList();
  }

  @Override
  public boolean isInstrumentationEnabled() {
    return this.instrumentationEnabled;
  }

  @Override
  public void write(D record) throws IOException {
    if (!isInstrumentationEnabled()) {
      writeImpl(record);
      return;
    }

    try {
      long startTimeNanos = System.nanoTime();
      beforeWrite(record);
      writeImpl(record);
      onSuccessfulWrite(startTimeNanos);
    } catch (IOException exception) {
      onException(exception);
      throw exception;
    }
  }

  /**
   * Called beforeWriting a record.
   * @param record record to write.
   */
  public void beforeWrite(D record) {
    Instrumented.markMeter(this.recordsInMeter);
  }

  /**
   * Called after a successful write of a record.
   * @param startTimeNanos time at which writing started.
   */
  public void onSuccessfulWrite(long startTimeNanos) {
    Instrumented.updateTimer(this.dataWriterTimer, System.nanoTime() - startTimeNanos, TimeUnit.NANOSECONDS);
    Instrumented.markMeter(this.successfulWritesMeter);
  }

  /** Called after a failed writing of a record.
   * @param exception exception thrown.
   */
  public void onException(Exception exception) {
    Instrumented.markMeter(this.failedWritesMeter);
  }

  /**
   * Subclasses should implement this instead of {@link gobblin.writer.DataWriter#write}
   */
  public abstract void writeImpl(D record) throws IOException;

  /**
   * Get final state for this object. By default this returns an empty {@link gobblin.configuration.State}, but
   * concrete subclasses can add information that will be added to the task state.
   * @return Empty {@link gobblin.configuration.State}.
   */
  @Override
  public State getFinalState() {
    return new State();
  }

  @Override
  public void close() throws IOException {
    try {
      this.closer.close();
    } finally {
      if (this.writerMetricsUpdater.isPresent()) {
        ExecutorsUtils.shutdownExecutorService(this.writerMetricsUpdater.get(), Optional.of(log));
      }
    }
  }

  @Override
  public MetricContext getMetricContext() {
    return this.metricContext;
  }

  /**
   * Update the {@link #recordsWrittenMeter} and {@link #bytesWrittenMeter}. This method should be invoked after the
   * wrapped {@link DataWriter#commit()} is invoked. This ensures that the record-level and byte-level meters are
   * updated at least once.
   */
  @Override
  public void commit() throws IOException {
    updateRecordsWrittenMeter();
    updateBytesWrittenMeter();
  }

  /**
   * Update the {@link #recordsWrittenMeter} using the {@link DataWriter#recordsWritten()} method..
   */
  private synchronized void updateRecordsWrittenMeter() {
    if (this.recordsWrittenMeter.isPresent()) {
      this.recordsWrittenMeter.get().mark(recordsWritten() - this.recordsWrittenMeter.get().getCount());
    }
  }

  /**
   * Update the {@link #bytesWrittenMeter} using the {@link DataWriter#bytesWritten()} method.
   */
  private synchronized void updateBytesWrittenMeter() {
    if (this.bytesWrittenMeter.isPresent()) {
      try {
        this.bytesWrittenMeter.get().mark(bytesWritten() - this.bytesWrittenMeter.get().getCount());
      } catch (IOException e) {
        log.error("Cannot get bytesWritten for DataWriter, will not update " + this.bytesWrittenMeter.get().toString(),
            e);
      }
    }
  }

  /**
   * Build a {@link ScheduledThreadPoolExecutor} that updates record-level and byte-level metrics.
   */
  private static ScheduledThreadPoolExecutor buildWriterMetricsUpdater() {
    return new ScheduledThreadPoolExecutor(1,
        ExecutorsUtils.newThreadFactory(Optional.of(log), Optional.of("WriterMetricsUpdater-%d")));
  }

  /**
   * Get the interval that the Writer Metrics Updater should be scheduled on.
   */
  private static long getWriterMetricsUpdaterInterval(State state) {
    return state.getPropAsLong(WRITER_METRICS_UPDATER_INTERVAL, DEFAULT_WRITER_METRICS_UPDATER_INTERVAL);
  }

  /**
   * Schedule the given {@link ScheduledThreadPoolExecutor} to run at the given interval.
   */
  private ScheduledFuture<?> scheduleWriterMetricsUpdater(ScheduledThreadPoolExecutor writerMetricsUpdater,
      long scheduleInterval) {
    return writerMetricsUpdater.scheduleAtFixedRate(new WriterMetricsUpdater(), scheduleInterval, scheduleInterval,
        TimeUnit.MILLISECONDS);
  }

  /**
   * An implementation of {@link Runnable} that updates record-level and byte-level metrics.
   */
  private class WriterMetricsUpdater implements Runnable {

    @Override
    public void run() {
      updateRecordsWrittenMeter();
      updateBytesWrittenMeter();
    }
  }
}
