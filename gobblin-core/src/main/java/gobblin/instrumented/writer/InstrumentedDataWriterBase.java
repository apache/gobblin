/*
 *
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.instrumented.writer;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import gobblin.configuration.State;
import gobblin.instrumented.Instrumentable;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.MetricContext;
import gobblin.metrics.MetricNames;
import gobblin.metrics.Tag;
import gobblin.util.FinalState;
import gobblin.writer.DataWriter;


/**
 * package-private implementation of instrumentation for {@link gobblin.writer.DataWriter}.
 * See {@link gobblin.instrumented.writer.InstrumentedDataWriter} for extensible class.
 */
abstract class InstrumentedDataWriterBase <D> implements DataWriter<D>, Instrumentable, Closeable, FinalState {

  private final boolean instrumentationEnabled;

  protected final Closer closer;
  private MetricContext metricContext;
  private Optional<Meter> recordsInMeter;
  private Optional<Meter> successfulWriteMeter;
  private Optional<Meter> exceptionWriteMeter;
  private Optional<Timer> dataWriterTimer;

  public InstrumentedDataWriterBase(State state) {
    this(state, Optional.<Class<?>>absent());
  }

  protected InstrumentedDataWriterBase(State state, Optional<Class<?>> classTag) {
    this.closer = Closer.create();
    this.instrumentationEnabled = GobblinMetrics.isEnabled(state);
    this.metricContext =
        this.closer.register(Instrumented.getMetricContext(state, classTag.or(this.getClass())));

    regenerateMetrics();
  }

  @Override
  public void switchMetricContext(List<Tag<?>> tags) {
    this.metricContext = this.closer.register(Instrumented.newContextFromReferenceContext(this.metricContext, tags,
        Optional.<String>absent()));

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
    if(isInstrumentationEnabled()) {
      this.recordsInMeter = Optional.of(this.metricContext.meter(MetricNames.DataWriterMetrics.RECORDS_IN_METER));
      this.successfulWriteMeter = Optional.of(
          this.metricContext.meter(MetricNames.DataWriterMetrics.RECORDS_WRITTEN_METER));
      this.exceptionWriteMeter = Optional.of(
          this.metricContext.meter(MetricNames.DataWriterMetrics.RECORDS_FAILED_METER));
      this.dataWriterTimer = Optional.of(this.metricContext.timer(MetricNames.DataWriterMetrics.WRITE_TIMER));
    } else {
      this.recordsInMeter = Optional.absent();
      this.successfulWriteMeter = Optional.absent();
      this.exceptionWriteMeter = Optional.absent();
      this.dataWriterTimer = Optional.absent();
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
  public void write(D record)
      throws IOException {
    if(!isInstrumentationEnabled()) {
      writeImpl(record);
      return;
    }

    try {
      long startTimeNanos = System.nanoTime();
      beforeWrite(record);
      writeImpl(record);
      onSuccessfulWrite(startTimeNanos);
    } catch(IOException exception) {
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
    Instrumented.markMeter(this.successfulWriteMeter);
  }

  /** Called after a failed writing of a record.
   * @param exception exception thrown.
   */
  public void onException(Exception exception) {
    Instrumented.markMeter(this.exceptionWriteMeter);
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
  public void close()
      throws IOException {
    this.closer.close();
  }

  @Override
  public MetricContext getMetricContext() {
    return this.metricContext;
  }
}
