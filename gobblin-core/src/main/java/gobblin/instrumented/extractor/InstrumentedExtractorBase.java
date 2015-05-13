/*
 * (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.instrumented.extractor;

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
import gobblin.configuration.WorkUnitState;
import gobblin.instrumented.Instrumentable;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.MetricContext;
import gobblin.metrics.MetricNames;
import gobblin.metrics.Tag;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;


/**
 * package-private implementation of instrumentation for {@link gobblin.source.extractor.Extractor}.
 * See {@link gobblin.instrumented.extractor.InstrumentedExtractor} for extensible class.
 */
abstract class InstrumentedExtractorBase<S, D> implements Extractor<S, D>, Instrumentable, Closeable {

  private final boolean instrumentationEnabled;
  protected final MetricContext metricContext;
  protected final Optional<Meter> readRecordsMeter;
  protected final Optional<Meter> dataRecordExceptionsMeter;
  protected final Optional<Timer> extractorTimer;
  protected final Closer closer;

  @SuppressWarnings("unchecked")
  public InstrumentedExtractorBase(WorkUnitState workUnitState) {
    super();
    this.closer = Closer.create();

    this.instrumentationEnabled = GobblinMetrics.isEnabled(workUnitState);

    this.metricContext =
        closer.register(Instrumented.getMetricContext(workUnitState, this.getClass(), generateTags(workUnitState)));

    if(isInstrumentationEnabled()) {
      this.readRecordsMeter = Optional.of(this.metricContext.meter(MetricNames.ExtractorMetrics.RECORDS_READ_METER));
      this.dataRecordExceptionsMeter = Optional.of(
          this.metricContext.meter(MetricNames.ExtractorMetrics.RECORDS_FAILED_METER));
      this.extractorTimer = Optional.of(this.metricContext.timer(MetricNames.ExtractorMetrics.EXTRACT_TIMER));
    } else {
      this.readRecordsMeter = Optional.absent();
      this.dataRecordExceptionsMeter = Optional.absent();
      this.extractorTimer = Optional.absent();
    }
  }

  @Override
  public boolean isInstrumentationEnabled() {
    return this.instrumentationEnabled;
  }

  @Override
  public List<Tag<?>> generateTags(State state) {
    return Lists.newArrayList();
  }

  @Override
  public D readRecord(D reuse)
      throws DataRecordException, IOException {
    if (!isInstrumentationEnabled()) {
      return readRecordImpl(reuse);
    }

    try {
      long startTimeNanos = System.nanoTime();
      beforeRead();
      D record = readRecordImpl(reuse);
      afterRead(record, startTimeNanos);
      return record;
    } catch(DataRecordException exception) {
      onException(exception);
      throw exception;
    } catch(IOException exception) {
      onException(exception);
      throw exception;
    }

  }

  /**
   * Called before each record is read.
   */
  public void beforeRead() {}

  /**
   * Called after each record is read.
   * @param record record read.
   * @param startTime reading start time.
   */
  public void afterRead(D record, long startTime) {
    Instrumented.updateTimer(this.extractorTimer, System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
    if(record != null){
      Instrumented.markMeter(readRecordsMeter);
    }
  }

  /**
   * Called on exception when trying to read.
   * @param exception exception thrown.
   */
  public void onException(Exception exception) {
    if (DataRecordException.class.isInstance(exception)) {
      Instrumented.markMeter(dataRecordExceptionsMeter);
    }
  }

  /**
   * Subclasses should implement this instead of {@link gobblin.source.extractor.Extractor#readRecord}
   */
  public abstract D readRecordImpl(D reuse) throws DataRecordException, IOException;

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
