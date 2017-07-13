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
import gobblin.stream.RecordEnvelope;
import gobblin.util.FinalState;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;
import javax.annotation.Nullable;


/**
 * package-private implementation of instrumentation for {@link gobblin.source.extractor.Extractor}.
 * See {@link gobblin.instrumented.extractor.InstrumentedExtractor} for extensible class.
 */
public abstract class InstrumentedExtractorBase<S, D>
    implements Extractor<S, D>, Instrumentable, Closeable, FinalState {

  private final boolean instrumentationEnabled;
  private MetricContext metricContext;
  private Optional<Meter> readRecordsMeter;
  private Optional<Meter> dataRecordExceptionsMeter;
  private Optional<Timer> extractorTimer;
  protected final Closer closer;

  public InstrumentedExtractorBase(WorkUnitState workUnitState) {
    this(workUnitState, Optional.<Class<?>> absent());
  }

  protected InstrumentedExtractorBase(WorkUnitState workUnitState, Optional<Class<?>> classTag) {
    super();
    this.closer = Closer.create();

    this.instrumentationEnabled = GobblinMetrics.isEnabled(workUnitState);

    this.metricContext = this.closer.register(
        Instrumented.getMetricContext(workUnitState, classTag.or(this.getClass()), generateTags(workUnitState)));

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
      this.readRecordsMeter = Optional.of(this.metricContext.meter(MetricNames.ExtractorMetrics.RECORDS_READ_METER));
      this.dataRecordExceptionsMeter =
          Optional.of(this.metricContext.meter(MetricNames.ExtractorMetrics.RECORDS_FAILED_METER));
      this.extractorTimer = Optional.<Timer>of(this.metricContext.timer(MetricNames.ExtractorMetrics.EXTRACT_TIMER));
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

  /** Default with no additional tags */
  @Override
  public List<Tag<?>> generateTags(State state) {
    return Lists.newArrayList();
  }

  @Override
  public RecordEnvelope<D> readRecordEnvelope() throws DataRecordException, IOException {
    if (!isInstrumentationEnabled()) {
      return readRecordEnvelopeImpl();
    }

    try {
      long startTimeNanos = System.nanoTime();
      beforeRead();
      RecordEnvelope<D> record = readRecordEnvelopeImpl();
      afterRead(record == null ? null : record.getRecord(), startTimeNanos);
      return record;
    } catch (DataRecordException exception) {
      onException(exception);
      throw exception;
    } catch (IOException exception) {
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
    if (record != null) {
      Instrumented.markMeter(this.readRecordsMeter);
    }
  }

  /**
   * Called on exception when trying to read.
   * @param exception exception thrown.
   */
  public void onException(Exception exception) {
    if (DataRecordException.class.isInstance(exception)) {
      Instrumented.markMeter(this.dataRecordExceptionsMeter);
    }
  }

  /**
   * Subclasses should implement this or {@link #readRecordImpl(Object)}
   * instead of {@link gobblin.source.extractor.Extractor#readRecord}
   */
  @SuppressWarnings(value = "RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE",
      justification = "Findbugs believes readRecord(null) is non-null. This is not true.")
  protected RecordEnvelope<D> readRecordEnvelopeImpl() throws DataRecordException, IOException {
    D record = readRecordImpl(null);
    return  record == null ? null : new RecordEnvelope<>(record);
  }

  /**
   * Subclasses should implement this or {@link #readRecordEnvelopeImpl()}
   * instead of {@link gobblin.source.extractor.Extractor#readRecord}
   */
  @Nullable
  protected D readRecordImpl(D reuse) throws DataRecordException, IOException {
    throw new UnsupportedOperationException();
  }

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
    this.closer.close();
  }

  @Override
  public MetricContext getMetricContext() {
    return this.metricContext;
  }
}
