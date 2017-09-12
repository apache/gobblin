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

package org.apache.gobblin.instrumented.extractor;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.instrumented.Instrumentable;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metadata.GlobalMetadata;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.MetricNames;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.records.RecordStreamWithMetadata;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.stream.RecordEnvelope;
import org.apache.gobblin.stream.StreamEntity;
import org.apache.gobblin.util.FinalState;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;
import io.reactivex.Emitter;
import io.reactivex.Flowable;
import io.reactivex.functions.BiConsumer;

import javax.annotation.Nullable;


/**
 * package-private implementation of instrumentation for {@link org.apache.gobblin.source.extractor.Extractor}.
 * See {@link org.apache.gobblin.instrumented.extractor.InstrumentedExtractor} for extensible class.
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
   * @param shutdownRequest an {@link AtomicBoolean} that becomes true when a shutdown has been requested.
   * @return a {@link Flowable} with the records from this source. Note the flowable should honor downstream backpressure.
   */
  @Override
   public RecordStreamWithMetadata<D, S> recordStream(AtomicBoolean shutdownRequest) throws IOException {
    S schema = getSchema();
    Flowable<StreamEntity<D>> recordStream = Flowable.generate(() -> shutdownRequest, (BiConsumer<AtomicBoolean, Emitter<StreamEntity<D>>>) (state, emitter) -> {
      if (state.get()) {
        emitter.onComplete();
      }
      try {
        long startTimeNanos = 0;

        if (isInstrumentationEnabled()) {
          startTimeNanos = System.nanoTime();
          beforeRead();
        }

        StreamEntity<D> record = readStreamEntityImpl();

        if (isInstrumentationEnabled()) {
          D unwrappedRecord = null;

          if (record instanceof RecordEnvelope) {
            unwrappedRecord = ((RecordEnvelope<D>) record).getRecord();
          }

          afterRead(unwrappedRecord, startTimeNanos);
        }

        if (record != null) {
          emitter.onNext(record);
        } else {
          emitter.onComplete();
        }
      } catch (DataRecordException | IOException exc) {
        if (isInstrumentationEnabled()) {
          onException(exc);
        }
        emitter.onError(exc);
      }
    });
    recordStream = recordStream.doFinally(this::close);
    return new RecordStreamWithMetadata<>(recordStream, GlobalMetadata.<S>builder().schema(schema).build());

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
   * Subclasses should implement this or {@link #readRecordEnvelopeImpl()}
   * instead of {@link org.apache.gobblin.source.extractor.Extractor#readRecord}
   */
  protected StreamEntity<D> readStreamEntityImpl() throws DataRecordException, IOException {
    return readRecordEnvelopeImpl();
  }

  /**
   * Subclasses should implement this or {@link #readRecordImpl(Object)}
   * instead of {@link org.apache.gobblin.source.extractor.Extractor#readRecord}
   */
  @SuppressWarnings(value = "RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE",
      justification = "Findbugs believes readRecord(null) is non-null. This is not true.")
  protected RecordEnvelope<D> readRecordEnvelopeImpl() throws DataRecordException, IOException {
    D record = readRecordImpl(null);
    return  record == null ? null : new RecordEnvelope<>(record);
  }

  /**
   * Subclasses should implement this or {@link #readRecordEnvelopeImpl()}
   * instead of {@link org.apache.gobblin.source.extractor.Extractor#readRecord}
   */
  @Nullable
  protected D readRecordImpl(D reuse) throws DataRecordException, IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Get final state for this object. By default this returns an empty {@link org.apache.gobblin.configuration.State}, but
   * concrete subclasses can add information that will be added to the task state.
   * @return Empty {@link org.apache.gobblin.configuration.State}.
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
