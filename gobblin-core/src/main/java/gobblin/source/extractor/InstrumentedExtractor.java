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

package gobblin.source.extractor;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.io.Closer;

import gobblin.instrumented.Instrumentable;
import gobblin.instrumented.Instrumented;
import gobblin.configuration.WorkUnitState;
import gobblin.metrics.MetricContext;


/**
 * Instrumented version of {@link gobblin.source.extractor.Extractor} automatically captures certain metrics.
 * Subclasses should implement readRecordImpl instead of readRecord.
 */
public abstract class InstrumentedExtractor<S, D> implements Extractor<S, D>, Instrumentable, Closeable {
  protected MetricContext metricContext;
  protected Meter readRecordsMeter;
  protected Meter dataRecordExceptionsMeter;
  protected Timer extractorTimer;
  protected Closer closer;

  @SuppressWarnings("unchecked")
  public InstrumentedExtractor(WorkUnitState workUnitState) {
    super();
    closer = Closer.create();

    this.metricContext = closer.register(Instrumented.getMetricContext(workUnitState, this.getClass()));

    this.readRecordsMeter = this.metricContext.contextAwareMeter("gobblin.extractor.records.read");
    this.dataRecordExceptionsMeter = this.metricContext.contextAwareMeter("gobblin.extractor.records.failed");
    this.extractorTimer = this.metricContext.contextAwareTimer("gobblin.extractor.extract.time");
  }

  @Override
  public final D readRecord(D reuse)
      throws DataRecordException, IOException {
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
    extractorTimer.update(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
    if(record != null){
      readRecordsMeter.mark();
    }
  }

  /**
   * Called on exception when trying to read.
   * @param exception exception thrown.
   */
  public void onException(Exception exception) {
    if (DataRecordException.class.isInstance(exception)) {
      dataRecordExceptionsMeter.mark();
    }
  }

  /**
   * Subclasses should implement this instead of {@link gobblin.source.extractor.Extractor#readRecord}
   */
  public abstract D readRecordImpl(D reuse) throws DataRecordException, IOException;

  @Override
  public void close()
      throws IOException {
    closer.close();
  }

  @Override
  public MetricContext getMetricContext() {
    return this.metricContext;
  }
}
