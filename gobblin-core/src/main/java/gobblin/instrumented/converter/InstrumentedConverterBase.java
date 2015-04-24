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

package gobblin.instrumented.converter;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.io.Closer;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.instrumented.Instrumentable;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.MetricContext;


/**
 * package-private implementation of instrumentation for {@link gobblin.converter.Converter}.
 * See {@link gobblin.instrumented.converter.InstrumentedConverter} for extensible class.
 */
abstract class InstrumentedConverterBase<SI, SO, DI, DO> extends Converter<SI, SO, DI, DO>
    implements Instrumentable, Closeable {

  protected MetricContext metricContext = MetricContext.builder("TMP").build();
  protected Meter recordsIn = new Meter();
  protected Meter recordsOut = new Meter();
  protected Meter recordsException = new Meter();
  protected Timer converterTimer = new Timer();
  protected Closer closer = Closer.create();

  @Override
  public Converter<SI, SO, DI, DO> init(WorkUnitState workUnit) {
    Converter<SI, SO, DI, DO> converter = super.init(workUnit);

    this.metricContext = closer.register(Instrumented.getMetricContext(workUnit, this.getClass()));

    this.recordsIn = this.metricContext.contextAwareMeter("gobblin.converter.records.in");
    this.recordsOut = this.metricContext.contextAwareMeter("gobblin.converter.records.out");
    this.recordsException = this.metricContext.contextAwareMeter("gobblin.converter.records.failed");
    this.converterTimer = this.metricContext.contextAwareTimer("gobblin.converter.conversion.time");

    return converter;
  }

  @Override
  public Iterable<DO> convertRecord(SO outputSchema, DI inputRecord, WorkUnitState workUnit)
      throws DataConversionException {

    try {
      long startTime = System.nanoTime();

      beforeConvert(outputSchema, inputRecord, workUnit);
      final Iterable<DO> it = convertRecordImpl(outputSchema, inputRecord, workUnit);
      afterConvert(it, startTime);

      return Iterables.transform(it, new Function<DO, DO>() {
        @Override
        public DO apply(DO input) {
          onIterableNext(input);
          return input;
        }
      });
    } catch(DataConversionException exception) {
      onException(exception);
      throw exception;
    }
  }

  /**
   * Called before conversion.
   * @param outputSchema
   * @param inputRecord
   * @param workUnit
   */
  public void beforeConvert(SO outputSchema, DI inputRecord, WorkUnitState workUnit) {
    recordsIn.mark();
  }

  /**
   * Called after conversion.
   * @param iterable conversion result.
   * @param startTimeNanos start time of conversion.
   */
  public void afterConvert(Iterable<DO> iterable, long startTimeNanos) {
    converterTimer.update(System.nanoTime() - startTimeNanos, TimeUnit.NANOSECONDS);
  }

  /**
   * Called every time next() method in iterable is called.
   * @param next next value in iterable.
   */
  public void onIterableNext(DO next) {
    recordsOut.mark();
  }

  /**
   * Called when converter throws an exception.
   * @param exception exception thrown.
   */
  public void onException(Exception exception) {
    if(DataConversionException.class.isInstance(exception)) {
      recordsException.mark();
    }
  }

  /**
   * Subclasses should implement this method instead of convertRecord.
   *
   * See {@link gobblin.converter.Converter#convertRecord}.
   */
  public abstract Iterable<DO> convertRecordImpl(SO outputSchema, DI inputRecord, WorkUnitState workUnit)
      throws DataConversionException;

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
