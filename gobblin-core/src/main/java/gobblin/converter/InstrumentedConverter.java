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

package gobblin.converter;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.io.Closer;

import gobblin.Instrumented;
import gobblin.configuration.WorkUnitState;

public abstract class InstrumentedConverter<SI, SO, DI, DO> extends Converter<SI, SO, DI, DO> implements Closeable {
  protected Instrumented instrumented;
  protected Meter recordsIn;
  protected Meter recordsOut;
  protected Meter recordsException;
  protected Timer converterTimer;
  protected Closer closer;

  @Override
  public Converter<SI, SO, DI, DO> init(WorkUnitState workUnit) {
    Converter<SI, SO, DI, DO> converter = super.init(workUnit);

    this.closer = Closer.create();
    this.instrumented = closer.register(new Instrumented(workUnit, this.getClass()));

    this.recordsIn = this.instrumented.getContext().contextAwareMeter("gobblin.converter.records.in");
    this.recordsOut = this.instrumented.getContext().contextAwareMeter("gobblin.converter.records.out");
    this.recordsException = this.instrumented.getContext().contextAwareMeter("gobblin.converter.records.failed");
    this.converterTimer = this.instrumented.getContext().contextAwareTimer("gobblin.converter.conversion.time");

    return converter;
  }

  @Override
  public final Iterable<DO> convertRecord(SO outputSchema, DI inputRecord, WorkUnitState workUnit)
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

  public void beforeConvert(SO outputSchema, DI inputRecord, WorkUnitState workUnit) {
    recordsIn.mark();
  }

  public void afterConvert(Iterable<DO> iterable, long startTimeNanos) {
    converterTimer.update(System.nanoTime() - startTimeNanos, TimeUnit.NANOSECONDS);
  }

  public void onIterableNext(DO next) {
    recordsOut.mark();
  }

  public void onException(Exception exception) {
    if(DataConversionException.class.isInstance(exception)) {
      recordsException.mark();
    }
  }

  public abstract Iterable<DO> convertRecordImpl(SO outputSchema, DI inputRecord, WorkUnitState workUnit)
      throws DataConversionException;

  @Override
  public void close()
      throws IOException {
    closer.close();
  }
}
