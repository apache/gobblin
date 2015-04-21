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

package gobblin.qualitychecker.row;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.io.Closer;

import gobblin.Instrumented;
import gobblin.configuration.State;

public abstract class InstrumentedRowLevelPolicy extends RowLevelPolicy implements Closeable {
  protected final Instrumented instrumented;
  protected final Meter recordsMeter;
  protected final Meter passedRecordsMeter;
  protected final Meter failedRecordsMeter;
  protected final Timer policyTimer;
  protected final Closer closer;

  public InstrumentedRowLevelPolicy(State state, Type type) {
    super(state, type);
    this.closer = Closer.create();
    this.instrumented = closer.register(new Instrumented(state, this.getClass()));
    this.recordsMeter = this.instrumented.getContext().contextAwareMeter("gobblin.qualitychecker.records.in");
    this.passedRecordsMeter = this.instrumented.getContext().contextAwareMeter("gobblin.qualitychecker.records.passed");
    this.failedRecordsMeter = this.instrumented.getContext().contextAwareMeter("gobblin.qualitychecker.records.failed");
    this.policyTimer = this.instrumented.getContext().contextAwareTimer("gobblin.qualitychecker.policy.timer");
  }

  @Override
  public Result executePolicy(Object record) {

    long startTime = System.nanoTime();

    beforeCheck(record);
    Result result = executePolicyImpl(record);
    afterCheck(result, startTime);

    return result;
  }

  public void beforeCheck(Object record) {
    this.recordsMeter.mark();
  }

  public void afterCheck(Result result, long startTimeNanos) {
    switch (result) {
      case FAILED:
        this.failedRecordsMeter.mark();
        break;
      case PASSED:
        this.passedRecordsMeter.mark();
        break;
      default:
    }

    this.policyTimer.update(System.nanoTime() - startTimeNanos, TimeUnit.NANOSECONDS);
  }

  public abstract Result executePolicyImpl(Object record);

  @Override
  public void close()
      throws IOException {
    closer.close();
  }
}
