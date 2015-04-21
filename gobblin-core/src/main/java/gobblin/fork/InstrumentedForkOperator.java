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

package gobblin.fork;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.io.Closer;

import gobblin.instrumented.Instrumented;
import gobblin.configuration.WorkUnitState;


public abstract class InstrumentedForkOperator<S, D> implements ForkOperator<S, D> {

  protected Instrumented instrumented;
  protected Closer closer = Closer.create();
  // Initialize as dummy metrics to avoid null pointer exception if init was skipped
  protected Meter inputMeter = new Meter();
  protected Meter outputForks = new Meter();
  protected Timer forkOperatorTimer = new Timer();

  @Override
  public void init(WorkUnitState workUnitState)
      throws Exception {
    this.instrumented = closer.register(new Instrumented(workUnitState, this.getClass()));

    this.inputMeter = this.instrumented.getContext().meter("gobblin.fork.operator.records.in");
    this.outputForks = this.instrumented.getContext().meter("gobblin.fork.operator.forks.out");
    this.forkOperatorTimer = this.instrumented.getContext().timer("gobblin.fork.operator.timer");
  }

  @Override
  public final List<Boolean> forkDataRecord(WorkUnitState workUnitState, D input) {
    long startTimeNanos = System.nanoTime();

    beforeFork(input);
    List<Boolean> result = forkDataRecordImpl(workUnitState, input);
    afterFork(result, startTimeNanos);

    return result;
  }

  protected void beforeFork(D input) {
    this.inputMeter.mark();
  }

  protected void afterFork(List<Boolean> forks, long startTimeNanos) {
    int forksGenerated = 0;
    for (Boolean fork : forks) {
      forksGenerated += fork ? 1 : 0;
    }
    this.outputForks.mark(forksGenerated);
    this.forkOperatorTimer.update(System.nanoTime() - startTimeNanos, TimeUnit.NANOSECONDS);
  }

  public abstract List<Boolean> forkDataRecordImpl(WorkUnitState workUnitState, D input);
}
