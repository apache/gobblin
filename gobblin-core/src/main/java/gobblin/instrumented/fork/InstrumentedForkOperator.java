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

package gobblin.instrumented.fork;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.io.Closer;

import gobblin.fork.ForkOperator;
import gobblin.instrumented.Instrumentable;
import gobblin.instrumented.Instrumented;
import gobblin.configuration.WorkUnitState;
import gobblin.metrics.MetricContext;


/**
 * Instrumented {@link gobblin.fork.ForkOperator} automatically capturing certain metrics.
 * Subclasses should implement forkDataRecordImpl instead of forkDataRecord.
 *
 * @author ibuenros
 */
public abstract class InstrumentedForkOperator<S, D> extends InstrumentedForkOperatorBase<S, D> {

  @Override
  public final List<Boolean> forkDataRecord(WorkUnitState workUnitState, D input) {
    return super.forkDataRecord(workUnitState, input);
  }
}
