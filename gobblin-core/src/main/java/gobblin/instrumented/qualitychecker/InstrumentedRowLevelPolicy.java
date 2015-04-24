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

package gobblin.instrumented.qualitychecker;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.io.Closer;

import gobblin.instrumented.Instrumentable;
import gobblin.instrumented.Instrumented;
import gobblin.configuration.State;
import gobblin.metrics.MetricContext;
import gobblin.qualitychecker.row.RowLevelPolicy;


/**
 * Instrumented {@link gobblin.qualitychecker.row.RowLevelPolicy} automatically capturing certain metrics.
 * Subclasses should implement executePolicyImpl instead of executePolicy.
 *
 * @author ibuenros
 */
public abstract class InstrumentedRowLevelPolicy extends InstrumentedRowLevelPolicyBase {

  public InstrumentedRowLevelPolicy(State state, Type type) {
    super(state, type);
  }

  @Override
  public final Result executePolicy(Object record) {
    return super.executePolicy(record);
  }

}
