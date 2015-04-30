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

import java.util.List;

import com.google.common.base.Optional;

import gobblin.configuration.WorkUnitState;
import gobblin.fork.ForkOperator;
import gobblin.metrics.MetricContext;


/**
 * Decorator that automatically instruments {@link gobblin.fork.ForkOperator}.
 * Handles already instrumented {@link gobblin.instrumented.fork.InstrumentedForkOperator} appropriately to
 * avoid double metric reporting.
 */
public class InstrumentedForkOperatorDecorator<S, D> extends InstrumentedForkOperatorBase<S, D> {

  private ForkOperator<S, D> embeddedFork;
  private boolean isEmbeddedInstrumented;

  public InstrumentedForkOperatorDecorator(ForkOperator<S, D> forkOperator) {
    this.embeddedFork = forkOperator;
    this.isEmbeddedInstrumented = InstrumentedForkOperatorBase.class.isInstance(forkOperator);
  }

  @Override
  public void init(WorkUnitState workUnitState)
      throws Exception {
    this.embeddedFork.init(workUnitState);
    super.init(workUnitState);
  }

  @Override
  public Optional<MetricContext> getMetricContext() {
    return this.isEmbeddedInstrumented ?
        ((InstrumentedForkOperatorBase)embeddedFork).getMetricContext() :
        super.getMetricContext();
  }

  @Override
  public List<Boolean> forkDataRecord(WorkUnitState workUnitState, D input) {
    return this.isEmbeddedInstrumented ?
        forkDataRecordImpl(workUnitState, input) :
        super.forkDataRecord(workUnitState, input);
  }

  @Override
  public List<Boolean> forkDataRecordImpl(WorkUnitState workUnitState, D input) {
    return embeddedFork.forkDataRecord(workUnitState, input);
  }

  @Override
  public int getBranches(WorkUnitState workUnitState) {
    return embeddedFork.getBranches(workUnitState);
  }

  @Override
  public List<Boolean> forkSchema(WorkUnitState workUnitState, S input) {
    return embeddedFork.forkSchema(workUnitState, input);
  }
}
