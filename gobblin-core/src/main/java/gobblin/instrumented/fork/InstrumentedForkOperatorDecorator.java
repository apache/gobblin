/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
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

import gobblin.configuration.WorkUnitState;
import gobblin.fork.ForkOperator;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.MetricContext;
import gobblin.util.Decorator;
import gobblin.util.DecoratorUtils;


/**
 * Decorator that automatically instruments {@link gobblin.fork.ForkOperator}.
 * Handles already instrumented {@link gobblin.instrumented.fork.InstrumentedForkOperator}
 * appropriately to avoid double metric reporting.
 */
public class InstrumentedForkOperatorDecorator<S, D> extends InstrumentedForkOperatorBase<S, D> implements Decorator {

  private ForkOperator<S, D> embeddedForkOperator;
  private boolean isEmbeddedInstrumented;

  public InstrumentedForkOperatorDecorator(ForkOperator<S, D> forkOperator) {
    this.embeddedForkOperator = this.closer.register(forkOperator);
    this.isEmbeddedInstrumented = Instrumented.isLineageInstrumented(forkOperator);
  }

  @Override
  public void init(WorkUnitState workUnitState)
      throws Exception {
    this.embeddedForkOperator.init(workUnitState);
    super.init(workUnitState,
        DecoratorUtils.resolveUnderlyingObject(this).getClass());
  }

  @Override
  public MetricContext getMetricContext() {
    return this.isEmbeddedInstrumented ?
        ((InstrumentedForkOperatorBase) embeddedForkOperator).getMetricContext() :
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
    return embeddedForkOperator.forkDataRecord(workUnitState, input);
  }

  @Override
  public int getBranches(WorkUnitState workUnitState) {
    return embeddedForkOperator.getBranches(workUnitState);
  }

  @Override
  public List<Boolean> forkSchema(WorkUnitState workUnitState, S input) {
    return embeddedForkOperator.forkSchema(workUnitState, input);
  }

  @Override
  public Object getDecoratedObject() {
    return this.embeddedForkOperator;
  }
}
