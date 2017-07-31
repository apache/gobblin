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

package org.apache.gobblin.instrumented.fork;

import java.util.List;

import com.google.common.base.Optional;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.fork.ForkOperator;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.util.Decorator;
import org.apache.gobblin.util.DecoratorUtils;


/**
 * Decorator that automatically instruments {@link org.apache.gobblin.fork.ForkOperator}.
 * Handles already instrumented {@link org.apache.gobblin.instrumented.fork.InstrumentedForkOperator}
 * appropriately to avoid double metric reporting.
 */
public class InstrumentedForkOperatorDecorator<S, D> extends InstrumentedForkOperatorBase<S, D> implements Decorator {

  private ForkOperator<S, D> embeddedForkOperator;
  private boolean isEmbeddedInstrumented;

  public InstrumentedForkOperatorDecorator(ForkOperator<S, D> forkOperator) {
    super(Optional.<Class<?>> of(DecoratorUtils.resolveUnderlyingObject(forkOperator).getClass()));
    this.embeddedForkOperator = this.closer.register(forkOperator);
    this.isEmbeddedInstrumented = Instrumented.isLineageInstrumented(forkOperator);
  }

  @Override
  public void init(WorkUnitState workUnitState) throws Exception {
    this.embeddedForkOperator.init(workUnitState);
    super.init(workUnitState, DecoratorUtils.resolveUnderlyingObject(this).getClass());
  }

  @Override
  public MetricContext getMetricContext() {
    return this.isEmbeddedInstrumented
        ? ((InstrumentedForkOperatorBase<S, D>) this.embeddedForkOperator).getMetricContext()
        : super.getMetricContext();
  }

  @Override
  public List<Boolean> forkDataRecord(WorkUnitState workUnitState, D input) {
    return this.isEmbeddedInstrumented ? forkDataRecordImpl(workUnitState, input)
        : super.forkDataRecord(workUnitState, input);
  }

  @Override
  public List<Boolean> forkDataRecordImpl(WorkUnitState workUnitState, D input) {
    return this.embeddedForkOperator.forkDataRecord(workUnitState, input);
  }

  @Override
  public int getBranches(WorkUnitState workUnitState) {
    return this.embeddedForkOperator.getBranches(workUnitState);
  }

  @Override
  public List<Boolean> forkSchema(WorkUnitState workUnitState, S input) {
    return this.embeddedForkOperator.forkSchema(workUnitState, input);
  }

  @Override
  public Object getDecoratedObject() {
    return this.embeddedForkOperator;
  }
}
