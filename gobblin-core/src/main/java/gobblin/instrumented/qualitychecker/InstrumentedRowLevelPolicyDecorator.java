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

import gobblin.metrics.MetricContext;
import gobblin.qualitychecker.row.RowLevelPolicy;


/**
 * Decorator that automatically instruments {@link gobblin.qualitychecker.row.RowLevelPolicy}.
 * Handles already instrumented {@link gobblin.instrumented.qualitychecker.InstrumentedRowLevelPolicy}
 * appropriately to avoid double metric reporting.
 */
public class InstrumentedRowLevelPolicyDecorator extends InstrumentedRowLevelPolicyBase {

  private RowLevelPolicy embeddedPolicy;
  private boolean isEmbeddedInstrumented;

  public InstrumentedRowLevelPolicyDecorator(RowLevelPolicy policy) {
    super(policy.getTaskState(), policy.getType());
    this.embeddedPolicy = policy;
    this.isEmbeddedInstrumented = InstrumentedRowLevelPolicyBase.class.isInstance(policy);
  }

  @Override
  public MetricContext getMetricContext() {
    return this.isEmbeddedInstrumented ?
        ((InstrumentedRowLevelPolicyBase)embeddedPolicy).getMetricContext() :
        super.getMetricContext();
  }

  @Override
  public Result executePolicy(Object record) {
    return this.isEmbeddedInstrumented ?
        executePolicyImpl(record) :
        super.executePolicy(record);
  }

  @Override
  public Result executePolicyImpl(Object record) {
    return this.embeddedPolicy.executePolicy(record);
  }
}
