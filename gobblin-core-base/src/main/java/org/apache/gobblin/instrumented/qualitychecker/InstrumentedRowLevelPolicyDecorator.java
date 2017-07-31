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

package org.apache.gobblin.instrumented.qualitychecker;

import java.io.IOException;

import com.google.common.base.Optional;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.qualitychecker.row.RowLevelPolicy;
import org.apache.gobblin.util.Decorator;
import org.apache.gobblin.util.DecoratorUtils;


/**
 * Decorator that automatically instruments {@link org.apache.gobblin.qualitychecker.row.RowLevelPolicy}.
 * Handles already instrumented {@link org.apache.gobblin.instrumented.qualitychecker.InstrumentedRowLevelPolicy}
 * appropriately to avoid double metric reporting.
 */
public class InstrumentedRowLevelPolicyDecorator extends InstrumentedRowLevelPolicyBase implements Decorator {

  private RowLevelPolicy embeddedPolicy;
  private boolean isEmbeddedInstrumented;

  public InstrumentedRowLevelPolicyDecorator(RowLevelPolicy policy) {
    super(policy.getTaskState(), policy.getType(),
        Optional.<Class<?>> of(DecoratorUtils.resolveUnderlyingObject(policy).getClass()));
    this.embeddedPolicy = policy;
    this.isEmbeddedInstrumented = Instrumented.isLineageInstrumented(policy);
  }

  @Override
  public MetricContext getMetricContext() {
    return this.isEmbeddedInstrumented ? ((InstrumentedRowLevelPolicyBase) this.embeddedPolicy).getMetricContext()
        : super.getMetricContext();
  }

  @Override
  public Result executePolicy(Object record) {
    return this.isEmbeddedInstrumented ? executePolicyImpl(record) : super.executePolicy(record);
  }

  @Override
  public Result executePolicyImpl(Object record) {
    return this.embeddedPolicy.executePolicy(record);
  }

  @Override
  public void close() throws IOException {
    this.embeddedPolicy.close();
  }

  @Override
  public Object getDecoratedObject() {
    return this.embeddedPolicy;
  }

  @Override
  public State getFinalState() {
    return this.embeddedPolicy.getFinalState();
  }
}
