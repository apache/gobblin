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

package gobblin.metrics;

import lombok.experimental.Delegate;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;

import gobblin.metrics.metric.InnerMetric;


/**
 * A type of {@link com.codahale.metrics.Histogram} that are aware of their {@link gobblin.metrics.MetricContext}
 * and can have associated {@link Tag}s.
 *
 * <p>
 *   Any updates to a {@link ContextAwareHistogram} will be applied automatically to the
 *   {@link ContextAwareHistogram} of the same name in the parent {@link MetricContext}.
 * </p>
 *
 * <p>
 *   This class wraps a {@link com.codahale.metrics.Histogram} and delegates calls to public methods of
 *   {@link com.codahale.metrics.Histogram} to the wrapped {@link com.codahale.metrics.Histogram}.
 * </p>
 *
 * @author Yinan Li
 */
class ContextAwareHistogram extends Histogram implements ContextAwareMetric {

  @Delegate
  private final InnerHistogram innerHistogram;

  private final MetricContext context;

  ContextAwareHistogram(MetricContext context, String name) {
    super(new ExponentiallyDecayingReservoir());
    this.innerHistogram = new InnerHistogram(context, name, this);
    this.context = context;

  }

  @Override
  public MetricContext getContext() {
    return this.context;
  }

  @Override public InnerMetric getInnerMetric() {
    return this.innerHistogram;
  }
}
