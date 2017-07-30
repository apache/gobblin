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

import java.lang.ref.WeakReference;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.google.common.base.Optional;

import gobblin.metrics.metric.InnerMetric;


/**
 * Implementation of {@link InnerMetric} for {@link Histogram}.
 */
public class InnerHistogram extends Histogram implements InnerMetric {

  private final String name;
  private final Optional<ContextAwareHistogram> parentHistogram;
  private final WeakReference<ContextAwareHistogram> contextAwareHistogram;

  InnerHistogram(MetricContext context, String name, ContextAwareHistogram contextAwareHistogram) {
    super(new ExponentiallyDecayingReservoir());

    this.name = name;

    Optional<MetricContext> parentContext = context.getParent();
    if (parentContext.isPresent()) {
      this.parentHistogram = Optional.fromNullable(parentContext.get().contextAwareHistogram(name));
    } else {
      this.parentHistogram = Optional.absent();
    }

    this.contextAwareHistogram = new WeakReference<>(contextAwareHistogram);
  }

  @Override
  public void update(int value) {
    update((long) value);
  }

  @Override
  public void update(long value) {
    super.update(value);
    if (this.parentHistogram.isPresent()) {
      this.parentHistogram.get().update(value);
    }
  }

  public String getName() {
    return this.name;
  }

  @Override
  public ContextAwareMetric getContextAwareMetric() {
    return this.contextAwareHistogram.get();
  }
}
