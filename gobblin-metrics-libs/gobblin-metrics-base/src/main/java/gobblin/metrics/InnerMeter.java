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

import com.codahale.metrics.Meter;
import com.google.common.base.Optional;

import gobblin.metrics.metric.InnerMetric;


/**
 * Implementation of {@link InnerMetric} for {@link Meter}.
 */
public class InnerMeter extends Meter implements InnerMetric {

  private final String name;
  private final Optional<ContextAwareMeter> parentMeter;
  private final WeakReference<ContextAwareMeter> contextAwareMeter;

  InnerMeter(MetricContext context, String name, ContextAwareMeter contextAwareMeter) {
    this.name = name;

    Optional<MetricContext> parentContext = context.getParent();
    if (parentContext.isPresent()) {
      this.parentMeter = Optional.fromNullable(parentContext.get().contextAwareMeter(name));
    } else {
      this.parentMeter = Optional.absent();
    }
    this.contextAwareMeter = new WeakReference<>(contextAwareMeter);
  }

  @Override
  public void mark(long n) {
    super.mark(n);
    if (this.parentMeter.isPresent()) {
      this.parentMeter.get().mark(n);
    }
  }

  public String getName() {
    return this.name;
  }

  @Override public ContextAwareMetric getContextAwareMetric() {
    return this.contextAwareMeter.get();
  }
}
