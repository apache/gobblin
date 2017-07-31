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

package org.apache.gobblin.metrics;

import java.lang.ref.WeakReference;

import com.codahale.metrics.Counter;
import com.google.common.base.Optional;

import org.apache.gobblin.metrics.metric.InnerMetric;


/**
 * Implementation of {@link InnerMetric} for {@link Counter}.
 */
public class InnerCounter extends Counter implements InnerMetric {
  protected final String name;
  protected final Tagged tagged;
  protected final Optional<ContextAwareCounter> parentCounter;
  private final WeakReference<ContextAwareCounter> contextAwareCounter;

  public InnerCounter(MetricContext context, String name, ContextAwareCounter counter) {
    this.tagged = new Tagged();
    this.name = name;

    Optional<MetricContext> parentContext = context.getParent();
    if (parentContext.isPresent()) {
      this.parentCounter = Optional.fromNullable(parentContext.get().contextAwareCounter(name));
    } else {
      this.parentCounter = Optional.absent();
    }

    this.contextAwareCounter = new WeakReference<>(counter);
  }

  @Override
  public void inc(long n) {
    super.inc(n);
    if (this.parentCounter.isPresent()) {
      this.parentCounter.get().inc(n);
    }
  }

  @Override
  public void dec(long n) {
    super.dec(n);
    if (this.parentCounter.isPresent()) {
      this.parentCounter.get().dec(n);
    }
  }

  public String getName() {
    return this.name;
  }

  @Override
  public ContextAwareMetric getContextAwareMetric() {
    return this.contextAwareCounter.get();
  }
}
