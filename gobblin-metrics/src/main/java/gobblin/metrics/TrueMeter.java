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

package gobblin.metrics;

import java.lang.ref.WeakReference;

import com.codahale.metrics.Meter;
import com.google.common.base.Optional;

import gobblin.metrics.metric.TrueMetric;


/**
 * Created by ibuenros on 10/30/15.
 */
public class TrueMeter extends Meter implements TrueMetric {

  private final String name;
  private final Optional<ContextAwareMeter> parentMeter;
  private final WeakReference<ContextAwareMeter> contextAwareMeter;

  TrueMeter(MetricContext context, String name, ContextAwareMeter contextAwareMeter) {
    this.name = name;

    Optional<MetricContext> parentContext = context.getParent();
    if (parentContext.isPresent()) {
      this.parentMeter = Optional.fromNullable(parentContext.get().contextAwareMeter(name));
    } else {
      this.parentMeter = Optional.absent();
    }
    this.contextAwareMeter = new WeakReference<ContextAwareMeter>(contextAwareMeter);
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
