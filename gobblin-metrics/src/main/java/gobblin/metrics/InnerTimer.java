/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
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
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Timer;
import com.google.common.base.Optional;

import gobblin.metrics.metric.InnerMetric;


/**
 * Implementation of {@link InnerMetric} for {@link Timer}.
 */
public class InnerTimer extends Timer implements InnerMetric {

  private final String name;
  private final Optional<ContextAwareTimer> parentTimer;
  private final WeakReference<ContextAwareTimer> timer;

  InnerTimer(MetricContext context, String name, ContextAwareTimer contextAwareTimer) {
    this.name = name;

    Optional<MetricContext> parentContext = context.getParent();
    if (parentContext.isPresent()) {
      this.parentTimer = Optional.fromNullable(parentContext.get().contextAwareTimer(name));
    } else {
      this.parentTimer = Optional.absent();
    }
    this.timer = new WeakReference<ContextAwareTimer>(contextAwareTimer);
  }

  @Override
  public void update(long duration, TimeUnit unit) {
    super.update(duration, unit);
    if (this.parentTimer.isPresent()) {
      this.parentTimer.get().update(duration, unit);
    }
  }

  public String getName() {
    return this.name;
  }

  @Override public ContextAwareMetric getContextAwareMetric() {
    return this.timer.get();
  }
}
