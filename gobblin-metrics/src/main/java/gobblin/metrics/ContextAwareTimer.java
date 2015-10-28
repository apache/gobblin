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

import lombok.experimental.Delegate;

import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import com.google.common.base.Optional;

import gobblin.metrics.metric.TrueMetric;
import gobblin.util.Either;


/**
 * A type of {@link com.codahale.metrics.Timer}s that are aware of their {@link gobblin.metrics.MetricContext}
 * and can have associated {@link Tag}s.
 *
 * <p>
 *   Any updates to a {@link ContextAwareTimer} will be applied automatically to the
 *   {@link ContextAwareTimer} of the same name in the parent {@link MetricContext}.
 * </p>
 *
 * <p>
 *   This class wraps a {@link com.codahale.metrics.Timer} and delegates calls to public methods of
 *   {@link com.codahale.metrics.Timer} to the wrapped {@link com.codahale.metrics.Timer}.
 * </p>
 *
 * @author ynli
 */
class ContextAwareTimer extends Timer implements ContextAwareMetric {

  @Delegate
  private final TrueTimer trueTimer;
  private final MetricContext context;

  ContextAwareTimer(MetricContext context, String name) {
    this.trueTimer = new TrueTimer(context, name, this);
    this.context = context;
  }

  @Override
  public MetricContext getContext() {
    return this.context;
  }

  @Override public TrueMetric getTrueMetric() {
    return this.trueTimer;
  }
}
