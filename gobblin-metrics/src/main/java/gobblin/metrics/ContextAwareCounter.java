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

import com.codahale.metrics.Counter;

import gobblin.metrics.metric.ProxyMetric;
import gobblin.metrics.metric.TrueMetric;


/**
 * A type of {@link Counter}s that are aware of their {@link MetricContext} and can have associated
 * {@link Tag}s.
 *
 * <p>
 *   Any updates to a {@link ContextAwareCounter} will be applied automatically to the
 *   {@link ContextAwareCounter} of the same name in the parent {@link MetricContext}.
 * </p>
 *
 * <p>
 *   This class wraps a {@link com.codahale.metrics.Counter} and delegates calls to public methods of
 *   {@link com.codahale.metrics.Counter} to the wrapped {@link com.codahale.metrics.Counter}.
 * </p>
 *
 * @author ynli
 */
class ContextAwareCounter extends Counter implements ProxyMetric, ContextAwareMetric {

  private final MetricContext metricContext;
  @Delegate
  private final TrueCounter trueCounter;

  ContextAwareCounter(MetricContext context, String name) {
    this.trueCounter = new TrueCounter(context, name, this);
    this.metricContext = context;
  }

  public MetricContext getContext() {
    return this.metricContext;
  }

  @Override public TrueMetric getTrueMetric() {
    return this.trueCounter;
  }
}
