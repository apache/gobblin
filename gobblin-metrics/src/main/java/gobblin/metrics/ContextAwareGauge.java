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

import java.util.Collection;
import java.util.List;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import gobblin.metrics.metric.TrueMetric;


/**
 * A type of {@link com.codahale.metrics.Gauge}s that that are aware of their {@link MetricContext}
 * and can have associated {@link Tag}s.
 *
 * <p>
 *   This class wraps a {@link com.codahale.metrics.Gauge} and delegates calls to public methods of
 *   {@link com.codahale.metrics.Gauge} to the wrapped {@link com.codahale.metrics.Gauge}.
 * </p>
 *
 * @param <T> the type of the {@link ContextAwareGauge}'s value
 *
 * @author ynli
 */
public class ContextAwareGauge<T> implements Gauge<T>, ContextAwareMetric {

  @Delegate
  private final TrueGauge<T> trueGauge;
  private final MetricContext context;

  ContextAwareGauge(MetricContext context, String name, Gauge<T> gauge) {
    this.trueGauge = new TrueGauge<T>(context, name, gauge, this);
    this.context = context;
  }

  @Override
  public MetricContext getContext() {
    return this.context;
  }

  @Override public TrueMetric getTrueMetric() {
    return this.trueGauge;
  }
}
