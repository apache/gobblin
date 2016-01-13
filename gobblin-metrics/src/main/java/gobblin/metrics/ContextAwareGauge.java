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

import com.codahale.metrics.Gauge;

import gobblin.metrics.metric.InnerMetric;


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
 * @author Yinan Li
 */
public class ContextAwareGauge<T> implements Gauge<T>, ContextAwareMetric {

  @Delegate
  private final InnerGauge<T> innerGauge;
  private final MetricContext context;

  ContextAwareGauge(MetricContext context, String name, Gauge<T> gauge) {
    this.innerGauge = new InnerGauge<T>(context, name, gauge, this);
    this.context = context;
  }

  @Override
  public MetricContext getContext() {
    return this.context;
  }

  @Override public InnerMetric getInnerMetric() {
    return this.innerGauge;
  }
}
