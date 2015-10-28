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

import com.codahale.metrics.Gauge;

import gobblin.metrics.metric.TrueMetric;


/**
 * Created by ibuenros on 10/30/15.
 */
public class TrueGauge<T> implements TrueMetric, Gauge<T> {

  private final String name;
  private final Gauge<T> gauge;
  private final WeakReference<ContextAwareGauge<T>> contextAwareGauge;

  TrueGauge(MetricContext context, String name, Gauge<T> gauge, ContextAwareGauge<T> contextAwareGauge) {
    this.name = name;
    this.gauge = gauge;
    this.contextAwareGauge = new WeakReference<ContextAwareGauge<T>>(contextAwareGauge);
  }

  @Override
  public T getValue() {
    return this.gauge.getValue();
  }

  public String getName() {
    return this.name;
  }

  @Override public ContextAwareMetric getContextAwareMetric() {
    return this.contextAwareGauge.get();
  }
}
