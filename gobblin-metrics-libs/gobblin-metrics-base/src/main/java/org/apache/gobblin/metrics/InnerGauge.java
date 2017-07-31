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

import com.codahale.metrics.Gauge;

import org.apache.gobblin.metrics.metric.InnerMetric;


/**
 * Implementation of {@link InnerMetric} for {@link Gauge}.
 */
public class InnerGauge<T> implements InnerMetric, Gauge<T> {

  private final String name;
  private final Gauge<T> gauge;
  private final WeakReference<ContextAwareGauge<T>> contextAwareGauge;

  InnerGauge(MetricContext context, String name, Gauge<T> gauge, ContextAwareGauge<T> contextAwareGauge) {
    this.name = name;
    this.gauge = gauge;
    this.contextAwareGauge = new WeakReference<>(contextAwareGauge);
  }

  @Override
  public T getValue() {
    return this.gauge.getValue();
  }

  public String getName() {
    return this.name;
  }

  @Override
  public ContextAwareMetric getContextAwareMetric() {
    return this.contextAwareGauge.get();
  }
}
