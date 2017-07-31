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

package org.apache.gobblin.metrics.performance;

import lombok.Builder;
import lombok.Getter;

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.collect.Lists;

import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.RootMetricContext;


/**
 * A {@link Runnable} that updates a set of metrics for every call to {@link #run}.
 *
 * <p>
 *   This class will automatically generate a metric context tree and a set of metrics at the lowest level context.
 *   An instance is created through a builder with the following optional parameters:
 *   * depth: number of levels in generated {@link MetricContext} tree. Default: 0
 *   * context: base {@link MetricContext} used as the root of the generated tree. Default: {@link RootMetricContext}.
 *   * counters: number of counters to generate. Named counter0, counter1, ...
 *   * meters: number of meters to generate. Named meter0, meter1, ...
 *   * histograms: number of histograms to generate. Named histogram0, histogram1, ...
 *   * timers: number of timers to generate. Named timer0, timer1, ...
 * </p>
 */
public class MetricsUpdater implements Runnable {

  private final int depth;
  @Getter
  private final MetricContext context;
  private final List<Counter> counters;
  private final List<Meter> meters;
  private final List<Histogram> histograms;
  private final List<Timer> timers;
  private final Random random;

  @Builder
  private MetricsUpdater(int depth, int counters, int meters, int histograms,
      int timers, MetricContext baseContext) {
    this.depth = depth;
    this.random = new Random();

    MetricContext tmpContext = baseContext == null ? RootMetricContext.get() : baseContext;
    while(depth > 0) {
      tmpContext = tmpContext.childBuilder(UUID.randomUUID().toString()).build();
      depth--;
    }
    this.context = tmpContext;

    this.counters = Lists.newArrayList();
    for(int i = 0; i < counters; i++) {
      this.counters.add(this.context.counter("gobblin.performance.test.counter" + i));
    }

    this.meters = Lists.newArrayList();
    for(int i = 0; i < meters; i++) {
      this.meters.add(this.context.meter("gobblin.performance.test.meter" + i));
    }

    this.histograms = Lists.newArrayList();
    for(int i = 0; i < histograms; i++) {
      this.histograms.add(this.context.histogram("gobblin.performance.test.histogram" + i));
    }

    this.timers = Lists.newArrayList();
    for(int i = 0; i < timers; i++) {
      this.timers.add(this.context.timer("gobblin.performance.test.timer" + i));
    }

  }

  @Override public void run() {
    updateCounters();
    updateMeters();
    updateHistograms();
    updateTimers();
  }

  private void updateCounters() {
    for(Counter counter : this.counters) {
      counter.inc();
    }
  }

  private void updateMeters() {
    for(Meter meter : this.meters) {
      meter.mark();
    }
  }

  private void updateTimers() {
    for(Timer timer : this.timers) {
      timer.update(this.random.nextInt(1000), TimeUnit.SECONDS);
    }
  }

  private void updateHistograms() {
    for(Histogram histogram : this.histograms) {
      histogram.update(this.random.nextInt(1000));
    }
  }

}
