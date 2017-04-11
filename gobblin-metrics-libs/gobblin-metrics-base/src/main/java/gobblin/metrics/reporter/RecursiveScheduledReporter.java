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

package gobblin.metrics.reporter;

import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;

import gobblin.metrics.MetricContext;


/**
 * Reports a Metric context and all of its descendants recursively.
 */
public abstract class RecursiveScheduledReporter extends ScheduledReporter {

  protected final MetricRegistry registry;

  public RecursiveScheduledReporter(MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit,
      TimeUnit durationUnit) {
    super(registry, name, filter, rateUnit, durationUnit);
    this.registry = registry;
  }

  @Override
  public final void report() {
    reportContextTree(this.registry);
  }

  /**
   * Report a {@link com.codahale.metrics.MetricRegistry}. If the input is a {@link gobblin.metrics.MetricContext}
   * it will also report all of its children recursively.
   * @param registry MetricRegistry to report.
   */
  public void reportContextTree(MetricRegistry registry) {

    reportRegistry(registry);

    if (registry instanceof MetricContext) {
      for (MetricContext context : ((MetricContext) registry).getChildContextsAsMap().values()) {
        reportContextTree(context);
      }
    }

  }

  public abstract void reportRegistry(MetricRegistry registry);

  /**
   * This is an abstract method of {@link com.codahale.metrics.ScheduledReporter} which is no longer used.
   * Implement as a NOOP.
   */
  @Override
  public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
  }
}
