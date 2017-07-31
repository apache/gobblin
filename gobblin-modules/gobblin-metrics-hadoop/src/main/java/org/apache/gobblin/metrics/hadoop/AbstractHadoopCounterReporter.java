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

package org.apache.gobblin.metrics.hadoop;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import com.google.common.collect.Maps;

import org.apache.gobblin.metrics.reporter.ContextAwareScheduledReporter;
import org.apache.gobblin.metrics.Measurements;
import org.apache.gobblin.metrics.MetricContext;


/**
 * An extension to {@link ContextAwareScheduledReporter} that serves as the basis
 * for implementations that report applicable metrics through Hadoop counters.
 *
 * @author Yinan Li
 */
public abstract class AbstractHadoopCounterReporter extends ContextAwareScheduledReporter {

  private final Map<String, Long> previousCounts = Maps.newHashMap();

  protected AbstractHadoopCounterReporter(MetricContext context, String name, MetricFilter filter,
      TimeUnit rateUnit, TimeUnit durationUnit) {
    super(context, name, filter, rateUnit, durationUnit);
  }

  @Override
  protected void reportInContext(MetricContext context,
                                 SortedMap<String, Gauge> gauges,
                                 SortedMap<String, Counter> counters,
                                 SortedMap<String, Histogram> histograms,
                                 SortedMap<String, Meter> meters,
                                 SortedMap<String, Timer> timers) {
    for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
      Gauge gauge = entry.getValue();
      if (gauge.getValue() instanceof Long ||
          gauge.getValue() instanceof Integer ||
          gauge.getValue() instanceof Short ||
          gauge.getValue() instanceof Byte)
        reportValue(context, entry.getKey(), ((Number) gauge.getValue()).longValue());
    }

    for (Map.Entry<String, Counter> entry : counters.entrySet()) {
      reportCount(context, entry.getKey(), entry.getValue().getCount());
    }

    for (Map.Entry<String, Meter> entry : meters.entrySet()) {
      reportCount(context, entry.getKey(), entry.getValue().getCount());
    }

    for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
      reportCount(context, entry.getKey(), entry.getValue().getCount());
    }

    for (Map.Entry<String, Timer> entry : timers.entrySet()) {
      reportCount(context, entry.getKey(), entry.getValue().getCount());
    }
  }

  /**
   * Report a given incremental value of a metric.
   *
   * @param context the {@link MetricContext} this is associated to
   * @param name metric name
   * @param incremental the given incremental value
   */
  protected abstract void reportIncremental(MetricContext context, String name, long incremental);

  /**
   * Report a given value of a metric.
   *
   * @param context the {@link MetricContext} this is associated to
   * @param name metric name
   * @param value the given value
   */
  protected abstract void reportValue(MetricContext context, String name, long value);

  private void reportCount(MetricContext context, String name, long currentCount) {
    reportIncremental(context, MetricRegistry.name(name, Measurements.COUNT.getName()),
        calculateIncremental(name, currentCount));
    // Remember the current count
    this.previousCounts.put(name, currentCount);
  }

  private long calculateIncremental(String name, long currentCount) {
    if (this.previousCounts.containsKey(name)) {
      return currentCount - this.previousCounts.get(name);
    }
    return currentCount;
  }
}
