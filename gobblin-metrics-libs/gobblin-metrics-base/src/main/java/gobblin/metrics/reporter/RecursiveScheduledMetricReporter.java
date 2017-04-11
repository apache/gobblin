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
import com.google.common.base.Function;
import com.google.common.collect.Maps;

import gobblin.metrics.MetricContext;


/**
 * Reports the metrics of a {@link gobblin.metrics.MetricContext} following a schedule.
 */
public abstract class RecursiveScheduledMetricReporter extends RecursiveScheduledReporter {

  private MetricFilter filter;

  public RecursiveScheduledMetricReporter(MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit,
      TimeUnit durationUnit) {
    super(registry, name, filter, rateUnit, durationUnit);
    this.filter = filter;
  }

  public void reportRegistry(MetricRegistry registry) {
    Map<String, String> tags = Maps.newHashMap();
    if (registry instanceof MetricContext) {
      tags = Maps.transformValues(((MetricContext) registry).getTagMap(), new Function<Object, String>() {
        @Override
        public String apply(Object input) {
          return input.toString();
        }
      });
    }

    report(registry.getGauges(this.filter), registry.getCounters(this.filter), registry.getHistograms(this.filter),
        registry.getMeters(this.filter), registry.getTimers(this.filter), tags);
  }

  /**
   * Report the input metrics. The input tags apply to all input metrics.
   */
  public abstract void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers,
      Map<String, String> tags);

}
