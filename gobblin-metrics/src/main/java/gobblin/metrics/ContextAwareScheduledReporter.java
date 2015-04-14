/* (c) 2014 LinkedIn Corp. All rights reserved.
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

import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;

import com.google.common.base.Optional;


/**
 * A custom {@link com.codahale.metrics.ScheduledReporter} that is aware of the
 * {@link gobblin.metrics.MetricContext} it is associated to.
 *
 * @author ynli
 */
public abstract class ContextAwareScheduledReporter extends ScheduledReporter {

  private final MetricContext context;
  private final Optional<MetricFilter> filter;

  protected ContextAwareScheduledReporter(MetricContext context, String name, MetricFilter filter,
      TimeUnit rateUnit, TimeUnit durationUnit) {
    super(context, name, filter, rateUnit, durationUnit);
    this.context = context;
    this.filter = Optional.fromNullable(filter);
  }

  @Override
  public void report() {
    if (this.filter.isPresent()) {
      report(this.context.getGauges(this.filter.get()),
             this.context.getCounters(this.filter.get()),
             this.context.getHistograms(this.filter.get()),
             this.context.getMeters(this.filter.get()),
             this.context.getTimers(this.filter.get()));
    } else {
      report(this.context.getGauges(),
             this.context.getCounters(),
             this.context.getHistograms(),
             this.context.getMeters(),
             this.context.getTimers());
    }

  }

  @Override
  public void report(SortedMap<String, Gauge> gauges,
                     SortedMap<String, Counter> counters,
                     SortedMap<String, Histogram> histograms,
                     SortedMap<String, Meter> meters,
                     SortedMap<String, Timer> timers) {
    reportInContext(this.context, gauges, counters, histograms, meters, timers);
  }

  /**
   * Report all the given metrics in the {@link MetricContext}.
   *
   * <p>
   *   Called periodically by the polling thread. Subclasses should report all the given metrics.
   * </p>
   *
   * @param gauges     all of the gauges in the {@link MetricContext}
   * @param counters   all of the counters in the {@link MetricContext}
   * @param histograms all of the histograms in the {@link MetricContext}
   * @param meters     all of the meters in the {@link MetricContext}
   * @param timers     all of the timers in the {@link MetricContext}
   */
  protected abstract void reportInContext(MetricContext context,
                                       SortedMap<String, Gauge> gauges,
                                       SortedMap<String, Counter> counters,
                                       SortedMap<String, Histogram> histograms,
                                       SortedMap<String, Meter> meters,
                                       SortedMap<String, Timer> timers);

  /**
   * A builder class for {@link ContextAwareScheduledReporter}.
   */
  public abstract static class Builder {

    protected final String name;
    protected MetricFilter filter = MetricFilter.ALL;
    protected TimeUnit rateUnit = TimeUnit.SECONDS;
    protected TimeUnit durationUnit = TimeUnit.MILLISECONDS;

    public Builder(String name) {
      this.name = name;
    }

    /**
     * Build a new {@link ContextAwareScheduledReporter}.
     *
     * @param context the {@link MetricContext} of this {@link ContextAwareScheduledReporter}
     * @return the newly built {@link ContextAwareScheduledReporter}
     */
    public abstract ContextAwareScheduledReporter build(MetricContext context);

    /**
     * Only report metrics which match the given filter.
     *
     * @param filter a {@link MetricFilter}
     * @return {@code this}
     */
    public Builder filter(MetricFilter filter) {
      this.filter = filter;
      return this;
    }

    /**
     * Convert rates to the given time unit.
     *
     * @param rateUnit a unit of time
     * @return {@code this}
     */
    public Builder convertRatesTo(TimeUnit rateUnit) {
      this.rateUnit = rateUnit;
      return this;
    }

    /**
     * Convert durations to the given time unit.
     *
     * @param durationUnit a unit of time
     * @return {@code this}
     */
    public Builder convertDurationsTo(TimeUnit durationUnit) {
      this.durationUnit = durationUnit;
      return this;
    }
  }
}
