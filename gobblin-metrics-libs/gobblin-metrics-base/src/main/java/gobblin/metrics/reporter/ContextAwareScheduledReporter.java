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
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;

import com.google.common.base.Optional;

import gobblin.metrics.MetricContext;


/**
 * A custom {@link com.codahale.metrics.ScheduledReporter} that is aware of the
 * {@link gobblin.metrics.MetricContext} it is associated to.
 *
 * @author Yinan Li
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
   * <p>
   *   The metric names (the keys in the given {@link SortedMap}s) may or may not include the
   *   {@link gobblin.metrics.Tag}s of the {@link MetricContext} depending on if the {@link MetricContext} is
   *   configured to report fully-qualified metric names or not using the method
   *   {@link MetricContext.Builder#reportFullyQualifiedNames(boolean)}. It is up to the
   *   implementation of this method to decide on whether to include the name of the
   *   {@link MetricContext} (given by {@link MetricContext#getName()}) and the {@link gobblin.metrics.Tag}s
   *   of individual {@link gobblin.metrics.ContextAwareMetric}s when reporting them.
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
   *
   * @param <R> type of a subclass of {@link ContextAwareScheduledReporter}
   * @param <B> type of a subclass of {@link Builder}
   */
  @SuppressWarnings("unchecked")
  public abstract static class Builder<R extends ContextAwareScheduledReporter, B extends Builder> {

    protected final String name;
    protected MetricFilter filter = MetricFilter.ALL;
    protected TimeUnit rateUnit = TimeUnit.SECONDS;
    protected TimeUnit durationUnit = TimeUnit.MILLISECONDS;

    public Builder(String name) {
      this.name = name;
    }

    /**
     * Get the name of the {@link ContextAwareScheduledReporter} that is going to be built by this
     * {@link ContextAwareScheduledReporter.Builder}.
     *
     * @return name of the {@link ContextAwareScheduledReporter} that is going to be built
     */
    public String getName() {
      return this.name;
    }

    /**
     * Build a new {@link ContextAwareScheduledReporter}.
     *
     * @param context the {@link MetricContext} of this {@link ContextAwareScheduledReporter}
     * @return the newly built {@link ContextAwareScheduledReporter}
     */
    public abstract R build(MetricContext context);

    /**
     * Only report metrics which match the given filter.
     *
     * @param filter a {@link MetricFilter}
     * @return {@code this}
     */
    public B filter(MetricFilter filter) {
      this.filter = filter;
      return (B) this;
    }

    /**
     * Convert rates to the given time unit.
     *
     * @param rateUnit a unit of time
     * @return {@code this}
     */
    public B convertRatesTo(TimeUnit rateUnit) {
      this.rateUnit = rateUnit;
      return (B) this;
    }

    /**
     * Convert durations to the given time unit.
     *
     * @param durationUnit a unit of time
     * @return {@code this}
     */
    public B convertDurationsTo(TimeUnit durationUnit) {
      this.durationUnit = durationUnit;
      return (B) this;
    }
  }
}
