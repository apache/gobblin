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

package gobblin.metrics.hadoop;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.mapred.Reporter;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Timer;

import gobblin.metrics.ContextAwareScheduledReporter;
import gobblin.metrics.MetricContext;


/**
 * An implementation of {@link gobblin.metrics.ContextAwareScheduledReporter} that reports
 * applicable metrics as Hadoop counters using a {@link org.apache.hadoop.mapred.Reporter}.
 *
 * @author ynli
 */
public class HadoopCounterReporter extends ContextAwareScheduledReporter {

  private final Reporter reporter;

  protected HadoopCounterReporter(MetricContext context, String name, MetricFilter filter,
      TimeUnit rateUnit, TimeUnit durationUnit, Reporter reporter) {
    super(context, name, filter, rateUnit, durationUnit);
    this.reporter = reporter;
  }

  @Override
  protected void reportInContext(MetricContext context,
                                 SortedMap<String, Gauge> gauges,
                                 SortedMap<String, Counter> counters,
                                 SortedMap<String, Histogram> histograms,
                                 SortedMap<String, Meter> meters,
                                 SortedMap<String, Timer> timers) {
    // Only counters are appropriate to be reported as Hadoop counters
    reportCounters(context, counters);
  }

  /**
   * Create a new {@link gobblin.metrics.hadoop.HadoopCounterReporter.Builder} that
   * uses the simple name of {@link HadoopCounterReporter} as the reporter name.
   *
   * @return a new {@link gobblin.metrics.hadoop.HadoopCounterReporter.Builder}
   */
  public static Builder builder(Reporter reporter) {
    return builder(HadoopCounterReporter.class.getName(), reporter);
  }

  /**
   * Create a new {@link gobblin.metrics.hadoop.HadoopCounterReporter.Builder} that
   * uses a given reporter name.
   *
   * @param name the given reporter name
   * @return a new {@link gobblin.metrics.hadoop.HadoopCounterReporter.Builder}
   */
  public static Builder builder(String name, Reporter reporter) {
    return new Builder(name, reporter);
  }

  private void reportCounters(MetricContext context, SortedMap<String, Counter> counters) {
    for (Map.Entry<String, Counter> entry : counters.entrySet()) {
      this.reporter.getCounter(context.getName(), entry.getKey()).setValue(entry.getValue().getCount());
    }
  }

  /**
   * A builder class for {@link HadoopCounterReporter}.
   */
  public static class Builder extends ContextAwareScheduledReporter.Builder<HadoopCounterReporter, Builder> {

    private final Reporter reporter;

    public Builder(String name, Reporter reporter) {
      super(name);
      this.reporter = reporter;
    }

    @Override
    public HadoopCounterReporter build(MetricContext context) {
      return new HadoopCounterReporter(
          context, this.name, this.filter, this.rateUnit, this.durationUnit, this.reporter);
    }
  }
}
