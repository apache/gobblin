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

import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Timer;

import gobblin.metrics.ContextAwareScheduledReporter;
import gobblin.metrics.MetricContext;


/**
 * An implementation of {@link gobblin.metrics.ContextAwareScheduledReporter} that reports applicable
 * metrics as Hadoop counters using a {@link org.apache.hadoop.mapreduce.TaskInputOutputContext}.
 *
 * @param <KI> the input key type of {@code hadoopContext}
 * @param <VI> the input value type of {@code hadoopContext}
 * @param <KO> the output key type of {@code hadoopContext}
 * @param <VO> the output value type of {@code hadoopContext}
 *
 * @author ynli
 */
public class NewAPIHadoopCounterReporter<KI, VI, KO, VO> extends ContextAwareScheduledReporter {

  private final TaskInputOutputContext<KI, VI, KO, VO> hadoopContext;

  protected NewAPIHadoopCounterReporter(MetricContext context, String name, MetricFilter filter,
      TimeUnit rateUnit, TimeUnit durationUnit, TaskInputOutputContext<KI, VI, KO, VO> hadoopContext) {
    super(context, name, filter, rateUnit, durationUnit);
    this.hadoopContext = hadoopContext;
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
   * Create a new {@link gobblin.metrics.hadoop.NewAPIHadoopCounterReporter.Builder}
   * that uses the simple name of {@link NewAPIHadoopCounterReporter} as the reporter name.
   *
   * @param hadoopContext a {@link org.apache.hadoop.mapreduce.TaskInputOutputContext}
   *                      used to access Hadoop counters
   * @param <KI> the input key type of {@code hadoopContext}
   * @param <VI> the input value type of {@code hadoopContext}
   * @param <KO> the output key type of {@code hadoopContext}
   * @param <VO> the output value type of {@code hadoopContext}
   * @return a new {@link gobblin.metrics.hadoop.NewAPIHadoopCounterReporter.Builder}
   */
  public static <KI, VI, KO, VO> Builder<KI, VI, KO, VO> builder(
      TaskInputOutputContext<KI, VI, KO, VO> hadoopContext) {
    return builder(NewAPIHadoopCounterReporter.class.getName(), hadoopContext);
  }

  /**
   * Create a new {@link gobblin.metrics.hadoop.NewAPIHadoopCounterReporter.Builder}
   * that uses a given reporter name.
   *
   * @param name the given reporter name
   * @param hadoopContext a {@link org.apache.hadoop.mapreduce.TaskInputOutputContext}
   *                      used to access Hadoop counters
   * @param <KI> the input key type of {@code hadoopContext}
   * @param <VI> the input value type of {@code hadoopContext}
   * @param <KO> the output key type of {@code hadoopContext}
   * @param <VO> the output value type of {@code hadoopContext}
   * @return a new {@link gobblin.metrics.hadoop.NewAPIHadoopCounterReporter.Builder}
   */
  public static <KI, VI, KO, VO> Builder<KI, VI, KO, VO> builder(String name,
      TaskInputOutputContext<KI, VI, KO, VO> hadoopContext) {
    return new Builder<KI, VI, KO, VO>(name, hadoopContext);
  }


  private void reportCounters(MetricContext context, SortedMap<String, Counter> counters) {
    for (Map.Entry<String, Counter> entry : counters.entrySet()) {
      this.hadoopContext.getCounter(context.getName(), entry.getKey()).setValue(entry.getValue().getCount());
    }
  }

  /**
   * A builder class for {@link NewAPIHadoopCounterReporter}.
   *
   * @param <KI> the input key type of {@code hadoopContext}
   * @param <VI> the input value type of {@code hadoopContext}
   * @param <KO> the output key type of {@code hadoopContext}
   * @param <VO> the output value type of {@code hadoopContext}
   */
  public static class Builder<KI, VI, KO, VO> extends ContextAwareScheduledReporter.Builder<
      NewAPIHadoopCounterReporter<KI, VI, KO, VO>, Builder<KI, VI, KO, VO>> {

    private final TaskInputOutputContext<KI, VI, KO, VO> hadoopContext;

    public Builder(String name, TaskInputOutputContext<KI, VI, KO, VO> hadoopContext) {
      super(name);
      this.hadoopContext = hadoopContext;
    }

    @Override
    public NewAPIHadoopCounterReporter<KI, VI, KO, VO> build(MetricContext context) {
      return new NewAPIHadoopCounterReporter<KI, VI, KO, VO>(
          context, this.name, this.filter, this.rateUnit, this.durationUnit, this.hadoopContext);
    }
  }
}
