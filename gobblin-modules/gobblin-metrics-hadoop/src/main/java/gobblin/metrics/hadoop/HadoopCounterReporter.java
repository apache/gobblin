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

package gobblin.metrics.hadoop;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.mapred.Reporter;

import com.codahale.metrics.MetricFilter;

import gobblin.metrics.reporter.ContextAwareScheduledReporter;
import gobblin.metrics.MetricContext;


/**
 * An implementation of {@link gobblin.metrics.reporter.ContextAwareScheduledReporter} that reports
 * applicable metrics as Hadoop counters using a {@link org.apache.hadoop.mapred.Reporter}.
 *
 * @author Yinan Li
 */
public class HadoopCounterReporter extends AbstractHadoopCounterReporter {

  private final Reporter reporter;

  protected HadoopCounterReporter(MetricContext context, String name, MetricFilter filter,
      TimeUnit rateUnit, TimeUnit durationUnit, Reporter reporter) {
    super(context, name, filter, rateUnit, durationUnit);
    this.reporter = reporter;
  }

  @Override
  protected void reportIncremental(MetricContext context, String name, long incremental) {
    this.reporter.getCounter(context.getName(), name).increment(incremental);
  }

  @Override
  protected void reportValue(MetricContext context, String name, long value) {
    this.reporter.getCounter(context.getName(), name).setValue(value);
  }

  /**
   * Create a new {@link gobblin.metrics.hadoop.HadoopCounterReporter.Builder} that
   * uses the simple name of {@link HadoopCounterReporter} as the reporter name.
   *
   * @param reporter a {@link org.apache.hadoop.mapred.Reporter} used to access Hadoop counters
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
   * @param reporter a {@link org.apache.hadoop.mapred.Reporter} used to access Hadoop counters
   * @return a new {@link gobblin.metrics.hadoop.HadoopCounterReporter.Builder}
   */
  public static Builder builder(String name, Reporter reporter) {
    return new Builder(name, reporter);
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
