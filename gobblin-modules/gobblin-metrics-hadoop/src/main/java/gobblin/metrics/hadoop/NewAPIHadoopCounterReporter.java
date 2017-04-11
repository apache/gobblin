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

import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.codahale.metrics.MetricFilter;

import gobblin.metrics.reporter.ContextAwareScheduledReporter;
import gobblin.metrics.MetricContext;


/**
 * An implementation of {@link gobblin.metrics.reporter.ContextAwareScheduledReporter} that reports applicable
 * metrics as Hadoop counters using a {@link org.apache.hadoop.mapreduce.TaskInputOutputContext}.
 *
 * @param <KI> the input key type of {@code hadoopContext}
 * @param <VI> the input value type of {@code hadoopContext}
 * @param <KO> the output key type of {@code hadoopContext}
 * @param <VO> the output value type of {@code hadoopContext}
 *
 * @author Yinan Li
 */
public class NewAPIHadoopCounterReporter<KI, VI, KO, VO> extends AbstractHadoopCounterReporter {

  private final TaskInputOutputContext<KI, VI, KO, VO> hadoopContext;

  protected NewAPIHadoopCounterReporter(MetricContext context, String name, MetricFilter filter,
      TimeUnit rateUnit, TimeUnit durationUnit, TaskInputOutputContext<KI, VI, KO, VO> hadoopContext) {
    super(context, name, filter, rateUnit, durationUnit);
    this.hadoopContext = hadoopContext;
  }

  @Override
  protected void reportIncremental(MetricContext context, String name, long incremental) {
    this.hadoopContext.getCounter(context.getName(), name).increment(incremental);
  }

  @Override
  protected void reportValue(MetricContext context, String name, long value) {
    this.hadoopContext.getCounter(context.getName(), name).setValue(value);
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
