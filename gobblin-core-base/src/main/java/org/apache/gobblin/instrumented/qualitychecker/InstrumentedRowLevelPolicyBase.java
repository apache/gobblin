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

package org.apache.gobblin.instrumented.qualitychecker;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.instrumented.Instrumentable;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.MetricNames;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.qualitychecker.row.RowLevelPolicy;


/**
 * package-private implementation of instrumentation for {@link org.apache.gobblin.qualitychecker.row.RowLevelPolicy}.
 * See {@link org.apache.gobblin.instrumented.qualitychecker.InstrumentedRowLevelPolicy} for extensible class.
 */
abstract class InstrumentedRowLevelPolicyBase extends RowLevelPolicy implements Instrumentable, Closeable {

  private final boolean instrumentationEnabled;

  private MetricContext metricContext;
  private Optional<Meter> recordsMeter;
  private Optional<Meter> passedRecordsMeter;
  private Optional<Meter> failedRecordsMeter;
  private Optional<Timer> policyTimer;
  protected final Closer closer;

  public InstrumentedRowLevelPolicyBase(State state, Type type) {
    this(state, type, Optional.<Class<?>>absent());
  }

  protected InstrumentedRowLevelPolicyBase(State state, Type type, Optional<Class<?>> classTag) {
  super(state, type);
    this.instrumentationEnabled = GobblinMetrics.isEnabled(state);
    this.closer = Closer.create();
    this.metricContext =
        this.closer.register(Instrumented.getMetricContext(state, classTag.or(this.getClass())));

    regenerateMetrics();
  }

  @Override
  public void switchMetricContext(List<Tag<?>> tags) {
    this.metricContext = this.closer.register(Instrumented.newContextFromReferenceContext(this.metricContext, tags,
        Optional.<String>absent()));

    regenerateMetrics();
  }

  @Override
  public void switchMetricContext(MetricContext context) {
    this.metricContext = context;
    regenerateMetrics();
  }

  /**
   * Generates metrics for the instrumentation of this class.
   */
  protected void regenerateMetrics() {
    if(isInstrumentationEnabled()) {
      this.recordsMeter = Optional.of(this.metricContext.meter(MetricNames.RowLevelPolicyMetrics.RECORDS_IN_METER));
      this.passedRecordsMeter = Optional.of(
          this.metricContext.meter(MetricNames.RowLevelPolicyMetrics.RECORDS_PASSED_METER));
      this.failedRecordsMeter = Optional.of(
          this.metricContext.meter(MetricNames.RowLevelPolicyMetrics.RECORDS_FAILED_METER));
      this.policyTimer = Optional.<Timer>of(
          this.metricContext.timer(MetricNames.RowLevelPolicyMetrics.CHECK_TIMER));
    } else {
      this.recordsMeter = Optional.absent();
      this.passedRecordsMeter = Optional.absent();
      this.failedRecordsMeter = Optional.absent();
      this.policyTimer = Optional.absent();
    }
  }

  /** Default with no additional tags */
  @Override
  public List<Tag<?>> generateTags(State state) {
    return Lists.newArrayList();
  }

  @Override
  public boolean isInstrumentationEnabled() {
    return this.instrumentationEnabled;
  }

  @Override
  public Result executePolicy(Object record) {
    if(!isInstrumentationEnabled()) {
      return executePolicyImpl(record);
    }

    long startTime = System.nanoTime();

    beforeCheck(record);
    Result result = executePolicyImpl(record);
    afterCheck(result, startTime);

    return result;
  }

  /**
   * Called before check is run.
   * @param record
   */
  public void beforeCheck(Object record) {
    Instrumented.markMeter(this.recordsMeter);
  }

  /**
   * Called after check is run.
   * @param result result from check.
   * @param startTimeNanos start time of check.
   */
  public void afterCheck(Result result, long startTimeNanos) {
    switch (result) {
      case FAILED:
        Instrumented.markMeter(this.failedRecordsMeter);
        break;
      case PASSED:
        Instrumented.markMeter(this.passedRecordsMeter);
        break;
      default:
    }

    Instrumented.updateTimer(this.policyTimer, System.nanoTime() - startTimeNanos, TimeUnit.NANOSECONDS);
  }

  /**
   * Subclasses should implement this instead of {@link org.apache.gobblin.qualitychecker.row.RowLevelPolicy#executePolicy}.
   */
  public abstract Result executePolicyImpl(Object record);

  @Override
  public void close()
      throws IOException {
    this.closer.close();
  }

  @Override
  public MetricContext getMetricContext() {
    return this.metricContext;
  }
}
