/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.instrumented.fork;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.fork.ForkOperator;
import gobblin.instrumented.Instrumentable;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.MetricContext;
import gobblin.metrics.MetricNames;
import gobblin.metrics.Tag;


/**
 * package-private implementation of instrumentation for {@link gobblin.fork.ForkOperator}.
 * See {@link gobblin.instrumented.fork.InstrumentedForkOperator} for extensible class.
 */
abstract class InstrumentedForkOperatorBase<S, D> implements Instrumentable, ForkOperator<S, D> {

  private boolean instrumentationEnabled = false;
  protected final Closer closer = Closer.create();

  private MetricContext metricContext = new MetricContext.Builder(InstrumentedForkOperatorBase.class.getName())
      .build();
  private Optional<Meter> inputMeter = Optional.absent();
  private Optional<Meter> outputForks = Optional.absent();
  private Optional<Timer> forkOperatorTimer = Optional.absent();

  @Override
  public void init(WorkUnitState workUnitState) throws Exception {
    init(workUnitState, this.getClass());
  }

  protected void init(WorkUnitState workUnitState, Class<?> classTag)
      throws Exception {
    this.instrumentationEnabled = GobblinMetrics.isEnabled(workUnitState);
    this.metricContext = closer.register(Instrumented.getMetricContext(workUnitState, classTag));
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
      this.inputMeter = Optional.of(this.metricContext.meter(MetricNames.ForkOperatorMetrics.RECORDS_IN_METER));
      this.outputForks = Optional.of(this.metricContext.meter(MetricNames.ForkOperatorMetrics.FORKS_OUT_METER));
      this.forkOperatorTimer = Optional.of(this.metricContext.timer(MetricNames.ForkOperatorMetrics.FORK_TIMER));
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
  public List<Boolean> forkDataRecord(WorkUnitState workUnitState, D input) {
    if (!isInstrumentationEnabled()) {
      return forkDataRecordImpl(workUnitState, input);
    }

    long startTimeNanos = System.nanoTime();

    beforeFork(input);
    List<Boolean> result = forkDataRecordImpl(workUnitState, input);
    afterFork(result, startTimeNanos);

    return result;
  }

  /**
   * Called before forkDataRecord.
   * @param input an input data record
   */
  protected void beforeFork(D input) {
    Instrumented.markMeter(this.inputMeter);
  }

  /**
   * Called after forkDataRecord.
   * @param forks result from forkDataRecord.
   * @param startTimeNanos start time of forkDataRecord.
   */
  protected void afterFork(List<Boolean> forks, long startTimeNanos) {
    int forksGenerated = 0;
    for (Boolean fork : forks) {
      forksGenerated += fork ? 1 : 0;
    }
    Instrumented.markMeter(this.outputForks, forksGenerated);
    Instrumented.updateTimer(this.forkOperatorTimer, System.nanoTime() - startTimeNanos, TimeUnit.NANOSECONDS);
  }

  /**
   * Subclasses should implement this instead of {@link gobblin.fork.ForkOperator#forkDataRecord}.
   */
  public abstract List<Boolean> forkDataRecordImpl(WorkUnitState workUnitState, D input);

  @Override
  public MetricContext getMetricContext() {
    return this.metricContext;
  }

  @Override
  public void close()
      throws IOException {
    this.closer.close();
  }
}
