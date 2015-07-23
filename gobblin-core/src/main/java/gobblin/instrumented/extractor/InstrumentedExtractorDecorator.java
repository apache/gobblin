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

package gobblin.instrumented.extractor;

import java.io.IOException;

import com.google.common.base.Optional;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.MetricContext;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.util.Decorator;
import gobblin.util.DecoratorUtils;
import gobblin.util.FinalState;


/**
 * Decorator that automatically instruments {@link gobblin.source.extractor.Extractor}.
 * Handles already instrumented {@link gobblin.instrumented.extractor.InstrumentedExtractor}
 * appropriately to avoid double metric reporting.
 */
public class InstrumentedExtractorDecorator<S, D> extends InstrumentedExtractorBase<S, D> implements Decorator {

  private final Extractor<S, D> embeddedExtractor;
  private final boolean isEmbeddedInstrumented;

  public InstrumentedExtractorDecorator(WorkUnitState workUnitState, Extractor<S, D> extractor) {
    super(workUnitState, Optional.<Class<?>>of(DecoratorUtils.resolveUnderlyingObject(extractor).getClass()));
    this.embeddedExtractor = this.closer.register(extractor);
    this.isEmbeddedInstrumented = Instrumented.isLineageInstrumented(extractor);
  }

  @Override
  public MetricContext getMetricContext() {
    return this.isEmbeddedInstrumented ?
        ((InstrumentedExtractorBase)embeddedExtractor).getMetricContext() :
        super.getMetricContext();
  }

  @Override
  public D readRecord(D reuse)
      throws DataRecordException, IOException {
    return this.isEmbeddedInstrumented ?
        readRecordImpl(reuse) :
        super.readRecord(reuse);
  }

  @Override
  public D readRecordImpl(D reuse)
      throws DataRecordException, IOException {
    return this.embeddedExtractor.readRecord(reuse);
  }

  @Override
  public S getSchema()
      throws IOException {
    return this.embeddedExtractor.getSchema();
  }

  @Override
  public long getExpectedRecordCount() {
    return this.embeddedExtractor.getExpectedRecordCount();
  }

  @Override
  public long getHighWatermark() {
    return this.embeddedExtractor.getHighWatermark();
  }

  @Override
  public State getFinalState() {
    if(this.embeddedExtractor instanceof FinalState) {
      return ((FinalState) this.embeddedExtractor).getFinalState();
    } else {
      return super.getFinalState();
    }
  }

  @Override
  public Object getDecoratedObject() {
    return this.embeddedExtractor;
  }
}
