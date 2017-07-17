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

package gobblin.instrumented.extractor;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Optional;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.MetricContext;
import gobblin.records.RecordStreamWithMetadata;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.stream.RecordEnvelope;
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
  private volatile long lastRecordTime;

  public InstrumentedExtractorDecorator(WorkUnitState workUnitState, Extractor<S, D> extractor) {
    super(workUnitState, Optional.<Class<?>> of(DecoratorUtils.resolveUnderlyingObject(extractor).getClass()));
    this.embeddedExtractor = this.closer.register(extractor);
    this.isEmbeddedInstrumented = Instrumented.isLineageInstrumented(extractor);
  }

  @Override
  public MetricContext getMetricContext() {
    return this.isEmbeddedInstrumented ? ((InstrumentedExtractorBase<S, D>) this.embeddedExtractor).getMetricContext()
        : super.getMetricContext();
  }

  @Override
  public RecordEnvelope<D> readRecordEnvelope() throws DataRecordException, IOException {
    return this.isEmbeddedInstrumented ? this.embeddedExtractor.readRecordEnvelope() : super.readRecordEnvelope();
  }

  @Override
  protected RecordEnvelope<D> readRecordEnvelopeImpl() throws DataRecordException, IOException {
    return this.embeddedExtractor.readRecordEnvelope();
  }

  @Override
  public RecordStreamWithMetadata<D, S> recordStream(AtomicBoolean shutdownRequest) throws IOException {
    if (this.isEmbeddedInstrumented) {
      return this.embeddedExtractor.recordStream(shutdownRequest);
    }
    RecordStreamWithMetadata<D, S> stream = this.embeddedExtractor.recordStream(shutdownRequest);
    stream = stream.mapRecords(r -> {
      if (this.lastRecordTime == 0) {
        this.lastRecordTime = System.nanoTime();
      }
      afterRead(r.getRecord(), this.lastRecordTime);
      this.lastRecordTime = System.nanoTime();
      return r;
    });
    return stream;
  }

  @Override
  public S getSchema() throws IOException {
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
    if (this.embeddedExtractor instanceof FinalState) {
      return ((FinalState) this.embeddedExtractor).getFinalState();
    }
    return super.getFinalState();
  }

  @Override
  public Object getDecoratedObject() {
    return this.embeddedExtractor;
  }
}
