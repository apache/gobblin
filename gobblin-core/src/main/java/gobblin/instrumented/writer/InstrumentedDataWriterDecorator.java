/*
 *
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

package gobblin.instrumented.writer;

import java.io.IOException;

import com.google.common.base.Optional;

import gobblin.configuration.State;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.MetricContext;
import gobblin.util.Decorator;
import gobblin.util.DecoratorUtils;
import gobblin.util.FinalState;
import gobblin.writer.DataWriter;


/**
 * Decorator that automatically instruments {@link gobblin.writer.DataWriter}.
 * Handles already instrumented {@link gobblin.instrumented.writer.InstrumentedDataWriter}
 * appropriately to avoid double metric reporting.
 */
public class InstrumentedDataWriterDecorator<D> extends InstrumentedDataWriterBase<D> implements Decorator {

  private DataWriter<D> embeddedWriter;
  private boolean isEmbeddedInstrumented;

  public InstrumentedDataWriterDecorator(DataWriter<D> writer, State state) {
    super(state, Optional.<Class<?>>of(DecoratorUtils.resolveUnderlyingObject(writer).getClass()));
    this.embeddedWriter = this.closer.register(writer);
    this.isEmbeddedInstrumented = Instrumented.isLineageInstrumented(writer);
  }

  @Override
  public MetricContext getMetricContext() {
    return this.isEmbeddedInstrumented ?
        ((InstrumentedDataWriterBase)this.embeddedWriter).getMetricContext() :
        super.getMetricContext();
  }

  @Override
  public void write(D record)
      throws IOException {
    if(this.isEmbeddedInstrumented) {
      writeImpl(record);
    } else {
      super.write(record);
    }
  }

  @Override
  public void writeImpl(D record)
      throws IOException {
    this.embeddedWriter.write(record);
  }

  @Override
  public void commit()
      throws IOException {
    this.embeddedWriter.commit();
  }

  @Override
  public void cleanup()
      throws IOException {
    this.embeddedWriter.cleanup();
  }

  @Override
  public long recordsWritten() {
    return this.embeddedWriter.recordsWritten();
  }

  @Override
  public long bytesWritten()
      throws IOException {
    return this.embeddedWriter.bytesWritten();
  }

  @Override
  public State getFinalState() {
    if(this.embeddedWriter instanceof FinalState) {
      return ((FinalState) this.embeddedWriter).getFinalState();
    } else {
      return super.getFinalState();
    }
  }

  @Override
  public Object getDecoratedObject() {
    return this.embeddedWriter;
  }
}
