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

package org.apache.gobblin.instrumented.writer;

import java.io.IOException;

import com.google.common.base.Optional;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.dataset.Descriptor;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.records.ControlMessageHandler;
import org.apache.gobblin.stream.RecordEnvelope;
import org.apache.gobblin.util.Decorator;
import org.apache.gobblin.util.DecoratorUtils;
import org.apache.gobblin.util.FinalState;
import org.apache.gobblin.writer.DataWriter;
import org.apache.gobblin.writer.WatermarkAwareWriter;


/**
 * Decorator that automatically instruments {@link org.apache.gobblin.writer.DataWriter}. Handles already instrumented
 * {@link org.apache.gobblin.instrumented.writer.InstrumentedDataWriter} appropriately to avoid double metric reporting.
 */
public class InstrumentedDataWriterDecorator<D> extends InstrumentedDataWriterBase<D> implements Decorator, WatermarkAwareWriter<D> {

  private DataWriter<D> embeddedWriter;
  private boolean isEmbeddedInstrumented;
  private Optional<WatermarkAwareWriter> watermarkAwareWriter;

  public InstrumentedDataWriterDecorator(DataWriter<D> writer, State state) {
    super(state, Optional.<Class<?>> of(DecoratorUtils.resolveUnderlyingObject(writer).getClass()));
    this.embeddedWriter = this.closer.register(writer);
    this.isEmbeddedInstrumented = Instrumented.isLineageInstrumented(writer);
    Object underlying = DecoratorUtils.resolveUnderlyingObject(embeddedWriter);
    if (underlying instanceof WatermarkAwareWriter) {
      this.watermarkAwareWriter = Optional.of((WatermarkAwareWriter) underlying);
    } else {
      this.watermarkAwareWriter = Optional.absent();
    }
  }

  @Override
  public MetricContext getMetricContext() {
    return this.isEmbeddedInstrumented ? ((InstrumentedDataWriterBase<D>) this.embeddedWriter).getMetricContext()
        : super.getMetricContext();
  }

  @Override
  public final void write(D record) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeEnvelope(RecordEnvelope<D> record) throws IOException {
    if (this.isEmbeddedInstrumented) {
      this.embeddedWriter.writeEnvelope(record);
    } else {

      if (!isInstrumentationEnabled()) {
        this.embeddedWriter.writeEnvelope(record);
        return;
      }

      try {
        long startTimeNanos = System.nanoTime();
        beforeWrite(record.getRecord());
        this.embeddedWriter.writeEnvelope(record);
        onSuccessfulWrite(startTimeNanos);
      } catch (IOException exception) {
        onException(exception);
        throw exception;
      }
    }
  }

  @Override
  public void writeImpl(D record) throws IOException {
    this.embeddedWriter.write(record);
  }

  @Override
  public void commit() throws IOException {
    this.embeddedWriter.commit();
    super.commit();
  }

  @Override
  public void cleanup() throws IOException {
    this.embeddedWriter.cleanup();
  }

  @Override
  public long recordsWritten() {
    return this.embeddedWriter.recordsWritten();
  }

  @Override
  public long bytesWritten() throws IOException {
    return this.embeddedWriter.bytesWritten();
  }

  @Override
  public State getFinalState() {
    if (this.embeddedWriter instanceof FinalState) {
      return ((FinalState) this.embeddedWriter).getFinalState();
    }
    return super.getFinalState();
  }

  @Override
  public Descriptor getDataDescriptor() {
    return this.embeddedWriter.getDataDescriptor();
  }

  @Override
  public Object getDecoratedObject() {
    return this.embeddedWriter;
  }

  @Override
  public boolean isWatermarkCapable() {
    return watermarkAwareWriter.isPresent() && watermarkAwareWriter.get().isWatermarkCapable();
  }

  @Override
  public ControlMessageHandler getMessageHandler() {
    return this.embeddedWriter.getMessageHandler();
  }

  @Override
  public void flush() throws IOException {
    this.embeddedWriter.flush();
  }
}
