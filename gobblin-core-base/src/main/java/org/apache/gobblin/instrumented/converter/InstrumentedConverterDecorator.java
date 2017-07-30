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

package gobblin.instrumented.converter;

import java.io.IOException;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.MetricContext;
import gobblin.records.ControlMessageHandler;
import gobblin.records.RecordStreamWithMetadata;
import gobblin.util.Decorator;
import gobblin.util.DecoratorUtils;


/**
 * Decorator that automatically instruments {@link gobblin.converter.Converter}.
 * Handles already instrumented {@link gobblin.instrumented.converter.InstrumentedConverter}
 * appropriately to avoid double metric reporting.
 */
public class InstrumentedConverterDecorator<SI, SO, DI, DO> extends InstrumentedConverterBase<SI, SO, DI, DO>
    implements Decorator {

  private Converter<SI, SO, DI, DO> embeddedConverter;
  private final boolean isEmbeddedInstrumented;

  public InstrumentedConverterDecorator(Converter<SI, SO, DI, DO> converter) {
    this.embeddedConverter = converter;
    this.isEmbeddedInstrumented = Instrumented.isLineageInstrumented(converter);
  }

  @Override
  public Converter<SI, SO, DI, DO> init(WorkUnitState workUnit) {
    this.embeddedConverter = this.embeddedConverter.init(workUnit);
    return super.init(workUnit, DecoratorUtils.resolveUnderlyingObject(this).getClass());
  }

  @Override
  public MetricContext getMetricContext() {
    return this.isEmbeddedInstrumented
        ? ((InstrumentedConverterBase<SI, SO, DI, DO>) this.embeddedConverter).getMetricContext()
        : super.getMetricContext();
  }

  @Override
  public Iterable<DO> convertRecord(SO outputSchema, DI inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    return this.isEmbeddedInstrumented ? convertRecordImpl(outputSchema, inputRecord, workUnit)
        : super.convertRecord(outputSchema, inputRecord, workUnit);
  }

  @Override
  public Iterable<DO> convertRecordImpl(SO outputSchema, DI inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    return this.embeddedConverter.convertRecord(outputSchema, inputRecord, workUnit);
  }

  @Override
  public SO convertSchema(SI inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return this.embeddedConverter.convertSchema(inputSchema, workUnit);
  }

  @Override
  public void close() throws IOException {
    this.embeddedConverter.close();
  }

  @Override
  public State getFinalState() {
    return this.embeddedConverter.getFinalState();
  }

  @Override
  public Object getDecoratedObject() {
    return this.embeddedConverter;
  }

  /**
   * This workarounds the issue that {@link Converter#processStream(RecordStreamWithMetadata, WorkUnitState)} will invoke
   * {@link gobblin.converter.AsyncConverter1to1#convertRecord(Object, Object, WorkUnitState)} directly, which is an unsupported method.
   */
  @Override
  public RecordStreamWithMetadata<DO, SO> processStream(RecordStreamWithMetadata<DI, SI> inputStream,
      WorkUnitState workUnitState) throws SchemaConversionException {
    return this.embeddedConverter.processStream(inputStream, workUnitState);
  }

  @Override
  public ControlMessageHandler getMessageHandler() {
    return this.embeddedConverter.getMessageHandler();
  }
}
