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

package gobblin.converter;

import java.io.Closeable;
import java.io.IOException;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.initializer.ConverterInitializer;
import gobblin.converter.initializer.NoopConverterInitializer;
import gobblin.stream.ControlMessage;
import gobblin.records.ControlMessageHandler;
import gobblin.records.RecordStreamProcessor;
import gobblin.records.RecordStreamWithMetadata;
import gobblin.stream.RecordEnvelope;
import gobblin.source.workunit.WorkUnitStream;
import gobblin.stream.StreamEntity;
import gobblin.util.FinalState;

import io.reactivex.Flowable;


/**
 * An interface for classes that implement data transformations, e.g., data type
 * conversions, schema projections, data manipulations, data filtering, etc.
 *
 * <p>
 *   This interface is responsible for converting both schema and data records. Classes
 *   implementing this interface are composible and can be chained together to achieve
 *   more complex data transformations.
 * </p>
 *
 * @author kgoodhop
 *
 * @param <SI> input schema type
 * @param <SO> output schema type
 * @param <DI> input data type
 * @param <DO> output data type
 */
public abstract class Converter<SI, SO, DI, DO> implements Closeable, FinalState, RecordStreamProcessor<SI, SO, DI, DO> {
  /**
   * Initialize this {@link Converter}.
   *
   * @param workUnit a {@link WorkUnitState} object carrying configuration properties
   * @return an initialized {@link Converter} instance
   */
  public Converter<SI, SO, DI, DO> init(WorkUnitState workUnit) {
    return this;
  }

  @Override
  public void close() throws IOException {
  }

  /**
   * Convert an input schema to an output schema.
   *
   * <p>
   *   Schema conversion is limited to have a 1-to-1 mapping between the input and output schema.
   * </p>
   *
   * @param inputSchema input schema to be converted
   * @param workUnit a {@link WorkUnitState} object carrying configuration properties
   * @return converted output schema
   * @throws SchemaConversionException if it fails to convert the input schema
   */
  public abstract SO convertSchema(SI inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException;

  /**
   * Convert an input data record to an {@link java.lang.Iterable} of output records
   * conforming to the output schema of {@link Converter#convertSchema}.
   *
   * <p>
   *   Record conversion can have a 1-to-0 mapping, 1-to-1 mapping, or 1-to-many mapping between the
   *   input and output records as long as all the output records conforms to the same converted schema.
   * </p>
   *
   * <p>Converter for data record, both record type conversion, and record manipulation conversion.</p>
   * @param outputSchema output schema converted using the {@link Converter#convertSchema} method
   * @param inputRecord input data record to be converted
   * @param workUnit a {@link WorkUnitState} object carrying configuration properties
   * @return converted data record
   * @throws DataConversionException if it fails to convert the input data record
   */
  public abstract Iterable<DO> convertRecord(SO outputSchema, DI inputRecord, WorkUnitState workUnit)
      throws DataConversionException;

  /**
   * Get final state for this object. By default this returns an empty {@link gobblin.configuration.State}, but
   * concrete subclasses can add information that will be added to the task state.
   * @return Empty {@link gobblin.configuration.State}.
   */
  @Override
  public State getFinalState() {
    return new State();
  }

  /**
   * Apply conversions to the input {@link RecordStreamWithMetadata}.
   */
  @Override
  public RecordStreamWithMetadata<DO, SO> processStream(RecordStreamWithMetadata<DI, SI> inputStream,
      WorkUnitState workUnitState) throws SchemaConversionException {
    init(workUnitState);
    SO outputSchema = convertSchema(inputStream.getSchema(), workUnitState);
    Flowable<StreamEntity<DO>> outputStream =
        inputStream.getRecordStream()
            .flatMap(in -> {
              if (in instanceof ControlMessage) {
                getMessageHandler().handleMessage((ControlMessage) in);
                return Flowable.just(((ControlMessage<DO>) in));
              } else if (in instanceof RecordEnvelope) {
                RecordEnvelope<DI> recordEnvelope = (RecordEnvelope<DI>) in;
                return Flowable.fromIterable(convertRecord(outputSchema, recordEnvelope.getRecord(), workUnitState)).
                    map(recordEnvelope::withRecord);
              } else {
                throw new UnsupportedOperationException();
              }
            }, 1);
    outputStream = outputStream.doOnComplete(this::close);
    return inputStream.withRecordStream(outputStream, outputSchema);
  }

  /**
   * @return {@link ControlMessageHandler} to call for each {@link ControlMessage} received.
   */
  public ControlMessageHandler getMessageHandler() {
    return ControlMessageHandler.NOOP;
  }

  public ConverterInitializer getInitializer(State state, WorkUnitStream workUnits, int branches, int branchId) {
    return NoopConverterInitializer.INSTANCE;
  }
}
