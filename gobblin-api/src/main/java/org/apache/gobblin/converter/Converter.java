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

package org.apache.gobblin.converter;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.initializer.ConverterInitializer;
import org.apache.gobblin.converter.initializer.NoopConverterInitializer;
import org.apache.gobblin.metadata.GlobalMetadata;
import org.apache.gobblin.stream.ControlMessage;
import org.apache.gobblin.records.ControlMessageHandler;
import org.apache.gobblin.records.RecordStreamProcessor;
import org.apache.gobblin.records.RecordStreamWithMetadata;
import org.apache.gobblin.stream.MetadataUpdateControlMessage;
import org.apache.gobblin.stream.RecordEnvelope;
import org.apache.gobblin.source.workunit.WorkUnitStream;
import org.apache.gobblin.stream.StreamEntity;
import org.apache.gobblin.util.FinalState;

import com.google.common.base.Optional;

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
  // Metadata containing the output schema. This may be changed when a MetadataUpdateControlMessage is received.
  private GlobalMetadata<SO> outputGlobalMetadata;

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
   *   When try to convert avro schema, please call {@link AvroUtils.addSchemaCreationTime to preserve schame creationTime}
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
   * Converts a {@link RecordEnvelope}. This method can be overridden by implementations that need to manipulate the
   * {@link RecordEnvelope}, such as to set watermarks or metadata.
   * @param outputSchema output schema converted using the {@link Converter#convertSchema} method
   * @param inputRecordEnvelope input record envelope with data record to be converted
   * @param workUnitState a {@link WorkUnitState} object carrying configuration properties
   * @return a {@link Flowable} emitting the converted {@link RecordEnvelope}s
   * @throws DataConversionException
   */
  protected Flowable<RecordEnvelope<DO>> convertRecordEnvelope(SO outputSchema, RecordEnvelope<DI> inputRecordEnvelope,
      WorkUnitState workUnitState) throws DataConversionException {
    Iterator<DO> convertedIterable = convertRecord(outputSchema,
        inputRecordEnvelope.getRecord(), workUnitState).iterator();

    if (!convertedIterable.hasNext()) {
      inputRecordEnvelope.ack();
      // if the iterable is empty, ack the record, return an empty flowable
      return Flowable.empty();
    }

    DO firstRecord = convertedIterable.next();
    if (!convertedIterable.hasNext()) {
      // if the iterable has only one element, use RecordEnvelope.withRecord, which is more efficient
      return Flowable.just(inputRecordEnvelope.withRecord(firstRecord));
    } else {
      // if the iterable has multiple records, use a ForkRecordBuilder
      RecordEnvelope<DI>.ForkRecordBuilder<DO> forkRecordBuilder = inputRecordEnvelope.forkRecordBuilder();
      return Flowable.just(firstRecord).concatWith(Flowable.fromIterable(() -> convertedIterable))
          .map(forkRecordBuilder::childRecord).doOnComplete(forkRecordBuilder::close);
    }
  }

  /**
   * Get final state for this object. By default this returns an empty {@link org.apache.gobblin.configuration.State}, but
   * concrete subclasses can add information that will be added to the task state.
   * @return Empty {@link org.apache.gobblin.configuration.State}.
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
    this.outputGlobalMetadata = GlobalMetadata.<SI, SO>builderWithInput(inputStream.getGlobalMetadata(),
        Optional.fromNullable(convertSchema(inputStream.getGlobalMetadata().getSchema(), workUnitState))).build();
    Flowable<StreamEntity<DO>> outputStream =
        inputStream.getRecordStream()
            .flatMap(in -> {
              if (in instanceof ControlMessage) {
                ControlMessage out = (ControlMessage) in;

                getMessageHandler().handleMessage((ControlMessage) in);

                // update the output schema with the new input schema from the MetadataUpdateControlMessage
                if (in instanceof MetadataUpdateControlMessage) {
                  this.outputGlobalMetadata = GlobalMetadata.<SI, SO>builderWithInput(
                      ((MetadataUpdateControlMessage) in).getGlobalMetadata(),
                      Optional.fromNullable(convertSchema((SI)((MetadataUpdateControlMessage) in).getGlobalMetadata()
                          .getSchema(), workUnitState))).build();
                  out = new MetadataUpdateControlMessage<SO, DO>(this.outputGlobalMetadata);
                }

                return Flowable.just(((ControlMessage<DO>) out));
              } else if (in instanceof RecordEnvelope) {
                RecordEnvelope<DI> recordEnvelope = (RecordEnvelope<DI>) in;
                return convertRecordEnvelope(this.outputGlobalMetadata.getSchema(), recordEnvelope, workUnitState);
              } else {
                throw new UnsupportedOperationException();
              }
            }, 1);
    outputStream = outputStream.doOnComplete(this::close);
    return inputStream.withRecordStream(outputStream, this.outputGlobalMetadata);
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
