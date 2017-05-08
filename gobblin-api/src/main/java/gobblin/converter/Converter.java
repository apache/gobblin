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
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.base.Throwables;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Message;
import gobblin.source.extractor.RecordEnvelope;
import gobblin.util.FinalState;


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
public abstract class Converter<SI, SO, DI, DO> implements Closeable, FinalState {
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
  public Iterable<DO> convertRecord(SO outputSchema, DI inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    throw new UnsupportedOperationException();
  }

  /**
   * Modify the output {@link RecordEnvelope} for each output record. This can be used, for example, to add callbacks.
   * @param recordEnvelope
   */
  public void modifyOutputEnvelope(RecordEnvelope<DO> recordEnvelope) {}

  /**
   * Convert the input record stream into the output record stream conforming to the output schema of
   * {@link Converter#convertSchema(Object, WorkUnitState)}.
   *
   * <p>
   *   Record conversion can do arbitrary operation (e.g. 1-to-1, 1-to-many, 1-to-0, windowed joins, asynchronous conversion,
   *   etc.) following these rules:
   *   * For every input {@link RecordEnvelope}, exactly one of two must happen: the record is acked, or a child record
   *     created with {@link RecordEnvelope#withRecord(Object)} or {@link RecordEnvelope#forkedRecordBuilder()} must be
   *     in the output stream at some point.
   *   * Acking a {@link RecordEnvelope} means the record has been fully processed, meaning checkpoints may be committed,
   *     and a pipeline that allows record retries may not retry the record.
   * </p>
   *
   * <p>
   *   For synchronous conversion, it is recommended to use the method {@link #convertRecord(Object, Object, WorkUnitState)}
   *   and/or {@link #modifyOutputEnvelope(RecordEnvelope)}.
   * </p>
   */
  public Stream<RecordEnvelope<DO>> convertRecordStream(SO outputSchema, Stream<RecordEnvelope<DI>> inputStream, WorkUnitState workUnit) {
    return inputStream.flatMap(envelope -> {
      try {
        Iterator<DO> outputIt = convertRecord(outputSchema, envelope.getRecord(), workUnit).iterator();

        if (!outputIt.hasNext()) {
          // record filtered, ack and continue
          envelope.ack();
          return Stream.empty();
        }

        Stream<RecordEnvelope<DO>> outputRecordStream;
        DO firstEl = outputIt.next();
        if (outputIt.hasNext()) {
          // output iterator has multiple elements, need to fork the envelope
          RecordEnvelope.ForkedRecordBuilder forkedRecordBuilder = envelope.forkedRecordBuilder();
          outputRecordStream = Stream.concat(Stream.of(firstEl), StreamSupport.stream(Spliterators.spliteratorUnknownSize(outputIt,
              Spliterator.IMMUTABLE), false)).map(forkedRecordBuilder::forkWithRecord);
          outputRecordStream = Stream.concat(outputRecordStream, Stream.of(new Message<DO>(Message.MessageType.END_OF_STREAM)))
              .filter(env -> {
                if (env instanceof Message) {
                  if (((Message) env).getMessageType() == Message.MessageType.END_OF_STREAM) {
                    forkedRecordBuilder.close();
                  }
                  return false;
                }
                return true;
              });
        } else {
          // output has single output element
          outputRecordStream = Stream.of(envelope.withRecord(firstEl));
        }

        return outputRecordStream.map(env -> {
              modifyOutputEnvelope(env);
              return env;
            });
      } catch (DataConversionException dce) {
        Throwables.propagate(dce);
        return null;
      }
    });
  }

  /**
   * Get final state for this object. By default this returns an empty {@link gobblin.configuration.State}, but
   * concrete subclasses can add information that will be added to the task state.
   * @return Empty {@link gobblin.configuration.State}.
   */
  @Override
  public State getFinalState() {
    return new State();
  }
}
