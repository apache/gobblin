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
}
