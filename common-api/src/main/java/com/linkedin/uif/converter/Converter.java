/* (c) 2014 LinkedIn Corp. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.uif.converter;

import com.linkedin.uif.configuration.WorkUnitState;


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
public abstract class Converter<SI, SO, DI, DO> {
  /**
   * Initialize this {@link Converter}.
   *
   * @param workUnit a {@link WorkUnitState} object carrying configuration properties
   * @return an initialized {@link Converter} instance
   */
  public Converter<SI, SO, DI, DO> init(WorkUnitState workUnit) {
    return this;
  }

  /**
   * Convert an input schema.
   *
   * @param inputSchema input schema to be converted
   * @param workUnit a {@link WorkUnitState} object carrying configuration properties
   * @return converted output schema
   * @throws SchemaConversionException if it fails to convert the input schema
   */
  public abstract SO convertSchema(SI inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException;

  /**
   * Convert an input data record.
   * <p>Converter for data record, both record type conversion, and record manipulation conversion</p>
   * @param outputSchema output schema converted using the {@link Converter#convertSchema} method
   * @param inputRecord input data record to be converted
   * @param workUnit a {@link WorkUnitState} object carrying configuration properties
   * @return converted data record
   * @throws DataConversionException if it fails to convert the input data record
   */
  public abstract DO convertRecord(SO outputSchema, DI inputRecord, WorkUnitState workUnit)
      throws DataConversionException;
}
