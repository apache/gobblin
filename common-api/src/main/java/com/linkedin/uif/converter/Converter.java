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
 * <p> Operator for schemas and data.  Subclasses can implement a Converter for data type conversion, schema projection, 
 * data manipulation, data filtering, etc...  Converters can be chained together. For examples, see classes implementing this interface.
 * </p>
 * @author kgoodhop
 *
 * @param <SI> input schema type
 * @param <SO> output schema type
 * @param <DI> input data type
 * @param <DO> output data type
 */
public abstract class Converter<SI, SO, DI, DO>
{
  /**
   * <p>initializer for converter</p>
   * @param workUnit
   * @return void
   */
  public Converter<SI, SO, DI, DO> init(WorkUnitState workUnit)
  {
    return this;
  }
  
  /**
   * <p>Converter for schema, both schema type conversion, and schema manipulation conversion</p>
   * @param inputSchema
   * @param workUnit
   * @return
   * @throws SchemaConversionException
   */
  public abstract SO convertSchema(SI inputSchema, WorkUnitState workUnit) throws SchemaConversionException;

  /**
   * <p>Converter for data record, both record type conversion, and record manipulation conversion</p>
   * @param outputSchema
   * @param inputRecord
   * @param workUnit
   * @return
   * @throws DataConversionException
   */
  public abstract DO convertRecord(SO outputSchema, DI inputRecord, WorkUnitState workUnit) throws DataConversionException;

}
