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
public interface Converter<SI, SO, DI, DO>
{
  
  /**
   * <p>Converter for schema, both schema type conversion, and schema manipulation conversion</p>
   * @param inputSchema
   * @param workUnit
   * @return
   * @throws SchemaConversionException
   */
  public SO convertSchema(SI inputSchema, WorkUnitState workUnit) throws SchemaConversionException;

  /**
   * <p>Converter for data record, both record type conversion, and record manipulation conversion</p>
   * @param outputSchema
   * @param inputRecord
   * @param workUnit
   * @return
   * @throws DataConversionException
   */
  public DO convertRecord(SO outputSchema, DI inputRecord, WorkUnitState workUnit) throws DataConversionException;

}
