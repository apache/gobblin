package com.linkedin.uif.converter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.linkedin.uif.configuration.WorkUnitState;

public abstract class ToAvroConverterBase<SI, DI> implements Converter<SI, Schema, DI, GenericRecord>
{

  @Override
  public abstract Schema convertSchema(SI schema, WorkUnitState workUnit) throws SchemaConversionException;

  @Override
  public abstract GenericRecord convertRecord(Schema outputSchema, DI inputRecord, WorkUnitState workUnit) throws DataConversionException;

}
