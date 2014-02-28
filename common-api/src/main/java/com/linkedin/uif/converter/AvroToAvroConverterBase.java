package com.linkedin.uif.converter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.linkedin.uif.configuration.WorkUnitState;

public abstract class AvroToAvroConverterBase implements Converter<Schema, Schema, GenericRecord, GenericRecord>
{

  @Override
  public abstract Schema convertSchema(Schema inputSchema, WorkUnitState workUnit)  throws SchemaConversionException;

  @Override
  public abstract GenericRecord convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnitState workUnit) throws DataConversionException;

}
