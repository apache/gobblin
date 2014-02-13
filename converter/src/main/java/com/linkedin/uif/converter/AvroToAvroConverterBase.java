package com.linkedin.uif.converter;

import com.linkedin.uif.source.workunit.WorkUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.linkedin.uif.configuration.SourceState;

public abstract class AvroToAvroConverterBase implements Converter<Schema, Schema, GenericRecord, GenericRecord>
{

  @Override
  public abstract Schema convertSchema(Schema inputSchema, WorkUnit workUnit);

  @Override
  public abstract GenericRecord convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnit workUnit);

}
