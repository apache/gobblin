package com.linkedin.uif.converter;

import com.linkedin.uif.source.workunit.WorkUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.linkedin.uif.configuration.SourceState;

public abstract class ToAvroConverterBase<SI, DI> implements Converter<SI, Schema, DI, GenericRecord>
{

  @Override
  public abstract Schema convertSchema(SI schema, WorkUnit workUnit);

  @Override
  public abstract GenericRecord convertRecord(Schema outputSchema, DI inputRecord, WorkUnit workUnit);

}
