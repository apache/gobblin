package com.linkedin.uif.converter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.linkedin.uif.configuration.SourceContext;

public abstract class ToAvroConverterBase<SI,D> implements Converter<SI, Schema, D, GenericRecord>
{
  
  @Override
  public abstract Schema convertSchema(SI schema, SourceContext context);

  @Override
  public abstract GenericRecord convertRecord(Schema outputSchema, D inputRecord, SourceContext context);

}
