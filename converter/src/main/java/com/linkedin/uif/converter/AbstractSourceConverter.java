package com.linkedin.uif.converter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public abstract class AbstractSourceConverter<I> implements Convertable<Schema, I, GenericRecord>
{
  
  @Override
  public Schema convertSchema(Schema schema)
  {
    return schema;
  }

  @Override
  public abstract GenericRecord convertRecord(Schema schema, I inputRecord);

}
