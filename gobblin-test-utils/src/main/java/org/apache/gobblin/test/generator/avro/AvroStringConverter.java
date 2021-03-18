package org.apache.gobblin.test.generator.avro;

import org.apache.avro.util.Utf8;

import org.apache.gobblin.test.InMemoryFormat;
import org.apache.gobblin.test.generator.ValueConverter;
import org.apache.gobblin.test.generator.ValueConvertingFunction;
import org.apache.gobblin.test.type.Type;


@ValueConverter(inMemoryType= InMemoryFormat.AVRO_GENERIC, logicalTypes= Type.String)
public class AvroStringConverter implements ValueConvertingFunction<String, Utf8> {
  @Override
  public Utf8 apply(String string) { return new Utf8(string); }
}
