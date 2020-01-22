package org.apache.gobblin.test.generator.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;

import org.apache.gobblin.test.InMemoryFormat;
import org.apache.gobblin.test.generator.ValueConverter;
import org.apache.gobblin.test.generator.ValueConvertingFunction;
import org.apache.gobblin.test.generator.config.FieldConfig;
import org.apache.gobblin.test.type.Type;


@ValueConverter(inMemoryType= InMemoryFormat.AVRO_GENERIC, logicalTypes= Type.Enum)
public class AvroEnumConverter implements ValueConvertingFunction<String, GenericEnumSymbol> {

  private Schema enumSchema;

  @Override
  public GenericEnumSymbol apply(String stringVal) {
    return new GenericData.EnumSymbol(this.enumSchema, stringVal);
  }

  @Override
  public ValueConvertingFunction withConfig(FieldConfig config) {
    this.enumSchema = Schema.createEnum(config.getName(), config.getDoc(), config.getName(), config.getSymbols());
    return this;
  }

}
