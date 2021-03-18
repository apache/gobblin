package org.apache.gobblin.test.generator.json;

import java.util.function.Function;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

import org.apache.gobblin.test.InMemoryFormat;
import org.apache.gobblin.test.generator.ValueConverter;
import org.apache.gobblin.test.generator.ValueConvertingFunction;
import org.apache.gobblin.test.generator.config.FieldConfig;
import org.apache.gobblin.test.type.Type;


@ValueConverter(
    inMemoryType= InMemoryFormat.JSON,
    logicalTypes= {Type.Integer, Type.Long, Type.Boolean, Type.String, Type.Enum})
public class JsonPrimitiveConverter implements ValueConvertingFunction<Object, JsonElement> {

  private Function<Object, JsonPrimitive> typedConverter;

  public ValueConvertingFunction withConfig(FieldConfig config) {
    switch (config.getType()) {
      case Integer: { typedConverter = x -> new JsonPrimitive((Integer) x); break; }
      case Long : { typedConverter = x -> new JsonPrimitive((Long) x); break; }
      case String:
        case Enum: { typedConverter = x -> new JsonPrimitive((String) x); break; }
      case Boolean: { typedConverter = x -> new JsonPrimitive((Boolean) x); break; }
      default: throw new RuntimeException("Not supported");
    }
    return this;
  }

  @Override
  public JsonElement apply(Object input) {
    return typedConverter.apply(input);
  }
}
