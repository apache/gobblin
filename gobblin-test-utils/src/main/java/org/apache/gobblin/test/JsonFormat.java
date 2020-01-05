package org.apache.gobblin.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import org.apache.gobblin.test.generator.Field;
import org.apache.gobblin.test.generator.ValueGenerator;
import org.apache.gobblin.test.type.Type;


public class JsonFormat implements Format<JsonArray, JsonObject>, ValueGenerator<JsonObject> {


  private static Type[] supportedTypes = {Type.String, Type.Integer, Type.Boolean, Type.Long};

  private static Map<Type, String> typeToJsonTypeName = new HashMap<>();

  private interface JsonConvertor<T> {
    JsonElement convert(T input);
  }

  static {
    typeToJsonTypeName.put(Type.String, "string");
    typeToJsonTypeName.put(Type.Integer, "int");
    typeToJsonTypeName.put(Type.Boolean, "boolean");
    typeToJsonTypeName.put(Type.Long, "long");
  }

  private static JsonObject getJsonFieldSchema(Field field) {
    JsonObject fieldNode = new JsonObject();
    fieldNode.addProperty("columnName", field.getName());
    fieldNode.add("dataType", getDataTypeNode(field.getType()));
    switch (field.getOptionality()) {
      case OPTIONAL: {
        fieldNode.addProperty("isNullable", true);
        break;
      }
      case REQUIRED: {
        fieldNode.addProperty("isNullable", false);
        break;
      }
    }
    fieldNode.addProperty("comment", "foobar");
    return fieldNode;
  }

  private static JsonObject getDataTypeNode(Type dataType) {
    JsonObject dataTypeNode = new JsonObject();
    Preconditions.checkArgument(typeToJsonTypeName.containsKey(dataType));
    dataTypeNode.addProperty("type", typeToJsonTypeName.get(dataType));
    return dataTypeNode;
  }


  private List<Field> schema;
  private ArrayList<JsonConvertor> typeConvertors;

  @Override
  public JsonArray generateRandomSchema(List<Field> fields) {
    List<Field> finalFields = FormatUtils.generateRandomFields(supportedTypes);
    finalFields.addAll(fields);
    this.schema = finalFields;
    this.typeConvertors = new ArrayList<>(finalFields.size());
    for (Field f : schema) {
      JsonConvertor jsonConvertor = null;
      switch (f.getType()) {
        case Integer: {
          jsonConvertor = (JsonConvertor<Integer>) input -> new JsonPrimitive(input);
          break;
        }
        case Long: {
          jsonConvertor = (JsonConvertor<Long>) input -> new JsonPrimitive(input);
          break;
        }
        case String: {
          jsonConvertor = (JsonConvertor<String>) input -> new JsonPrimitive(input);
          break;
        }
        case Boolean: {
          jsonConvertor = (JsonConvertor<Boolean>) input -> new JsonPrimitive(input);
          break;
        }
        default:
          throw new RuntimeException("Not implemented Json generation for type " + f.getType());
      }
      this.typeConvertors.add(jsonConvertor);
    }

    JsonArray jsonSchema = new JsonArray();
    for (Field f : finalFields) {
      jsonSchema.add(getJsonFieldSchema(f));
    }
    return jsonSchema;
  }

  @Override
  public JsonObject generateRandomRecord() {
    JsonObject result = new JsonObject();
    int idx = 0;
    for (Field f : this.schema) {
      result.add(f.getName(), this.typeConvertors.get(idx).convert(
          f.getValueGenerator().get()
      ));
      idx++;
    }
    return result;
  }

  @Override
  public Type getLogicalType() {
    return Type.Struct;
  }

  @Override
  public JsonObject get() {
    return generateRandomRecord();
  }
}
