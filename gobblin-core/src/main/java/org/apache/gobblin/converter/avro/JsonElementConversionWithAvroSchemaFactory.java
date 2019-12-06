/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.converter.avro;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;


/**
 * Creates a converter for Json types to Avro types. Overrides {@link ArrayConverter}, {@link MapConverter},
 * and {@link EnumConverter} from {@link JsonElementConversionFactory} to use an Avro schema instead of Json schema for
 * determining type
 */
public class JsonElementConversionWithAvroSchemaFactory extends JsonElementConversionFactory {

  /**
   * Use to create a converter for a single field from a schema.
   */

  public static JsonElementConverter getConverter(String fieldName, String fieldType, Schema schemaNode,
      WorkUnitState state, boolean nullable, List<String> ignoreFields) throws UnsupportedDateTypeException {

    Type type;
    try {
      type = Type.valueOf(fieldType.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new UnsupportedDateTypeException(fieldType + " is unsupported");
    }

    switch (type) {
      case ARRAY:
        return new JsonElementConversionWithAvroSchemaFactory.ArrayConverter(fieldName, nullable, type.toString(),
            schemaNode, state, ignoreFields);

      case MAP:
        return new JsonElementConversionWithAvroSchemaFactory.MapConverter(fieldName, nullable, type.toString(),
            schemaNode, state, ignoreFields);

      case ENUM:
        return new JsonElementConversionWithAvroSchemaFactory.EnumConverter(fieldName, nullable, type.toString(),
            schemaNode);

      case RECORD:
        return new JsonElementConversionWithAvroSchemaFactory.RecordConverter(fieldName, nullable, type.toString(),
            schemaNode, state, ignoreFields);

      case UNION:
        return new JsonElementConversionWithAvroSchemaFactory.UnionConverter(fieldName, nullable, type.toString(),
            schemaNode, state, ignoreFields);

      default:
        return JsonElementConversionFactory.getConvertor(fieldName, fieldType, new JsonObject(), state, nullable);
    }
  }

  public static class ArrayConverter extends ComplexConverter {

    public ArrayConverter(String fieldName, boolean nullable, String sourceType, Schema schemaNode, WorkUnitState state,
        List<String> ignoreFields) throws UnsupportedDateTypeException {
      super(fieldName, nullable, sourceType);
      super.setElementConverter(
          getConverter(fieldName, schemaNode.getElementType().getType().getName(), schemaNode.getElementType(), state,
              isNullable(), ignoreFields));
    }

    @Override
    Object convertField(JsonElement value) {
      List<Object> list = new ArrayList<>();

      for (JsonElement elem : (JsonArray) value) {
        list.add(getElementConverter().convert(elem));
      }

      return new GenericData.Array<>(schema(), list);
    }

    @Override
    public Schema.Type getTargetType() {
      return Schema.Type.ARRAY;
    }

    @Override
    public Schema schema() {
      Schema schema = Schema.createArray(getElementConverter().schema());
      schema.addProp("source.type", "array");
      return schema;
    }
  }

  public static class MapConverter extends ComplexConverter {

    public MapConverter(String fieldName, boolean nullable, String sourceType, Schema schemaNode, WorkUnitState state,
        List<String> ignoreFields) throws UnsupportedDateTypeException {
      super(fieldName, nullable, sourceType);
      super.setElementConverter(
          getConverter(fieldName, schemaNode.getValueType().getType().getName(), schemaNode.getValueType(), state,
              isNullable(), ignoreFields));
    }

    @Override
    Object convertField(JsonElement value) {
      Map<String, Object> map = new HashMap<>();

      for (Map.Entry<String, JsonElement> entry : ((JsonObject) value).entrySet()) {
        map.put(entry.getKey(), getElementConverter().convert(entry.getValue()));
      }

      return map;
    }

    @Override
    public Schema.Type getTargetType() {
      return Schema.Type.MAP;
    }

    @Override
    public Schema schema() {
      Schema schema = Schema.createMap(getElementConverter().schema());
      schema.addProp("source.type", "map");
      return schema;
    }
  }

  public static class EnumConverter extends JsonElementConverter {
    String enumName;
    List<String> enumSet = new ArrayList<>();
    Schema schema;

    public EnumConverter(String fieldName, boolean nullable, String sourceType, Schema schemaNode) {
      super(fieldName, nullable, sourceType);

      this.enumSet.addAll(schemaNode.getEnumSymbols());

      this.enumName = schemaNode.getFullName();

      this.schema = schemaNode;
    }

    @Override
    Object convertField(JsonElement value) {
      String valueString = value.getAsString();
      Preconditions.checkArgument(this.enumSet.contains(valueString),
          "%s is not one of the valid symbols for the %s enum: %s", valueString, this.enumName, this.enumSet);
      return new GenericData.EnumSymbol(this.schema, valueString);
    }

    @Override
    public Schema.Type getTargetType() {
      return Schema.Type.ENUM;
    }

    @Override
    public Schema schema() {
      this.schema = Schema.createEnum(this.enumName, "", "", this.enumSet);
      this.schema.addProp("source.type", "enum");
      return this.schema;
    }
  }

  public static class RecordConverter extends ComplexConverter {

    List<String> ignoreFields;
    Schema schema;
    WorkUnitState state;

    public RecordConverter(String fieldName, boolean nullable, String sourceType, Schema schemaNode,
        WorkUnitState state, List<String> ignoreFields) {
      super(fieldName, nullable, sourceType);
      this.schema = schemaNode;
      this.state = state;
      this.ignoreFields = ignoreFields;
    }

    @Override
    Object convertField(JsonElement value) {
      try {
        return JsonRecordAvroSchemaToAvroConverter.convertNestedRecord(this.schema, value.getAsJsonObject(), this.state,
            this.ignoreFields);
      } catch (DataConversionException e) {
        throw new RuntimeException("Failed to convert nested record", e);
      }
    }

    @Override
    public Schema.Type getTargetType() {
      return Schema.Type.RECORD;
    }

    @Override
    public Schema schema() {
      return this.schema;
    }
  }

  /**
   * A converter to convert Union type to avro
   * Here it will try all the possible converters for one type, for example, to convert an int value, it will try all Number converters
   * until meet the first workable one.
   * So a known bug here is there's no guarantee on preserving precision from Json to Avro type as the exact type information is clear from JsonElement
   * We're doing this since there is no way to determine what exact type it is for a JsonElement
   */
  public static class UnionConverter extends ComplexConverter {
    private final List<Schema> schemas;
    private final List<JsonElementConverter> converters;
    private final Schema schemaNode;

    public UnionConverter(String fieldName, boolean nullable, String sourceType, Schema schemaNode,
        WorkUnitState state, List<String> ignoreFields) throws UnsupportedDateTypeException {
      super(fieldName, nullable, sourceType);
      this.schemas = schemaNode.getTypes();
      converters = new ArrayList<>();
      for(Schema schema: schemas) {
        converters.add(getConverter(fieldName, schema.getType().getName(), schemaNode, state, isNullable(), ignoreFields));
      }
      this.schemaNode = schemaNode;
    }

    @Override
    Object convertField(JsonElement value) {
       for(JsonElementConverter converter: converters)
       {
         try {
           switch (converter.getTargetType()) {
             case STRING: {
               if (value.isJsonPrimitive() && value.getAsJsonPrimitive().isString()) {
                 return converter.convert(value);
               }
               break;
             }
             case FIXED:
             case BYTES:
             case INT:
             case LONG:
             case FLOAT:
             case DOUBLE: {
               if (value.isJsonPrimitive() && value.getAsJsonPrimitive().isNumber()) {
                 return converter.convert(value);
               }
               break;
             }
             case BOOLEAN:{
               if (value.isJsonPrimitive() && value.getAsJsonPrimitive().isBoolean()) {
                 return converter.convert(value);
               }
               break;
             }
             case ARRAY:{
               if (value.isJsonArray()) {
                 return converter.convert(value);
               }
               break;
             }
             case MAP:
             case ENUM:
             case RECORD:{
               if (value.isJsonObject()) {
                 return converter.convert(value);
               }
               break;
             }
             case NULL:{
               if(value.isJsonNull()) {
                 return converter.convert(value);
               }
               break;
             }
             case UNION:
               return new UnsupportedDateTypeException("does not support union type in union");
             default:
               return converter.convert(value);
           }
         } catch (Exception e){}
       }
       throw new RuntimeException(String.format("Cannot convert %s to avro using schema %s", value.getAsString(), schemaNode.toString()));
    }

    @Override
    public Schema.Type getTargetType() {
      return schema().getType();
    }

    @Override
    public Schema schema() {
      if(schemas.size() == 2 && isNullable()) {
        if(schemas.get(0).getType() == Schema.Type.NULL) {
          return schemas.get(1);
        } else {
          return schemas.get(0);
        }
      }
      return Schema.createUnion(schemas);
    }

    @Override
    public boolean isNullable() {
      boolean isNullable = false;
      for(Schema schema: schemas) {
        if(schema.getType() == Schema.Type.NULL) {
          isNullable = true;
        }
      }
      return isNullable;
    }
  }
}