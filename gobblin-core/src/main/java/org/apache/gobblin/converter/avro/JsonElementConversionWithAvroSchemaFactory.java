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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.gobblin.configuration.WorkUnitState;

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

  public static JsonElementConverter getConvertor(String fieldName, String fieldType, Schema schemaNode,
      WorkUnitState state, boolean nullable)
      throws UnsupportedDateTypeException {

    Type type;
    try {
      type = Type.valueOf(fieldType.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new UnsupportedDateTypeException(fieldType + " is unsupported");
    }

    switch (type) {
      case ARRAY:
        return new JsonElementConversionWithAvroSchemaFactory.ArrayConverter(fieldName, nullable, type.toString(),
            schemaNode, state);

      case MAP:
        return new JsonElementConversionWithAvroSchemaFactory.MapConverter(fieldName, nullable, type.toString(),
            schemaNode, state);

      case ENUM:
        return new JsonElementConversionWithAvroSchemaFactory.EnumConverter(fieldName, nullable, type.toString(),
            schemaNode);

      default:
        return JsonElementConversionFactory.getConvertor(fieldName, fieldType, new JsonObject(), state, nullable);
    }
  }

  public static class ArrayConverter extends ComplexConverter {

    public ArrayConverter(String fieldName, boolean nullable, String sourceType, Schema schemaNode, WorkUnitState state)
        throws UnsupportedDateTypeException {
      super(fieldName, nullable, sourceType);
      super.setElementConverter(
          getConvertor(fieldName, schemaNode.getElementType().getType().getName(), schemaNode.getElementType(), state,
              isNullable()));
    }

    @Override
    Object convertField(JsonElement value) {
      List<Object> list = new ArrayList<>();

      for (JsonElement elem : (JsonArray) value) {
        list.add(getElementConverter().convertField(elem));
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

    public MapConverter(String fieldName, boolean nullable, String sourceType, Schema schemaNode, WorkUnitState state)
        throws UnsupportedDateTypeException {
      super(fieldName, nullable, sourceType);
      super.setElementConverter(
          getConvertor(fieldName, schemaNode.getValueType().getType().getName(), schemaNode.getValueType(), state,
              isNullable()));
    }

    @Override
    Object convertField(JsonElement value) {
      Map<String, Object> map = new HashMap<>();

      for (Map.Entry<String, JsonElement> entry : ((JsonObject) value).entrySet()) {
        map.put(entry.getKey(), getElementConverter().convertField(entry.getValue()));
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

      this.enumName = schemaNode.getType().getName();
    }

    @Override
    Object convertField(JsonElement value) {
      return new GenericData.EnumSymbol(this.schema, value.getAsString());
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
}
