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
package org.apache.gobblin.converter.parquet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.avro.UnsupportedDateTypeException;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import parquet.example.data.simple.BinaryValue;
import parquet.example.data.simple.BooleanValue;
import parquet.example.data.simple.DoubleValue;
import parquet.example.data.simple.FloatValue;
import parquet.example.data.simple.IntegerValue;
import parquet.example.data.simple.LongValue;
import parquet.example.data.simple.Primitive;
import parquet.io.api.Binary;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

import static org.apache.gobblin.converter.parquet.JsonElementConversionFactory.Type.*;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static parquet.schema.Type.Repetition.OPTIONAL;
import static parquet.schema.Type.Repetition.REQUIRED;


/**
 * <p>
 * Creates a JsonElement to Parquet converter for all supported data types.
 * </p>
 *
 * @author tilakpatidar
 *
 */
public class JsonElementConversionFactory {
  public enum Type {
    STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, ARRAY, ENUM, RECORD
  }

  private static HashMap<Type, PrimitiveTypeName> typeMap = new HashMap<>();

  static {
    typeMap.put(INT, INT32);
    typeMap.put(LONG, INT64);
    typeMap.put(FLOAT, PrimitiveTypeName.FLOAT);
    typeMap.put(DOUBLE, PrimitiveTypeName.DOUBLE);
    typeMap.put(BOOLEAN, PrimitiveTypeName.BOOLEAN);
    typeMap.put(STRING, BINARY);
  }

  /**
   * Use to create a converter for a single field from a schema.
   *
   * @param fieldName
   * @param fieldType
   * @param nullable
   * @param schemaNode
   * @param state
   * @return
   * @throws UnsupportedDateTypeException
   */
  public static JsonElementConverter getConvertor(String fieldName, String fieldType, JsonObject schemaNode,
      WorkUnitState state, boolean nullable)
      throws UnsupportedDateTypeException {
    Type type;

    try {
      type = valueOf(fieldType.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new UnsupportedDateTypeException(fieldType + " is unsupported");
    }

    switch (type) {
      case INT:
        return new IntConverter(fieldName, nullable);

      case LONG:
        return new LongConverter(fieldName, nullable);

      case FLOAT:
        return new FloatConverter(fieldName, nullable);

      case DOUBLE:
        return new DoubleConverter(fieldName, nullable);

      case BOOLEAN:
        return new BooleanConverter(fieldName, nullable);

      case STRING:
        return new StringConverter(fieldName, nullable);

      case ARRAY:
        return new ArrayConverter(fieldName, nullable, schemaNode, state);

      case ENUM:
        return new EnumConverter(fieldName, nullable, schemaNode, state);

      case RECORD:
        return new RecordConverter(fieldName, nullable, schemaNode, state);

      default:
        throw new UnsupportedDateTypeException(fieldType + " is unsupported");
    }
  }

  /**
   * Converts a JsonElement into a supported ParquetType
   * @author tilakpatidar
   *
   */
  public static abstract class JsonElementConverter {
    private String name;
    private boolean nullable;

    /**
     *
     * @param fieldName
     * @param nullable
     */
    public JsonElementConverter(String fieldName, boolean nullable) {
      this.name = fieldName;
      this.nullable = nullable;
    }

    /**
     * Field name from schema
     * @return
     */
    public String getName() {
      return this.name;
    }

    /**
     * is field nullable
     * @return
     */
    public boolean isNullable() {
      return this.nullable;
    }

    protected parquet.schema.Type schema() {
      return new PrimitiveType(repetitionType(), getTargetType(), getName());
    }

    /**
     * Convert value
     * @param value is JsonNull will return null if allowed or exception if not allowed
     * @return Parquet safe type
     */
    public Object convert(JsonElement value) {
      if (value.isJsonNull()) {
        if (this.nullable) {
          return null;
        }
        throw new RuntimeException("Field: " + getName() + " is not nullable and contains a null value");
      }
      return convertField(value);
    }

    /**
     * Convert JsonElement to Parquet type
     * @param value
     * @return
     */
    abstract Object convertField(JsonElement value);

    /**
     * Parquet data type after conversion
     * @return
     */
    public abstract PrimitiveTypeName getTargetType();

    /**
     * Returns the source type
     * @return
     */
    public abstract Type getSourceType();

    /**
     * Parquet RepetitionType Optional or Required
     * @return
     */
    protected parquet.schema.Type.Repetition repetitionType() {
      return isNullable() ? OPTIONAL : REQUIRED;
    }
  }

  public static abstract class ComplexConverter extends JsonElementConverter {
    private JsonElementConverter elementConverter;

    public ComplexConverter(String fieldName, boolean nullable) {
      super(fieldName, nullable);
    }

    protected void setElementConverter(JsonElementConverter elementConverter) {
      this.elementConverter = elementConverter;
    }

    public JsonElementConverter getElementConverter() {
      return this.elementConverter;
    }

    @Override
    public PrimitiveTypeName getTargetType() {
      throw new UnsupportedOperationException("Complex types does not support PrimitiveTypeName");
    }
  }

  public static abstract class ComplexConverterForUniformElementTypes extends ComplexConverter {
    private JsonElementConverter elementConverter;
    private PrimitiveTypeName elementPrimitiveName;
    private Type elementSourceType;

    public ComplexConverterForUniformElementTypes(String fieldName, boolean nullable, JsonObject schemaNode,
        String itemKey) {
      super(fieldName, nullable);
      this.elementPrimitiveName = getPrimitiveTypeInParquet(schemaNode, itemKey);
      this.elementSourceType = getPrimitiveTypeInSource(schemaNode, itemKey);
    }

    protected void setElementConverter(JsonElementConverter elementConverter) {
      this.elementConverter = elementConverter;
    }

    public JsonElementConverter getElementConverter() {
      return this.elementConverter;
    }

    @Override
    public PrimitiveTypeName getTargetType() {
      throw new UnsupportedOperationException("Complex types does not support PrimitiveTypeName");
    }

    private PrimitiveTypeName getPrimitiveTypeInParquet(JsonObject schemaNode, String itemKey) {
      return typeMap.get(getPrimitiveTypeInSource(schemaNode, itemKey));
    }

    private Type getPrimitiveTypeInSource(JsonObject schemaNode, String itemKey) {
      String type = schemaNode.get("dataType").getAsJsonObject().get(itemKey).getAsString().toUpperCase();
      return Type.valueOf(type);
    }

    public PrimitiveTypeName getElementTypeParquet() {
      return elementPrimitiveName;
    }

    public Type getElementTypeSource() {
      return elementSourceType;
    }

    protected JsonObject getElementSchema() {
      String typeOfElement = getElementTypeSource().toString();
      JsonObject temp = new JsonObject();
      JsonObject dataType = new JsonObject();
      temp.addProperty("columnName", "temp");
      dataType.addProperty("type", typeOfElement);
      temp.add("dataType", dataType);
      return temp;
    }
  }

  public static class IntConverter extends JsonElementConverter {

    public IntConverter(String fieldName, boolean nullable) {
      super(fieldName, nullable);
    }

    @Override
    IntegerValue convertField(JsonElement value) {
      return new IntegerValue(value.getAsInt());
    }

    @Override
    public PrimitiveTypeName getTargetType() {
      return INT32;
    }

    @Override
    public Type getSourceType() {
      return INT;
    }
  }

  public static class LongConverter extends JsonElementConverter {

    public LongConverter(String fieldName, boolean nullable) {
      super(fieldName, nullable);
    }

    @Override
    LongValue convertField(JsonElement value) {
      return new LongValue(value.getAsLong());
    }

    @Override
    public PrimitiveTypeName getTargetType() {
      return INT64;
    }

    @Override
    public Type getSourceType() {
      return LONG;
    }
  }

  public static class FloatConverter extends JsonElementConverter {

    public FloatConverter(String fieldName, boolean nullable) {
      super(fieldName, nullable);
    }

    @Override
    FloatValue convertField(JsonElement value) {
      return new FloatValue(value.getAsFloat());
    }

    @Override
    public PrimitiveTypeName getTargetType() {
      return PrimitiveTypeName.FLOAT;
    }

    @Override
    public Type getSourceType() {
      return FLOAT;
    }
  }

  public static class DoubleConverter extends JsonElementConverter {

    public DoubleConverter(String fieldName, boolean nullable) {
      super(fieldName, nullable);
    }

    @Override
    DoubleValue convertField(JsonElement value) {
      return new DoubleValue(value.getAsDouble());
    }

    @Override
    public PrimitiveTypeName getTargetType() {
      return PrimitiveTypeName.DOUBLE;
    }

    @Override
    public Type getSourceType() {
      return DOUBLE;
    }
  }

  public static class BooleanConverter extends JsonElementConverter {

    public BooleanConverter(String fieldName, boolean nullable) {
      super(fieldName, nullable);
    }

    @Override
    BooleanValue convertField(JsonElement value) {
      return new BooleanValue(value.getAsBoolean());
    }

    @Override
    public PrimitiveTypeName getTargetType() {
      return PrimitiveTypeName.BOOLEAN;
    }

    @Override
    public Type getSourceType() {
      return BOOLEAN;
    }
  }

  public static class StringConverter extends JsonElementConverter {

    public StringConverter(String fieldName, boolean nullable) {
      super(fieldName, nullable);
    }

    @Override
    BinaryValue convertField(JsonElement value) {
      return new BinaryValue(Binary.fromString(value.getAsString()));
    }

    @Override
    public PrimitiveTypeName getTargetType() {
      return BINARY;
    }

    @Override
    public Type getSourceType() {
      return STRING;
    }
  }

  public static class ArrayConverter extends ComplexConverterForUniformElementTypes {
    private final int len;
    private final JsonObject elementSchema;
    private static final String ITEM_KEY = "items";

    public ArrayConverter(String fieldName, boolean nullable, JsonObject schemaNode, WorkUnitState state)
        throws UnsupportedDateTypeException {
      super(fieldName, nullable, schemaNode, ITEM_KEY);
      len = schemaNode.get("length").getAsInt();
      elementSchema = getElementSchema();
      JsonElementConverter converter = getConvertor("", getElementTypeSource().toString(), elementSchema, state, false);
      super.setElementConverter(converter);
    }

    @Override
    Object convertField(JsonElement value) {
      ParquetGroup array = new ParquetGroup(schema());
      int index = 0;
      JsonElementConverter converter = getElementConverter();
      for (JsonElement elem : (JsonArray) value) {
        array.add(index, (Primitive) converter.convert(elem));
        index++;
      }
      return array;
    }

    @Override
    public Type getSourceType() {
      return ARRAY;
    }

    @Override
    public GroupType schema() {
      List<parquet.schema.Type> fields = new ArrayList<>();
      for (int i = 0; i < len; i++) {
        fields.add(i, new PrimitiveType(repetitionType(), getElementTypeParquet(), String.valueOf(i)));
      }
      return new GroupType(repetitionType(), getName(), fields);
    }
  }

  public static class EnumConverter extends ComplexConverter {
    private final JsonObject elementSchema;
    private final HashSet<String> symbols = new HashSet<>();

    public EnumConverter(String fieldName, boolean nullable, JsonObject schemaNode, WorkUnitState state)
        throws UnsupportedDateTypeException {
      super(fieldName, nullable);
      elementSchema = getElementSchema();
      JsonArray symbolsArray = schemaNode.get("dataType").getAsJsonObject().get("symbols").getAsJsonArray();
      symbolsArray.forEach(e -> symbols.add(e.getAsString()));
      JsonElementConverter converter = getConvertor(getName(), STRING.toString(), elementSchema, state, isNullable());
      super.setElementConverter(converter);
    }

    private JsonObject getElementSchema() {
      JsonObject temp = new JsonObject();
      JsonObject dataType = new JsonObject();
      dataType.addProperty("type", "string");
      temp.addProperty("columnName", "temp");
      temp.add("dataType", dataType);
      return temp;
    }

    @Override
    Object convertField(JsonElement value) {
      if (symbols.contains(value.getAsString()) || isNullable()) {
        return this.getElementConverter().convert(value);
      }
      throw new RuntimeException("Symbol " + value.getAsString() + " does not belong to set " + symbols.toString());
    }

    @Override
    public Type getSourceType() {
      return ENUM;
    }

    @Override
    public parquet.schema.Type schema() {
      return this.getElementConverter().schema();
    }
  }

  public static class RecordConverter extends ComplexConverter{

    private final HashMap<String, JsonElementConverter> converters;
    private final GroupType schema;

    public RecordConverter(String fieldName, boolean nullable, JsonObject schemaNode, WorkUnitState state){
      super(fieldName, nullable);
      converters = new HashMap<>();
      schema = buildSchema(schemaNode.get("dataType").getAsJsonObject().get("fields").getAsJsonArray(), state);

    }

    @Override
    Object convertField(JsonElement value) {
      ParquetGroup r1 = new ParquetGroup(schema);
      JsonObject inputRecord =value.getAsJsonObject();
      for (Map.Entry<String, JsonElement> entry : inputRecord.entrySet()) {
        JsonElementConverter converter = this.converters.get(entry.getKey());
        r1.add(entry.getKey(), converter.convert(entry.getValue()));
      }
      return r1;
    }

    @Override
    public Type getSourceType() {
      return RECORD;
    }

    @Override
    public GroupType schema(){
      return schema;
    }

    private GroupType buildSchema(JsonArray inputSchema, WorkUnitState workUnit) {
      List<parquet.schema.Type> parquetTypes = new ArrayList<>();
      for (JsonElement element : inputSchema) {
        JsonObject map = (JsonObject) element;

        String columnName = map.get("columnName").getAsString();
        String dataType = map.get("dataType").getAsJsonObject().get("type").getAsString();
        boolean nullable = map.has("isNullable") && map.get("isNullable").getAsBoolean();
        parquet.schema.Type schemaType;
        try {
          JsonElementConverter convertor =
              JsonElementConversionFactory.getConvertor(columnName, dataType, map, workUnit, nullable);
          schemaType = convertor.schema();
          this.converters.put(columnName, convertor);
        } catch (UnsupportedDateTypeException e) {
          throw new RuntimeException(e);
        }
        parquetTypes.add(schemaType);
      }
      String docName = getName();
      return new MessageType(docName, parquetTypes);
    }
  }
}
