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
import java.util.List;

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
    DATE, TIMESTAMP, TIME, FIXED, STRING, BYTES, INT, LONG, FLOAT, DOUBLE, BOOLEAN, ARRAY, MAP, ENUM
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

  public static class ArrayConverter extends ComplexConverter {
    private final int len;
    private final PrimitiveTypeName arrayTypeParquet;
    private final Type arrayTypeSource;
    private final JsonObject elementSchema;

    public ArrayConverter(String fieldName, boolean nullable, JsonObject schemaNode, WorkUnitState state)
        throws UnsupportedDateTypeException {
      super(fieldName, nullable);
      len = schemaNode.get("length").getAsInt();
      arrayTypeParquet = getPrimitiveTypeInParquet(schemaNode);
      arrayTypeSource = getPrimitiveTypeInSource(schemaNode);
      elementSchema = getElementSchema();
      JsonElementConverter converter = getConvertor("", arrayTypeSource.toString(), elementSchema, state, false);
      super.setElementConverter(converter);
    }

    @Override
    Object convertField(JsonElement value) {
      ParquetGroup array = new ParquetGroup(schema());
      int index = 0;
      for (JsonElement elem : (JsonArray) value) {
        JsonElementConverter converter = getElementConverter();
        array.add(index, (Primitive) converter.convert(elem));
        index++;
      }
      return array;
    }

    public PrimitiveTypeName getElementTypeParquet() {
      return arrayTypeParquet;
    }

    public Type getElementTypeSource() {
      return arrayTypeSource;
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

    private PrimitiveTypeName getPrimitiveTypeInParquet(JsonObject schemaNode) {
      return typeMap.get(getPrimitiveTypeInSource(schemaNode));
    }

    private Type getPrimitiveTypeInSource(JsonObject schemaNode) {
      String type = schemaNode.get("dataType").getAsJsonObject().get("items").getAsString().toUpperCase();
      return Type.valueOf(type);
    }

    private JsonObject getElementSchema() {
      String typeOfElement = getElementTypeSource().toString();
      JsonObject temp = new JsonObject();
      JsonObject dataType = new JsonObject();
      temp.addProperty("columnName", "temp");
      dataType.addProperty("type", typeOfElement);
      temp.add("dataType", dataType);
      return temp;
    }
  }
}
