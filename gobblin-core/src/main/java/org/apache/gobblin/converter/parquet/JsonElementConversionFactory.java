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

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.avro.UnsupportedDateTypeException;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import parquet.example.data.simple.BinaryValue;
import parquet.example.data.simple.BooleanValue;
import parquet.example.data.simple.DoubleValue;
import parquet.example.data.simple.FloatValue;
import parquet.example.data.simple.IntegerValue;
import parquet.example.data.simple.LongValue;
import parquet.io.api.Binary;
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
        return new IntConverter(fieldName, nullable, type.toString());

      case LONG:
        return new LongConverter(fieldName, nullable, type.toString());

      case FLOAT:
        return new FloatConverter(fieldName, nullable, type.toString());

      case DOUBLE:
        return new DoubleConverter(fieldName, nullable, type.toString());

      case BOOLEAN:
        return new BooleanConverter(fieldName, nullable, type.toString());

      case STRING:
        return new StringConverter(fieldName, nullable, type.toString());

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
    private String sourceType;

    /**
     *
     * @param fieldName
     * @param nullable
     */
    public JsonElementConverter(String fieldName, boolean nullable, String sourceType) {
      this.name = fieldName;
      this.nullable = nullable;
      this.sourceType = sourceType;
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

  public static class IntConverter extends JsonElementConverter {

    public IntConverter(String fieldName, boolean nullable, String sourceType) {
      super(fieldName, nullable, sourceType);
    }

    @Override
    Object convertField(JsonElement value) {
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

    public LongConverter(String fieldName, boolean nullable, String sourceType) {
      super(fieldName, nullable, sourceType);
    }

    @Override
    Object convertField(JsonElement value) {
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

    public FloatConverter(String fieldName, boolean nullable, String sourceType) {
      super(fieldName, nullable, sourceType);
    }

    @Override
    Object convertField(JsonElement value) {
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

    public DoubleConverter(String fieldName, boolean nullable, String sourceType) {
      super(fieldName, nullable, sourceType);
    }

    @Override
    Object convertField(JsonElement value) {
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

    public BooleanConverter(String fieldName, boolean nullable, String sourceType) {
      super(fieldName, nullable, sourceType);
    }

    @Override
    Object convertField(JsonElement value) {
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

    public StringConverter(String fieldName, boolean nullable, String sourceType) {
      super(fieldName, nullable, sourceType);
    }

    @Override
    Object convertField(JsonElement value) {
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
}
