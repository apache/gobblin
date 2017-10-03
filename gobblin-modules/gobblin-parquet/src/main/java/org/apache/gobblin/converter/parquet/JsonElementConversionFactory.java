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

import org.apache.gobblin.converter.parquet.JsonSchema.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import parquet.example.data.Group;
import parquet.example.data.simple.BinaryValue;
import parquet.example.data.simple.BooleanValue;
import parquet.example.data.simple.DoubleValue;
import parquet.example.data.simple.FloatValue;
import parquet.example.data.simple.IntegerValue;
import parquet.example.data.simple.LongValue;
import parquet.io.api.Binary;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type;
import parquet.schema.Types;

import static org.apache.gobblin.converter.parquet.JsonElementConversionFactory.RecordConverter.RecordType.CHILD;
import static org.apache.gobblin.converter.parquet.JsonSchema.*;
import static org.apache.gobblin.converter.parquet.JsonSchema.InputType.STRING;
import static parquet.schema.OriginalType.UTF8;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static parquet.schema.Type.Repetition.OPTIONAL;
import static parquet.schema.Type.Repetition.REPEATED;


/**
 * <p>
 * Creates a JsonElement to Parquet converter for all supported data types.
 * </p>
 *
 * @author tilakpatidar
 *
 */
public class JsonElementConversionFactory {

  /**
   * Use to create a converter for a single field from a parquetSchema.
   *
   * @param schema
   * @param repeated - Is the {@link Type} repeated in the parent {@link Group}
   * @return
   */
  public static JsonElementConverter getConverter(JsonSchema schema, boolean repeated) {

    InputType fieldType = schema.getInputType();
    switch (fieldType) {
      case INT:
        return new IntConverter(schema, repeated);

      case LONG:
        return new LongConverter(schema, repeated);

      case FLOAT:
        return new FloatConverter(schema, repeated);

      case DOUBLE:
        return new DoubleConverter(schema, repeated);

      case BOOLEAN:
        return new BooleanConverter(schema, repeated);

      case STRING:
        return new StringConverter(schema, repeated);

      case ARRAY:
        return new ArrayConverter(schema);

      case ENUM:
        return new EnumConverter(schema);

      case RECORD:
        return new RecordConverter(schema);

      case MAP:
        return new MapConverter(schema);

      default:
        throw new UnsupportedOperationException(fieldType + " is unsupported");
    }
  }

  /**
   * Converts a JsonElement into a supported ParquetType
   * @author tilakpatidar
   *
   */
  public static abstract class JsonElementConverter {
    protected final JsonSchema jsonSchema;

    protected JsonElementConverter(JsonSchema schema) {
      this.jsonSchema = schema;
    }

    /**
     * Convert value to a parquet type and perform null check.
     * @param value
     * @return Parquet safe type
     */
    public Object convert(JsonElement value) {
      if (value.isJsonNull()) {
        if (this.jsonSchema.isNullable()) {
          return null;
        }
        throw new RuntimeException(
            "Field: " + this.jsonSchema.getColumnName() + " is not nullable and contains a null value");
      }
      return convertField(value);
    }

    /**
     * Returns a {@link Type} parquet schema
     * @return
     */
    abstract public Type schema();

    /**
     * Convert JsonElement to Parquet type
     * @param value
     * @return
     */
    abstract Object convertField(JsonElement value);
  }

  /**
   * Converts a {@link JsonSchema} to a {@link PrimitiveType}
   */
  public static abstract class PrimitiveConverter extends JsonElementConverter {
    protected final boolean repeated;
    private PrimitiveTypeName outputType;
    protected Type schema;

    /**
     * @param jsonSchema
     * @param repeated
     * @param outputType
     */
    public PrimitiveConverter(JsonSchema jsonSchema, boolean repeated, PrimitiveTypeName outputType) {
      super(jsonSchema);
      this.repeated = repeated;
      this.outputType = outputType;
      this.schema = buildSchema();
    }

    protected Type buildSchema() {
      return new PrimitiveType(this.repeated ? REPEATED : this.jsonSchema.optionalOrRequired(), this.outputType,
          this.jsonSchema.getColumnName());
    }

    @Override
    public Type schema() {
      return this.schema;
    }
  }

  /**
   * Converts {@link JsonSchema} having collection of elements of {@link InputType} into a {@link GroupType}.
   */
  public static abstract class CollectionConverter extends JsonElementConverter {
    protected InputType elementType;
    protected JsonElementConverter elementConverter;
    protected Type schema;

    public CollectionConverter(JsonSchema collectionSchema, InputType elementType, boolean repeated) {
      super(collectionSchema);
      this.elementType = elementType;
      this.elementConverter = getConverter(getElementSchema(), repeated);
      this.schema = buildSchema();
    }

    @Override
    public Type schema() {
      return this.schema;
    }

    /**
     * Prepare a {@link JsonSchema} for the elements in a collection.
     * @return
     */
    abstract JsonSchema getElementSchema();

    abstract Type buildSchema();
  }

  public static class IntConverter extends PrimitiveConverter {

    public IntConverter(JsonSchema schema, boolean repeated) {
      super(schema, repeated, INT32);
    }

    @Override
    IntegerValue convertField(JsonElement value) {
      return new IntegerValue(value.getAsInt());
    }
  }

  public static class LongConverter extends PrimitiveConverter {

    public LongConverter(JsonSchema schema, boolean repeated) {
      super(schema, repeated, INT64);
    }

    @Override
    LongValue convertField(JsonElement value) {
      return new LongValue(value.getAsLong());
    }
  }

  public static class FloatConverter extends PrimitiveConverter {

    public FloatConverter(JsonSchema schema, boolean repeated) {
      super(schema, repeated, PrimitiveTypeName.FLOAT);
    }

    @Override
    FloatValue convertField(JsonElement value) {
      return new FloatValue(value.getAsFloat());
    }
  }

  public static class DoubleConverter extends PrimitiveConverter {

    public DoubleConverter(JsonSchema schema, boolean repeated) {
      super(schema, repeated, PrimitiveTypeName.DOUBLE);
    }

    @Override
    DoubleValue convertField(JsonElement value) {
      return new DoubleValue(value.getAsDouble());
    }
  }

  public static class BooleanConverter extends PrimitiveConverter {

    public BooleanConverter(JsonSchema schema, boolean repeated) {
      super(schema, repeated, PrimitiveTypeName.BOOLEAN);
    }

    @Override
    BooleanValue convertField(JsonElement value) {
      return new BooleanValue(value.getAsBoolean());
    }
  }

  public static class StringConverter extends PrimitiveConverter {

    public StringConverter(JsonSchema schema, boolean repeated) {
      super(schema, repeated, BINARY);
      this.schema = buildSchema();
    }

    @Override
    BinaryValue convertField(JsonElement value) {
      return new BinaryValue(Binary.fromString(value.getAsString()));
    }

    @Override
    protected Type buildSchema() {
      String columnName = this.jsonSchema.getColumnName();
      if (this.repeated) {
        return Types.repeated(BINARY).as(UTF8).named(columnName);
      }
      switch (this.jsonSchema.optionalOrRequired()) {
        case OPTIONAL:
          return Types.optional(BINARY).as(UTF8).named(columnName);
        case REQUIRED:
          return Types.required(BINARY).as(UTF8).named(columnName);
        default:
          throw new RuntimeException("Unsupported Repetition type");
      }
    }
  }

  public static class ArrayConverter extends CollectionConverter {

    public ArrayConverter(JsonSchema arraySchema) {
      super(arraySchema, arraySchema.getElementTypeUsingKey(ARRAY_ITEMS_KEY), true);
    }

    @Override
    Object convertField(JsonElement value) {
      ParquetGroup array = new ParquetGroup((GroupType) schema());
      JsonElementConverter converter = this.elementConverter;
      for (JsonElement elem : (JsonArray) value) {
        array.add(ARRAY_KEY, converter.convert(elem));
      }
      return array;
    }

    @Override
    protected Type buildSchema() {
      List<Type> fields = new ArrayList<>();
      fields.add(0, this.elementConverter.schema());
      return new GroupType(this.jsonSchema.optionalOrRequired(), this.jsonSchema.getColumnName(), fields);
    }

    @Override
    JsonSchema getElementSchema() {
      JsonSchema jsonSchema = JsonSchema.buildBaseSchema(this.elementType);
      jsonSchema.setColumnName(ARRAY_KEY);
      return jsonSchema;
    }
  }

  public static class EnumConverter extends CollectionConverter {
    private final HashSet<String> symbols = new HashSet<>();

    public EnumConverter(JsonSchema enumSchema) {
      super(enumSchema, STRING, false);
      JsonArray symbolsArray = enumSchema.getSymbols();
      symbolsArray.forEach(e -> symbols.add(e.getAsString()));
    }

    @Override
    Object convertField(JsonElement value) {
      if (symbols.contains(value.getAsString()) || this.jsonSchema.isNullable()) {
        return this.elementConverter.convert(value);
      }
      throw new RuntimeException("Symbol " + value.getAsString() + " does not belong to set " + symbols.toString());
    }

    @Override
    protected Type buildSchema() {
      return this.elementConverter.schema();
    }

    @Override
    JsonSchema getElementSchema() {
      JsonSchema jsonSchema = JsonSchema.buildBaseSchema(STRING);
      jsonSchema.setColumnName(this.jsonSchema.getColumnName());
      return jsonSchema;
    }
  }

  public static class RecordConverter extends JsonElementConverter {

    private final HashMap<String, JsonElementConverter> converters;
    private final RecordType recordType;
    private final Type schema;

    public enum RecordType {
      ROOT, CHILD
    }

    public RecordConverter(JsonSchema recordSchema) {
      this(recordSchema, CHILD);
    }

    public RecordConverter(JsonSchema recordSchema, RecordType recordType) {
      super(recordSchema);
      this.converters = new HashMap<>();
      this.recordType = recordType;
      this.schema = buildSchema();
    }

    @Override
    Object convertField(JsonElement value) {
      ParquetGroup r1 = new ParquetGroup((GroupType) schema());
      JsonObject inputRecord = value.getAsJsonObject();
      for (Map.Entry<String, JsonElement> entry : inputRecord.entrySet()) {
        String key = entry.getKey();
        JsonElementConverter converter = this.converters.get(key);
        Object convertedValue = converter.convert(entry.getValue());
        boolean valueIsNull = convertedValue == null;
        Type.Repetition repetition = converter.jsonSchema.optionalOrRequired();
        if (valueIsNull && repetition.equals(OPTIONAL)) {
          continue;
        }
        r1.add(key, convertedValue);
      }
      return r1;
    }

    private Type buildSchema() {
      JsonArray inputSchema = this.jsonSchema.getDataTypeValues();
      List<Type> parquetTypes = new ArrayList<>();
      for (JsonElement element : inputSchema) {
        JsonObject map = (JsonObject) element;
        JsonSchema elementSchema = new JsonSchema(map);
        String columnName = elementSchema.getColumnName();
        JsonElementConverter converter = JsonElementConversionFactory.getConverter(elementSchema, false);
        Type schemaType = converter.schema();
        this.converters.put(columnName, converter);
        parquetTypes.add(schemaType);
      }
      String docName = this.jsonSchema.getColumnName();
      switch (recordType) {
        case ROOT:
          return new MessageType(docName, parquetTypes);
        case CHILD:
          return new GroupType(this.jsonSchema.optionalOrRequired(), docName, parquetTypes);
        default:
          throw new RuntimeException("Unsupported Record type");
      }
    }

    @Override
    public Type schema() {
      return this.schema;
    }
  }

  public static class MapConverter extends CollectionConverter {

    public MapConverter(JsonSchema mapSchema) {
      super(mapSchema, mapSchema.getElementTypeUsingKey(MAP_ITEMS_KEY), false);
    }

    @Override
    Object convertField(JsonElement value) {
      ParquetGroup mapGroup = new ParquetGroup((GroupType) schema());
      JsonElementConverter converter = this.elementConverter;
      JsonObject map = (JsonObject) value;

      for (Map.Entry<String, JsonElement> entry : map.entrySet()) {
        ParquetGroup entrySet = (ParquetGroup) mapGroup.addGroup(MAP_KEY);
        entrySet.add(MAP_KEY_COLUMN_NAME, entry.getKey());
        entrySet.add(MAP_VALUE_COLUMN_NAME, converter.convert(entry.getValue()));
      }

      return mapGroup;
    }

    @Override
    protected Type buildSchema() {
      JsonElementConverter elementConverter = this.elementConverter;
      JsonElementConverter keyConverter = getKeyConverter();
      GroupType mapGroup =
          Types.repeatedGroup().addFields(keyConverter.schema(), elementConverter.schema()).named(MAP_KEY)
              .asGroupType();
      String columnName = this.jsonSchema.getColumnName();
      switch (this.jsonSchema.optionalOrRequired()) {
        case OPTIONAL:
          return Types.optionalGroup().addFields(mapGroup).named(columnName).asGroupType();
        case REQUIRED:
          return Types.requiredGroup().addFields(mapGroup).named(columnName).asGroupType();
        default:
          return null;
      }
    }

    @Override
    JsonSchema getElementSchema() {
      JsonSchema jsonSchema = JsonSchema.buildBaseSchema(this.elementType);
      jsonSchema.setColumnName(MAP_VALUE_COLUMN_NAME);
      return jsonSchema;
    }

    public JsonElementConverter getKeyConverter() {
      JsonSchema jsonSchema = JsonSchema.buildBaseSchema(STRING);
      jsonSchema.setColumnName(MAP_KEY_COLUMN_NAME);
      return getConverter(jsonSchema, false);
    }
  }
}
