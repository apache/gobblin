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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

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
import parquet.schema.Types;

import static org.apache.gobblin.converter.parquet.JsonElementConversionFactory.RecordConverter.RecordType.CHILD;
import static org.apache.gobblin.converter.parquet.JsonElementConversionFactory.Type.*;
import static parquet.schema.OriginalType.UTF8;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static parquet.schema.Type.Repetition.OPTIONAL;
import static parquet.schema.Type.Repetition.REPEATED;
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
    STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN, ARRAY, ENUM, RECORD, MAP
  }

  /**
   * Use to create a converter for a single field from a schema.
   *
   * @param fieldName
   * @param fieldType
   * @param schemaNode
   * @param state
   * @param nullable
   * @param repeated - Is the {@link parquet.schema.Type} repeated in the parent {@link parquet.example.data.Group}
   * @return
   */
  public static JsonElementConverter getConvertor(String fieldName, String fieldType, JsonObject schemaNode,
      WorkUnitState state, boolean nullable, boolean repeated) {
    Type type;

    try {
      type = valueOf(fieldType.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new UnsupportedOperationException(fieldType + " is unsupported");
    }

    switch (type) {
      case INT:
        return new IntConverter(fieldName, nullable, repeated);

      case LONG:
        return new LongConverter(fieldName, nullable, repeated);

      case FLOAT:
        return new FloatConverter(fieldName, nullable, repeated);

      case DOUBLE:
        return new DoubleConverter(fieldName, nullable, repeated);

      case BOOLEAN:
        return new BooleanConverter(fieldName, nullable, repeated);

      case STRING:
        return new StringConverter(fieldName, nullable, repeated);

      case ARRAY:
        return new ArrayConverter(fieldName, nullable, schemaNode, state);

      case ENUM:
        return new EnumConverter(fieldName, nullable, schemaNode, state);

      case RECORD:
        return new RecordConverter(fieldName, nullable, schemaNode, state);

      case MAP:
        return new MapConverter(fieldName, nullable, schemaNode, state);

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
    private final String name;
    private final boolean nullable;
    private final boolean repeated;

    /**
     * HashMap to convert {@link Type} to {@link PrimitiveTypeName}
     */
    protected final static HashMap<Type, PrimitiveTypeName> typeMap = new HashMap<>();

    static {
      typeMap.put(INT, INT32);
      typeMap.put(LONG, INT64);
      typeMap.put(FLOAT, PrimitiveTypeName.FLOAT);
      typeMap.put(DOUBLE, PrimitiveTypeName.DOUBLE);
      typeMap.put(BOOLEAN, PrimitiveTypeName.BOOLEAN);
      typeMap.put(STRING, BINARY);
    }

    /**
     * @param fieldName
     * @param nullable
     * @param repeated
     */
    public JsonElementConverter(String fieldName, boolean nullable, boolean repeated) {
      this.name = fieldName;
      this.nullable = nullable;
      this.repeated = repeated;
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

    /**
     * Returns a {@link PrimitiveType} schema
     * @return
     */
    protected parquet.schema.Type schema() {
      return new PrimitiveType(repeated ? REPEATED : optionalOrRequired(), getTargetType(), getName());
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
    protected parquet.schema.Type.Repetition optionalOrRequired() {
      return isNullable() ? OPTIONAL : REQUIRED;
    }

    /**
     * Is Parquet type repeated in the {@link parquet.example.data.Group}
     * @return
     */
    protected boolean isRepeated() {
      return this.repeated;
    }
  }

  /**
   * For converting Complex data types like ARRAY, ENUM, MAP etc.
   */
  public static abstract class ComplexConverter extends JsonElementConverter {
    private JsonElementConverter elementConverter;

    public ComplexConverter(String fieldName, boolean nullable, boolean repeated) {
      super(fieldName, nullable, repeated);
    }

    /**
     * Set a {@link JsonElementConverter} for the elements present within Complex data types.
     * @param elementConverter
     */
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

  /**
   * For converting complex types which compose same element type.
   */
  public static abstract class ComplexConverterForUniformElementTypes extends ComplexConverter {
    private JsonElementConverter elementConverter;
    private PrimitiveTypeName elementPrimitiveName;
    private Type elementSourceType;

    public ComplexConverterForUniformElementTypes(String fieldName, boolean nullable, JsonObject schemaNode,
        String itemKey, boolean repeated) {
      super(fieldName, nullable, repeated);
      this.elementPrimitiveName = getPrimitiveTypeInParquet(schemaNode, itemKey);
      this.elementSourceType = getPrimitiveTypeInSource(schemaNode, itemKey);
    }

    /**
     * Set a {@link JsonElementConverter} for the elements present within Complex data types.
     * @param elementConverter
     */
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

    /**
     * {@link PrimitiveTypeName} of the elements composed within complex type.
     * @param schemaNode
     * @param itemKey
     * @return
     */
    private PrimitiveTypeName getPrimitiveTypeInParquet(JsonObject schemaNode, String itemKey) {
      return typeMap.get(getPrimitiveTypeInSource(schemaNode, itemKey));
    }

    /**
     * {@link Type} of the elements composed within complex type.
     * @param schemaNode
     * @param itemKey
     * @return
     */
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

    /**
     * Prepare a temp schema object of composed elements for passing to element converters.
     * @return
     */
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

    public IntConverter(String fieldName, boolean nullable, boolean repeated) {
      super(fieldName, nullable, repeated);
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

    public LongConverter(String fieldName, boolean nullable, boolean repeated) {
      super(fieldName, nullable, repeated);
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

    public FloatConverter(String fieldName, boolean nullable, boolean repeated) {
      super(fieldName, nullable, repeated);
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

    public DoubleConverter(String fieldName, boolean nullable, boolean repeated) {
      super(fieldName, nullable, repeated);
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

    public BooleanConverter(String fieldName, boolean nullable, boolean repeated) {
      super(fieldName, nullable, repeated);
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

    public StringConverter(String fieldName, boolean nullable, boolean repeated) {
      super(fieldName, nullable, repeated);
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

    @Override
    public parquet.schema.Type schema() {
      if (isRepeated()) {
        return Types.repeated(BINARY).as(UTF8).named(getName());
      }
      switch (optionalOrRequired()) {
        case OPTIONAL:
          return Types.optional(BINARY).as(UTF8).named(getName());
        case REQUIRED:
          return Types.required(BINARY).as(UTF8).named(getName());
        default:
          throw new RuntimeException("Unsupported Repetition type");
      }
    }
  }

  public static class ArrayConverter extends ComplexConverterForUniformElementTypes {
    public static final String ARRAY_KEY = "item";
    private static final String SOURCE_SCHEMA_ITEMS_KEY = "items";
    private final JsonObject elementSchema;

    public ArrayConverter(String fieldName, boolean nullable, JsonObject schemaNode, WorkUnitState state) {
      super(fieldName, nullable, schemaNode, SOURCE_SCHEMA_ITEMS_KEY, true);
      this.elementSchema = getElementSchema();
      JsonElementConverter converter =
          getConvertor(ARRAY_KEY, getElementTypeSource().toString(), elementSchema, state, isNullable(), true);
      super.setElementConverter(converter);
    }

    @Override
    Object convertField(JsonElement value) {
      ParquetGroup array = new ParquetGroup(schema());
      JsonElementConverter converter = getElementConverter();
      for (JsonElement elem : (JsonArray) value) {
        array.add(ARRAY_KEY, converter.convert(elem));
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
      fields.add(0, getElementConverter().schema());
      return new GroupType(optionalOrRequired(), getName(), fields);
    }
  }

  public static class EnumConverter extends ComplexConverter {
    private final JsonObject elementSchema;
    private final HashSet<String> symbols = new HashSet<>();

    public EnumConverter(String fieldName, boolean nullable, JsonObject schemaNode, WorkUnitState state) {
      super(fieldName, nullable, false);
      elementSchema = getElementSchema();
      JsonArray symbolsArray = schemaNode.get("dataType").getAsJsonObject().get("symbols").getAsJsonArray();
      symbolsArray.forEach(e -> symbols.add(e.getAsString()));
      JsonElementConverter converter =
          getConvertor(getName(), STRING.toString(), elementSchema, state, isNullable(), false);
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

  public static class RecordConverter extends ComplexConverter {

    private final HashMap<String, JsonElementConverter> converters;
    private final GroupType schema;
    private final RecordType recordType;

    public enum RecordType {
      ROOT, CHILD
    }

    public RecordConverter(String fieldName, boolean nullable, JsonObject schemaNode, WorkUnitState state) {
      this(fieldName, nullable, schemaNode, state, CHILD);
    }

    public RecordConverter(String fieldName, boolean nullable, JsonObject schemaNode, WorkUnitState state,
        RecordType recordType) {
      super(fieldName, nullable, false);
      this.converters = new HashMap<>();
      this.recordType = recordType;
      this.schema = buildSchema(schemaNode.get("dataType").getAsJsonObject().get("fields").getAsJsonArray(), state);
    }

    @Override
    Object convertField(JsonElement value) {
      ParquetGroup r1 = new ParquetGroup(schema);
      JsonObject inputRecord = value.getAsJsonObject();
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
    public GroupType schema() {
      return schema;
    }

    private GroupType buildSchema(JsonArray inputSchema, WorkUnitState workUnit) {
      List<parquet.schema.Type> parquetTypes = new ArrayList<>();
      for (JsonElement element : inputSchema) {
        JsonObject map = (JsonObject) element;

        String columnName = map.get("columnName").getAsString();
        String dataType = map.get("dataType").getAsJsonObject().get("type").getAsString();
        boolean nullable = map.has("isNullable") && map.get("isNullable").getAsBoolean();
        JsonElementConverter convertor =
            JsonElementConversionFactory.getConvertor(columnName, dataType, map, workUnit, nullable, false);
        parquet.schema.Type schemaType = convertor.schema();
        this.converters.put(columnName, convertor);
        parquetTypes.add(schemaType);
      }
      String docName = getName();
      switch (recordType) {
        case ROOT:
          return new MessageType(docName, parquetTypes);
        case CHILD:
          return new GroupType(optionalOrRequired(), docName, parquetTypes);
        default:
          throw new RuntimeException("Unsupported Record type");
      }
    }
  }

  public static class MapConverter extends ComplexConverterForUniformElementTypes {

    private static final String SOURCE_SCHEMA_ITEMS_KEY = "values";
    private static final String MAP_KEY = "map";
    private final JsonObject elementSchema;
    private final WorkUnitState state;

    public MapConverter(String fieldName, boolean nullable, JsonObject schemaNode, WorkUnitState state) {
      super(fieldName, nullable, schemaNode, SOURCE_SCHEMA_ITEMS_KEY, false);
      this.elementSchema = getElementSchema();
      this.state = state;
    }

    @Override
    Object convertField(JsonElement value) {
      ParquetGroup mapGroup = new ParquetGroup(schema());
      JsonElementConverter converter = getElementConverter();
      JsonObject map = (JsonObject) value;

      for (Map.Entry<String, JsonElement> entry : map.entrySet()) {
        ParquetGroup entrySet = (ParquetGroup) mapGroup.addGroup("map");
        entrySet.add("key", entry.getKey());
        entrySet.add("value", converter.convert(entry.getValue()));
      }

      return mapGroup;
    }

    @Override
    public Type getSourceType() {
      return MAP;
    }

    @Override
    public GroupType schema() {
      JsonElementConverter elementConverter = getElementConverter();
      JsonElementConverter keyConverter = getKeyConverter();
      GroupType mapGroup =
          Types.repeatedGroup().addFields(keyConverter.schema(), elementConverter.schema()).named(MAP_KEY)
              .asGroupType();
      switch (optionalOrRequired()) {
        case OPTIONAL:
          return Types.optionalGroup().addFields(mapGroup).named(getName()).asGroupType();
        case REQUIRED:
          return Types.requiredGroup().addFields(mapGroup).named(getName()).asGroupType();
        default:
          return null;
      }
    }

    @Override
    public JsonElementConverter getElementConverter() {
      Type type = getElementTypeSource();
      return getConvertor("value", type.toString(), getElementSchema(), state, false, false);
    }

    public JsonElementConverter getKeyConverter() {
      return getConvertor("key", STRING.toString(), getElementSchema(), state, false, false);
    }
  }
}
