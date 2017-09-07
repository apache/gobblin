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

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import sun.util.calendar.ZoneInfo;

import static org.apache.gobblin.converter.json.JsonStringToJsonIntermediateConverter.JsonUtils.*;

/**
 * <p>
 * Creates a JsonElement to Avro converter for all supported data types.
 * </p>
 *
 * @author kgoodhop
 *
 */
public class JsonElementConversionFactory {

  public enum Type {
    DATE,
    TIMESTAMP,
    TIME,
    FIXED,
    STRING,
    BYTES,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    BOOLEAN,
    ARRAY,
    MAP,
    ENUM,
    RECORD,
    NULL,
    UNION
  }

  /**
   * Use to create a converter for a single field from a schema.
   *
   * @param fieldName
   * @param namespace
   *@param fieldType
   * @param schemaNode
   * @param state
   * @param nullable     @return
   * @throws UnsupportedDateTypeException
   */
  public static JsonElementConverter getConvertor(String fieldName, String namespace, String fieldType,
      JsonObject schemaNode, WorkUnitState state, boolean nullable)
      throws UnsupportedDateTypeException {

    Type type;
    try {
      type = Type.valueOf(fieldType.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new UnsupportedDateTypeException(fieldType + " is unsupported");
    }

    DateTimeZone timeZone = getTimeZone(state.getProp(ConfigurationKeys.CONVERTER_AVRO_DATE_TIMEZONE, "UTC"));
    switch (type) {
      case DATE:
        return new DateConverter(fieldName, nullable, type.toString(),
            state.getProp(ConfigurationKeys.CONVERTER_AVRO_DATE_FORMAT, "yyyy-MM-dd HH:mm:ss"), timeZone, state);

      case TIMESTAMP:
        return new DateConverter(fieldName, nullable, type.toString(),
            state.getProp(ConfigurationKeys.CONVERTER_AVRO_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss"), timeZone, state);

      case TIME:
        return new DateConverter(fieldName, nullable, type.toString(),
            state.getProp(ConfigurationKeys.CONVERTER_AVRO_TIME_FORMAT, "HH:mm:ss"), timeZone, state);

      case FIXED:
        throw new UnsupportedDateTypeException(fieldType + " is unsupported");

      case STRING:
        return new StringConverter(fieldName, nullable, type.toString());

      case BYTES:
        return new BinaryConverter(fieldName, nullable, type.toString(),
            state.getProp(ConfigurationKeys.CONVERTER_AVRO_BINARY_CHARSET, "UTF8"));

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

      case ARRAY:
        return new ArrayConverter(fieldName, nullable, type.toString(), schemaNode, state);

      case MAP:
        return new MapConverter(fieldName, nullable, type.toString(), schemaNode, state);

      case ENUM:
        return new EnumConverter(fieldName, nullable, type.toString(), schemaNode, namespace);

      case RECORD:
        return new RecordConverter(fieldName, nullable, type.toString(), schemaNode, state, namespace);

      case NULL:
        return new NullConverter(fieldName, type.toString());

      case UNION:
        return new UnionConverter(fieldName, type.toString(), schemaNode, state);

      default:
        throw new UnsupportedDateTypeException(fieldType + " is unsupported");
    }
  }

  private static DateTimeZone getTimeZone(String id) {
    DateTimeZone zone;
    try {
      zone = DateTimeZone.forID(id);
    } catch (IllegalArgumentException e) {
      TimeZone timeZone = ZoneInfo.getTimeZone(id);

      //throw error if unrecognized zone
      if (timeZone == null) {
        throw new IllegalArgumentException("TimeZone " + id + " not recognized");
      }
      zone = DateTimeZone.forTimeZone(timeZone);
    }
    return zone;
  }

  /**
   * Converts a JsonElement into a supported AvroType
   * @author kgoodhop
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

    /**
     * avro schema for the converted type
     * @return
     */
    public Schema getSchema() {
      if (this.nullable) {
        List<Schema> list = new ArrayList<>();
        list.add(Schema.create(Schema.Type.NULL));
        list.add(schema());
        return Schema.createUnion(list);
      }
      return schema();
    }

    protected Schema schema() {
      Schema schema = Schema.create(getTargetType());
      schema.addProp("source.type", this.sourceType.toLowerCase());
      return schema;
    }

    /**
     * Convert value
     * @param value is JsonNull will return null if allowed or exception if not allowed
     * @return Avro safe type
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
     * Convert JsonElement to Avro type
     * @param value
     * @return
     */
    abstract Object convertField(JsonElement value);

    /**
     * Avro data type after conversion
     * @return
     */
    public abstract Schema.Type getTargetType();

    protected static String buildNamespace(String namespace, String name) {
      if (namespace == null || namespace.isEmpty()) {
        return null;
      }
      if (name == null || name.isEmpty()) {
        return null;
      }
      return namespace.trim() + "." + name.trim();
    }
  }

  public static class StringConverter extends JsonElementConverter {

    public StringConverter(String fieldName, boolean nullable, String sourceType) {
      super(fieldName, nullable, sourceType);
    }

    @Override
    Object convertField(JsonElement value) {
      return new Utf8(value.getAsString());
    }

    @Override
    public org.apache.avro.Schema.Type getTargetType() {
      return Schema.Type.STRING;
    }
  }

  public static class IntConverter extends JsonElementConverter {

    public IntConverter(String fieldName, boolean nullable, String sourceType) {
      super(fieldName, nullable, sourceType);
    }

    @Override
    Object convertField(JsonElement value) {

      return value.getAsInt();
    }

    @Override
    public org.apache.avro.Schema.Type getTargetType() {
      return Schema.Type.INT;
    }
  }

  public static class LongConverter extends JsonElementConverter {

    public LongConverter(String fieldName, boolean nullable, String sourceType) {
      super(fieldName, nullable, sourceType);
    }

    @Override
    Object convertField(JsonElement value) {

      return value.getAsLong();
    }

    @Override
    public org.apache.avro.Schema.Type getTargetType() {
      return Schema.Type.LONG;
    }
  }

  public static class DoubleConverter extends JsonElementConverter {

    public DoubleConverter(String fieldName, boolean nullable, String sourceType) {
      super(fieldName, nullable, sourceType);
    }

    @Override
    Object convertField(JsonElement value) {
      return value.getAsDouble();
    }

    @Override
    public org.apache.avro.Schema.Type getTargetType() {
      return Schema.Type.DOUBLE;
    }
  }

  public static class FloatConverter extends JsonElementConverter {

    public FloatConverter(String fieldName, boolean nullable, String sourceType) {
      super(fieldName, nullable, sourceType);
    }

    @Override
    Object convertField(JsonElement value) {
      return value.getAsFloat();
    }

    @Override
    public org.apache.avro.Schema.Type getTargetType() {
      return Schema.Type.FLOAT;
    }
  }

  public static class BooleanConverter extends JsonElementConverter {

    public BooleanConverter(String fieldName, boolean nullable, String sourceType) {
      super(fieldName, nullable, sourceType);
    }

    @Override
    Object convertField(JsonElement value) {

      return value.getAsBoolean();
    }

    @Override
    public org.apache.avro.Schema.Type getTargetType() {
      return Schema.Type.BOOLEAN;
    }
  }

  public static class DateConverter extends JsonElementConverter {
    private String inputPatterns;
    private DateTimeZone timeZone;
    private WorkUnitState state;

    public DateConverter(String fieldName, boolean nullable, String sourceType, String pattern, DateTimeZone zone,
        WorkUnitState state) {
      super(fieldName, nullable, sourceType);
      this.inputPatterns = pattern;
      this.timeZone = zone;
      this.state = state;
    }

    @Override
    Object convertField(JsonElement value) {
      List<String> patterns = Arrays.asList(this.inputPatterns.split(","));
      int patternFailCount = 0;
      Object formattedDate = null;
      for (String pattern : patterns) {
        DateTimeFormatter dtf = DateTimeFormat.forPattern(pattern).withZone(this.timeZone);
        try {
          formattedDate = dtf.parseDateTime(value.getAsString()).withZone(DateTimeZone.forID("UTC")).getMillis();
          if (Boolean.valueOf(this.state.getProp(ConfigurationKeys.CONVERTER_IS_EPOCH_TIME_IN_SECONDS))) {
            formattedDate = (Long) formattedDate / 1000;
          }
          break;
        } catch (Exception e) {
          patternFailCount++;
        }
      }

      if (patternFailCount == patterns.size()) {
        throw new RuntimeException("Failed to parse the date");
      }

      return formattedDate;
    }

    @Override
    public org.apache.avro.Schema.Type getTargetType() {
      return Schema.Type.LONG;
    }
  }

  public static class BinaryConverter extends JsonElementConverter {
    private String charSet;

    public BinaryConverter(String fieldName, boolean nullable, String sourceType, String charSet) {
      super(fieldName, nullable, sourceType);
      this.charSet = charSet;
    }

    @Override
    Object convertField(JsonElement value) {
      try {
        return ByteBuffer.wrap(value.getAsString().getBytes(this.charSet));
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public org.apache.avro.Schema.Type getTargetType() {
      return Schema.Type.BYTES;
    }
  }

  public static abstract class ComplexConverter extends JsonElementConverter {
    private JsonElementConverter elementConverter;

    public ComplexConverter(String fieldName, boolean nullable, String sourceType) {
      super(fieldName, nullable, sourceType);
    }

    protected void setElementConverter(JsonElementConverter elementConverter) {
      this.elementConverter = elementConverter;
    }

    public JsonElementConverter getElementConverter() {
      return this.elementConverter;
    }

    protected void processNestedItems(String fieldName, JsonObject schemaNode, WorkUnitState state,
        JsonElement arrayItems)
        throws UnsupportedDateTypeException {
      if (arrayItems.isJsonPrimitive()) {
        this.setElementConverter(
            getConvertor(fieldName, "", arrayItems.getAsString(), getDataType(schemaNode), state, isNullable()));
      } else if (arrayItems.isJsonObject()) {
        String nestedType = getType(getDataTypeTypeFromSchema(arrayItems)).getAsString();
        JsonObject asJsonObject = arrayItems.getAsJsonObject();
        this.setElementConverter(getConvertor(fieldName, "", nestedType, asJsonObject, state, isNullable()));
      } else if (arrayItems.isJsonArray() || arrayItems.isJsonNull()) {
        throw new UnsupportedOperationException("Array types only allow values in schema as Primitive or a JsonObject");
      }
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
    public org.apache.avro.Schema.Type getTargetType() {
      return Schema.Type.ARRAY;
    }

    @Override
    public Schema schema() {
      Schema schema = Schema.createArray(getElementConverter().schema());
      schema.addProp("source.type", "array");
      return schema;
    }
  }

  public static class ArrayConverter extends ComplexConverter {

    public ArrayConverter(String fieldName, boolean nullable, String sourceType, JsonObject schemaNode,
        WorkUnitState state)
        throws UnsupportedDateTypeException {
      super(fieldName, nullable, sourceType);
      JsonElement arrayItems = getItemsWithinDataType(getDataType(schemaNode));
      processNestedItems(fieldName, schemaNode, state, arrayItems);
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
    public org.apache.avro.Schema.Type getTargetType() {
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

    public MapConverter(String fieldName, boolean nullable, String sourceType, JsonObject schemaNode,
        WorkUnitState state)
        throws UnsupportedDateTypeException {
      super(fieldName, nullable, sourceType);
      JsonElement mapValues = getValuesWithinDataType(schemaNode);
      processNestedItems(fieldName, schemaNode, state, mapValues);
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
    public org.apache.avro.Schema.Type getTargetType() {
      return Schema.Type.MAP;
    }

    @Override
    public Schema schema() {
      Schema schema = Schema.createMap(getElementConverter().schema());
      schema.addProp("source.type", "map");
      return schema;
    }
  }

  public static class RecordConverter extends ComplexConverter {

    private HashMap<String, JsonElementConverter> converters = new HashMap<>();
    private JsonObject _schemaNode;
    private Schema _schema;

    public RecordConverter(String fieldName, boolean nullable, String sourceType, JsonObject schemaNode,
        WorkUnitState state, String namespace)
        throws UnsupportedDateTypeException {
      super(fieldName, nullable, sourceType);
      _schemaNode = schemaNode;
      _schema = buildRecordSchema(getValuesWithinDataType(_schemaNode).getAsJsonArray(), state,
          getPropOrBlankString(getDataType(_schemaNode), "name"), namespace);
    }

    public Schema buildRecordSchema(JsonArray schema, WorkUnitState workUnit, String name, String namespace) {
      List<Schema.Field> fields = new ArrayList<>();
      for (JsonElement elem : schema) {
        JsonObject map = (JsonObject) elem;

        String columnName = getPropOrBlankString(map, "columnName");
        String comment = getPropOrBlankString(map, "comment");
        boolean nullable = getBooleanIfExists(map, "isNullable");
        Schema fldSchema;
        String childNamespace = buildNamespace(namespace, name);
        JsonArray dataTypeType = getDataTypeTypeFromSchema(map);
        JsonElementConverter converter;
        String sourceType;
        try {
          sourceType = dataTypeType.size() == 2 ? "union" : getFirstType(dataTypeType).getAsString();
          converter = getConvertor(columnName, childNamespace, sourceType, map, workUnit, nullable);
          this.converters.put(columnName, converter);
          fldSchema = converter.schema();
        } catch (UnsupportedDateTypeException e) {
          throw new UnsupportedOperationException(e);
        }

        Schema.Field fld =
            new Schema.Field(columnName, fldSchema, comment, nullable ? JsonNodeFactory.instance.nullNode() : null);
        fld.addProp("source.type", sourceType);
        fields.add(fld);
      }
      Schema avroSchema = Schema.createRecord(name.isEmpty() ? null : name, "", namespace, false);
      avroSchema.setFields(fields);

      return avroSchema;
    }

    @Override
    Object convertField(JsonElement value) {
      GenericRecord avroRecord = new GenericData.Record(_schema);

      for (Map.Entry<String, JsonElement> entry : ((JsonObject) value).entrySet()) {
        JsonElementConverter converter = this.converters.get(entry.getKey());
        avroRecord.put(entry.getKey(), converter.convert(entry.getValue()));
      }
      return avroRecord;
    }

    @Override
    public org.apache.avro.Schema.Type getTargetType() {
      return Schema.Type.RECORD;
    }

    @Override
    public Schema schema() {
      Schema schema = _schema;
      schema.addProp("source.type", "record");
      return schema;
    }
  }

  public static class EnumConverter extends JsonElementConverter {
    String enumName;
    String namespace;
    List<String> enumSet = new ArrayList<>();
    Schema schema;

    public EnumConverter(String fieldName, boolean nullable, String sourceType, JsonObject schemaNode,
        String namespace) {
      super(fieldName, nullable, sourceType);

      JsonObject dataType = getDataType(schemaNode);
      for (JsonElement elem : dataType.get("symbols").getAsJsonArray()) {
        this.enumSet.add(elem.getAsString());
      }
      String enumName = getPropOrBlankString(dataType, "name");
      this.enumName = enumName.isEmpty() ? null : enumName;
      this.namespace = namespace;
    }

    @Override
    Object convertField(JsonElement value) {
      return new GenericData.EnumSymbol(this.schema, value.getAsString());
    }

    @Override
    public org.apache.avro.Schema.Type getTargetType() {
      return Schema.Type.ENUM;
    }

    @Override
    public Schema schema() {
      this.schema = Schema.createEnum(this.enumName, "", namespace, this.enumSet);
      this.schema.addProp("source.type", "enum");
      return this.schema;
    }
  }

  public static class NullConverter extends JsonElementConverter {

    public NullConverter(String fieldName, String sourceType) {
      super(fieldName, true, sourceType);
    }

    @Override
    Object convertField(JsonElement value) {
      return value.getAsJsonNull();
    }

    @Override
    public org.apache.avro.Schema.Type getTargetType() {
      return Schema.Type.NULL;
    }
  }

  public static class UnionConverter extends JsonElementConverter {
    private final Schema firstSchema;
    private final Schema secondSchema;
    private final JsonElementConverter firstConverter;
    private final JsonElementConverter secondConverter;

    public UnionConverter(String fieldName, String sourceType, JsonObject schemaNode, WorkUnitState state) {
      super(fieldName, getBooleanIfExists(schemaNode, "isNullable"), sourceType);
      JsonArray types = buildUnionSchema(schemaNode);
      String columnName = getPropOrBlankString(schemaNode, "columnName");
      boolean nullable = getBooleanIfExists(schemaNode, "isNullable");
      firstConverter = getConverter(getFirstType(types), columnName, state, nullable);
      secondConverter = getConverter(getSecondType(types), columnName, state, nullable);
      firstSchema = firstConverter.schema();
      secondSchema = secondConverter.schema();
    }

    private JsonElementConverter getConverter(JsonElement schemaElement, String columnName, WorkUnitState state,
        boolean nullable) {
      JsonElement type = getType(getDataTypeTypeFromSchema(schemaElement));
      if (type.isJsonPrimitive()) {
        try {
          return getConvertor(columnName, null, type.getAsString(), schemaElement.getAsJsonObject(), state, nullable);
        } catch (UnsupportedDateTypeException e) {
          throw new UnsupportedOperationException(e);
        }
      } else if (type.isJsonObject()) {
        try {
          return getConvertor(columnName, null, getType(getDataTypeTypeFromSchema(type)).getAsString(),
              schemaElement.getAsJsonObject(), state, nullable);
        } catch (UnsupportedDateTypeException e) {
          throw new UnsupportedOperationException(e);
        }
      } else {
        throw new UnsupportedOperationException("Unexpected data type in UNION type " + type.toString());
      }
    }

    private JsonArray buildUnionSchema(JsonElement schemaElement) {
      JsonArray dataType = getDataTypeTypeFromSchema(schemaElement);
      JsonArray unionSchema = new JsonArray();
      unionSchema.add(createType(schemaElement, getFirstType(dataType)));
      unionSchema.add(createType(schemaElement, getSecondType(dataType)));
      return unionSchema;
    }

    private JsonElement createType(JsonElement schemaElement, JsonElement type) {
      JsonElement otherSchema = new JsonParser().parse(schemaElement.toString());
      if (type.isJsonObject()) {
        otherSchema.getAsJsonObject().add("dataType", getDataType(type.getAsJsonObject()));
      } else {
        getDataType(otherSchema.getAsJsonObject()).add("type", type.getAsJsonPrimitive());
      }
      return otherSchema;
    }

    @Override
    Object convertField(JsonElement value) {
      try {
        return firstConverter.convert(value);
      } catch (Exception e) {
        return secondConverter.convert(value);
      }
    }

    @Override
    public Schema.Type getTargetType() {
      return Schema.Type.UNION;
    }

    @Override
    protected Schema schema() {
      return Schema.createUnion(firstSchema, secondSchema);
    }
  }
}
