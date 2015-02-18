/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.converter.avro;

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
import org.apache.avro.util.Utf8;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import sun.util.calendar.ZoneInfo;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;


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
    ENUM
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
        return new EnumConverter(fieldName, nullable, type.toString(), schemaNode);

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
      return nullable;
    }

    /**
     * avro schema for the converted type
     * @return
     */
    public Schema getSchema() {
      if (nullable) {
        List<Schema> list = new ArrayList<Schema>();
        list.add(Schema.create(Schema.Type.NULL));
        list.add(schema());
        return Schema.createUnion(list);
      } else {
        return schema();
      }
    }

    protected Schema schema() {
      Schema schema = Schema.create(getTargetType());
      schema.addProp("source.type", sourceType.toLowerCase());
      return schema;
    }

    /**
     * Convert value
     * @param value is JsonNull will return null if allowed or exception if not allowed
     * @return Avro safe type
     */
    public Object convert(JsonElement value) {
      if (value.isJsonNull()) {
        if (nullable) {
          return null;
        } else {
          throw new RuntimeException("Field: " + getName() + " is not nullable and contains a null value");
        }
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
        return ByteBuffer.wrap(value.getAsString().getBytes(charSet));
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

    public ComplexConverter(String fieldName, boolean nullable, String sourceType, JsonObject schemaNode,
        WorkUnitState state)
        throws UnsupportedDateTypeException {
      super(fieldName, nullable, sourceType);
    }

    protected void setElementConverter(JsonElementConverter elementConverter) {
      this.elementConverter = elementConverter;
    }

    public JsonElementConverter getElementConverter() {
      return elementConverter;
    }
  }

  public static class ArrayConverter extends ComplexConverter {

    public ArrayConverter(String fieldName, boolean nullable, String sourceType, JsonObject schemaNode,
        WorkUnitState state)
        throws UnsupportedDateTypeException {
      super(fieldName, nullable, sourceType, schemaNode, state);
      super.setElementConverter(
          getConvertor(fieldName, schemaNode.get("dataType").getAsJsonObject().get("items").getAsString(),
              schemaNode.get("dataType").getAsJsonObject(), state, isNullable()));
    }

    @Override
    Object convertField(JsonElement value) {
      List<Object> list = new ArrayList<Object>();

      for (JsonElement elem : (JsonArray) value) {
        list.add(getElementConverter().convertField(elem));
      }

      return new GenericData.Array<Object>(schema(), list);
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
      super(fieldName, nullable, sourceType, schemaNode, state);
      super.setElementConverter(
          getConvertor(fieldName, schemaNode.get("dataType").getAsJsonObject().get("values").getAsString(),
              schemaNode.get("dataType").getAsJsonObject(), state, isNullable()));
    }

    @Override
    Object convertField(JsonElement value) {
      Map<String, Object> map = new HashMap<String, Object>();

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

  public static class EnumConverter extends JsonElementConverter {
    String enumName;
    List<String> enumSet = new ArrayList<String>();
    Schema schema;

    public EnumConverter(String fieldName, boolean nullable, String sourceType, JsonObject schemaNode) {
      super(fieldName, nullable, sourceType);

      for (JsonElement elem : schemaNode.get("dataType").getAsJsonObject().get("symbols").getAsJsonArray()) {
        enumSet.add(elem.getAsString());
      }
      enumName = schemaNode.get("dataType").getAsJsonObject().get("name").getAsString();
    }

    @Override
    Object convertField(JsonElement value) {
      return new GenericData.EnumSymbol(schema, value.getAsString());
    }

    @Override
    public org.apache.avro.Schema.Type getTargetType() {
      return Schema.Type.ENUM;
    }

    @Override
    public Schema schema() {
      schema = Schema.createEnum(enumName, "", "", enumSet);
      schema.addProp("source.type", "enum");
      return schema;
    }
  }
}
