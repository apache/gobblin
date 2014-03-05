package com.linkedin.uif.converter.avro;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;


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
      WorkUnitState state) throws UnsupportedDateTypeException {
    boolean nullable = schemaNode.has("nullable") ? schemaNode.get("nullable").getAsBoolean() : false;

    Type type;
    try {
      type = Type.valueOf(fieldType.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new UnsupportedDateTypeException(fieldType + " is unsupported");
    }

    DateTimeZone timeZone = getTimeZone(state.getProp(ConfigurationKeys.CONVERTER_AVRO_DATE_TIMEZONE, "UTC"));
    switch (type) {
      case DATE:
        return new DateConverter(fieldName, nullable, state.getProp(ConfigurationKeys.CONVERTER_AVRO_DATE_FORMAT,
            "yyyy-MM-dd HH:mm:ss"), timeZone);

      case TIMESTAMP:
        return new DateConverter(fieldName, nullable, state.getProp(ConfigurationKeys.CONVERTER_AVRO_TIMESTAMP_FORMAT,
            "yyyy-MM-dd HH:mm:ss"), timeZone);

      case TIME:
        return new DateConverter(fieldName, nullable, state.getProp(ConfigurationKeys.CONVERTER_AVRO_TIME_FORMAT,
            "yyyy-MM-dd HH:mm:ss"), timeZone);

      case FIXED:
        throw new UnsupportedDateTypeException(fieldType + " is unsupported");

      case STRING:
        return new StringConverter(fieldName, nullable);

      case BYTES:
        return new BinaryConverter(fieldName, nullable, state.getProp(ConfigurationKeys.CONVERTER_AVRO_BINARY_CHARSET,
            "UTF8"));

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

      case ARRAY:
        return new ArrayConverter(fieldName, nullable, schemaNode, state);

      case MAP:
        return new MapConverter(fieldName, nullable, schemaNode, state);

      case ENUM:
        return new EnumConverter(fieldName, nullable, schemaNode);

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
      if (timeZone == null)
        throw new IllegalArgumentException("TimeZone " + id + " not recognized");
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
     * avro schema for the converted type
     * @return
     */
    public Schema getSchema() {
      if (nullable) {
        List<Schema> list = new ArrayList<Schema>();
        list.add(schema());
        list.add(Schema.create(Schema.Type.NULL));
        return Schema.createUnion(list);
      } else {
        return schema();
      }
    }

    protected Schema schema() {
      Schema schema = Schema.create(getTargetType());
      schema.addProp("source.type", getTargetType().toString().toLowerCase());
      return schema;
    }

    /**
     * Convert value
     * @param value if empty will return null if allowed or exception if not allowed
     * @return Avro safe type
     */
    abstract public Object convert(JsonElement value);

    /**
     * Avro data type after conversion
     * @return
     */
    public abstract Schema.Type getTargetType();
  }

  public static class StringConverter extends JsonElementConverter {

    public StringConverter(String fieldName, boolean nullable) {
      super(fieldName, nullable);
    }

    @Override
    public Object convert(JsonElement value) {
      return new Utf8(value.getAsString());
    }

    @Override
    public org.apache.avro.Schema.Type getTargetType() {
      return Schema.Type.STRING;
    }
  }

  public static class IntConverter extends JsonElementConverter {

    public IntConverter(String fieldName, boolean nullable) {
      super(fieldName, nullable);
    }

    @Override
    public Object convert(JsonElement value) {

      return value.getAsInt();
    }

    @Override
    public org.apache.avro.Schema.Type getTargetType() {
      return Schema.Type.INT;
    }
  }

  public static class LongConverter extends JsonElementConverter {

    public LongConverter(String fieldName, boolean nullable) {
      super(fieldName, nullable);
    }

    @Override
    public Object convert(JsonElement value) {

      return value.getAsLong();
    }

    @Override
    public org.apache.avro.Schema.Type getTargetType() {
      return Schema.Type.LONG;

    }
  }

  public static class DoubleConverter extends JsonElementConverter {

    public DoubleConverter(String fieldName, boolean nullable) {
      super(fieldName, nullable);
    }

    @Override
    public Object convert(JsonElement value) {
      return value.getAsDouble();
    }

    @Override
    public org.apache.avro.Schema.Type getTargetType() {
      return Schema.Type.DOUBLE;

    }
  }

  public static class FloatConverter extends JsonElementConverter {

    public FloatConverter(String fieldName, boolean nullable) {
      super(fieldName, nullable);
    }

    @Override
    public Object convert(JsonElement value) {
      return value.getAsFloat();
    }

    @Override
    public org.apache.avro.Schema.Type getTargetType() {
      return Schema.Type.FLOAT;

    }
  }

  public static class BooleanConverter extends JsonElementConverter {

    public BooleanConverter(String fieldName, boolean nullable) {
      super(fieldName, nullable);
    }

    @Override
    public Object convert(JsonElement value) {

      return value.getAsBoolean();
    }

    @Override
    public org.apache.avro.Schema.Type getTargetType() {
      return Schema.Type.BOOLEAN;

    }
  }

  public static class DateConverter extends JsonElementConverter {
    private DateTimeFormatter dtf;

    public DateConverter(String fieldName, boolean nullable, String pattern, DateTimeZone zone) {
      super(fieldName, nullable);
      dtf = DateTimeFormat.forPattern(pattern).withZone(zone);
    }

    @Override
    public Object convert(JsonElement value) {
      return dtf.parseDateTime(value.getAsString()).getMillis();
    }

    @Override
    public org.apache.avro.Schema.Type getTargetType() {
      return Schema.Type.LONG;

    }
  }

  public static class BinaryConverter extends JsonElementConverter {
    private String charSet;

    public BinaryConverter(String fieldName, boolean nullable, String charSet) {
      super(fieldName, nullable);
      this.charSet = charSet;
    }

    @Override
    public Object convert(JsonElement value) {
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

    public ComplexConverter(String fieldName, boolean nullable, JsonObject schemaNode, WorkUnitState state)
        throws UnsupportedDateTypeException {
      super(fieldName, nullable);
    }

    protected void setElementConverter(JsonElementConverter elementConverter) {
      this.elementConverter = elementConverter;
    }

    public JsonElementConverter getElementConverter() {
      return elementConverter;
    }
  }

  public static class ArrayConverter extends ComplexConverter {

    public ArrayConverter(String fieldName, boolean nullable, JsonObject schemaNode, WorkUnitState state)
        throws UnsupportedDateTypeException {
      super(fieldName, nullable, schemaNode, state);
      super.setElementConverter(getConvertor(fieldName, schemaNode.get("dataType").getAsJsonObject().get("items")
          .getAsString(), schemaNode.get("dataType").getAsJsonObject(), state));

    }

    @Override
    public Object convert(JsonElement value) {
      List<Object> list = new ArrayList<Object>();

      for (JsonElement elem : (JsonArray) value) {
        list.add(getElementConverter().convert(elem));
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

    public MapConverter(String fieldName, boolean nullable, JsonObject schemaNode, WorkUnitState state)
        throws UnsupportedDateTypeException {
      super(fieldName, nullable, schemaNode, state);
      super.setElementConverter(getConvertor(fieldName, schemaNode.get("dataType").getAsJsonObject().get("values")
          .getAsString(), schemaNode.get("dataType").getAsJsonObject(), state));

    }

    @Override
    public Object convert(JsonElement value) {
      Map<String, Object> map = new HashMap<String, Object>();

      for (Map.Entry<String, JsonElement> entry : ((JsonObject) value).entrySet()) {
        map.put(entry.getKey(), getElementConverter().convert(entry.getValue()));
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

    public EnumConverter(String fieldName, boolean nullable, JsonObject schemaNode) {
      super(fieldName, nullable);

      for (JsonElement elem : schemaNode.get("dataType").getAsJsonObject().get("symbols").getAsJsonArray()) {
        enumSet.add(elem.getAsString());
      }
      enumName = schemaNode.get("dataType").getAsJsonObject().get("name").getAsString();
    }

    @Override
    public Object convert(JsonElement value) {
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
