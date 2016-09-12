package gobblin.converter.json;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;


/**
 * A {@link Converter} to transform {@link JsonObject} to strings.
 */
public class JsonToStringConverter extends Converter<String, String, JsonObject, String> {

  private static Gson GSON = new Gson();

  @Override
  public String convertSchema(String inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return inputSchema;
  }

  @Override
  public Iterable<String> convertRecord(String outputSchema, JsonObject inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    return Lists.newArrayList(GSON.toJson(inputRecord));
  }

}
