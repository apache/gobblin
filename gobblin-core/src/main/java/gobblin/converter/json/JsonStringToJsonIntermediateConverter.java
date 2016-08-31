package gobblin.converter.json;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


/**
 * Converts a json string to a {@link JsonObject}.
 */
public class JsonStringToJsonIntermediateConverter extends Converter<String, JsonArray, String, JsonObject> {

  private static Logger log = LoggerFactory.getLogger(JsonStringToJsonIntermediateConverter.class);

  /**
   * Take in an input schema of type string, the schema must be in JSON format
   * @return a JsonArray representation of the schema
   */
  @Override
  public JsonArray convertSchema(String inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    JsonParser jsonParser = new JsonParser();
    log.info("Schema: " + inputSchema);
    JsonElement jsonSchema = jsonParser.parse(inputSchema);
    return jsonSchema.getAsJsonArray();
  }

  /**
   * Takes in a record with format String and Uses the inputSchema to convert the record to a JsonObject
   * @return a JsonObject representing the record
   * @throws IOException
   */
  @Override
  public Iterable<JsonObject> convertRecord(JsonArray outputSchema, String strInputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    JsonParser jsonParser = new JsonParser();
    JsonObject inputRecord = (JsonObject) jsonParser.parse(strInputRecord);
    return Lists.newArrayList(inputRecord);
  }
}
