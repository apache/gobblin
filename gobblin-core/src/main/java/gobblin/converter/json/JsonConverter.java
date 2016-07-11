package gobblin.converter.json;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;

import java.io.IOException;
import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class JsonConverter extends Converter<String, JsonArray, String, JsonObject> {

  /**
   * Take in an input schema of type string, the schema must be in JSON format
   * @return a JsonArray representation of the schema
   */
  @Override
  public JsonArray convertSchema(String inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    JsonParser jsonParser = new JsonParser();
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
    JsonObject outputRecord = new JsonObject();

    for (int i = 0; i < outputSchema.size(); i++) {
      String expectedColumnName = outputSchema.get(i).getAsJsonObject().get("columnName").getAsString();

      if (inputRecord.has(expectedColumnName)) {
        //As currently Gobblin is not able to handle complex schema's so storing it as string

        if (inputRecord.get(expectedColumnName).isJsonArray()) {
          outputRecord.addProperty(expectedColumnName, inputRecord.get(expectedColumnName).toString());
        } else if (inputRecord.get(expectedColumnName).isJsonObject()) {
          //To check if internally in an JsonObject there is multiple hierarchy
          Boolean isMultiHierarchyInsideJsonObject = false;
          for (Map.Entry<String, JsonElement> entry : ((JsonObject) inputRecord.get(expectedColumnName)).entrySet()) {
            if (entry.getValue().isJsonArray() || entry.getValue().isJsonObject()) {
              isMultiHierarchyInsideJsonObject = true;
              break;
            }
          }
          if (isMultiHierarchyInsideJsonObject) {
            outputRecord.addProperty(expectedColumnName, inputRecord.get(expectedColumnName).toString());
          } else {
            outputRecord.add(expectedColumnName, inputRecord.get(expectedColumnName));
          }

        } else {
          outputRecord.add(expectedColumnName, inputRecord.get(expectedColumnName));
        }
      } else {
        outputRecord.add(expectedColumnName, JsonNull.INSTANCE);
      }

    }
    return new SingleRecordIterable<>(outputRecord);
  }
}
