package gobblin.converter.csv;

import java.math.BigDecimal;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;


/**
 * This converter simply converts double string than ends with % to a validate double number.
 * e.g. 99% -> 0.99
 */
public class PercentageDoubleStringConverter extends Converter<JsonArray, JsonArray, JsonObject, JsonObject> {
  @Override
  public Converter<JsonArray, JsonArray, JsonObject, JsonObject> init(WorkUnitState workUnit) {
    super.init(workUnit);
    return this;
  }

  @Override
  public JsonArray convertSchema(JsonArray inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return inputSchema;
  }

  @Override
  public Iterable<JsonObject> convertRecord(JsonArray outputSchema, JsonObject inputRecord, WorkUnitState workUnit)
      throws DataConversionException {

    JsonObject outputRecord = new JsonObject();
    for (int i = 0; i < outputSchema.size(); i++) {
      JsonElement schemaElement = outputSchema.get(i);
      String col = getColumnName(schemaElement);
      JsonElement record = inputRecord.get(col);

      if ("double".equals(getTypeName(schemaElement))) {
        String recordString = record.getAsString();
        if (recordString.endsWith("%")) {
          outputRecord.addProperty(col, convertPercentage(recordString));
          continue;
        }
      }
      outputRecord.add(col, record);
    }

    return new SingleRecordIterable<>(outputRecord);
  }

  private String convertPercentage(String record) {
    BigDecimal d = new BigDecimal(record.substring(0, record.length() - 1));
    return d.divide(BigDecimal.valueOf(100)).toString();
  }

  private static String getColumnName(JsonElement schemaElement) {
    return schemaElement.getAsJsonObject().get("columnName").getAsString();
  }

  private static String getTypeName(JsonElement schemaElement) {
    return schemaElement.getAsJsonObject().get("dataType").getAsJsonObject().get("type").getAsString().toLowerCase();
  }
}
