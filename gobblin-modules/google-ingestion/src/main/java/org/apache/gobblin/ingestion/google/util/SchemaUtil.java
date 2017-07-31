package org.apache.gobblin.ingestion.google.util;

import com.google.gson.JsonObject;

import org.apache.gobblin.converter.avro.JsonElementConversionFactory;


public class SchemaUtil {
  public static JsonObject createColumnJson(String columnName, boolean isNullable,
      JsonElementConversionFactory.Type columnType) {
    JsonObject columnJson = new JsonObject();
    columnJson.addProperty("columnName", columnName);
    columnJson.addProperty("isNullable", isNullable);

    JsonObject typeJson = new JsonObject();
    typeJson.addProperty("type", columnType.toString());
    columnJson.add("dataType", typeJson);

    return columnJson;
  }
}
