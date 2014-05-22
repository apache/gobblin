package com.linkedin.uif.converter.csv;

import java.util.List;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.converter.Converter;
import com.linkedin.uif.converter.DataConversionException;
import com.linkedin.uif.converter.SchemaConversionException;

public class CsvToJsonConverter implements Converter<String, JsonArray, String, JsonObject>
{
    /**
     * Take in an input schema of type string, the schema must be in JSON format
     * @return a JsonArray representation of the schema
     */
    @Override
    public JsonArray convertSchema(String inputSchema, WorkUnitState workUnit) throws SchemaConversionException
    {        
        JsonParser jsonParser = new JsonParser();
        JsonElement jsonSchema = jsonParser.parse(inputSchema);
        return jsonSchema.getAsJsonArray();
    }

    /**
     * Takes in a record with format String and splits the data based on SOURCE_SCHEMA_DELIMITER
     * Uses the inputSchema and the split record to convert the record to a JsonObject
     * @return a JsonObject representing the record
     */
    @Override
    public JsonObject convertRecord(JsonArray outputSchema, String inputRecord, WorkUnitState workUnit) throws DataConversionException
    {
        List<String> recordSplit = Lists.newArrayList(Splitter.on(workUnit.getProp(ConfigurationKeys.SOURCE_SCHEMA_DELIMITER)).split(inputRecord));
        JsonObject outputRecord = new JsonObject();
        
        for (int i = 0; i < outputSchema.size(); i++) {
            if (i < recordSplit.size()) {
                if (recordSplit.get(i).isEmpty()) {
                    outputRecord.add(outputSchema.get(i).getAsJsonObject().get("columnName").getAsString(), JsonNull.INSTANCE);
                } else {
                    outputRecord.addProperty(outputSchema.get(i).getAsJsonObject().get("columnName").getAsString(), recordSplit.get(i));
                }
            } else {
                outputRecord.add(outputSchema.get(i).getAsString(), JsonNull.INSTANCE);
            }
        }
        return outputRecord;
    }
}
