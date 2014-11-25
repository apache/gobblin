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

package com.linkedin.uif.converter.avro;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.converter.Converter;
import com.linkedin.uif.converter.DataConversionException;
import com.linkedin.uif.converter.SchemaConversionException;

/**
 * Converts Avro record to Json record
 * 
 * @author nveeramr
 * 
 */
public class AvroToJsonConverter extends Converter<String, JsonArray, GenericRecord, JsonObject> {
    private Gson gson;

    @Override
    public Converter<String, JsonArray, GenericRecord, JsonObject> init(WorkUnitState workUnit) {
        this.gson = new GsonBuilder().create();
        return this;
    }

    @Override
    public JsonArray convertSchema(String inputSchema, WorkUnitState workUnit)
            throws SchemaConversionException {
        JsonParser jsonParser = new JsonParser();
        JsonElement jsonSchema = jsonParser.parse(inputSchema.toString());
        JsonArray array = jsonSchema.getAsJsonArray();
        return array;
    }

    @Override
    public JsonObject convertRecord(JsonArray outputSchema, GenericRecord inputRecord,
            WorkUnitState workUnit) throws DataConversionException {
        Map<String, Object> record = new HashMap<String, Object>();
        for (Field field : inputRecord.getSchema().getFields()) {
            Object col = inputRecord.get(field.name());
            if (col != null && col instanceof Utf8) {
                col = col.toString();
            }
            record.put(field.name(), col);
        }
        String json = this.gson.toJson(record);
        JsonElement element = this.gson.fromJson(json.toString(), JsonObject.class);
        JsonObject jsonObject = element.getAsJsonObject();
        return jsonObject;
    }
}
