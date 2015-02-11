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

import gobblin.converter.Converter;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.DataConversionException;


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
    return new JsonParser().parse(inputSchema).getAsJsonArray();
  }

  @Override
  public Iterable<JsonObject> convertRecord(JsonArray outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    Map<String, Object> record = new HashMap<String, Object>();
    for (Field field : inputRecord.getSchema().getFields()) {
      Object col = inputRecord.get(field.name());
      if (col != null && col instanceof Utf8) {
        col = col.toString();
      }
      record.put(field.name(), col);
    }

    return new SingleRecordIterable<JsonObject>(
        this.gson.fromJson(this.gson.toJson(record), JsonObject.class).getAsJsonObject());
  }
}
