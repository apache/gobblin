/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.converter.avro;

import java.util.Map;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;


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
  public JsonArray convertSchema(String inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return new JsonParser().parse(inputSchema).getAsJsonArray();
  }

  @Override
  public Iterable<JsonObject> convertRecord(JsonArray outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    Map<String, Object> record = Maps.newHashMap();
    for (Field field : inputRecord.getSchema().getFields()) {
      Object col = inputRecord.get(field.name());
      if (col != null && col instanceof Utf8) {
        col = col.toString();
      }
      record.put(field.name(), col);
    }

    return new SingleRecordIterable<>(this.gson.fromJson(this.gson.toJson(record), JsonObject.class).getAsJsonObject());
  }
}
