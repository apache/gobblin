/*
 *
 *  * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 *  * this file except in compliance with the License. You may obtain a copy of the
 *  * License at  http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software distributed
 *  * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 *  * CONDITIONS OF ANY KIND, either express or implied.
 *
 */

package gobblin.couchbase.converter;

import com.couchbase.client.java.document.RawJsonDocument;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;


/**
 * Takes any object in and converts it into a CouchBase compatible
 * {@link com.couchbase.client.java.document.RawJsonDocument}
 * It expects to be configured to pick out the String key field from the incoming Object
 */
public class AnyToCouchbaseJsonConverter extends Converter<String, String, Object, RawJsonDocument> {

  private static final Gson GSON = new Gson();
  private String keyField = "key";

  @Override
  public String convertSchema(String inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    //TODO: Use the schema and config to determine which fields to pull out
    return "";
  }

  @Override
  public Iterable<RawJsonDocument> convertRecord(String outputSchema, Object inputRecord, WorkUnitState workUnit)
      throws DataConversionException {

    JsonElement jsonElement = GSON.toJsonTree(inputRecord);
    if (!jsonElement.isJsonObject())
    {
      throw new DataConversionException("Expecting json element " + jsonElement.toString()
          + " to be of type JsonObject.");
    }

    JsonObject jsonObject = jsonElement.getAsJsonObject();

    if (!jsonObject.has(keyField))
    {
      throw new DataConversionException("Could not find key field " + keyField
          + " in json object " + jsonObject.toString());
    }

    JsonElement keyValueElement = jsonObject.get(keyField);
    String keyString;
    try {
      keyString = keyValueElement.getAsString();
    }
    catch (Exception e)
    {
      throw new DataConversionException("Could not get the key " + keyValueElement.toString() + " as a string", e);
    }
    String valueString = GSON.toJson(jsonElement);

    RawJsonDocument jsonDocument = RawJsonDocument.create(keyString, valueString);
    return new SingleRecordIterable<>(jsonDocument);
  }
}
