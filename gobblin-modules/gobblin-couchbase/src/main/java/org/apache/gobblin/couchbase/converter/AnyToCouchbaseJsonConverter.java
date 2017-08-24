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

package org.apache.gobblin.couchbase.converter;

import com.couchbase.client.java.document.RawJsonDocument;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.apache.gobblin.util.ForkOperatorUtils;


/**
 * Takes any object in and converts it into a CouchBase compatible
 * {@link com.couchbase.client.java.document.RawJsonDocument}
 * It expects to be configured to pick out the String key field from the incoming Object
 */
@Slf4j
public class AnyToCouchbaseJsonConverter extends Converter<String, String, Object, RawJsonDocument> {

  private static final Gson GSON = new Gson();
  private String keyField = "key";

  public static final String KEY_FIELD_CONFIG = "converter.any2couchbase.key.field";


  @Override
  public Converter<String, String, Object, RawJsonDocument> init(WorkUnitState workUnit) {

    String keyFieldPath =
        ForkOperatorUtils.getPropertyNameForBranch(workUnit, KEY_FIELD_CONFIG);

    if (!workUnit.contains(keyFieldPath)) {
      log.warn("No configuration for which field to use as the key. Using the default {}", this.keyField);
    } else {
      this.keyField = workUnit.getProp(keyFieldPath);
      log.info("Using the field {} from config for writing converter", this.keyField);
    }

    return this;
  }

  @Override
  public String convertSchema(String inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
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
