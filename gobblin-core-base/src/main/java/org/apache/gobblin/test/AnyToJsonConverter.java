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
package org.apache.gobblin.test;

import java.util.Collections;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.util.io.GsonInterfaceAdapter;


/**
 * Converts any Object into a Json object
 */
public class AnyToJsonConverter extends Converter<String, String, Object, JsonElement> {
  private static final Gson GSON = GsonInterfaceAdapter.getGson(Object.class);
  private boolean stripTopLevelType = true; // TODO: Configure

  @Override
  public String convertSchema(String inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return "";
  }

  @Override
  public Iterable<JsonElement> convertRecord(String outputSchema, Object inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    JsonElement jsonElement = GSON.toJsonTree(inputRecord);
    // The interface adapter packs everything into object-type, object-data pairs.
    // Strip out the top level.
    if (stripTopLevelType) {
      jsonElement = jsonElement.getAsJsonObject().get("object-data");
    }
    return Collections.singletonList(jsonElement);
  }
}
