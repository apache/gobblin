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

package org.apache.gobblin.converter.json;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;


/**
 * A {@link Converter} to transform {@link JsonObject} to strings.
 */
public class JsonToStringConverter extends Converter<String, String, JsonObject, String> {

  private static Gson GSON = new Gson();

  @Override
  public String convertSchema(String inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return inputSchema;
  }

  @Override
  public Iterable<String> convertRecord(String outputSchema, JsonObject inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    return Lists.newArrayList(GSON.toJson(inputRecord));
  }

}
