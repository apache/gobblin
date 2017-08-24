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

import com.google.common.base.Charsets;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


/**
 * Converter that takes a UTF-8 encoded JSON string and converts it to a {@link JsonObject}
 */
public class BytesToJsonConverter extends Converter<String, String, byte[], JsonObject> {

  @Override
  public String convertSchema(String inputSchema, WorkUnitState workUnit) {
      return inputSchema;
  }

  @Override
  public Iterable<JsonObject> convertRecord(String outputSchema, byte[] inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    if (inputRecord == null) {
      throw new DataConversionException("Input record is null");
    }

    String jsonString = new String(inputRecord, Charsets.UTF_8);
    JsonParser parser = new JsonParser();
    JsonObject outputRecord = parser.parse(jsonString).getAsJsonObject();

    return new SingleRecordIterable<>(outputRecord);
  }
}
