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
package org.apache.gobblin.elasticsearch.typemapping;

import java.io.IOException;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.typesafe.config.Config;


public class JsonTypeMapper implements TypeMapper<JsonElement> {

  private final JsonSerializer serializer = new GsonJsonSerializer();
  @Override
  public void configure(Config config) {

  }

  @Override
  public JsonSerializer<JsonElement> getSerializer() {
    return serializer;
  }

  @Override
  public String getValue(String fieldName, JsonElement record)
      throws FieldMappingException {
    assert record.isJsonObject();
    JsonObject jsonObject = record.getAsJsonObject();
    if (jsonObject.has(fieldName)) {
      return jsonObject.get(fieldName).getAsString();
    } else {
      throw new FieldMappingException("Could not find field :" + fieldName);
    }
  }

  @Override
  public void close()
      throws IOException {

  }
}
