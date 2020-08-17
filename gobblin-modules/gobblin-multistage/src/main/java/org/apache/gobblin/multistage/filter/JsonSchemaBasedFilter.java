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

package org.apache.gobblin.multistage.filter;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.util.Map;
import org.apache.gobblin.multistage.util.JsonElementTypes;
import org.apache.gobblin.multistage.util.JsonIntermediateSchema;
import org.apache.gobblin.multistage.util.JsonUtils;


/**
 * Filter Json records by Json Intermediate schema
 *
 * TODO handle UNIONs
 *
 * @author kgoodhop, chrli
 *
 */
public class JsonSchemaBasedFilter extends MultistageSchemaBasedFilter<JsonObject> {

  public JsonSchemaBasedFilter(JsonIntermediateSchema schema) {
    super(schema);
  }

  /**
   * top level filter function
   * @param input the input row object
   * @return the filtered row object
   */
  @Override
  public JsonObject filter(JsonObject input) {
    return this.filter(schema, input);
  }

  private JsonElement filter(JsonIntermediateSchema.JisDataType dataType, JsonElement input) {
    if (dataType.isPrimitive()) {
      return input.isJsonPrimitive() ? filter(dataType, input.getAsJsonPrimitive()) : null;
    } else if (dataType.getType() == JsonElementTypes.RECORD) {
      return filter(dataType.getChildRecord(), input.getAsJsonObject());
    } else if (dataType.getType() == JsonElementTypes.ARRAY) {
      return filter(dataType.getItemType(), input.getAsJsonArray());
    }
    return null;
  }

  private JsonPrimitive filter(JsonIntermediateSchema.JisDataType dataType, JsonPrimitive input) {
    return dataType.isPrimitive() ? JsonUtils.deepCopy(input).getAsJsonPrimitive() : null;
  }

  private JsonObject filter(JsonIntermediateSchema.JisDataType dataType, JsonObject input) {
    JsonObject output = new JsonObject();
    if (dataType.getType() == JsonElementTypes.RECORD) {
      return filter(dataType.getChildRecord(), input);
    }
    return output;
  }

  /**
   * process the JsonArray
   *
   * @param dataType should be the item type of the JsonArray
   * @param input JsonArray object
   * @return filtered JsonArray object
   */
  private JsonArray filter(JsonIntermediateSchema.JisDataType dataType, JsonArray input) {
    JsonArray output = new JsonArray();
    for (JsonElement element: input) {
      output.add(filter(dataType, element));
    }
    return output;
  }

  private JsonObject filter(JsonIntermediateSchema schema, JsonObject input) {
    JsonObject output = new JsonObject();
    for (Map.Entry<String, JsonElement> entry: input.entrySet()) {
      if (schema.getColumns().containsKey(entry.getKey())) {
        output.add(entry.getKey(),
            filter(schema.getColumns().get(entry.getKey()).getDataType(), entry.getValue()));
      }
    }
    return output;
  }

  private JsonArray filter(JsonArray input) {
    JsonArray output = new JsonArray();
    return output;
  }
}
