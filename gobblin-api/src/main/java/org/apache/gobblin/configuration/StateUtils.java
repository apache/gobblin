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

package org.apache.gobblin.configuration;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;


/**
 * Utility class for dealing with {@link State} objects.
 */
public class StateUtils {

  /**
   * Converts a {@link JsonObject} to a {@link State} object. It does not add any keys specified in the excludeKeys array
   */
  public static State jsonObjectToState(JsonObject jsonObject, String... excludeKeys) {
    State state = new State();
    List<String> excludeKeysList = excludeKeys == null ? Lists.<String>newArrayList() : Arrays.asList(excludeKeys);
    for (Map.Entry<String, JsonElement> jsonObjectEntry : jsonObject.entrySet()) {
      if (!excludeKeysList.contains(jsonObjectEntry.getKey())) {
        state.setProp(jsonObjectEntry.getKey(), jsonObjectEntry.getValue().getAsString());
      }
    }
    return state;
  }
}
