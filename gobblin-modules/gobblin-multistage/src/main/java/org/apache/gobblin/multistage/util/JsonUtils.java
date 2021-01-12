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

package org.apache.gobblin.multistage.util;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.Map;


public interface JsonUtils {
  static Gson GSON = new Gson();

  /**
   * This deepCopy is a workaround. When it is possible to upgrade Gson to 2.8.1+,
   * we shall change code to use Gson deepCopy.
   *
   * This function is intended to use use small Json, like schema objects. It is not
   * suitable to deep copy large Json objects.
   *
   * @param source the source Json object, can be JsonArray, JsonObject, or JsonPrimitive
   * @return the deeply copied Json object
   */
  static JsonElement deepCopy(JsonElement source) {
    return GSON.fromJson(source.toString(), source.getClass());
  }

  /**
   * Check if JsonObject A contains everything in b
   * @param superObject the super set JsonObject
   * @param subObject the sub set JsonObject
   * @return if superObject doesn't have an element in b, or the value of an element in superObject differs with
   * the same element in b, return false, else return true
   */
  static boolean contains(JsonObject superObject, JsonObject subObject) {
    for (Map.Entry<String, JsonElement> entry: subObject.entrySet()) {
      if (!superObject.has(entry.getKey())
          || !superObject.get(entry.getKey()).toString().equalsIgnoreCase(entry.getValue().toString())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check if JsonObject A contains everything in b
   * @param superString the super set JsonObject
   * @param subObject the sub set JsonObject
   * @return if a doesn't have an element in b, or the value of an element in a differs with
   * the same element in b, return false, else return true
   */
  static boolean contains(String superString, JsonObject subObject) {
    JsonObject a = GSON.fromJson(superString, JsonObject.class);
    return contains(a, subObject);
  }

  /**
   * Replace parts of Original JsonObject with substitutes
   * @param origObject the original JsonObject
   * @param newComponent the substitution values
   * @return the replaced JsonObject
   */
  static JsonObject replace(JsonObject origObject, JsonObject newComponent) {
    JsonObject replacedObject = new JsonObject();
    for (Map.Entry<String, JsonElement> entry: origObject.entrySet()) {
      if (newComponent.has(entry.getKey())) {
        replacedObject.add(entry.getKey(), newComponent.get(entry.getKey()));
      } else {
        replacedObject.add(entry.getKey(), entry.getValue());
      }
    }
    return replacedObject;
  }
}
