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

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import org.apache.commons.lang.StringUtils;


/**
 * This class is designed to bridge the gaps between Json and Avro in handling nullability and in key words.
 *
 * Nullable non-null elements are defined with NULLABLE prefixes.
 *
 * Different ways of defining a nullable string are:
 * {"name": {"type": [null, "string"]}}
 * {"columnName": "name", "isNullable", "true", "dataType": {"type": "string"}}
 *
 * Differences in naming the element types:
 * - "object" is "record" in avro
 * - "integer" is "int" in avro
 * - "int64" is "long" in avro
 * - "number" is a generic numeric type, and bet matched to "double" in avro
 *
 * we only need nullable version of elements for avro types
 */
public enum JsonElementTypes {

  ARRAY("array", "array", false),
  BOOLEAN("boolean"),
  DATE("date"),
  DOUBLE("double"),
  ENUM("enum"),
  FLOAT("float", "double"),
  INT("int"),
  INTEGER("integer", "int"),
  INT64("int64", "long"),
  LONG("long"),
  NUMBER("number", "double"),
  OBJECT("object", "record", false),
  PRIMITIVE("primitive"),
  RECORD("record", "record", false),
  STRING("string"),
  TIME("time"),
  TIMESTAMP("timestamp"),
  UNION("union", "union", false),
  UNKNOWN("unknown"),
  NULL(true, "null", "null"),
  NULLABLEARRAY(true, "array|null", "array", false),
  NULLABLEBOOLEAN(true, "boolean|null", "boolean"),
  NULLABLEDOUBLE(true, "double|null", "double"),
  NULLABLEINT(true, "int|null", "int"),
  NULLABLELONG(true, "long|null", "long"),
  NULLABLEOBJECT(true, "object|null", "record", false),
  NULLABLERECORD(true, "record|null", "record", false),
  NULLABLESTRING(true, "string|null", "string"),
  NULLABLETIME(true, "time|null", "time"),
  NULLABLETIMESTAMP(true, "timestamp|null", "timestamp");

  private final String name;
  private final String altName;
  private final boolean isPrimitive;
  private final boolean isNullable;

  public static boolean isPrimitive(JsonElementTypes type) {
    return type.isPrimitive();
  }

  /**
   * initialize the enum item with a default name
   * @param name the title of the enum item
   */
  JsonElementTypes(String name) {
    this.name = name;
    this.altName = name;
    this.isPrimitive = true;
    this.isNullable = false;
  }

  /**
   * initialize the enum item with a default name and an alternative name
   * @param name the title of the enum item
   * @param altName the alternative Avro/Json Intermediate name
   */
  JsonElementTypes(String name, String altName) {
    this.name = name;
    this.altName = altName;
    this.isPrimitive = true;
    this.isNullable = false;
  }

  /**
   * initialize the enum item with a default name, an alternative name, and a primitive type
   * @param name the title of the enum item
   * @param altName the alternative Avro/Json Intermediate name
   * @param isPrimitive the primitive type indicator
   */
  JsonElementTypes(String name, String altName, boolean isPrimitive) {
    this.name = name;
    this.altName = altName;
    this.isPrimitive = isPrimitive;
    this.isNullable = false;
  }

    /**
   * initialize the enum item with a nullable flag, a default name, and an alternative name
   * @param name the title of the enum item
   * @param altName the alternative Avro/Json Intermediate name
   */
  JsonElementTypes(boolean isNullable, String name, String altName) {
    this.name = name;
    this.altName = altName;
    this.isPrimitive = true;
    this.isNullable = isNullable;
  }

  /**
   * initialize the enum item with a nullable flag, a default name, an alternative name, and a primitive type
   * @param name the title of the enum item
   * @param altName the alternative Avro/Json Intermediate name
   * @param isPrimitive the primitive type indicator
   */
  JsonElementTypes(boolean isNullable, String name, String altName, boolean isPrimitive) {
    this.name = name;
    this.altName = altName;
    this.isPrimitive = isPrimitive;
    this.isNullable = isNullable;
  }

  @Override
  public String toString() {
    return this.name;
  }

  public String getAltName() {
    return this.altName;
  }

  /**
   * check if the enum is nullable
   *
   * @return true if the enum is in one of those non-nullable types
   */
  public boolean isNullable() {
    return isNullable;
  }

  /**
   * check if the enum is a primitive
   *
   * @return true if the enum is not an object or an array (nullable or non-nullable)
   */
  public boolean isPrimitive() {
    return isPrimitive;
  }

  public boolean isArray() {
    return this == ARRAY || this == NULLABLEARRAY;
  }

  public boolean isObject() {
    return this == OBJECT || this == NULLABLEOBJECT;
  }

  public boolean isNull() {
    return this == NULL;
  }

  /**
   * This function makes an element nullable by changing its nullability
   *
   * Note here, none avro types are also converted to avro types in the process
   * except for OBJECT, its nullable counterpart remains NULLABLEOBJECT.
   *
   * @return nullability reversed element type
   */
  public JsonElementTypes reverseNullability() {
    if (isNullable) {
      if (this == JsonElementTypes.NULLABLEOBJECT) {
        return JsonElementTypes.OBJECT;
      } else {
        return JsonElementTypes.valueOf(StringUtils.upperCase(altName));
      }
    } else {
      if (this == JsonElementTypes.OBJECT) {
        return JsonElementTypes.NULLABLEOBJECT;
      } else {
        return JsonElementTypes.valueOf(StringUtils.upperCase("NULLABLE" + altName));
      }
    }
  }

  /**
   * Infers a ElementType from an array of values so that nullable columns are correctly inferred
   *
   * Currently, this doesn't support UNION of more than 1 solid types, like UNION of INT and STRING
   *
   * @param data a JsonArray of values
   * @return inferred element type with nullability
   */
  public static JsonElementTypes getTypeFromMultiple(JsonArray data) {

    boolean nullable = false;
    JsonElementTypes itemType = NULL;
    for (JsonElement arrayItem: data) {
      if (arrayItem.isJsonNull()) {
        nullable = true;
      } else if (arrayItem.isJsonObject()) {
        itemType = nullable ? NULLABLEOBJECT : OBJECT;
      } else if (arrayItem.isJsonArray()) {
        if (arrayItem.getAsJsonArray().size() == 0) {
          nullable = true;
        }
        itemType = nullable ? NULLABLEARRAY : ARRAY;
      } else if (arrayItem.toString().matches("^\".*\"$")) {
        itemType = nullable ? NULLABLESTRING : STRING;
      } else if (Ints.tryParse(arrayItem.getAsString()) != null) {
        itemType = nullable ? NULLABLEINT : INT;
      } else if (Longs.tryParse(arrayItem.getAsString()) != null) {
        itemType = nullable ? NULLABLELONG : LONG;
      } else if (Doubles.tryParse(arrayItem.getAsString()) != null) {
        itemType = nullable ? NULLABLEDOUBLE : DOUBLE;
      } else if (arrayItem.getAsString().toLowerCase().matches("(true|false)")) {
        itemType = nullable ? NULLABLEBOOLEAN : BOOLEAN;
      } else if (Floats.tryParse(arrayItem.getAsString()) != null) {
        itemType = nullable ? NULLABLEDOUBLE : DOUBLE;
      } else {
        itemType = UNKNOWN;
      }
    }

    if (nullable && !itemType.isNullable()) {
      return itemType.reverseNullability();
    }

    return itemType;
  }

  /**
   * Convert strings like ["array", "null"] to a JsonElementType object
   * @param input a Json schema type value like "string", "integer", or ["string", "null"]
   * @return a JsonElementTypes enum object
   */
  public static JsonElementTypes forType(String input) {
    /**
     * Strip off any extra double quote marks
     */
    String name = input.replaceAll("^\"|\"$", "");;
    boolean isNullable = false;
    if (name.matches("^\\[.*\\]$")) {
      JsonArray jsonArray = new Gson().fromJson(name, JsonArray.class);

      // we will treat the JsonElement as UNION if
      // 1. it has more than two types
      // 2. any type is array or object
      if (jsonArray.size() > 2) {
        return UNION;
      }

      for (JsonElement ele: jsonArray) {
        if (ele.isJsonArray() || ele.isJsonObject()) {
          return UNION;
        }
      }

      isNullable = jsonArray.contains(new JsonPrimitive("null"));
      if (isNullable) {
        jsonArray.remove(new JsonPrimitive("null"));
        name = "nullable" + jsonArray.get(0).getAsString();
      }
    }
    return Enum.valueOf(JsonElementTypes.class, name.toUpperCase());
  }
}
