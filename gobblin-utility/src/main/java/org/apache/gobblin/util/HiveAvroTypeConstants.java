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

package org.apache.gobblin.util;

import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;


public class HiveAvroTypeConstants {

  // Avro to Hive schema mapping
  public static final Map<Schema.Type, String> AVRO_TO_HIVE_COLUMN_MAPPING_V_12 = ImmutableMap
      .<Schema.Type, String>builder()
      .put(Schema.Type.NULL,    "void")
      .put(Schema.Type.BOOLEAN, "boolean")
      .put(Schema.Type.INT,     "int")
      .put(Schema.Type.LONG,    "bigint")
      .put(Schema.Type.FLOAT,   "float")
      .put(Schema.Type.DOUBLE,  "double")
      .put(Schema.Type.BYTES,   "binary")
      .put(Schema.Type.STRING,  "string")
      .put(Schema.Type.RECORD,  "struct")
      .put(Schema.Type.MAP,     "map")
      .put(Schema.Type.ARRAY,   "array")
      .put(Schema.Type.UNION,   "uniontype")
      .put(Schema.Type.ENUM,    "string")
      .put(Schema.Type.FIXED,   "binary")
      .build();
  // Hive evolution types supported
  public static final Map<String, Set<String>> HIVE_COMPATIBLE_TYPES = ImmutableMap
      .<String, Set<String>>builder()
      .put("tinyint", ImmutableSet.<String>builder()
          .add("smallint", "int", "bigint", "float", "double", "decimal", "string", "varchar").build())
      .put("smallint",  ImmutableSet.<String>builder().add("int", "bigint", "float", "double", "decimal", "string",
          "varchar").build())
      .put("int",       ImmutableSet.<String>builder().add("bigint", "float", "double", "decimal", "string", "varchar")
          .build())
      .put("bigint",    ImmutableSet.<String>builder().add("float", "double", "decimal", "string", "varchar").build())
      .put("float",     ImmutableSet.<String>builder().add("double", "decimal", "string", "varchar").build())
      .put("double",    ImmutableSet.<String>builder().add("decimal", "string", "varchar").build())
      .put("decimal",   ImmutableSet.<String>builder().add("string", "varchar").build())
      .put("string",    ImmutableSet.<String>builder().add("double", "decimal", "varchar").build())
      .put("varchar",   ImmutableSet.<String>builder().add("double", "string", "varchar").build())
      .put("timestamp", ImmutableSet.<String>builder().add("string", "varchar").build())
      .put("date",      ImmutableSet.<String>builder().add("string", "varchar").build())
      .put("binary",    Sets.<String>newHashSet())
      .put("boolean",    Sets.<String>newHashSet()).build();
}
