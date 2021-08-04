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

import org.apache.avro.Schema;

/**
 * Avro schema utility class to perform schema property conversion to the appropriate data types
 */
public class AvroSchemaUtils {

  private AvroSchemaUtils() {

  }

  public static Integer getValueAsInteger(final Schema schema, String prop) {
    Object value = schema.getObjectProp(prop);
    if(value instanceof Integer) {
      return (Integer) value;
    } else if(value instanceof String) {
      return Integer.parseInt((String) value);
    }
    return null;
  }

  public static Integer getValueAsInteger(final Schema.Field field, String prop) {
    Object value = field.getObjectProp(prop);
    if(value instanceof Integer) {
      return (Integer) value;
    } else if(value instanceof String) {
      return Integer.parseInt((String) value);
    }
    return null;
  }

  public static String getValueAsString(final Schema schema, String prop) {
    Object value = schema.getObjectProp(prop);
    if(value instanceof String) {
      return (String) value;
    }
    return null;
  }

  public static String getValueAsString(final Schema.Field field, String prop) {
    Object value = field.getObjectProp(prop);
    if(value instanceof String) {
      return (String) value;
    }
    return null;
  }

}
