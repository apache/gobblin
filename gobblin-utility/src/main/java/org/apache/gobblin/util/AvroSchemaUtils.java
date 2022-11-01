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

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;


/**
 * Avro schema utility class to perform schema property conversion to the appropriate data types
 */
@Slf4j
public class AvroSchemaUtils {

  private AvroSchemaUtils() {

  }

  /**
   * Get schema property value as integer
   * @param schema
   * @param prop
   * @return Integer
   */
  public static Integer getValueAsInteger(final Schema schema, String prop) {
    String value = AvroCompatibilityHelper.getSchemaPropAsJsonString(schema, prop, 
        false, false);
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException ex) {
      log.error("Exception while converting to integer ", ex.getCause());
      throw new IllegalArgumentException(ex);
    }
  }

  /***
   * Copy properties to an Avro Schema field
   * @param fromField Avro Schema Field to copy properties from
   * @param toField Avro Schema Field to copy properties to
   */
  public static void copyFieldProperties(final Schema.Field fromField, final Schema.Field toField) {
    List<String> allPropNames = AvroCompatibilityHelper.getAllPropNames(fromField);
    if (null != allPropNames) {
      for (String propName : allPropNames) {
        String propValue = AvroCompatibilityHelper.getFieldPropAsJsonString(fromField, propName, 
            true, false);
        AvroCompatibilityHelper.setFieldPropFromJsonString(toField, propName, propValue, false);
      }
    }
  }

  /***
   * Copy properties to an Avro Schema
   * @param fromSchema Avro Schema to copy properties from
   * @param toSchema Avro Schema to copy properties to
   */
  public static void copySchemaProperties(final Schema fromSchema, final Schema toSchema) {
    List<String> allPropNames = AvroCompatibilityHelper.getAllPropNames(fromSchema);
    if (null != allPropNames) {
      for (String propName : allPropNames) {
        String propValue = AvroCompatibilityHelper.getSchemaPropAsJsonString(fromSchema, propName, 
            true, false);
        AvroCompatibilityHelper.setSchemaPropFromJsonString(toSchema, propName, propValue, false);
      }
    }
  }
}
