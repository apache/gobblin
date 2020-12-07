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

package org.apache.gobblin.util.schema_check;

import java.util.HashSet;
import java.util.Set;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;


/**
 * default strategy to check the compatibility of avro schema
 */
public class AvroSchemaCheckDefaultStrategy implements AvroSchemaCheckStrategy {
  /**
   * This method will compare the name and types of the two schema
   * @param expected The expected schema
   * @param toValidate The real schema
   * @return true when expected schema and toValidate schema have matching field names and types
   */
  public boolean compare(Schema expected, Schema toValidate)
  {
    if (toValidate.getType() != expected.getType() || !toValidate.getName().equals(expected.getName())) {return false;}
    else {
      switch (toValidate.getType()) {
        case NULL:
        case BOOLEAN:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case BYTES:
        case STRING: {
          return true;
        }
        case ARRAY: {
          return compare(toValidate.getElementType(), expected.getElementType());
        }
        case MAP: {
          return compare(toValidate.getValueType(), expected.getValueType());
        }
        case FIXED: {
          // fixed size and name must match:
          if (toValidate.getFixedSize() != expected.getFixedSize()) {
            return false;
          }
          return true;
        }
        case ENUM: {
          // expected symbols must contain all toValidate symbols:
          final Set<String> expectedSymbols = new HashSet<>(expected.getEnumSymbols());
          final Set<String> toValidateSymbols = new HashSet<String>(toValidate.getEnumSymbols());
          if (expectedSymbols.size() != toValidateSymbols.size()) {
            return false;
          }
          if (!expectedSymbols.containsAll(toValidateSymbols)) {
            return false;
          }
          return true;
        }

        case RECORD: {
          // Check that each field of toValidate schema is in expected schema
          if (toValidate.getFields().size() != expected.getFields().size()) {
            return false;
          }
          for (final Schema.Field expectedFiled : expected.getFields()) {
            final Schema.Field toValidateField = toValidate.getField(expectedFiled.name());
            if (toValidateField == null) {
              // expected field does not correspond to any field in the toValidate record schema
              return false;
            } else {
              if (!compare(toValidateField.schema(), expectedFiled.schema())) {
                return false;
              }
            }
          }
          return true;
        }
        case UNION: {
          // Check existing schema contains all the type in toValidate schema
          if (toValidate.getTypes().size() != expected.getTypes().size()) {
            return false;
          }
          HashSet<Schema> types = new HashSet<Schema>(expected.getTypes());
          for (Schema toValidateType : toValidate.getTypes()) {
            Schema equalSchema = null;
            for (Schema type : types) {
              if (compare(type, toValidateType)) {
                equalSchema = type;
                break;
              }
            }
            if (equalSchema == null) {
              return false;
            }
            types.remove(equalSchema);
          }
          return true;
        }
        default: {
          throw new AvroRuntimeException("Unknown schema type: " + toValidate.getType());
        }
      }
    }
  }
}
