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

package org.apache.gobblin.typedconfig;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.typedconfig.compiletime.EnumOptions;
import org.apache.gobblin.typedconfig.compiletime.IntRange;
import org.apache.gobblin.typedconfig.compiletime.LongRange;
import org.apache.gobblin.typedconfig.compiletime.StringRegex;


/**
 * Util class for handling constraint annotation
 * supports: IntRange, LongRange, EnumOption, StringRegex
 */
@Slf4j
public class ConstraintUtil {
  private ConstraintUtil() {
  }

  public static Object constraint(Field field, Object value) {
    Class type = field.getType();
    if (type.isEnum()) {
      return getEnumValue(field, value);
    }
    switch (type.getName()) {
      case "int": case "java.lang.Integer":
        return getIntValue(field, value);
      case "long": case "java.lang.Long":
        return getLongValue(field, value);
      case "java.lang.String":
        return getStringValue(field, value);
      case "boolean": case "java.lang.Boolean":
        return getBooleanValue(field, value);
      case "java.util.Date":
        return getDateValue(field, value);
      default:
        throw new RuntimeException("not supported the return type: " + type.getName());
    }
  }

  static private Object getIntValue(Field field, Object value) {
    IntRange intRange = field.getAnnotation(IntRange.class);
    if (intRange == null) {
      return value;
    }
    int[] range = intRange.value();
    int intValue = Integer.parseInt(value.toString());
    if (range.length != 2) {
      throw new RuntimeException(String.format("Field [%s]: Long range is invalid.", field.getName()));
    }
    if (intValue >= range[0] && intValue <= range[1]) {
      return value;
    } else {
      throw new RuntimeException(
          String.format("Field [%s]: value [%s] is out of range [%s, %s].", field.getName(), value, range[0], range[1])
      );
    }
  }
  static private Object getLongValue(Field field, Object value) {
    LongRange longRange = field.getAnnotation(LongRange.class);
    long[] range = longRange.value();
    if (range == null) {
      return value;
    }
    long longValue = Long.parseLong(value.toString());
    if (range.length != 2) {
      throw new RuntimeException(String.format("Field [%s]: Long range is invalid.", field.getName()));
    }
    if (longValue > range[0] && longValue < range[1]) {
      return value;
    } else {
      throw new RuntimeException(
          String.format("Field [%s]: value [%s] is out of range [%s, %s].", field.getName(), value, range[0], range[1])
      );
    }
  }

  static private Object getStringValue(Field field, Object value) {
    StringRegex stringRegex = field.getAnnotation(StringRegex.class);
    if (stringRegex == null) {
      return value;
    }
    String regex = stringRegex.value();
    if (regex == null) {
      return value;
    }
    boolean isMatching = value.toString().matches(regex);
    if (isMatching) {
      return value;
    } else {
      throw new RuntimeException(String.format("[%s] is not matching pattern [%s]", value, regex));
    }
  }

  static private Object getBooleanValue(Field field, Object value) {
    return value; // there is no restriction for boolean value.
  }

  static private Object getDateValue(Field field, Object value) {
    return value; // there is no restriction for Date value.
  }

  static private Object getEnumValue(Field field, Object value) {
    EnumOptions enumOptions = field.getAnnotation(EnumOptions.class);
    if (enumOptions == null) {
      return value;
    }
    List<String> options = Arrays.asList(enumOptions.value());
    if (options.indexOf(value) >= 0) {
      return value;
    } else {
      throw new RuntimeException(String.format("Enum [%s] is not allowed.", value));
    }
  }
}
