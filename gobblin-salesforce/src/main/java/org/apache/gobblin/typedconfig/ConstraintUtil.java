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
import java.util.Date;
import java.util.List;
import org.apache.gobblin.typedconfig.compiletime.EnumOptions;
import org.apache.gobblin.typedconfig.compiletime.IntRange;
import org.apache.gobblin.typedconfig.compiletime.LongRange;
import org.apache.gobblin.typedconfig.compiletime.StringRegex;


/**
 * Util class for handling constrain annotation
 * supports: IntRange, LongRange, EnumOption, StringRegex
 */
public class ConstraintUtil {
  private ConstraintUtil() {
  }

  public static Object constraint(Field field, Object value, Object defaultValue) {
    if (value == null) {
      return defaultValue;
    }
    Class type = field.getType();
    if (type == int.class) {
      IntRange intRange = field.getAnnotation(IntRange.class);
      if (intRange == null) {
        return value;
      }
      int[] range = intRange.value();
      int intValue = Integer.parseInt(value.toString());
      if (range.length != 2 || (range.length == 2 && intValue >= range[0] && intValue <= range[1])) {
        return value;
      } else {
        return defaultValue;
      }
    } else if (type == long.class) {
      LongRange longRange = field.getAnnotation(LongRange.class);
      long[] range = longRange.value();
      if (range == null) {
        return value;
      }
      long longValue = Long.parseLong(value.toString());
      if (range.length != 2 || (range.length == 2 && longValue > range[0] && longValue < range[1])) {
        return value;
      } else {
        return defaultValue;
      }
    } else if (type == String.class) {
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
        return defaultValue;
      }
    } else if (type.isEnum()) {
      EnumOptions enumOptions = field.getAnnotation(EnumOptions.class);
      if (enumOptions == null) {
        return value;
      }
      List<String> options = Arrays.asList(enumOptions.value());
      if (options.indexOf(value) >= 0) {
        return value;
      } else {
        return defaultValue;
      }
    } else if (type == Boolean.class || type == boolean.class) {
      return value;
    } else if (type == Date.class) {
      return value;
    } else {
      throw new RuntimeException("not supported the return type: " + type.getName());
    }
  }
}
