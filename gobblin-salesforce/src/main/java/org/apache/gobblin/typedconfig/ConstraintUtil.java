package org.apache.gobblin.typedconfig;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.apache.gobblin.typedconfig.compiletime.EnumOptions;
import org.apache.gobblin.typedconfig.compiletime.IntRange;
import org.apache.gobblin.typedconfig.compiletime.LongRange;
import org.apache.gobblin.typedconfig.compiletime.StringRegex;


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
