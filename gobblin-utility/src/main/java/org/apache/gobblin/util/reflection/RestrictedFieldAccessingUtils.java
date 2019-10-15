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

package org.apache.gobblin.util.reflection;

import java.lang.reflect.Field;
import java.util.Arrays;


/**
 * These are hacky methods that should only be used when there are access-modifiers prevents accessing fields that
 * are essential for application code to access.
 */
public class RestrictedFieldAccessingUtils {
  private RestrictedFieldAccessingUtils() {
  }

  /**
   * Getting field defined in containingObj which was not publicly-accessible, using java-reflection.
   */
  public static Object getRestrictedFieldByReflection(Object containingObj, String fieldName, Class clazz)
      throws NoSuchFieldException, IllegalAccessException {
    Field field = clazz.getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.get(containingObj);
  }

  /**
   * Getting field defined in superclass(es) which was not publicly-accessible, using java-reflection.
   */
  public static Object getRestrictedFieldByReflectionRecursively(Object containingObj, String fieldName, Class clazz)
      throws NoSuchFieldException, IllegalAccessException {

    // When it reaches Object.class level and still not find the field, throw exception.
    if (clazz.getCanonicalName().equals("java.lang.Object")) {
      throw new NoSuchFieldException(
          String.format("Field %s doesn't exist in specified class and its ancestors", fieldName));
    }

    if (!Arrays.asList(clazz.getDeclaredFields()).stream()
        .anyMatch(x -> x.getName().equals(fieldName))) {
      return getRestrictedFieldByReflectionRecursively(containingObj, fieldName, clazz.getSuperclass());
    } else {
      return getRestrictedFieldByReflection(containingObj, fieldName, clazz);
    }
  }
}
